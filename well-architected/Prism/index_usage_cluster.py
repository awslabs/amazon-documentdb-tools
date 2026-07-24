"""Cluster-wide index usage collection.

DocumentDB's `$indexStats.accesses.ops` is a PER-INSTANCE counter — each instance
counts only the operations IT served (and resets on its own restart). The primary
analysis path (db_analyzer) reads `$indexStats` from the primary/writer only, so an
index actively used by reads routed to a SECONDARY can falsely appear unused.

This module resolves the true, cluster-wide usage by querying the readers as well.
It is used LAZILY and only for the candidate set of indexes that already show
zero ops on the primary — there is no point checking readers for an index the
primary already proves is used.

Design (agreed):
- Lazy + primary-zero-ops optimization: only readers, only for primary-zero indexes.
- Both connection modes: SSH tunnel (per-instance tunnels) and direct (rewrite host).
- Partial coverage (option b): if some instances are unreachable, still decide from
  the reachable ones, but report coverage so the UI can warn.
- Short-TTL cache so repeated renders/polls don't re-query the cluster.
"""
import time
import logging
import threading

import pymongo

logger = logging.getLogger(__name__)

# Cache: key -> {"ts": float, "data": result_dict}
_CACHE_TTL_SEC = 120
_cache = {}
_cache_lock = threading.Lock()

# Per-instance $indexStats query timeout (ms)
_QUERY_TIMEOUT_MS = 4000


def _is_tunnel_mode(conn_str):
    """Detect if connection goes through an SSH tunnel (localhost)."""
    return bool(conn_str) and ("localhost" in conn_str or "127.0.0.1" in conn_str)


def _discover_instances(cluster_id, region):
    """Return [{id, endpoint, port, role, type}] for the cluster, via the AWS API."""
    import boto3
    try:
        docdb = boto3.client("docdb", region_name=region)
        cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        insts = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]
        members = {m.get("DBInstanceIdentifier"): m.get("IsClusterWriter", False)
                   for m in cl.get("DBClusterMembers", [])}
        result = []
        for inst in insts:
            iid = inst["DBInstanceIdentifier"]
            ep = inst.get("Endpoint", {})
            result.append({
                "id": iid,
                "endpoint": ep.get("Address", ""),
                "port": ep.get("Port", 27017),
                "role": "Writer" if members.get(iid) else "Reader",
                "type": inst.get("DBInstanceClass", ""),
            })
        return result
    except Exception as e:
        logger.warning("index_usage_cluster: instance discovery failed: %s", e)
        return []


def _tunnel_instance_conn(instance_id, base_conn_str):
    """Per-instance connection string in tunnel mode; None if no tunnel for it."""
    try:
        from ssh_tunnel import get_instance_connection_string, _get_credentials
        u, p, tls = _get_credentials()
        if u and p:
            return get_instance_connection_string(instance_id, u, p, tls)
    except Exception as e:
        logger.debug("tunnel instance conn failed for %s: %s", instance_id, e)
    return None


def _direct_instance_conn(base_conn_str, endpoint, port):
    """Per-instance connection string in direct mode by rewriting the host and
    forcing directConnection=true (so the op pins to that exact instance)."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(base_conn_str)
        user = parsed.username or ""
        pwd = parsed.password or ""
        params = parsed.query
        # Drop replicaSet (incompatible with directConnection), ensure directConnection.
        parts = [p for p in params.split("&") if p and not p.startswith("replicaSet")]
        if not any(p.startswith("directConnection") for p in parts):
            parts.append("directConnection=true")
        param_str = "&".join(parts)
        cred = f"{user}:{pwd}@" if user else ""
        return f"mongodb://{cred}{endpoint}:{port}/?{param_str}"
    except Exception as e:
        logger.debug("direct instance conn build failed for %s: %s", endpoint, e)
        return None


def _instance_conn_str(inst, base_conn_str, tunnel_mode):
    """Resolve a per-instance connection string for either mode."""
    if tunnel_mode:
        return _tunnel_instance_conn(inst["id"], base_conn_str)
    if inst.get("endpoint"):
        return _direct_instance_conn(base_conn_str, inst["endpoint"], inst["port"])
    return None


def _ops_for_db(conn_str, db_name, collections):
    """Run $indexStats on one instance for the given collections.

    Returns {collection: {index_name: ops}} or raises on connection failure.
    """
    client = pymongo.MongoClient(conn_str, appname="DocDB-Prism",
                                 serverSelectionTimeoutMS=_QUERY_TIMEOUT_MS,
                                 connectTimeoutMS=_QUERY_TIMEOUT_MS,
                                 socketTimeoutMS=_QUERY_TIMEOUT_MS)
    try:
        client.admin.command("ping")
        db = client[db_name]
        out = {}
        for coll in collections:
            try:
                stats = list(db[coll].aggregate([{"$indexStats": {}}]))
                out[coll] = {s.get("name", "?"): s.get("accesses", {}).get("ops", 0)
                             for s in stats}
            except Exception as e:
                logger.debug("$indexStats failed on %s.%s: %s", db_name, coll, e)
                # Collection-level failure: omit (treated as no extra ops for this coll)
                out[coll] = {}
        return out
    finally:
        client.close()


def get_reader_ops(cluster_id, region, base_conn_str, db_name, candidates,
                   force=False):
    """Collect reader-side ops for candidate (collection, index) pairs.

    candidates: dict {collection: set(index_names)} — the indexes that showed
                zero ops on the primary and thus need a reader check.

    Returns:
      {
        "ops": {collection: {index_name: max_reader_ops}},
        "instances_total": int,     # reader instances known
        "instances_checked": int,   # readers successfully queried
        "unreachable": [instance_id, ...],
        "partial": bool,            # True if not all readers were reachable
      }
    The 'ops' map only contains reader ops (caller already has primary ops).
    """
    if not candidates:
        return {"ops": {}, "instances_total": 0, "instances_checked": 0,
                "unreachable": [], "partial": False}

    cache_key = (cluster_id, db_name)
    if not force:
        with _cache_lock:
            entry = _cache.get(cache_key)
            if entry and (time.time() - entry["ts"]) < _CACHE_TTL_SEC:
                return entry["data"]

    tunnel_mode = _is_tunnel_mode(base_conn_str)
    instances = _discover_instances(cluster_id, region) if cluster_id else []
    readers = [i for i in instances if i.get("role") == "Reader"]

    collections = list(candidates.keys())
    merged_ops = {}
    checked = 0
    unreachable = []

    for inst in readers:
        conn = _instance_conn_str(inst, base_conn_str, tunnel_mode)
        if not conn:
            unreachable.append(inst["id"])
            continue
        try:
            inst_ops = _ops_for_db(conn, db_name, collections)
            checked += 1
            for coll, idx_ops in inst_ops.items():
                bucket = merged_ops.setdefault(coll, {})
                for idx_name, ops in idx_ops.items():
                    # Only track candidate indexes; keep the max across readers.
                    if idx_name in candidates.get(coll, set()):
                        bucket[idx_name] = max(bucket.get(idx_name, 0), ops)
        except Exception as e:
            logger.info("index_usage_cluster: reader %s unreachable: %s", inst["id"], e)
            unreachable.append(inst["id"])

    result = {
        "ops": merged_ops,
        "instances_total": len(readers),
        "instances_checked": checked,
        "unreachable": unreachable,
        "partial": checked < len(readers),
    }

    with _cache_lock:
        _cache[cache_key] = {"ts": time.time(), "data": result}
    return result


def get_reader_connection_string(cluster_id, region, base_conn_str, verify=True):
    """Resolve a connection string pinned to a reachable READER instance, or None.

    Used to offload read-only document sampling (index cardinality, compression
    estimation) from the writer onto a secondary when one is available. Works in
    both connection modes (SSH tunnel → per-instance local port; direct → host
    rewrite with directConnection=true).

    Returns the first reader whose `ping` succeeds (when verify=True), else None
    so callers transparently fall back to the primary. Never raises.
    """
    if not cluster_id:
        return None
    try:
        tunnel_mode = _is_tunnel_mode(base_conn_str)
        instances = _discover_instances(cluster_id, region)
        readers = [i for i in instances if i.get("role") == "Reader"]
        for inst in readers:
            conn = _instance_conn_str(inst, base_conn_str, tunnel_mode)
            if not conn:
                continue
            if not verify:
                return conn
            try:
                c = pymongo.MongoClient(
                    conn, appname="DocDB-Prism",
                    serverSelectionTimeoutMS=_QUERY_TIMEOUT_MS,
                    connectTimeoutMS=_QUERY_TIMEOUT_MS,
                    socketTimeoutMS=_QUERY_TIMEOUT_MS)
                try:
                    c.admin.command("ping")
                    logger.info("reader offload available: %s", inst.get("id"))
                    return conn
                finally:
                    c.close()
            except Exception as e:
                logger.info("reader %s not reachable for offload: %s",
                            inst.get("id"), str(e)[:120])
                continue
    except Exception as e:
        logger.debug("get_reader_connection_string failed: %s", e)
    return None


def reset_cache():
    """Clear the reader-ops cache (call on reconnect / re-analyze)."""
    with _cache_lock:
        _cache.clear()
