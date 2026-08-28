#!/usr/bin/env python3
"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Atlas Capacity Metrics Collector for DocumentDB Migration Sizing

Collects CPU, Memory, IOPS, Operations, Storage, Connections, Cache,
Replication metrics from MongoDB Atlas Admin API v2 and generates
a sizing summary report.

API Reference: https://www.mongodb.com/docs/atlas/reference/alert-host-metrics/
Atlas Admin API: https://www.mongodb.com/docs/api/doc/atlas-admin-api-v2/

Usage:
    # Set credentials
    export ATLAS_PUBLIC_KEY="your-public-key"
    export ATLAS_PRIVATE_KEY="your-private-key"
    export ATLAS_PROJECT_ID="your-project-id"

    # Standard sizing run: 14 days at 5-min granularity (recommended)
    python atlas_metrics.py --all \\
        --uri "mongodb+srv://user:pass@cluster.abcde.mongodb.net" \\
        --cluster cluster-name

    # Debug / short window (last 48h at 1-min granularity)
    python atlas_metrics.py \\
        --uri "mongodb+srv://user:pass@cluster.abcde.mongodb.net" \\
        --cluster cluster-name

    # Custom window (max 14 days at PT5M, 48h at PT1M, 12 months at PT1H)
    python atlas_metrics.py --granularity PT5M --period P14D \\
        --uri "..." --cluster ...

Note: --uri and --cluster are REQUIRED. The tool preflights connectivity
before starting the (long) Atlas API collection to fail fast on bad credentials
or unreachable hosts.
"""

import argparse, csv, difflib, json, logging, os, re, shutil, socket, subprocess, sys, time, urllib.request, zipfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from requests.auth import HTTPDigestAuth
import requests

__version__ = "2.3.1"

# Runtime logger -- writes to runtime.log in output directory
_runtime_log = None

def _init_log(log_dir):
    global _runtime_log
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)
    if _runtime_log and _runtime_log.handlers:
        return  # Already initialized for this cluster
    _runtime_log = logging.getLogger("atlas_metrics")
    _runtime_log.setLevel(logging.DEBUG)
    _runtime_log.handlers.clear()
    fh = logging.FileHandler(log_path / "runtime.log", mode="a")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S"))
    _runtime_log.addHandler(fh)

def _log(msg, level="info"):
    if _runtime_log:
        getattr(_runtime_log, level)(msg)

class _Timer:
    """Context manager for timing steps and logging to runtime.log."""
    def __init__(self, label):
        self.label = label
    def __enter__(self):
        self.start = time.time()
        _log(f"START {self.label}")
        return self
    def __exit__(self, *exc):
        elapsed = time.time() - self.start
        if exc[0]:
            _log(f"ERROR {self.label}: {exc[1]} ({elapsed:.1f}s)", "error")
        else:
            _log(f"END   {self.label} ({elapsed:.1f}s)")
        return False


class _Step:
    """Same shape as _Timer, but catches exceptions so downstream pipeline
    steps can still run. Failures are logged to runtime.log (via _log) and
    recorded on the class so main() can print a partial-success summary
    and exit non-zero if any step failed.

    Use for artifact-generation steps that are downstream of collect_metrics
    (e.g. the sizing-summary MD render, the cost CSV, the compat scan, the
    zip). A crash in one MUST NOT block the others -- customer runs that
    take 30+ minutes of Atlas API scraping should always produce every
    artifact they can, even if one of them fails.

    Do NOT use for foundational steps like collect_metrics itself -- if the
    raw data collection fails there is nothing to work with. Those keep
    _Timer semantics (log + re-raise)."""

    _failures = []  # class-level; main() reads via _Step.summary()

    def __init__(self, label):
        self.label = label

    def __enter__(self):
        self.start = time.time()
        _log(f"START {self.label}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        elapsed = time.time() - self.start
        if exc_type is None:
            _log(f"END   {self.label} ({elapsed:.1f}s)")
            return False
        _log(f"ERROR {self.label}: {exc_val} ({elapsed:.1f}s)", "error")
        _Step._failures.append((self.label, f"{exc_type.__name__}: {exc_val}"))
        print(f"\nWARNING: step '{self.label}' failed after {elapsed:.1f}s: {exc_val}")
        print(f"  Continuing with remaining pipeline steps.")
        print(f"  See runtime.log for the full trace.")
        return True  # suppress the exception -- downstream steps continue

    @classmethod
    def failures(cls):
        return list(cls._failures)

    @classmethod
    def any_failed(cls):
        return len(cls._failures) > 0

BASE_URL = "https://cloud.mongodb.com/api/atlas/v2"
HEADERS = {"Accept": "application/vnd.atlas.2023-01-01+json"}

# --- Metric batches organized by sizing concern ---
PROCESS_METRIC_BATCHES = {
    "cpu": [
        "PROCESS_CPU_USER",
    ],
    "memory": [
        "SYSTEM_MEMORY_USED", "SYSTEM_MEMORY_FREE",
        "MEMORY_RESIDENT",
    ],
    "operations": [
        "OPCOUNTER_INSERT", "OPCOUNTER_QUERY", "OPCOUNTER_UPDATE",
        "OPCOUNTER_DELETE", "OPCOUNTER_GETMORE", "OPCOUNTER_CMD",
    ],
    "infrastructure": [
        "CONNECTIONS",
        "NETWORK_BYTES_IN", "NETWORK_BYTES_OUT",
        "DB_DATA_SIZE_TOTAL", "DB_STORAGE_TOTAL", "DB_INDEX_SIZE_TOTAL",
    ],
    "query": [
        "OPERATIONS_SCAN_AND_ORDER",
        "OP_EXECUTION_TIME_READS", "OP_EXECUTION_TIME_WRITES", "OP_EXECUTION_TIME_COMMANDS",
    ],
    "wiredtiger": [
        "CACHE_BYTES_READ_INTO", "CACHE_BYTES_WRITTEN_FROM",
        "CACHE_DIRTY_BYTES", "CACHE_USED_BYTES",
    ],
    "replication": [
        "OPLOG_RATE_GB_PER_HOUR",
    ],
}

# Granularity -> (retention_label, default_period_if_not_specified)
# Retentions verified 2026-07-02 against MongoDB Cloud Manager docs:
# https://www.mongodb.com/docs/cloud-manager/reference/monitoring-metrics-per-plan/
# Atlas uses the same underlying retention model per its Monitoring Data
# Storage Granularity reference.
#
# PT10S is only available on M40+ clusters ("Premium Monitoring Granularity").
# PT1M default period is kept small (P2D) for debugging speed; the retention
# supports up to P14D if requested explicitly via --period.
GRANULARITY_RETENTION = {
    "PT10S": ("~24 hours retention (M40+ only)", "P1D"),
    "PT1M":  ("~14 days retention, default P2D",  "P2D"),
    "PT5M":  ("~14 days retention",               "P14D"),
    "PT1H":  ("~12 months retention",             "P365D"),
    "P1D":   ("effectively forever",              "P730D"),
}

# Max retention in days per granularity - used to warn on --period overrides.
# Beyond these limits, Atlas silently downsamples to hourly rollups presented
# as fake fine-grained buckets, biasing P95s low.
GRANULARITY_MAX_DAYS = {
    "PT10S": 1,
    "PT1M":  14,
    "PT5M":  14,
    "PT1H":  365,
    "P1D":   730,
}


def _period_days(period_str):
    """Parse ISO 8601 duration into days (approx). Returns None if unparseable."""
    if not period_str:
        return None
    m = re.match(r"^P(\d+)D$", period_str)
    if m:
        return int(m.group(1))
    m = re.match(r"^PT(\d+)H$", period_str)
    if m:
        return int(m.group(1)) / 24.0
    m = re.match(r"^PT(\d+)M$", period_str)
    if m:
        return int(m.group(1)) / (24 * 60.0)
    m = re.match(r"^P(\d+)W$", period_str)
    if m:
        return int(m.group(1)) * 7
    return None


class AtlasClient:
    def __init__(self, public_key, private_key, project_id):
        self.auth = HTTPDigestAuth(public_key, private_key)
        self.project_id = project_id
        self._lock = __import__('threading').Lock()
        self._last_call = 0

    def get(self, path, params=None, retries=3, api_version=None):
        # Throttle: minimum 1.5s between API calls across all threads (~40 req/min)
        with self._lock:
            now = time.time()
            elapsed = now - self._last_call
            if elapsed < 1.5:
                time.sleep(1.5 - elapsed)
            self._last_call = time.time()
        url = f"{BASE_URL}/groups/{self.project_id}{path}"
        hdrs = dict(HEADERS)
        if api_version:
            hdrs["Accept"] = f"application/vnd.atlas.{api_version}+json"
        for attempt in range(retries):
            try:
                r = requests.get(url, auth=self.auth, headers=hdrs, params=params, timeout=30)
                if r.status_code == 429:
                    try:
                        wait = int(r.headers.get("Retry-After", 10))
                    except (ValueError, TypeError):
                        wait = 10
                    if attempt == 0:
                        print(f"    Rate limited, waiting {wait}s...")
                    time.sleep(wait + 2)  # add buffer to avoid immediate re-trigger
                    continue
                if r.status_code == 404:
                    return None
                r.raise_for_status()
                return r.json()
            except requests.exceptions.RequestException as e:
                if attempt == retries - 1:
                    print(f"    ERROR: {e}")
                    return None
                time.sleep(2 ** attempt)
        return None

    def get_paginated(self, path, params=None):
        """Handle Atlas API pagination."""
        results = []
        params = params or {}
        while True:
            data = self.get(path, params)
            if not data:
                break
            results.extend(data.get("results", []))
            links = {l["rel"]: l["href"] for l in data.get("links", [])}
            if "next" not in links:
                break
            m = re.search(r'pageNum=(\d+)', links["next"])
            if m:
                params["pageNum"] = int(m.group(1))
            else:
                break
        return results


def period_to_start(period):
    """Convert ISO 8601 period (e.g., P2D, P30D, P365D) to a start datetime string."""
    days = 0
    m = re.match(r'P(\d+)D', period)
    if m:
        days = int(m.group(1))
    m = re.match(r'P(\d+)Y', period)
    if m:
        days = int(m.group(1)) * 365
    if not days:
        days = 30
    start = datetime.now(timezone.utc) - timedelta(days=days)
    return start.strftime("%Y-%m-%dT%H:%M:%SZ")


_MONGO_URI = None  # Set by main() when --uri is provided
_URI_CLUSTER_NAME = None  # Resolved cluster name matching --uri

# --- BSON type detection and index enrichment ---

# BSON type -> fixed byte width (None = variable, needs avg from sample)
_BSON_TYPE_BYTES = {
    "objectId": 12, "int32": 4, "int64": 8, "double": 8, "date": 8,
    "timestamp": 8, "bool": 1, "decimal128": 16, "uuid": 16,
    "binData": None, "string": None, "null": 0, "undefined": 0,
    "minKey": 0, "maxKey": 0, "regex": None, "array": None, "object": None,
}

# WiredTiger KeyString: 1 byte type discriminator + value bytes per field
_WT_KS_OVERHEAD_PER_FIELD = 1
_WT_KS_END_DISCRIMINATOR = 1
_WT_KS_RECORD_ID = 8


def _python_to_bson_type(val):
    """Map a Python value (from pymongo) to BSON type name."""
    if val is None:
        return "null"
    import bson
    t = type(val)
    if t == bson.ObjectId:
        return "objectId"
    if t == bson.Int64:
        return "int64"
    if t == bson.Decimal128:
        return "decimal128"
    if t == int:
        return "int64" if abs(val) > 2147483647 else "int32"
    if t == float:
        return "double"
    if t == bool:
        return "bool"
    if t == str:
        return "string"
    if t == bytes:
        return "binData"
    if hasattr(bson, "Binary") and isinstance(val, bson.Binary):
        if getattr(val, "subtype", 0) == 4:
            return "uuid"
        return "binData"
    import datetime as _dt
    if isinstance(val, _dt.datetime):
        return "date"
    if isinstance(val, bson.Timestamp):
        return "timestamp"
    if isinstance(val, bson.Regex):
        return "regex"
    if isinstance(val, list):
        return "array"
    if isinstance(val, dict):
        return "object"
    try:
        import uuid as _uuid
        if isinstance(val, _uuid.UUID):
            return "uuid"
    except Exception:
        pass
    return "unknown"


def _get_nested_value(doc, dotted_path):
    """Traverse a document using dot-notation path. Returns (value, found)."""
    parts = dotted_path.split(".")
    cur = doc
    for p in parts:
        if isinstance(cur, dict) and p in cur:
            cur = cur[p]
        else:
            return None, False
    return cur, True


def _value_byte_size(val, bson_type):
    """Estimate byte size of a single value for index key purposes."""
    fixed = _BSON_TYPE_BYTES.get(bson_type)
    if fixed is not None:
        return fixed
    if bson_type == "string" and isinstance(val, str):
        return len(val.encode("utf-8"))
    if bson_type == "binData" and isinstance(val, (bytes, bytearray)):
        return len(val)
    try:
        import bson as _bson
        if hasattr(_bson, "Binary") and isinstance(val, _bson.Binary):
            return len(val)
    except Exception:
        pass
    return 32  # conservative fallback for unknown variable types


def _sample_compression_ratio(db, coll_name, sample_size=100):
    """Sample first 50 + last 50 docs, train zstd dictionary, compress with zstd-3-dict.
    Matches DocumentDB 8.0 compression (zstd level 3 with dictionary).
    Returns {ratio, sampled_docs, avg_original_bytes, avg_compressed_bytes} or None."""
    try:
        import zstandard as zstd
        import bson
    except ImportError:
        print("  WARNING: zstandard library not installed. Real compression sampling disabled.")
        print("           Sizing will fall back to per-collection collStats compression ratio")
        print("           (WiredTiger/Snappy), which underestimates DocDB Zstandard by 20-40%.")
        print("           Install with: pip install zstandard")
        return None

    half = sample_size // 2
    try:
        first = list(db[coll_name].find().limit(half))
        last = list(db[coll_name].find().sort([("_id", -1)]).limit(half))
        seen = set()
        docs = []
        for doc in first + last:
            did = doc.get("_id")
            key = str(did) if isinstance(did, (dict, list)) else did
            if key not in seen:
                seen.add(key)
                docs.append(doc)
    except Exception as e:
        print(f"  WARNING: compression sampling failed for {coll_name}: {e}")
        return None

    if not docs:
        return None

    # Encode docs to BSON bytes (what DocumentDB actually compresses)
    raw_docs = []
    for doc in docs:
        try:
            raw_docs.append(bson.BSON.encode(doc))
        except Exception:
            raw_docs.append(json.dumps(doc, default=str).encode("utf-8"))

    # Train dictionary from all sampled docs (DocumentDB uses 100-doc dictionary)
    try:
        dict_data = zstd.train_dictionary(65536, raw_docs)
        compressor = zstd.ZstdCompressor(level=3, dict_data=dict_data)
    except Exception:
        # Fallback: no dictionary (small collections)
        compressor = zstd.ZstdCompressor(level=3)

    total_raw = 0
    total_compressed = 0
    for raw in raw_docs:
        total_raw += len(raw)
        total_compressed += len(compressor.compress(raw))

    if total_compressed == 0:
        return None

    return {
        "ratio": round(total_raw / total_compressed, 2),
        "sampled_docs": len(raw_docs),
        "avg_original_bytes": int(total_raw / len(raw_docs)),
        "avg_compressed_bytes": int(total_compressed / len(raw_docs)),
    }


def _sample_index_key_types(db, coll_name, index_key, sample_size=100):
    """Sample documents to detect BSON types and avg byte sizes for index key fields.
    Samples first 50 (oldest) and last 50 (newest) docs to capture schema evolution.
    Returns list of {field, bson_type, avg_bytes, is_array} per key field."""
    fields = list(index_key.keys()) if isinstance(index_key, dict) else []
    if not fields:
        return []

    # Build projection: only fetch indexed fields + _id for dedup
    proj = {f: 1 for f in fields}
    if "_id" not in fields:
        proj["_id"] = 1  # keep _id for dedup, strip later

    half = sample_size // 2
    try:
        first = list(db[coll_name].find({}, proj).limit(half))
        last = list(db[coll_name].find({}, proj).sort([("_id", -1)]).limit(half))
        # Deduplicate (small collections where first/last overlap)
        seen = set()
        docs = []
        for doc in first + last:
            did = doc.get("_id")
            key = str(did) if isinstance(did, (dict, list)) else did
            if key not in seen:
                seen.add(key)
                docs.append(doc)
    except Exception:
        return [{"field": f, "bson_type": "unknown", "avg_bytes": 32, "is_array": False} for f in fields]

    if not docs:
        return [{"field": f, "bson_type": "unknown", "avg_bytes": 32, "is_array": False} for f in fields]

    result = []
    for field in fields:
        types = {}
        sizes = []
        array_count = 0
        found_count = 0

        for doc in docs:
            val, found = _get_nested_value(doc, field)
            if not found:
                continue
            found_count += 1
            if isinstance(val, list):
                array_count += 1
                # For multikey, sample first element for type
                if val:
                    val = val[0]
                else:
                    continue
            bt = _python_to_bson_type(val)
            types[bt] = types.get(bt, 0) + 1
            sizes.append(_value_byte_size(val, bt))

        # Pick dominant type
        dominant_type = max(types, key=types.get) if types else "unknown"
        avg_bytes = int(sum(sizes) / len(sizes)) if sizes else 32
        is_array = array_count > (found_count * 0.5) if found_count > 0 else False

        # Compute avg array length for multikey indexes
        avg_arr_len = None
        if is_array and array_count > 0:
            total_len = 0
            for doc in docs:
                val, found = _get_nested_value(doc, field)
                if found and isinstance(val, list):
                    total_len += len(val)
            avg_arr_len = round(total_len / array_count, 1)

        result.append({
            "field": field,
            "bson_type": dominant_type,
            "avg_bytes": avg_bytes,
            "is_array": is_array,
            "avg_array_length": avg_arr_len,
        })

    return result


def _compute_wt_keystring_size(key_types):
    """Compute theoretical WiredTiger KeyString entry size from key type info."""
    key_data = sum(_WT_KS_OVERHEAD_PER_FIELD + kt["avg_bytes"] for kt in key_types)
    return key_data + _WT_KS_END_DISCRIMINATOR + _WT_KS_RECORD_ID


def _cluster_for_alias(user_alias, cluster_map, cluster_names=()):
    """Return cluster name for a process userAlias using full-hostname map.
    Falls back to prefix matching against known cluster names for nodes
    not listed in connection strings (analytics nodes, hidden members)."""
    host = user_alias.split(":")[0].strip()
    result = cluster_map.get(host)
    if result:
        return result
    # Fallback: match userAlias first segment against cluster names
    # userAlias = "cluster-name-shard-00-00.domain" -> first = "cluster-name-shard-00-00"
    first = host.split(".")[0] if "." in host else host
    # Sort longest-first so "cdvrdevbgmongo" matches before "cdvrdev"
    for cn in sorted(cluster_names, key=len, reverse=True):
        if first == cn or first.startswith(cn + "-"):
            return cn
    return "unknown"


def _resolve_uri_cluster(uri, cluster_names, cluster_map):
    """Match a MongoDB URI to a cluster name."""
    try:
        from urllib.parse import urlparse
        parsed = urlparse(uri.replace("mongodb+srv://", "https://").replace("mongodb://", "https://"))
        host = parsed.hostname or ""
        # SRV: host = cluster-name.xxxxx.mongodb.net -> first segment is cluster name
        first = host.split(".")[0] if "." in host else host
        for cn in cluster_names:
            if first == cn or first.startswith(cn + "-"):
                return cn
        # Standard URI: host may be a member hostname in the map
        return cluster_map.get(host)
    except Exception:
        return None


def _discover_shard_primaries(client):
    """Detect if connected to a sharded cluster and return [(shard_name, primary_host), ...].
    Returns empty list if not sharded or detection fails."""
    try:
        hello = client.admin.command("hello")
        # mongos responds with msg: "isdbgrid"
        if hello.get("msg") == "isdbgrid":
            # Get shard list from config.shards
            shards = list(client.config.shards.find())
            primaries = []
            for shard in shards:
                shard_name = shard["_id"]
                # host format: "replicaSetName/host1:port,host2:port,host3:port"
                host_str = shard.get("host", "")
                if "/" in host_str:
                    rs_name, hosts = host_str.split("/", 1)
                else:
                    hosts = host_str
                    rs_name = None
                # Connect to the shard RS and find the primary
                host_list = hosts.split(",")
                try:
                    import pymongo
                    # Inherit TLS settings from the original URI
                    tls_opts = {}
                    if _MONGO_URI:
                        if "tlsCAFile=" in _MONGO_URI or "tlsCAFile=" in str(_MONGO_URI):
                            for param in str(_MONGO_URI).split("?")[-1].split("&"):
                                if param.startswith("tlsCAFile="):
                                    tls_opts["tlsCAFile"] = param.split("=", 1)[1]
                        if "tls=true" in str(_MONGO_URI).lower():
                            tls_opts["tls"] = True
                    if not tls_opts.get("tls"):
                        tls_opts["tls"] = True
                    shard_client = pymongo.MongoClient(
                        host_list, serverSelectionTimeoutMS=5000,
                        replicaSet=rs_name, readPreference="primary",
                        **tls_opts,
                    )
                    shard_hello = shard_client.admin.command("hello")
                    primary = shard_hello.get("primary") or shard_hello.get("me")
                    shard_client.close()
                    if primary:
                        primaries.append((shard_name, primary))
                except Exception as e:
                    _check_auth_error(e, f"shard primary discovery ({shard_name})")
                    print(f"    WARNING: Cannot reach shard {shard_name}: {e}")
                    # Fall back to first host in the list
                    primaries.append((shard_name, host_list[0]))
            return primaries
    except Exception:
        pass
    return []


def _collect_collstats_single_node(client, namespaces, shard_name=None):
    """Collect collStats + $indexStats from a single node. Returns (coll_metrics, all_indexes).
    If shard_name is provided, tags each entry with it."""
    ss = client.admin.command("serverStatus")
    uptime = ss.get("uptime", 1)
    SAMPLE_INTERVAL = 5

    # WT server cache
    wt_server_cache = {}
    try:
        wt_cache = ss.get("wiredTiger", {}).get("cache", {})
        wt_server_cache = {
            "max_bytes_configured": wt_cache.get("maximum bytes configured", 0),
            "bytes_in_cache": wt_cache.get("bytes currently in the cache", 0),
        }
    except Exception:
        pass

    # Discover namespaces directly from this node (may have different collections per shard)
    try:
        node_ns = []
        for dbn in client.list_database_names():
            if dbn in ("admin", "local", "config"):
                continue
            for cn in client[dbn].list_collection_names():
                if not cn.startswith("system."):
                    node_ns.append(f"{dbn}.{cn}")
        if node_ns:
            # Merge: node-discovered namespaces + caller-provided, dedup
            namespaces = list(dict.fromkeys(node_ns + list(namespaces)))
    except Exception:
        pass

    # Snapshot for ops/sec
    def latency_snapshot():
        snap = {}
        for ns in namespaces:
            db_name, coll_name = ns.split(".", 1)
            try:
                r = list(client[db_name][coll_name].aggregate([
                    {"$collStats": {"latencyStats": {"histograms": False}}}
                ]))
                if r:
                    ls = r[0].get("latencyStats", {})
                    snap[ns] = {
                        "reads_ops": ls.get("reads", {}).get("ops", 0),
                        "writes_ops": ls.get("writes", {}).get("ops", 0),
                        "commands_ops": ls.get("commands", {}).get("ops", 0),
                        "reads_latency_us": ls.get("reads", {}).get("latency", 0),
                        "writes_latency_us": ls.get("writes", {}).get("latency", 0),
                    }
            except Exception:
                pass
        return snap

    shard_label = f" [{shard_name}]" if shard_name else ""
    print(f"  Taking two snapshots {SAMPLE_INTERVAL}s apart{shard_label}...")
    snap1 = latency_snapshot()
    time.sleep(SAMPLE_INTERVAL)
    snap2 = latency_snapshot()

    coll_metrics = []
    all_indexes = []

    for ns in namespaces:
        db_name, coll_name = ns.split(".", 1)
        db = client[db_name]
        entry = {"namespace": ns, "metrics": {}, "cumulative": {}, "working_set": {}, "cursor_stats": {}, "io_estimate": {}}
        if shard_name:
            entry["shard"] = shard_name

        # Ops/sec from snapshot diff
        s1 = snap1.get(ns, {})
        s2 = snap2.get(ns, {})
        if s1 and s2:
            reads_sec = max(0, (s2["reads_ops"] - s1["reads_ops"]) / SAMPLE_INTERVAL)
            writes_sec = max(0, (s2["writes_ops"] - s1["writes_ops"]) / SAMPLE_INTERVAL)
            rd = s2["reads_ops"] - s1["reads_ops"]
            wd = s2["writes_ops"] - s1["writes_ops"]
            entry["metrics"] = {
                "reads_per_sec": round(reads_sec, 2),
                "writes_per_sec": round(writes_sec, 2),
                "total_ops_per_sec": round(reads_sec + writes_sec, 2),
                "avg_read_latency_us": round((s2["reads_latency_us"] - s1["reads_latency_us"]) / rd, 0) if rd > 0 else 0,
                "avg_write_latency_us": round((s2["writes_latency_us"] - s1["writes_latency_us"]) / wd, 0) if wd > 0 else 0,
            }
            entry["cumulative"] = {
                "reads_ops": s2["reads_ops"],
                "writes_ops": s2["writes_ops"],
                "commands_ops": s2["commands_ops"],
            }

        # collStats for storage + WiredTiger
        stats = None
        try:
            stats = db.command("collStats", coll_name)
            avg_obj_size = stats.get("avgObjSize", 0)
            data_size = stats.get("size", 0)
            wt = stats.get("wiredTiger", {})
            cache = wt.get("cache", {})
            pages_read = cache.get("pages read into cache", 0)
            bytes_read = cache.get("bytes read into cache", 0)
            data_ws = min(bytes_read, data_size) if bytes_read > 0 else 0
            entry["working_set"] = {
                "pages_read_into_cache": pages_read,
                "bytes_read_into_cache": bytes_read,
                "data_size": data_size,
                "data_working_set_bytes": data_ws,
                "data_working_set_pct": round(data_ws / data_size * 100, 1) if data_size > 0 else 0,
            }
            cursor = wt.get("cursor", {})
            entry["cursor_stats"] = {
                "insert_calls": cursor.get("insert calls", 0),
                "update_calls": cursor.get("modify", cursor.get("update calls", 0)),
                "remove_calls": cursor.get("remove calls", 0),
                "search_calls": cursor.get("search calls", 0),
            }
            daily_ops = (entry["cumulative"].get("reads_ops", 0) + entry["cumulative"].get("writes_ops", 0))
            if uptime > 0:
                daily_ops = daily_ops / uptime * 86400
            entry["io_estimate"] = {
                "read_io_per_day": int(daily_ops),
                "write_io_per_day": int(daily_ops),
                "avg_doc_size": avg_obj_size,
            }
            entry["storage_size"] = stats.get("storageSize", 0)
            stor_sz = entry["storage_size"]
            entry["compression_ratio"] = round(data_size / stor_sz, 2) if stor_sz > 0 else 1.0
            entry["doc_count"] = stats.get("count", 0)
            entry["avg_doc_size"] = avg_obj_size
            entry["total_index_size"] = stats.get("totalIndexSize", 0)
            entry["nindexes"] = stats.get("nindexes", 0)
            # Sample real zstd compression ratio
            zstd_sample = _sample_compression_ratio(db, coll_name)
            if zstd_sample:
                entry["zstd_compression"] = zstd_sample
        except Exception:
            pass

        # $indexStats + index specs
        try:
            idx_stats = list(db[coll_name].aggregate([{"$indexStats": {}}]))
            idx_sizes = stats.get("indexSizes", {}) if stats else {}
            doc_count = stats.get("count", 0) if stats else 0
            idx_specs = {}
            try:
                for spec in db[coll_name].list_indexes():
                    idx_specs[spec["name"]] = spec
                _record_raw_index_specs(f"{db.name}.{coll_name}", idx_specs)
            except Exception:
                pass

            for idx in idx_stats:
                idx_name = idx.get("name", "")
                accesses = idx.get("accesses", {}).get("ops", 0)
                idx_size = idx_sizes.get(idx_name, 0)
                idx_key = idx.get("key", {})
                idx_entry = {
                    "namespace": ns,
                    "name": idx_name,
                    "key": idx_key,
                    "size_bytes": idx_size,
                    "accesses": accesses,
                    "doc_count": doc_count,
                    "unused": accesses == 0 and idx_name != "_id_",
                }
                if shard_name:
                    idx_entry["shard"] = shard_name

                spec = idx_specs.get(idx_name, {})
                is_sparse = spec.get("sparse", False)
                partial_expr = spec.get("partialFilterExpression")
                if is_sparse:
                    idx_entry["sparse"] = True
                if partial_expr:
                    idx_entry["partial"] = True

                # Detect special index types
                for v in idx_key.values():
                    if v == "2dsphere":
                        idx_entry["index_type"] = "2dsphere"
                    elif v == "text":
                        idx_entry["index_type"] = "text"

                # Key type sampling
                key_types = _sample_index_key_types(db, coll_name, idx_key)
                if key_types:
                    idx_entry["key_types"] = key_types
                    wt_ks_size = _compute_wt_keystring_size(key_types)
                    idx_entry["wt_keystring_bytes_per_entry"] = wt_ks_size

                all_indexes.append(idx_entry)
        except Exception:
            pass

        coll_metrics.append(entry)

    # Detect redundant indexes
    for ns in set(e["namespace"] for e in all_indexes):
        ns_indexes = [i for i in all_indexes if i["namespace"] == ns]
        for idx in ns_indexes:
            keys = list(idx["key"].items()) if isinstance(idx["key"], dict) else []
            for other in ns_indexes:
                if other["name"] == idx["name"]:
                    continue
                other_keys = list(other["key"].items()) if isinstance(other["key"], dict) else []
                if len(keys) < len(other_keys) and keys == other_keys[:len(keys)]:
                    idx["redundant"] = True
                    idx["redundant_of"] = other["name"]
                    break

    return coll_metrics, all_indexes, wt_server_cache


def _collect_collstats_via_mongos_sharded(mongos_client, namespaces, output_dir, cluster_name):
    """Fallback for sharded clusters when direct-shard connect is unavailable.

    Uses mongos-aggregated `collStats`, which returns per-shard data in the
    `shards` sub-document (both for sharded AND unsharded-in-a-sharded-cluster
    collections). Preserves all sizing-critical data:

      - Per-shard size / count / storageSize / avgObjSize / nindexes / totalIndexSize
      - Per-shard wiredTiger.cache (pages_read_into_cache, bytes_read_into_cache)
      - Per-shard wiredTiger.cursor (insert/update/remove/search calls)
      - Per-shard $collStats latencyStats for ops/sec sampling
      - Per-shard $indexStats (each entry tagged with `shard` field)
      - Live Zstd compression sampling (mongos-routed find works)
      - Live index key type sampling (mongos-routed find works)

    Loss: `wt_server_cache.max_bytes_configured` per shard (only in direct-shard
    serverStatus). Inferable from Atlas API metrics OR instance tier lookup.

    Output shape mirrors what direct-shard would produce plus a `data_source`
    marker for transparency.
    """
    import pymongo

    print("  Collecting via mongos-aggregated collStats (fallback path)...")
    all_coll_metrics = []
    all_indexes = []
    shards_seen = set()

    # Discover additional namespaces via mongos (list_databases + list_collection_names route to shards)
    try:
        for dbn in mongos_client.list_database_names():
            if dbn in ("admin", "local", "config"):
                continue
            for cn in mongos_client[dbn].list_collection_names():
                if not cn.startswith("system."):
                    ns = f"{dbn}.{cn}"
                    if ns not in namespaces:
                        namespaces.append(ns)
    except Exception:
        pass

    SAMPLE_INTERVAL = 5

    def latency_snapshot_per_shard():
        """Take a $collStats latencyStats snapshot per (ns, shard). Returns dict keyed by (ns, shard_name)."""
        snap = {}
        for ns in namespaces:
            db_name, coll_name = ns.split(".", 1)
            try:
                r = list(mongos_client[db_name][coll_name].aggregate([
                    {"$collStats": {"latencyStats": {"histograms": False}}}
                ]))
                for entry in r:
                    shard_name = entry.get("shard", "primary")
                    ls = entry.get("latencyStats", {})
                    snap[(ns, shard_name)] = {
                        "reads_ops": ls.get("reads", {}).get("ops", 0),
                        "writes_ops": ls.get("writes", {}).get("ops", 0),
                        "commands_ops": ls.get("commands", {}).get("ops", 0),
                        "reads_latency_us": ls.get("reads", {}).get("latency", 0),
                        "writes_latency_us": ls.get("writes", {}).get("latency", 0),
                    }
            except Exception:
                pass
        return snap

    print(f"  Taking two snapshots {SAMPLE_INTERVAL}s apart [mongos aggregated]...")
    snap1 = latency_snapshot_per_shard()
    time.sleep(SAMPLE_INTERVAL)
    snap2 = latency_snapshot_per_shard()

    # Mongos uptime (used for daily-ops estimate; mongos uptime is close enough for rate math)
    mongos_uptime = 1
    try:
        ss = mongos_client.admin.command("serverStatus")
        mongos_uptime = ss.get("uptime", 1)
    except Exception:
        pass

    for ns in namespaces:
        db_name, coll_name = ns.split(".", 1)
        db = mongos_client[db_name]

        # collStats via mongos returns 'shards' sub-doc for both sharded and unsharded collections
        try:
            stats = db.command("collStats", coll_name)
        except Exception as e:
            print(f"    WARNING: collStats failed for {ns}: {e}")
            continue

        if "shards" not in stats:
            print(f"    WARNING: {ns} has no 'shards' sub-doc (may be nonexistent). Skipping.")
            continue

        # Sample compression ratio + index key types once per namespace (same for all shards)
        zstd_sample = _sample_compression_ratio(db, coll_name)

        # $indexStats via mongos: returns per-shard entries with `shard` field
        try:
            idx_stats_all = list(db[coll_name].aggregate([{"$indexStats": {}}]))
        except Exception:
            idx_stats_all = []

        # Index specs (same across shards for a given collection)
        idx_specs = {}
        try:
            for spec in db[coll_name].list_indexes():
                idx_specs[spec["name"]] = spec
            _record_raw_index_specs(f"{db.name}.{coll_name}", idx_specs)
        except Exception:
            pass

        # Iterate per-shard data from mongos collStats
        for shard_name, shard_stats in stats["shards"].items():
            shards_seen.add(shard_name)

            entry = {
                "namespace": ns, "shard": shard_name,
                "metrics": {}, "cumulative": {},
                "working_set": {}, "cursor_stats": {},
                "io_estimate": {},
            }

            # Ops/sec from per-shard latency snapshots
            s1 = snap1.get((ns, shard_name), {})
            s2 = snap2.get((ns, shard_name), {})
            if s1 and s2:
                reads_sec = max(0, (s2["reads_ops"] - s1["reads_ops"]) / SAMPLE_INTERVAL)
                writes_sec = max(0, (s2["writes_ops"] - s1["writes_ops"]) / SAMPLE_INTERVAL)
                rd = s2["reads_ops"] - s1["reads_ops"]
                wd = s2["writes_ops"] - s1["writes_ops"]
                entry["metrics"] = {
                    "reads_per_sec": round(reads_sec, 2),
                    "writes_per_sec": round(writes_sec, 2),
                    "total_ops_per_sec": round(reads_sec + writes_sec, 2),
                    "avg_read_latency_us": round((s2["reads_latency_us"] - s1["reads_latency_us"]) / rd, 0) if rd > 0 else 0,
                    "avg_write_latency_us": round((s2["writes_latency_us"] - s1["writes_latency_us"]) / wd, 0) if wd > 0 else 0,
                }
                entry["cumulative"] = {
                    "reads_ops": s2["reads_ops"],
                    "writes_ops": s2["writes_ops"],
                    "commands_ops": s2["commands_ops"],
                }

            # Per-shard collStats fields (same shape as _collect_collstats_single_node produces)
            data_size = shard_stats.get("size", 0)
            entry["storage_size"] = shard_stats.get("storageSize", 0)
            stor_sz = entry["storage_size"]
            entry["compression_ratio"] = round(data_size / stor_sz, 2) if stor_sz > 0 else 1.0
            entry["doc_count"] = shard_stats.get("count", 0)
            entry["avg_doc_size"] = shard_stats.get("avgObjSize", 0)
            entry["total_index_size"] = shard_stats.get("totalIndexSize", 0)
            entry["nindexes"] = shard_stats.get("nindexes", 0)

            wt = shard_stats.get("wiredTiger", {})
            cache = wt.get("cache", {})
            pages_read = cache.get("pages read into cache", 0)
            bytes_read = cache.get("bytes read into cache", 0)
            data_ws = min(bytes_read, data_size) if bytes_read > 0 else 0
            entry["working_set"] = {
                "pages_read_into_cache": pages_read,
                "bytes_read_into_cache": bytes_read,
                "data_size": data_size,
                "data_working_set_bytes": data_ws,
                "data_working_set_pct": round(data_ws / data_size * 100, 1) if data_size > 0 else 0,
            }

            cursor = wt.get("cursor", {})
            entry["cursor_stats"] = {
                "insert_calls": cursor.get("insert calls", 0),
                "update_calls": cursor.get("modify", cursor.get("update calls", 0)),
                "remove_calls": cursor.get("remove calls", 0),
                "search_calls": cursor.get("search calls", 0),
            }

            daily_ops = (entry["cumulative"].get("reads_ops", 0) + entry["cumulative"].get("writes_ops", 0))
            if mongos_uptime > 0:
                daily_ops = daily_ops / mongos_uptime * 86400
            entry["io_estimate"] = {
                "read_io_per_day": int(daily_ops),
                "write_io_per_day": int(daily_ops),
                "avg_doc_size": entry["avg_doc_size"],
            }

            # Zstd compression sample (same value across all shards for a namespace)
            if zstd_sample:
                entry["zstd_compression"] = zstd_sample

            all_coll_metrics.append(entry)

        # Build per-shard index entries from mongos $indexStats results
        idx_sizes = {}
        for shard_name, shard_stats in stats["shards"].items():
            for idx_name, idx_size in shard_stats.get("indexSizes", {}).items():
                idx_sizes[(shard_name, idx_name)] = idx_size

        doc_counts_per_shard = {sn: ss.get("count", 0) for sn, ss in stats["shards"].items()}

        for idx in idx_stats_all:
            idx_name = idx.get("name", "")
            shard_name = idx.get("shard", "unknown")
            accesses = idx.get("accesses", {}).get("ops", 0)
            idx_size = idx_sizes.get((shard_name, idx_name), 0)
            idx_key = idx.get("key", {})
            idx_entry = {
                "namespace": ns,
                "name": idx_name,
                "key": idx_key,
                "size_bytes": idx_size,
                "accesses": accesses,
                "doc_count": doc_counts_per_shard.get(shard_name, 0),
                "unused": accesses == 0 and idx_name != "_id_",
                "shard": shard_name,
            }

            spec = idx_specs.get(idx_name, {})
            if spec.get("sparse", False):
                idx_entry["sparse"] = True
            if spec.get("partialFilterExpression"):
                idx_entry["partial"] = True

            # Detect special index types
            for v in idx_key.values():
                if v == "2dsphere":
                    idx_entry["index_type"] = "2dsphere"
                elif v == "text":
                    idx_entry["index_type"] = "text"

            # Key type sampling via mongos (queries routed to shards)
            key_types = _sample_index_key_types(db, coll_name, idx_key)
            if key_types:
                idx_entry["key_types"] = key_types
                wt_ks_size = _compute_wt_keystring_size(key_types)
                idx_entry["wt_keystring_bytes_per_entry"] = wt_ks_size

            all_indexes.append(idx_entry)

    # Detect redundant indexes (same logic as direct-shard path)
    for ns in set(e["namespace"] for e in all_indexes):
        ns_indexes = [i for i in all_indexes if i["namespace"] == ns]
        for idx in ns_indexes:
            keys = list(idx["key"].items()) if isinstance(idx["key"], dict) else []
            for other in ns_indexes:
                if other["name"] == idx["name"]:
                    continue
                other_keys = list(other["key"].items()) if isinstance(other["key"], dict) else []
                if len(keys) < len(other_keys) and keys == other_keys[:len(keys)]:
                    idx["redundant"] = True
                    idx["redundant_of"] = other["name"]
                    break

    # Persist outputs
    cdir = Path(output_dir) / cluster_name
    cdir.mkdir(parents=True, exist_ok=True)

    with open(cdir / "collstats.json", "w") as f:
        output = {
            "sharded": True,
            "data_source": "mongos_aggregated",
            "shard_count": len(shards_seen),
            "shards": sorted(shards_seen),
            "wt_server_caches": {},
            "collections": all_coll_metrics,
        }
        json.dump(output, f, indent=2, default=str)

    unused = [i for i in all_indexes if i.get("unused")]
    redundant = [i for i in all_indexes if i.get("redundant")]
    index_report = {
        "sharded": True,
        "data_source": "mongos_aggregated",
        "total_indexes": len(all_indexes),
        "unused_indexes": len(unused),
        "redundant_indexes": len(redundant),
        "indexes": all_indexes,
    }
    with open(cdir / "index_analysis.json", "w") as f:
        json.dump(index_report, f, indent=2, default=str)
    _write_index_specs(cdir)

    shard_counts = {}
    for idx in all_indexes:
        s = idx.get("shard", "unknown")
        shard_counts[s] = shard_counts.get(s, 0) + 1
    print(f"  SHARDED INDEX ANALYSIS (mongos-aggregated): {len(all_indexes)} total across {len(shards_seen)} shards, "
          f"{len(unused)} unused, {len(redundant)} redundant")
    for s, c in sorted(shard_counts.items()):
        print(f"    {s}: {c} indexes")

    total_colls = len(set(e["namespace"] for e in all_coll_metrics))
    print(f"  Collected stats for {total_colls} unique collections across {len(shards_seen)} shards")
    return all_coll_metrics


def _collect_collstats_via_uri(namespaces, output_dir, cluster_name):
    """Collect per-collection metrics via direct MongoDB connection.
    Detects sharded clusters and delegates to per-shard collection.
    For replica sets, collects directly from the connected node."""
    if not _MONGO_URI or not namespaces:
        return [{"namespace": ns, "metrics": {}} for ns in namespaces]
    if _URI_CLUSTER_NAME and _URI_CLUSTER_NAME != cluster_name:
        return [{"namespace": ns, "metrics": {}} for ns in namespaces]

    try:
        import pymongo
    except ImportError:
        print("  WARNING: pymongo not installed")
        return [{"namespace": ns, "metrics": {}} for ns in namespaces]

    try:
        client = pymongo.MongoClient(host=_MONGO_URI, appname='atlas-metrics-collstats', serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        _check_auth_error(e, "collstats collection (post-preflight)", output_dir)
        print(f"  WARNING: Cannot connect: {e}")
        return [{"namespace": ns, "metrics": {}} for ns in namespaces]

    # Detect sharded cluster and delegate to per-shard collection
    shard_primaries = _discover_shard_primaries(client)
    if shard_primaries:
        print(f"  Sharded cluster detected: {len(shard_primaries)} shard(s)")
        for sname, shost in shard_primaries:
            print(f"    {sname} -> {shost}")
        # Direct-shard collStats path was removed after 2026-07-02 testing:
        # MongoDB server rejects `collStats` on sharded collections when connected
        # directly to a shard (error: "connecting to a sharded cluster improperly").
        # Only mongos-aggregated collStats works for sharded Atlas clusters.
        print("  Using mongos-aggregated collStats (per-shard data via `shards` sub-doc).")
        result = _collect_collstats_via_mongos_sharded(client, namespaces, output_dir, cluster_name)
        client.close()
        return result

    # Non-sharded: collect from this node directly
    ss = client.admin.command("serverStatus")
    uptime = ss.get("uptime", 1)
    SAMPLE_INTERVAL = 5

    # --- serverStatus WT cache breakdown ---
    wt_server_cache = {}
    try:
        wt_cache = ss.get("wiredTiger", {}).get("cache", {})
        wt_server_cache = {
            "max_bytes_configured": wt_cache.get("maximum bytes configured", 0),
            "bytes_in_cache": wt_cache.get("bytes currently in the cache", 0),
            "page_images_bytes": wt_cache.get("bytes belonging to page images in the cache", 0),
            "non_page_overhead_bytes": wt_cache.get("bytes not belonging to page images in the cache", 0),
            "internal_pages_bytes": wt_cache.get("tracked bytes belonging to internal pages in the cache", 0),
            "leaf_pages_bytes": wt_cache.get("tracked bytes belonging to leaf pages in the cache", 0),
            "dirty_bytes": wt_cache.get("tracked dirty bytes in the cache", 0),
            "pages_in_cache": wt_cache.get("pages currently held in the cache", 0),
            "percentage_overhead": wt_cache.get("percentage overhead", 8),
        }
    except Exception:
        pass

    # --- Snapshot for ops/sec ---
    def latency_snapshot():
        snap = {}
        for ns in namespaces:
            db_name, coll_name = ns.split(".", 1)
            try:
                r = list(client[db_name][coll_name].aggregate([
                    {"$collStats": {"latencyStats": {"histograms": False}}}
                ]))
                if r:
                    ls = r[0].get("latencyStats", {})
                    snap[ns] = {
                        "reads_ops": ls.get("reads", {}).get("ops", 0),
                        "writes_ops": ls.get("writes", {}).get("ops", 0),
                        "commands_ops": ls.get("commands", {}).get("ops", 0),
                        "reads_latency_us": ls.get("reads", {}).get("latency", 0),
                        "writes_latency_us": ls.get("writes", {}).get("latency", 0),
                    }
            except Exception:
                pass
        return snap

    print(f"  Taking two snapshots {SAMPLE_INTERVAL}s apart for per-collection ops/sec...")
    with _Timer(f"collstats_snapshot {cluster_name}"):
        snap1 = latency_snapshot()
        time.sleep(SAMPLE_INTERVAL)
        snap2 = latency_snapshot()

    # --- Per-collection deep stats ---
    coll_metrics = []
    all_indexes = []

    for ns in namespaces:
        db_name, coll_name = ns.split(".", 1)
        db = client[db_name]
        entry = {"namespace": ns, "metrics": {}, "cumulative": {}, "working_set": {}, "cursor_stats": {}, "io_estimate": {}}

        # Ops/sec from snapshot diff
        s1 = snap1.get(ns, {})
        s2 = snap2.get(ns, {})
        if s1 and s2:
            reads_sec = max(0, (s2["reads_ops"] - s1["reads_ops"]) / SAMPLE_INTERVAL)
            writes_sec = max(0, (s2["writes_ops"] - s1["writes_ops"]) / SAMPLE_INTERVAL)
            rd = s2["reads_ops"] - s1["reads_ops"]
            wd = s2["writes_ops"] - s1["writes_ops"]
            entry["metrics"] = {
                "reads_per_sec": round(reads_sec, 2),
                "writes_per_sec": round(writes_sec, 2),
                "total_ops_per_sec": round(reads_sec + writes_sec, 2),
                "avg_read_latency_us": round((s2["reads_latency_us"] - s1["reads_latency_us"]) / rd, 0) if rd > 0 else 0,
                "avg_write_latency_us": round((s2["writes_latency_us"] - s1["writes_latency_us"]) / wd, 0) if wd > 0 else 0,
            }
            entry["cumulative"] = {
                "reads_ops": s2["reads_ops"],
                "writes_ops": s2["writes_ops"],
                "commands_ops": s2["commands_ops"],
            }
            print(f"  {ns}: reads={reads_sec:.1f}/s writes={writes_sec:.1f}/s")

        # collStats for WiredTiger cache + cursor stats
        stats = None
        try:
            stats = db.command("collStats", coll_name)
            avg_obj_size = stats.get("avgObjSize", 0)

            # WiredTiger working set (cache access patterns)
            wt = stats.get("wiredTiger", {})
            cache = wt.get("cache", {})
            pages_read = cache.get("pages read into cache", 0)
            bytes_read = cache.get("bytes read into cache", 0)
            data_size = stats.get("size", 0)

            data_ws = min(bytes_read, data_size) if bytes_read > 0 else 0
            entry["working_set"] = {
                "pages_read_into_cache": pages_read,
                "bytes_read_into_cache": bytes_read,
                "data_size": data_size,
                "data_working_set_bytes": data_ws,
                "data_working_set_pct": round(data_ws / data_size * 100, 1) if data_size > 0 else 0,
            }

            # WiredTiger cursor stats (insert/update/delete/search breakdown)
            cursor = wt.get("cursor", {})
            entry["cursor_stats"] = {
                "insert_calls": cursor.get("insert calls", 0),
                "update_calls": cursor.get("modify", cursor.get("update calls", 0)),
                "remove_calls": cursor.get("remove calls", 0),
                "search_calls": cursor.get("search calls", 0),
            }

            # I/O estimation based on doc size
            daily_ops = (entry["cumulative"].get("reads_ops", 0) + entry["cumulative"].get("writes_ops", 0))
            if uptime > 0:
                daily_ops = daily_ops / uptime * 86400
            if avg_obj_size < 8096:
                read_io_day = int(daily_ops)
            else:
                read_io_day = int((avg_obj_size / 8096 + 1) * daily_ops)
            if avg_obj_size < 4048:
                write_io_day = int(daily_ops)
            else:
                write_io_day = int((avg_obj_size / 4048 + 1) * daily_ops)
            entry["io_estimate"] = {
                "read_io_per_day": read_io_day,
                "write_io_per_day": write_io_day,
                "avg_doc_size": avg_obj_size,
            }

        except Exception:
            pass

        # Index analysis: unused + redundant detection + type enrichment
        try:
            idx_stats = list(db[coll_name].aggregate([{"$indexStats": {}}]))
            idx_sizes = stats.get("indexSizes", {}) if stats else {}

            # Per-index cache bytes from indexDetails
            idx_cache_map = {}
            try:
                detailed = db.command("collStats", coll_name, indexDetails=True)
                for idx_name, idx_detail in detailed.get("indexDetails", {}).items():
                    ic = idx_detail.get("cache", {})
                    idx_cache_map[idx_name] = ic.get("bytes currently in the cache", 0)
            except Exception:
                pass

            doc_count = stats.get("count", 0) if stats else 0

            # Get index specs for sparse/partial detection
            idx_specs = {}
            try:
                for spec in db[coll_name].list_indexes():
                    idx_specs[spec["name"]] = spec
                _record_raw_index_specs(f"{db.name}.{coll_name}", idx_specs)
            except Exception:
                pass

            for idx in idx_stats:
                idx_name = idx.get("name", "")
                accesses = idx.get("accesses", {}).get("ops", 0)
                idx_size = idx_sizes.get(idx_name, 0)
                idx_key = idx.get("key", {})

                idx_entry = {
                    "namespace": ns,
                    "name": idx_name,
                    "key": idx_key,
                    "size_bytes": idx_size,
                    "accesses": accesses,
                    "doc_count": doc_count,
                    "unused": accesses == 0 and idx_name != "_id_",
                    "index_cache_bytes": idx_cache_map.get(idx_name, 0),
                }

                # Detect sparse/partial and estimate actual indexed doc count
                spec = idx_specs.get(idx_name, {})
                is_sparse = spec.get("sparse", False)
                partial_expr = spec.get("partialFilterExpression")
                if is_sparse or partial_expr:
                    try:
                        if partial_expr:
                            effective_count = db[coll_name].count_documents(partial_expr)
                        else:
                            # Sparse: count docs where the first key field exists
                            first_field = list(idx_key.keys())[0] if idx_key else None
                            if first_field:
                                effective_count = db[coll_name].count_documents({first_field: {"$exists": True}})
                            else:
                                effective_count = doc_count
                        idx_entry["effective_doc_count"] = effective_count
                        idx_entry["sparse"] = is_sparse
                        if partial_expr:
                            idx_entry["partial"] = True
                    except Exception:
                        pass

                # Type sampling for key fields
                key_types = _sample_index_key_types(db, coll_name, idx_key)
                # Detect special index types
                idx_type = "regular"
                for v in idx_key.values():
                    if v == "2dsphere":
                        idx_type = "2dsphere"
                    elif v == "text":
                        idx_type = "text"
                if idx_type != "regular":
                    idx_entry["index_type"] = idx_type
                if key_types:
                    idx_entry["key_types"] = key_types
                    wt_ks_size = _compute_wt_keystring_size(key_types)
                    idx_entry["wt_keystring_bytes_per_entry"] = wt_ks_size

                    # Theoretical uncompressed WT index size
                    # For multikey indexes, estimate expansion
                    multikey_factor = 1.0
                    for kt in key_types:
                        if kt.get("is_array"):
                            multikey_factor *= kt.get("avg_array_length") or 3.0
                    effective_docs = int(doc_count * multikey_factor)
                    theoretical = wt_ks_size * effective_docs
                    idx_entry["wt_theoretical_size"] = theoretical
                    idx_entry["multikey"] = multikey_factor > 1.0
                    if multikey_factor > 1.0:
                        idx_entry["avg_array_length"] = multikey_factor

                    # Empirical prefix compression ratio
                    if idx_size > 0 and theoretical > 0:
                        idx_entry["wt_prefix_compression_ratio"] = round(theoretical / idx_size, 2)

                all_indexes.append(idx_entry)
        except Exception:
            pass

        # Enrich collstats with per-collection and per-index WT cache bytes
        try:
            wt = stats.get("wiredTiger", {}) if stats else {}
            coll_cache = wt.get("cache", {})
            entry["wt_cache"] = {
                "collection_bytes_in_cache": coll_cache.get("bytes currently in the cache", 0),
                "pages_in_cache": coll_cache.get("pages currently held in the cache", 0),
            }
            if idx_cache_map:
                entry["wt_cache"]["index_cache_breakdown"] = idx_cache_map
            # Add storage_size and compression_ratio
            entry["storage_size"] = stats.get("storageSize", 0) if stats else 0
            data_sz = stats.get("size", 0) if stats else 0
            stor_sz = entry["storage_size"]
            entry["compression_ratio"] = round(data_sz / stor_sz, 2) if stor_sz > 0 else 1.0
            # Sample real zstd-3-dict compression ratio (matches DocumentDB 8.0)
            zstd_sample = _sample_compression_ratio(db, coll_name)
            if zstd_sample:
                entry["zstd_compression"] = zstd_sample
            entry["doc_count"] = stats.get("count", 0) if stats else 0
            entry["avg_doc_size"] = stats.get("avgObjSize", 0) if stats else 0
            entry["total_index_size"] = stats.get("totalIndexSize", 0) if stats else 0
        except Exception:
            pass

        coll_metrics.append(entry)

    # Detect redundant indexes (prefix subsets)
    for ns in namespaces:
        ns_indexes = [i for i in all_indexes if i["namespace"] == ns]
        for idx in ns_indexes:
            keys = list(idx["key"].items()) if isinstance(idx["key"], dict) else []
            for other in ns_indexes:
                if other["name"] == idx["name"]:
                    continue
                other_keys = list(other["key"].items()) if isinstance(other["key"], dict) else []
                # idx is redundant if it's a prefix of other
                if len(keys) < len(other_keys) and keys == other_keys[:len(keys)]:
                    idx["redundant"] = True
                    idx["redundant_of"] = other["name"]
                    break

    # Save outputs
    cdir = Path(output_dir) / cluster_name
    cdir.mkdir(parents=True, exist_ok=True)

    with open(cdir / "collstats.json", "w") as f:
        output = {
            "wt_server_cache": wt_server_cache,
            "collections": coll_metrics,
        }
        json.dump(output, f, indent=2)

    # Save index analysis
    unused = [i for i in all_indexes if i.get("unused")]
    redundant = [i for i in all_indexes if i.get("redundant")]
    index_report = {
        "total_indexes": len(all_indexes),
        "unused_indexes": len(unused),
        "redundant_indexes": len(redundant),
        "indexes": all_indexes,
    }
    with open(cdir / "index_analysis.json", "w") as f:
        json.dump(index_report, f, indent=2, default=str)
    _write_index_specs(cdir)

    if unused or redundant:
        print(f"  INDEX ANALYSIS: {len(all_indexes)} total, {len(unused)} unused, {len(redundant)} redundant")
        for i in unused[:5]:
            print(f"    UNUSED: {i['namespace']}.{i['name']} ({i['size_bytes']/1024:.0f}KB, 0 accesses)")
        for i in redundant[:5]:
            print(f"    REDUNDANT: {i['namespace']}.{i['name']} -> prefix of {i.get('redundant_of')}")

    print(f"  Collected metrics for {sum(1 for e in coll_metrics if e.get('metrics'))} collections")
    client.close()
    return coll_metrics


def collect_metrics(client, granularity, period, output_dir, cached_cs=None):
    """Collect all metrics for all processes in the project."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Discover clusters and build full-hostname->cluster mapping
    clusters_data = client.get_paginated("/clusters")
    cluster_map = {}  # full_userAlias_hostname -> cluster_name
    cluster_names = []
    if clusters_data:
        for c in clusters_data:
            cluster_names.append(c["name"])
            conn = c.get("connectionStrings", {}).get("standard", "")
            # Extract full hostnames from connection string
            for part in conn.replace("mongodb://", "").split(","):
                host = part.split(":")[0].strip().rstrip("/")
                if host:
                    cluster_map[host] = c["name"]

    # Resolve --uri to cluster name early so we can filter
    global _URI_CLUSTER_NAME
    if _MONGO_URI and not _URI_CLUSTER_NAME:
        _URI_CLUSTER_NAME = _resolve_uri_cluster(_MONGO_URI, cluster_names, cluster_map)
        if _URI_CLUSTER_NAME:
            print(f"  --uri matched to cluster: {_URI_CLUSTER_NAME}")
    if _URI_CLUSTER_NAME:
        cluster_names = [_URI_CLUSTER_NAME]

    # Discover processes
    print("Discovering cluster processes...")
    with _Timer("discover_processes"):
        processes = client.get_paginated("/processes")
    if not processes:
        print("ERROR: No processes found.")
        print("  - Verify ATLAS_PROJECT_ID is the 24-char hex string (not the project name)")
        print("  - Check the API key has access to this project")
        print("  - Ensure the cluster is not paused/terminated")
        sys.exit(1)

    # Save process inventory
    with open(output_dir / "processes.json", "w") as f:
        json.dump(processes, f, indent=2)

    print(f"Found {len(processes)} processes:")
    for p in processes:
        print(f"  {p['hostname']}:{p['port']} ({p['typeName']})")
    if _URI_CLUSTER_NAME:
        filtered = sum(1 for p in processes
            if _cluster_for_alias(p.get("userAlias", ""), cluster_map, cluster_names) == _URI_CLUSTER_NAME)
        print(f"\n  Filtering to cluster '{_URI_CLUSTER_NAME}': {filtered} of {len(processes)} processes")
        # Init runtime log in cluster subdirectory
        if not _runtime_log:
            log_dir = output_dir / _URI_CLUSTER_NAME
            log_dir.mkdir(parents=True, exist_ok=True)
            _init_log(log_dir)
            _log(f"atlas_metrics.py started | cluster={_URI_CLUSTER_NAME} | processes={filtered}/{len(processes)} | granularity={granularity} | period={period}")
    elif len(processes) > 9:
        print(f"\n  WARNING: {len(processes)} processes found. Use --cluster <name> to target a specific cluster")
        print(f"  Available clusters: {', '.join(cluster_names)}")
    print()

    all_metrics = {}

    def _collect_process(proc):
        """Collect all metrics for a single process. Thread-safe."""
        host_port = f"{proc['hostname']}:{proc['port']}"
        safe_name = host_port.replace(":", "_").replace(".", "_")
        proc_type = proc["typeName"]

        alias = proc.get("userAlias", "")
        proc_cluster = _cluster_for_alias(alias, cluster_map, cluster_names)

        if _URI_CLUSTER_NAME and proc_cluster != _URI_CLUSTER_NAME:
            _log(f"SKIP {host_port} (cluster={proc_cluster}, target={_URI_CLUSTER_NAME})")
            return host_port, None

        # Skip mongos routers -- they lack storage/cache/replication metrics
        if "MONGOS" in proc_type:
            _log(f"SKIP {host_port} (mongos router -- no storage/cache metrics)")
            return host_port, None

        print(f"=== {host_port} ({proc_type}) ===")
        result = {"type": proc_type, "cluster": proc_cluster, "batches": {}}

        proc_dir = output_dir / proc_cluster
        proc_dir.mkdir(parents=True, exist_ok=True)

        for batch_name, metrics in PROCESS_METRIC_BATCHES.items():
            with _Timer(f"api {host_port} {batch_name}"):
                params = [
                    ("granularity", granularity),
                    ("period", period),
                ] + [("m", m) for m in metrics]
                data = client.get(f"/processes/{host_port}/measurements", params)
                if not data:
                    print(f"    WARNING: batch '{batch_name}' returned 404 -- falling back to individual metrics ({len(metrics)} calls)")
                    combined = []
                    for m in metrics:
                        single = client.get(f"/processes/{host_port}/measurements",
                            [("granularity", granularity), ("period", period), ("m", m)])
                        if single:
                            combined.extend(single.get("measurements", []))
                        else:
                            print(f"      {m}: not available")
                    if combined:
                        data = {"measurements": combined}
                if data:
                    result["batches"][batch_name] = data.get("measurements", [])
                    with open(proc_dir / f"{safe_name}_{batch_name}.json", "w") as f:
                        json.dump(data, f, indent=2)

        # Disk partition metrics
        disks = client.get(f"/processes/{host_port}/disks")
        if disks:
            for part in disks.get("results", []):
                pname = part["partitionName"]
                data = client.get(
                    f"/processes/{host_port}/disks/{pname}/measurements",
                    {"granularity": granularity, "period": period},
                )
                if data:
                    with open(proc_dir / f"{safe_name}_disk_{pname}.json", "w") as f:
                        json.dump(data, f, indent=2)
                    result["batches"].setdefault("disk_partition", [])
                    result["batches"]["disk_partition"].extend(data.get("measurements", []))

        # Database-level storage (primary only)
        if proc_type in ("REPLICA_PRIMARY", "SHARD_PRIMARY"):
            dbs = client.get(f"/processes/{host_port}/databases")
            if dbs:
                for db in dbs.get("results", []):
                    dbname = db["databaseName"]
                    if dbname in ("admin", "local", "config"):
                        continue
                    data = client.get(
                        f"/processes/{host_port}/databases/{dbname}/measurements",
                        {"granularity": granularity, "period": period},
                    )
                    if data:
                        with open(proc_dir / f"{safe_name}_db_{dbname}.json", "w") as f:
                            json.dump(data, f, indent=2)

        return host_port, result

    # Collect metrics sequentially to avoid Atlas API rate limits (~100 req/min)
    from concurrent.futures import ThreadPoolExecutor, as_completed
    with _Timer("api_collection_all_processes"):
        with ThreadPoolExecutor(max_workers=1) as executor:
            futures = {executor.submit(_collect_process, proc): proc for proc in processes}
            for future in as_completed(futures):
                host_port, result = future.result()
                if result:
                    all_metrics[host_port] = result

    # Collect per-collection metrics: ranked namespaces + direct $collStats via --uri or Atlas API fallback
    collstats = {}
    # Use cached collStats if available (no need to re-collect per window)
    if cached_cs:
        _log("Using cached collStats from previous window")
        print("  Using cached collection-level metrics from previous window")
        collstats = cached_cs
    else:
        for cname in cluster_names:
            cluster_primary = None
            for proc in processes:
                if proc["typeName"] not in ("REPLICA_PRIMARY", "SHARD_PRIMARY"):
                    continue
                alias = proc.get("userAlias", "")
                if _cluster_for_alias(alias, cluster_map, cluster_names) == cname:
                    cluster_primary = f"{proc['hostname']}:{proc['port']}"
                    break
            if not cluster_primary:
                continue

            print(f"=== Collection-level metrics for {cname} ===")
            now_str = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            start_str = period_to_start(period)

            ns_data = client.get(
                f"/processes/{cluster_primary}/collStats/namespaces",
                {"start": start_str, "end": now_str},
                api_version="2024-08-05",
            )
            namespaces = ns_data.get("rankedNamespaces", []) if ns_data else []
            namespaces = [ns for ns in namespaces if not ns.startswith(("admin.", "local.", "config."))]

            # When --uri is provided, also discover namespaces directly from the database.
            # Atlas API namespace cache can be stale after cluster recreation.
            if _MONGO_URI and (not _URI_CLUSTER_NAME or _URI_CLUSTER_NAME == cname):
                try:
                    import pymongo
                    mc = pymongo.MongoClient(host=_MONGO_URI, serverSelectionTimeoutMS=5000)
                    mc.admin.command("ping")
                    uri_ns = []
                    for dbn in mc.list_database_names():
                        if dbn in ("admin", "local", "config"):
                            continue
                        for cn in mc[dbn].list_collection_names():
                            if not cn.startswith("system."):
                                uri_ns.append(f"{dbn}.{cn}")
                    mc.close()
                    if uri_ns:
                        merged = list(dict.fromkeys(uri_ns + namespaces))  # uri first, dedup
                        if set(merged) != set(namespaces):
                            print(f"  URI discovery added: {sorted(set(merged) - set(namespaces))}")
                        namespaces = merged
                except Exception as e:
                    _check_auth_error(e, "namespace discovery", output_dir)
                    print(f"  URI namespace discovery failed: {e}")

            print(f"  Found {len(namespaces)} active namespaces")

            collstats[cname] = _collect_collstats_via_uri(namespaces, output_dir, cname)

    return all_metrics, processes, cluster_names, collstats


def percentile(values, p):
    """Calculate p-th percentile from sorted values."""
    if not values:
        return None
    k = (len(values) - 1) * (p / 100)
    f = int(k)
    c = f + 1 if f + 1 < len(values) else f
    return values[f] + (values[c] - values[f]) * (k - f)


def extract_stats(measurements, pct=95):
    """Extract avg, max, min, pN, last from a measurement's dataPoints."""
    values = [dp["value"] for dp in measurements.get("dataPoints", []) if dp.get("value") is not None]
    if not values:
        return None
    sorted_vals = sorted(values)
    return {
        "avg": sum(values) / len(values),
        "max": max(values),
        "min": min(values),
        f"p{pct}": percentile(sorted_vals, pct),
        "last": values[-1],
        "samples": len(values),
    }


def find_metric(batches, metric_name, pct=95):
    """Find a specific metric across all batches."""
    for batch_measurements in batches.values():
        for m in batch_measurements:
            if m.get("name") == metric_name:
                return extract_stats(m, pct)
    return None


def fmt(val, unit="", decimals=1):
    if val is None:
        return "N/A"
    if unit == "GB":
        return f"{val / (1024**3):.{decimals}f} GB"
    if unit == "MB":
        return f"{val / (1024**2):.{decimals}f} MB"
    if unit == "%":
        return f"{val:.{decimals}f}%"
    if unit == "ms":
        return f"{val:.{decimals}f} ms"
    if isinstance(val, float):
        return f"{val:,.{decimals}f}"
    return f"{val:,}"



def generate_report(all_metrics, processes, granularity, period, output_dir, pct=95, cluster_names=None, label=None, collstats=None):
    """Generate one JSON sizing report per cluster."""
    output_dir = Path(output_dir)
    pk = f"p{pct}"

    clusters = {}
    for host_port, data in all_metrics.items():
        cname = data.get("cluster", "unknown")
        clusters.setdefault(cname, {})[host_port] = data

    report_paths = []
    for cluster_name, cluster_metrics in clusters.items():
        cluster_procs = [p for p in processes if f"{p['hostname']}:{p['port']}" in cluster_metrics]

        def _s(stats):
            """Convert stats dict to clean output (None-safe)."""
            if not stats:
                return None
            return {k: round(v, 2) if isinstance(v, float) else v for k, v in stats.items() if v is not None}

        proc_reports = []
        for host_port, data in cluster_metrics.items():
            batches = data["batches"]

            def fm(*names, _b=batches):
                """Find first available metric from name variants."""
                for name in names:
                    r = find_metric(_b, name, pct)
                    if r:
                        return r
                return None

            ops_i, ops_q, ops_u, ops_d, ops_g = fm("OPCOUNTER_INSERT"), fm("OPCOUNTER_QUERY"), fm("OPCOUNTER_UPDATE"), fm("OPCOUNTER_DELETE"), fm("OPCOUNTER_GETMORE")
            tw = {"avg": 0, pk: 0, "max": 0}
            for s in [ops_i, ops_u, ops_d]:
                if s:
                    tw["avg"] += s["avg"]
                    tw[pk] += s[pk]
                    tw["max"] += s["max"]

            mem_resident = fm("MEMORY_RESIDENT")
            data_size = fm("DB_DATA_SIZE_TOTAL")
            storage_size = fm("DB_STORAGE_TOTAL")
            index_size = fm("DB_INDEX_SIZE_TOTAL")
            conns = fm("CONNECTIONS")

            # Compute weighted-average zstd ratio from sampled collections
            cs = (collstats or {}).get(cluster_name, [])
            _zstd_total_raw = 0
            _zstd_total_comp = 0
            for _ce in cs:
                _zc = _ce.get("zstd_compression")
                if _zc and _zc.get("avg_original_bytes") and _zc.get("avg_compressed_bytes"):
                    _n = _zc.get("sampled_docs", 1)
                    _zstd_total_raw += _zc["avg_original_bytes"] * _n
                    _zstd_total_comp += _zc["avg_compressed_bytes"] * _n
            _sampled_zstd_ratio = round(_zstd_total_raw / _zstd_total_comp, 2) if _zstd_total_comp > 0 else None

            proc_reports.append({
                "host": host_port,
                "type": data["type"],
                "cpu": {
                    "user_normalized": _s(fm("PROCESS_CPU_USER")),
                    "steal": None,
                    "kernel": _s(fm("PROCESS_CPU_KERNEL")),
                    "iowait": None,
                },
                "memory": {
                    "system_used": _s(fm("SYSTEM_MEMORY_USED")),
                    "system_free": _s(fm("SYSTEM_MEMORY_FREE")),
                    "resident_mb": _s(mem_resident),
                    "virtual": _s(fm("MEMORY_VIRTUAL")),
                    "swap_used": _s(fm("SWAP_USAGE_USED")),
                },
                "operations_per_sec": {
                    "insert": _s(ops_i),
                    "query": _s(ops_q),
                    "update": _s(ops_u),
                    "delete": _s(ops_d),
                    "getmore": _s(ops_g),
                    "total_writes": {k: round(v, 2) for k, v in tw.items()},
                },
                "disk_io": {
                    "read_iops": _s(fm("DISK_PARTITION_IOPS_READ")),
                    "write_iops": _s(fm("DISK_PARTITION_IOPS_WRITE")),
                    "read_latency_ms": _s(fm("DISK_PARTITION_LATENCY_READ")),
                    "write_latency_ms": _s(fm("DISK_PARTITION_LATENCY_WRITE")),
                    "queue_depth": _s(fm("DISK_QUEUE_DEPTH")),
                },
                "storage": {
                    "data_size_bytes": _s(data_size),
                    "storage_size_bytes": _s(storage_size),
                    "index_size_bytes": _s(fm("DB_INDEX_SIZE_TOTAL")),
                    "compression_ratio": round(data_size["last"] / storage_size["last"], 2) if data_size and storage_size and storage_size.get("last") else None,
                },
                "connections": {
                    "current": _s(conns),
                    "utilization_pct": _s(fm("CONNECTIONS_PERCENT")),
                },
                "query_efficiency": {
                    "keys_scanned_per_returned": _s(fm("QUERY_TARGETING_SCANNED_PER_RETURNED")),
                    "docs_scanned_per_returned": _s(fm("QUERY_TARGETING_SCANNED_OBJECTS_PER_RETURNED")),
                    "scan_and_order_per_sec": _s(fm("OPERATIONS_SCAN_AND_ORDER")),
                    "avg_read_exec_ms": _s(fm("OP_EXECUTION_TIME_READS")),
                    "avg_write_exec_ms": _s(fm("OP_EXECUTION_TIME_WRITES")),
                },
                "wiredtiger": {
                    "cache_used_bytes": _s(fm("CACHE_USED_BYTES")),
                    "cache_dirty_bytes": _s(fm("CACHE_DIRTY_BYTES")),
                    "cache_fill_ratio_pct": _s(fm("CACHE_FILL_RATIO")),
                    "tickets_read": _s(fm("TICKETS_AVAILABLE_READ")),
                    "tickets_write": _s(fm("TICKETS_AVAILABLE_WRITE")),
                },
                "replication": {
                    "lag_sec": _s(fm("OPLOG_SLAVE_LAG_MASTER_TIME")),
                    "oplog_rate_gb_hr": _s(fm("OPLOG_RATE_GB_PER_HOUR")),
                    "page_faults_sec": _s(fm("EXTRA_INFO_PAGE_FAULTS")),
                },
                "sizing_hints": {
                    "total_writes_pct": round(tw[pk], 2),
                    "needs_elastic_clusters": tw[pk] > 35000,
                    "approaching_limits": 20000 < tw[pk] <= 35000,
                    "working_set_gb": round(mem_resident[pk] / 1024, 2) if mem_resident and mem_resident.get(pk) else None,
                    "working_set_compressed_gb": round(mem_resident[pk] / 1024 / (data_size["last"] / storage_size["last"]), 2) if mem_resident and mem_resident.get(pk) and data_size and storage_size and storage_size.get("last") and storage_size["last"] > 0 else None,
                    "index_size_gb": round(index_size["last"] / (1024**3), 2) if index_size and index_size.get("last") else None,
                    "min_ram_gb": round((mem_resident[pk] / 1024 / max(data_size["last"] / storage_size["last"], 1) if data_size and storage_size and storage_size.get("last") and storage_size["last"] > 0 else mem_resident[pk] / 1024) + (index_size["last"] / (1024**3) if index_size and index_size.get("last") else 0), 2) if mem_resident and mem_resident.get(pk) else None,
                    "data_size_gb": round(data_size["last"] / (1024**3), 2) if data_size and data_size.get("last") else None,
                    "estimated_zstd_gb": round(data_size["last"] / (1024**3) / (_sampled_zstd_ratio or 5), 2) if data_size and data_size.get("last") else None,
                    "estimated_zstd_ratio": _sampled_zstd_ratio,
                    "peak_connections": int(conns[pk]) if conns and conns.get(pk) else None,
                },
            })

        # Collection inventory
        cs = (collstats or {}).get(cluster_name, [])
        coll_inventory = []
        for entry in cs:
            m = entry.get("metrics", {})
            coll_inventory.append({
                "namespace": entry["namespace"],
                "reads_per_sec": m.get("reads_per_sec", 0),
                "writes_per_sec": m.get("writes_per_sec", 0),
                "total_ops_per_sec": m.get("total_ops_per_sec", 0),
                "avg_read_latency_us": m.get("avg_read_latency_us", 0),
                "avg_write_latency_us": m.get("avg_write_latency_us", 0),
                "cumulative": entry.get("cumulative"),
                "working_set": entry.get("working_set"),
                "cursor_stats": entry.get("cursor_stats"),
                "io_estimate": entry.get("io_estimate"),
            })

        report = {
            "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "cluster": cluster_name,
            "granularity": granularity,
            "period": period,
            "percentile": pct,
            "topology": [{"host": p["hostname"], "type": p["typeName"], "port": p["port"]} for p in cluster_procs],
            "processes": proc_reports,
            "collection_inventory": coll_inventory,
        }

        suffix = f"-{label}" if label else ""
        cluster_dir = output_dir / cluster_name
        cluster_dir.mkdir(parents=True, exist_ok=True)
        report_path = cluster_dir / f"{cluster_name}{suffix}-sizing-report.json"
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"Report saved: {report_path}")

        # Generate markdown sizing summary
        md_path = cluster_dir / f"{cluster_name}{suffix}-sizing-summary.md"
        _generate_sizing_summary_md(report, md_path, pct)
        print(f"Summary saved: {md_path}")

        report_paths.append(report_path)

    return report_paths


def _generate_sizing_summary_md(report, md_path, pct):
    """Generate a human-readable markdown sizing summary from the JSON report."""
    pk = f"p{pct}"
    pcol = f"P{pct}"
    cluster = report["cluster"]
    procs = report["processes"]
    primary = next((p for p in procs if p["type"] in ("REPLICA_PRIMARY", "SHARD_PRIMARY")), procs[0] if procs else {})
    secondaries = [p for p in procs if p["type"] in ("REPLICA_SECONDARY", "SHARD_SECONDARY")]
    colls = report.get("collection_inventory", [])

    def v(d, key="avg"):
        if not d:
            return "N/A"
        val = d.get(key)
        if val is None:
            return "N/A"
        if isinstance(val, float):
            return f"{val:,.2f}"
        return f"{val:,}"

    def vp(d):
        return v(d, pk)

    lines = []
    a = lines.append

    a(f"# {cluster} -- Sizing Summary")
    a(f"")
    a(f"**Generated:** {report.get('generated', 'N/A')}")
    a(f"**Granularity:** {report.get('granularity')} | **Period:** {report.get('period')} | **Percentile:** {pcol}")
    a(f"**Topology:** {len(procs)} nodes ({sum(1 for p in procs if 'PRIMARY' in p['type'])} primary, {len(secondaries)} secondary)")
    a(f"")

    # --- 1. CPU ---
    cpu = primary.get("cpu", {})
    a(f"## CPU")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    for label, key in [("User", "user_normalized"), ("Kernel", "kernel"), ("IOWait", "iowait")]:
        d = cpu.get(key)
        if d:
            a(f"| {label} | {v(d)}% | {vp(d)}% | {v(d,'max')}% |")
    a(f"")

    # --- 2. Memory ---
    mem = primary.get("memory", {})
    res = mem.get("resident_mb", {})
    a(f"## Memory")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    if mem.get("system_used"):
        total_mb = (mem["system_used"].get("max", 0) + mem.get("system_free", {}).get("max", 0)) / 1024
        a(f"| Total System RAM | | | {total_mb:,.0f} MB |")
    if res:
        a(f"| Resident (Working Set) | {v(res)} MB | {vp(res)} MB | {v(res,'max')} MB |")
    if mem.get("swap_used"):
        a(f"| Swap Used | {v(mem['swap_used'])} MB | {vp(mem['swap_used'])} MB | {v(mem['swap_used'],'max')} MB |")
    a(f"")

    # --- 3. Operations ---
    ops = primary.get("operations_per_sec") or {}
    tw = ops.get("total_writes") or {}
    a(f"## Operations (ops/sec)")
    a(f"")
    a(f"| Operation | Avg | {pcol} | Max |")
    a(f"|-----------|-----|-----|-----|")
    for label, key in [("Insert", "insert"), ("Query", "query"), ("Update", "update"), ("Delete", "delete"), ("GetMore", "getmore")]:
        d = ops.get(key)
        if d:
            a(f"| {label} | {v(d)} | {vp(d)} | {v(d,'max')} |")
    if tw:
        a(f"| **Total Writes** | **{v(tw)}** | **{vp(tw)}** | **{v(tw,'max')}** |")
    total_reads = (ops.get("query") or {}).get(pk, 0) or 0
    total_writes_pct = tw.get(pk, 0) or 0
    a(f"| **Total Ops ({pcol})** | | **{total_reads + total_writes_pct:,.2f}** | |")
    a(f"")

    # --- 4. Disk I/O ---
    disk = primary.get("disk_io", {})
    a(f"## Disk I/O")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    ri = disk.get("read_iops", {})
    wi = disk.get("write_iops", {})
    if ri:
        a(f"| Read IOPS | {v(ri)} | {vp(ri)} | {v(ri,'max')} |")
    if wi:
        a(f"| Write IOPS | {v(wi)} | {vp(wi)} | {v(wi,'max')} |")
    if ri and wi:
        total_avg = (ri.get("avg", 0) or 0) + (wi.get("avg", 0) or 0)
        total_pct = (ri.get(pk, 0) or 0) + (wi.get(pk, 0) or 0)
        total_max = (ri.get("max", 0) or 0) + (wi.get("max", 0) or 0)
        a(f"| **Total IOPS** | **{total_avg:,.2f}** | **{total_pct:,.2f}** | **{total_max:,.2f}** |")
    for label, key in [("Read Latency", "read_latency_ms"), ("Write Latency", "write_latency_ms"), ("Queue Depth", "queue_depth")]:
        d = disk.get(key)
        if d:
            unit = " ms" if "latency" in key else ""
            a(f"| {label} | {v(d)}{unit} | {vp(d)}{unit} | {v(d,'max')}{unit} |")
    a(f"")

    # --- 5. Storage ---
    stor = primary.get("storage") or {}
    a(f"## Storage")
    a(f"")
    data_bytes = (stor.get("data_size_bytes") or {}).get("last", 0) or 0
    storage_bytes = (stor.get("storage_size_bytes") or {}).get("last", 0) or 0
    index_bytes = (stor.get("index_size_bytes") or {}).get("last", 0) or 0
    total_bytes = data_bytes + index_bytes
    comp = stor.get("compression_ratio")

    a(f"| Metric | Value |")
    a(f"|--------|-------|")
    a(f"| Data Size (uncompressed) | {data_bytes / (1024**3):,.2f} GiB |")
    a(f"| Storage Size (on disk) | {storage_bytes / (1024**3):,.2f} GiB |")
    a(f"| Index Size | {index_bytes / (1024**3):,.2f} GiB |")
    a(f"| **Total (Data + Index)** | **{total_bytes / (1024**3):,.2f} GiB** |")
    zr = (primary.get("sizing_hints") or {}).get("estimated_zstd_ratio")
    zr_label = f"{zr}:1 (sampled)" if zr else "~3.5:1 (estimated conservative default -- install zstandard for real per-collection sampling)"
    zr_div = zr or 5
    a(f"| Current Compression Ratio | {comp}:1 |") if comp else None
    a(f"| Est. Zstandard Ratio (DocDB 8.0) | {zr_label} |")
    a(f"| Est. with Zstandard (DocDB 8.0) | ~{data_bytes / (1024**3) / zr_div:,.2f} GiB (data) + {index_bytes / (1024**3):,.2f} GiB (indexes) |")
    a(f"")

    # --- 6. Connections ---
    conns = (primary.get("connections") or {}).get("current", {})
    a(f"## Connections")
    a(f"")
    if conns:
        a(f"| Metric | Avg | {pcol} | Max |")
        a(f"|--------|-----|-----|-----|")
        a(f"| Current Connections | {v(conns)} | {vp(conns)} | {v(conns,'max')} |")
    a(f"")

    # --- 7. Query Efficiency ---
    qe = primary.get("query_efficiency", {})
    a(f"## Query Efficiency")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    for label, key in [("Keys Scanned / Returned", "keys_scanned_per_returned"),
                       ("Docs Scanned / Returned", "docs_scanned_per_returned"),
                       ("Scan & Order ops/sec", "scan_and_order_per_sec"),
                       ("Avg Read Latency", "avg_read_exec_ms"),
                       ("Avg Write Latency", "avg_write_exec_ms")]:
        d = qe.get(key)
        if d:
            unit = " ms" if "latency" in key or "exec" in key else ""
            a(f"| {label} | {v(d)}{unit} | {vp(d)}{unit} | {v(d,'max')}{unit} |")
    a(f"")

    # --- 8. Cache ---
    wt = primary.get("wiredtiger", {})
    a(f"## WiredTiger Cache")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    cu = wt.get("cache_used_bytes")
    if cu:
        a(f"| Cache Used | {(cu.get('avg',0) or 0)/1024/1024:,.1f} MB | {(cu.get(pk,0) or 0)/1024/1024:,.1f} MB | {(cu.get('max',0) or 0)/1024/1024:,.1f} MB |")
    cf = wt.get("cache_fill_ratio_pct")
    if cf:
        a(f"| Cache Fill Ratio | {v(cf)}% | {vp(cf)}% | {v(cf,'max')}% |")
    for label, key in [("Read Tickets", "tickets_read"), ("Write Tickets", "tickets_write")]:
        d = wt.get(key)
        if d:
            a(f"| {label} Available | {v(d)} | {vp(d)} | {v(d,'min')} (min) |")
    a(f"")

    # --- 9. Replication ---
    repl = primary.get("replication", {})
    a(f"## Replication")
    a(f"")
    a(f"| Metric | Avg | {pcol} | Max |")
    a(f"|--------|-----|-----|-----|")
    if repl.get("oplog_rate_gb_hr"):
        a(f"| Oplog Rate | {v(repl['oplog_rate_gb_hr'])} GB/hr | {vp(repl['oplog_rate_gb_hr'])} GB/hr | {v(repl['oplog_rate_gb_hr'],'max')} GB/hr |")
    if repl.get("page_faults_sec"):
        a(f"| Page Faults | {v(repl['page_faults_sec'])}/sec | {vp(repl['page_faults_sec'])}/sec | {v(repl['page_faults_sec'],'max')}/sec |")
    if secondaries:
        lag = secondaries[0].get("replication", {}).get("lag_sec")
        if lag:
            a(f"| Secondary Repl Lag | {v(lag)} sec | {vp(lag)} sec | {v(lag,'max')} sec |")
    a(f"")

    # --- 10. Per-Collection Workload ---
    if colls:
        a(f"## Per-Collection Workload")
        a(f"")
        has_cursor = any(c.get("cursor_stats") for c in colls)
        has_ws = any(c.get("working_set") for c in colls)

        def _n(x):
            # Safe numeric render: thousands-separated int/float, "-" for anything else
            # (missing key, None, empty cursor_stats {} that returned the default).
            # Preserves the "unsampled" signal instead of collapsing to a misleading 0.
            return f"{x:,}" if isinstance(x, (int, float)) and not isinstance(x, bool) else "-"

        if has_cursor:
            a(f"| Namespace | Reads/sec | Writes/sec | Inserts (cum) | Updates (cum) | Deletes (cum) | Searches (cum) | Working Set % |")
            a(f"|-----------|-----------|------------|---------------|---------------|---------------|----------------|---------------|")
            for c in colls:
                m = c.get("metrics", {})
                cur = c.get("cursor_stats", {})
                ws = c.get("working_set", {})
                a(f"| {c['namespace']} | {m.get('reads_per_sec',0)} | {m.get('writes_per_sec',0)} | "
                  f"{_n(cur.get('insert_calls'))} | {_n(cur.get('update_calls'))} | {_n(cur.get('remove_calls'))} | "
                  f"{_n(cur.get('search_calls'))} | {ws.get('data_working_set_pct','-')}% |")
        else:
            a(f"| Namespace | Reads/sec | Writes/sec | Total ops/sec |")
            a(f"|-----------|-----------|------------|---------------|")
            for c in colls:
                m = c.get("metrics", {})
                a(f"| {c['namespace']} | {m.get('reads_per_sec',0)} | {m.get('writes_per_sec',0)} | {m.get('total_ops_per_sec',0)} |")
        a(f"")

        # I/O estimates
        has_io = any(c.get("io_estimate") for c in colls)
        if has_io:
            a(f"### Estimated Daily I/O per Collection")
            a(f"")
            a(f"| Namespace | Avg Doc Size | Read I/O/day | Write I/O/day |")
            a(f"|-----------|-------------|-------------|--------------|")
            for c in colls:
                io = c.get("io_estimate", {})
                if io:
                    a(f"| {c['namespace']} | {io.get('avg_doc_size',0):,} B | {io.get('read_io_per_day',0):,} | {io.get('write_io_per_day',0):,} |")
            a(f"")

    # --- 11. Sizing Hints ---
    hints = primary.get("sizing_hints") or {}
    a(f"## DocumentDB Sizing Recommendation")
    a(f"")
    tw_pct = hints.get("total_writes_pct", 0) or 0
    if tw_pct > 35000:
        a(f"- **CLUSTER TYPE:** Elastic Clusters recommended ({pcol} writes {tw_pct:,.0f}/sec)")
    elif tw_pct > 20000:
        a(f"- **CLUSTER TYPE:** Instance-based ({pcol} writes {tw_pct:,.0f}/sec -- consider Elastic Clusters for growth)")
    else:
        a(f"- **CLUSTER TYPE:** Instance-based ({pcol} writes {tw_pct:,.0f}/sec)")

    ws_gb = hints.get("working_set_gb")
    if ws_gb:
        idx_gb = (index_bytes / (1024**3)) if index_bytes else 0
        compressed_ws = ws_gb / comp if comp and comp > 1 else ws_gb
        a(f"- **WORKING SET:** {ws_gb} GB (Atlas, uncompressed in cache)")
        a(f"  - DocumentDB keeps data compressed in buffer cache (Zstandard)")
        a(f"  - Estimated data in cache: ~{compressed_ws:.2f} GB (after compression)")
        a(f"  - Indexes are NOT compressed in DocumentDB: {idx_gb:.2f} GB")
        a(f"  - Minimum RAM needed: ~{compressed_ws + idx_gb:.2f} GB (compressed data + full indexes)")

    data_gb = hints.get("data_size_gb", 0)
    zstd_gb = hints.get("estimated_zstd_gb", 0)
    if data_gb:
        a(f"- **STORAGE:** {data_gb} GB uncompressed -> ~{zstd_gb} GB with Zstandard (DocumentDB 8.0)")

    peak_conns = hints.get("peak_connections")
    if peak_conns:
        a(f"- **CONNECTIONS:** {pcol} peak {peak_conns} -- verify against instance type limits")

    a(f"- **VERSION:** DocumentDB 8.0 recommended (Zstandard compression, new query planner)")
    a(f"")

    with open(md_path, "w") as f:
        f.write("\n".join(lines))


def generate_sizing_csv(uri, collstats, all_metrics, pct, output_dir):
    """Connect to MongoDB, get collStats + compression, merge with Atlas API ops/sec, output Cost Estimator CSV."""
    try:
        import pymongo
    except ImportError:
        print("WARNING: pymongo not installed -- skipping Cost Estimator CSV. Install with: pip install pymongo")
        return None

    output_dir = Path(output_dir)

    # Aggregate per-collection ops/sec from snapshot data (5s interval during collection)
    # Build per-namespace lookup from collstats (includes working set, cursor stats, cumulative)
    ns_data = {}
    for cname, entries in (collstats or {}).items():
        for entry in entries:
            ns_data[entry.get("namespace", "")] = entry

    print(f"\n=== Generating Cost Estimator CSV ===")
    uri_display = re.sub(r'://([^:]+):([^@]+)@', r'://\1:***@', uri[:80])
    print(f"Connecting to MongoDB: {uri_display}...")

    try:
        client = pymongo.MongoClient(host=uri, appname='atlas-metrics-sizing', serverSelectionTimeoutMS=5000)
        client.admin.command("ping")
    except Exception as e:
        _check_auth_error(e, "cost estimator CSV generation", output_dir)
        print(f"WARNING: Cannot connect to MongoDB: {e} -- skipping CSV")
        return None

    # Try loading compression-review module
    comp_data = {}
    try:
        import importlib.util
        # Search all three known locations rather than only script_dir.parent.
        # A script living in a home directory made parent.parent == /home, so
        # this silently fell through to the collStats ratio fallback -- which
        # underestimates DocumentDB Zstandard and oversizes the target.
        # auto_clone is off: degrade gracefully mid-sizing rather than clone.
        try:
            comp_script = _resolve_documentdb_tool(
                ("performance", "compression-review", "compression-review.py"),
                auto_clone=False)
        except RuntimeError:
            comp_script = None
        if comp_script is not None:
            spec = importlib.util.spec_from_file_location("compression_review", str(comp_script))
            comp_mod = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(comp_mod)
            server_alias = f"sizing-{os.getpid()}"
            app_config = {
                'uri': uri, 'serverAlias': server_alias, 'sampleSize': 1000,
                'compressor': 'zstd-3-dict', 'dictionarySampleSize': 100, 'dictionarySize': 4096,
            }
            print("Running compression analysis (zstd-3-dict)...")
            # compression-review writes its CSV to the current working directory.
            # When the tool is launched from a CWD the running user cannot write
            # (SSM Send-Command, cron, a systemd unit), getData() raises
            # PermissionError and the run silently falls back to collStats
            # ratios. Run it inside a temp directory so the result does not
            # depend on how the operator invoked the tool.
            import glob, tempfile
            _prev_cwd = os.getcwd()
            _tmp_ctx = tempfile.TemporaryDirectory(prefix="atlas_metrics_comp_")
            try:
                os.chdir(_tmp_ctx.name)
                comp_mod.getData(app_config)
                csv_files = sorted(glob.glob(f"{server_alias}-*-compression-review.csv"),
                                   key=os.path.getmtime)
                csv_files = [os.path.abspath(f) for f in csv_files]
            finally:
                os.chdir(_prev_cwd)
            if csv_files:
                with open(csv_files[-1]) as f:
                    lines = f.readlines()
                    header_idx = next((i for i, l in enumerate(lines) if l.startswith("dbName")), None)
                    if header_idx is not None:
                        reader = csv.DictReader(lines[header_idx:])
                        for row in reader:
                            try:
                                key = f"{row['dbName']}.{row['collName']}"
                                comp_data[key] = float(row['projectedCompRatio'])
                            except (KeyError, ValueError):
                                pass
                print(f"  Compression ratios for {len(comp_data)} collections")
            _tmp_ctx.cleanup()
        else:
            print("  compression-review.py not found -- falling back to per-collection collStats ratio (or 3.5x default)")
    except Exception as e:
        print(f"  Compression analysis failed ({e}) -- falling back to per-collection collStats ratio (or 3.5x default)")

    # Build CSV
    csv_dir = output_dir / _URI_CLUSTER_NAME if _URI_CLUSTER_NAME else output_dir
    csv_dir.mkdir(parents=True, exist_ok=True)

    # Load index analysis for working set calculation
    index_report = {"indexes": []}
    idx_path = csv_dir / "index_analysis.json"
    if idx_path.exists():
        try:
            with open(idx_path) as f:
                index_report = json.load(f)
        except Exception:
            pass

    csv_path = csv_dir / "cost-estimator.csv"
    sl_no = 0
    with open(csv_path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow([
            'ID (Ignored)',
            'Database Name',
            'Collection Name',
            'Document Count',
            'Avg Doc Size (Bytes)',
            'Total Indexes',
            'Total Index Size (GiB)',
            'Index Working Set (%)',
            'Data Working Set (%)',
            'Inserts / Day',
            'Updates / Day',
            'Deletes / Day',
            'Reads / Day',
            'Compression Ratio'
        ])

        db_list = client.admin.command("listDatabases", nameOnly=True, filter={"name": {"$nin": ["admin", "config", "local"]}})
        for db_info in db_list.get("databases", []):
            db_name = db_info["name"]
            db = client[db_name]
            for coll_name in db.list_collection_names(filter={"type": "collection"}):
                if coll_name.startswith("system."):
                    continue
                try:
                    stats = db.command("collStats", coll_name)
                except Exception:
                    continue
                if stats.get("count", 0) == 0:
                    continue

                sl_no += 1
                ns = f"{db_name}.{coll_name}"
                doc_count = stats.get("count", 0)
                avg_doc_size = int(stats.get("avgObjSize", 0))
                total_indexes = stats.get("nindexes", 0)
                index_size_gib = stats.get("totalIndexSize", 0) / (1024**3)

                # Compression: real ratio from compression-review, or from collStats, or estimate
                ratio = comp_data.get(ns)
                if not ratio:
                    coll_size = stats.get("size", 0)
                    storage_size = stats.get("storageSize", 0)
                    if coll_size > 0 and storage_size > 0:
                        ratio = round(coll_size / storage_size, 2)
                    else:
                        ratio = 3.5

                # Get per-collection data from collstats (working set, cursor stats, cumulative)
                ns_entry = ns_data.get(ns, {})
                snap_reads = ns_entry.get("metrics", {}).get("reads_per_sec", 0)
                snap_writes = ns_entry.get("metrics", {}).get("writes_per_sec", 0)
                cum = ns_entry.get("cumulative", {})
                ws = ns_entry.get("working_set", {})
                cur = ns_entry.get("cursor_stats", {})

                # Working set from WiredTiger cache access patterns
                data_working_set_pct = ws.get("data_working_set_pct", 10)

                # Index working set: % of indexes actually accessed
                idx_total = sum(1 for i in index_report.get("indexes", []) if i.get("namespace") == ns)
                idx_used = sum(1 for i in index_report.get("indexes", []) if i.get("namespace") == ns and not i.get("unused"))
                index_working_set_pct = round(idx_used / idx_total * 100) if idx_total > 0 else 100

                # Server uptime for cumulative extrapolation
                if not hasattr(generate_sizing_csv, '_uptime') or generate_sizing_csv._uptime_client_id != id(client):
                    try:
                        generate_sizing_csv._uptime = client.admin.command("serverStatus").get("uptime", 1)
                        generate_sizing_csv._uptime_client_id = id(client)
                    except Exception:
                        generate_sizing_csv._uptime = 1
                        generate_sizing_csv._uptime_client_id = id(client)
                up = generate_sizing_csv._uptime

                # Ops/day: use cursor stats for insert/update/delete breakdown (most accurate)
                if cur.get("insert_calls", 0) + cur.get("update_calls", 0) + cur.get("remove_calls", 0) > 0:
                    total_cursor = cur["insert_calls"] + cur["update_calls"] + cur["remove_calls"] + cur.get("search_calls", 0)
                    scale = 86400 / up if up > 0 else 0
                    inserts_day = int(cur["insert_calls"] * scale)
                    updates_day = int(cur["update_calls"] * scale)
                    deletes_day = int(cur["remove_calls"] * scale)
                    reads_day = int(cur.get("search_calls", 0) * scale)
                elif snap_reads > 0 or snap_writes > 0:
                    reads_day = int(snap_reads * 86400)
                    inserts_day = int(snap_writes * 0.5 * 86400)
                    updates_day = int(snap_writes * 0.5 * 86400)
                    deletes_day = 0
                elif cum.get("reads_ops", 0) + cum.get("writes_ops", 0) > 0:
                    scale = 86400 / up if up > 0 else 0
                    reads_day = int(cum.get("reads_ops", 0) * scale)
                    w_day = int(cum.get("writes_ops", 0) * scale)
                    inserts_day = w_day // 2
                    updates_day = w_day // 2
                    deletes_day = 0
                else:
                    reads_day = inserts_day = updates_day = deletes_day = 0

                w.writerow([
                    sl_no, db_name, coll_name, doc_count, avg_doc_size,
                    total_indexes, f"{index_size_gib:.4f}",
                    index_working_set_pct, data_working_set_pct,
                    inserts_day, updates_day, deletes_day, reads_day,
                    f"{ratio:.4f}"
                ])

    client.close()
    print(f"Cost Estimator CSV: {csv_path} ({sl_no} collections)")
    return csv_path


def _get_current_public_ip():
    """Fetch public IP for error messages. Non-fatal if it fails."""
    try:
        import urllib.request
        with urllib.request.urlopen("https://checkip.amazonaws.com", timeout=3) as resp:
            return resp.read().decode().strip()
    except Exception:
        return "<could not determine>"


def _fail(msg, exit_code=1):
    """Print error and exit."""
    print(f"\nERROR: {msg}", file=sys.stderr)
    sys.exit(exit_code)


def _check_auth_error(exc, phase_name, output_path=None):
    """If the exception is an auth-adjacent failure, print rotation guidance and exit.

    Called from all mid-run pymongo operations (post-preflight). Auth failures
    at this stage typically mean the credentials rotated during execution
    (Vault, Secrets Manager auto-rotation, manual password change).

    Preflight validates credentials at start; this handler covers the ~30-min
    window during which credentials can change out from under a running collection.
    """
    err_str = str(exc).lower()
    is_auth_failed = any(s in err_str for s in (
        "authentication fail", "authenticationfailed", "auth failed", "code 18"
    ))
    is_unauthorized = "not authorized" in err_str or "unauthorized" in err_str or "code 13" in err_str
    if not (is_auth_failed or is_unauthorized):
        return  # not auth-related; caller handles

    print()
    print("=" * 66)
    if is_auth_failed:
        print(f"AUTHENTICATION FAILED during {phase_name}")
        print("=" * 66)
        print("Credentials may have rotated since preflight succeeded.")
        print("")
        print("Common causes:")
        print("  - Vault or AWS Secrets Manager auto-rotated the DB user password")
        print("  - Manual password change during the run")
        print("  - Short-lived credential TTL expired (< 45 min is risky)")
        print("")
        print("Recommendation: use a static `atlasAdmin` user for the run window,")
        print("or ensure the credential TTL is longer than 45 minutes.")
    else:
        print(f"PERMISSION REVOKED during {phase_name}")
        print("=" * 66)
        print("The DB user lost required privileges mid-run.")
        print("")
        print("Common causes:")
        print("  - Role was revoked from the DB user during the run")
        print("  - User was reassigned to a lower-privilege role set")
        print("")
        print("Recommendation: restore `atlasAdmin` (or `clusterMonitor` +")
        print("`readAnyDatabase`) to the DB user, then re-run.")

    if output_path:
        print("")
        print(f"Partial output preserved at: {output_path}")
    print("")
    print(f"Original error: {exc}")
    print("=" * 66)
    sys.exit(2)


def _preflight_check(args):
    """Fail fast if any prerequisite is missing or broken.

    Runs BEFORE Atlas API metric collection to avoid wasting 30+ min on a run
    that will fail at the collstats step. Returns an authenticated AtlasClient.

    Checks (in order):
      1. Env vars set (ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID)
      2. Atlas API credentials work (list clusters)
      3. --cluster exists in the project
      4. --uri reachable and authenticated
      5. --uri connects to the same cluster as --cluster
      6. If --compat is set, MongoDB version >= 5.0
    """
    print("Preflight checks:")

    # 1. Env vars
    public_key = os.environ.get("ATLAS_PUBLIC_KEY")
    private_key = os.environ.get("ATLAS_PRIVATE_KEY")
    project_id = os.environ.get("ATLAS_PROJECT_ID")
    if not all([public_key, private_key, project_id]):
        _fail(
            "Missing Atlas API credentials.\n"
            "  Set ATLAS_PUBLIC_KEY, ATLAS_PRIVATE_KEY, ATLAS_PROJECT_ID environment variables.\n"
            "  To create an API key: Atlas UI -> Project -> Access Manager -> API Keys -> Create API Key.\n"
            "  Required role: Project Read Only (minimum)."
        )

    client = AtlasClient(public_key, private_key, project_id)

    # 2. Atlas API credentials valid + list clusters
    print("  [1/5] Verifying Atlas API credentials...")
    try:
        clusters_data = client.get_paginated("/clusters")
    except Exception as e:
        current_ip = _get_current_public_ip()
        msg = f"Atlas API call failed: {e}\n"
        err_str = str(e).lower()
        if "401" in str(e) or "unauthorized" in err_str:
            msg += "  -> HTTP 401 = Unauthorized. Verify ATLAS_PUBLIC_KEY and ATLAS_PRIVATE_KEY are correct."
        elif "403" in str(e) or "forbidden" in err_str:
            msg += f"  -> HTTP 403 = Forbidden. Your IP ({current_ip}) may not be on the API key access list.\n"
            msg += "     Add it: Atlas UI -> Organization -> Access Manager -> API Keys -> edit key -> Access List."
        else:
            msg += f"  -> Verify project ID and API key permissions. Current IP: {current_ip}"
        _fail(msg)

    if not clusters_data:
        _fail(
            f"No clusters found in Atlas project {project_id}.\n"
            f"  Verify ATLAS_PROJECT_ID is the 24-char hex project ID (not the project name)."
        )
    print(f"        OK: Found {len(clusters_data)} cluster(s) in project.")

    # 3. --cluster exists in project
    print(f"  [2/5] Verifying cluster '{args.cluster}' exists in project...")
    cluster_names = [c["name"] for c in clusters_data]
    if args.cluster not in cluster_names:
        # Check if it's a serverless cluster (different API endpoint, unsupported by this tool)
        serverless_names = []
        try:
            serverless_data = client.get_paginated("/serverless")
            if serverless_data:
                serverless_names = [c["name"] for c in serverless_data]
        except Exception:
            pass

        if args.cluster in serverless_names:
            _fail(
                f"Cluster '{args.cluster}' is a SERVERLESS cluster.\n"
                "  atlas_metrics.py does not currently support Serverless clusters.\n"
                "  Serverless uses a different metrics API and pricing model (RPU/WPU) that\n"
                "  requires separate handling not yet implemented here.\n"
                "  For provisioned clusters (M10-M700), this tool works as expected."
            )

        similar = difflib.get_close_matches(args.cluster, cluster_names, n=3, cutoff=0.75)
        # Fall back to substring match if difflib finds nothing (helps with very short queries)
        if not similar:
            similar = [n for n in cluster_names
                       if args.cluster.lower() in n.lower() or n.lower() in args.cluster.lower()]
        msg = f"Cluster '{args.cluster}' not found in project.\n"
        msg += f"  Available provisioned clusters ({len(cluster_names)}):\n"
        for cn in sorted(cluster_names):
            msg += f"    - {cn}\n"
        if serverless_names:
            msg += f"  Serverless clusters (not supported by this tool):\n"
            for cn in sorted(serverless_names):
                msg += f"    - {cn}\n"
        if similar:
            msg += f"  Did you mean: {', '.join(similar)}?"
        _fail(msg)

    # Extract cluster type + tier + region + state for logging and topology cross-check
    target_cluster_data = next(c for c in clusters_data if c["name"] == args.cluster)
    cluster_type = target_cluster_data.get("clusterType", "REPLICASET")
    state_name = target_cluster_data.get("stateName", "UNKNOWN")
    tier = "?"
    region = "?"
    try:
        first_spec = target_cluster_data.get("replicationSpecs", [{}])[0]
        first_rc = first_spec.get("regionConfigs", [{}])[0]
        tier = first_rc.get("electableSpecs", {}).get("instanceSize", "?")
        region = first_rc.get("regionName", "?")
    except (KeyError, IndexError, TypeError):
        pass
    print(f"        OK: Cluster '{args.cluster}' found. Type: {cluster_type}, Tier: {tier}, Region: {region}, State: {state_name}")

    # Fail fast on non-connectable states
    if target_cluster_data.get("paused") is True or state_name == "PAUSED":
        _fail(
            f"Cluster '{args.cluster}' is currently PAUSED.\n"
            "  Resume it in Atlas UI: cluster page -> Resume button.\n"
            "  Or via API: PATCH /clusters/{cluster} with {\"paused\": false}.\n"
            "  Resume takes ~3-4 minutes to transition through UPDATING -> IDLE.\n"
            "  Then re-run this tool."
        )
    if state_name in ("DELETING", "DELETED", "DELETION_FAILED"):
        _fail(f"Cluster '{args.cluster}' is in state {state_name}. Cannot collect metrics.")
    if state_name in ("CREATING", "REPAIRING"):
        print(f"        WARNING: Cluster state is '{state_name}'. Metrics may be incomplete or the run may time out.")

    # 4. --uri reachable and authenticated
    print("  [3/5] Testing --uri connectivity (10s timeout)...")
    try:
        import pymongo
        mc = pymongo.MongoClient(args.uri, serverSelectionTimeoutMS=10000)
        _hello = mc.admin.command("hello")
        mongo_version_full = mc.server_info().get("version", "0")
        # Probe serverStatus permission early. This is what the collstats phase
        # actually needs. `hello` is unauth'd wire protocol so it doesn't validate role.
        # Failing here means the collection phase would fail silently 30 min in.
        try:
            mc.admin.command("serverStatus")
        except Exception as ss_err:
            ss_str = str(ss_err).lower()
            if "not authorized" in ss_str or "unauthorized" in ss_str or "code 13" in ss_str:
                mc.close()
                _fail(
                    "DB user lacks required role for atlas_metrics.py.\n"
                    "  The tool needs `serverStatus` and per-DB `collStats` privileges.\n"
                    "  Grant the DB user one of:\n"
                    "    - `atlasAdmin` (recommended, includes all needed privileges)\n"
                    "    - `clusterMonitor` + `readAnyDatabase` (minimum privileged set)\n"
                    "  Grant in Atlas UI -> Project -> Security -> Database Access -> edit user.\n"
                    f"\n  Actual error: {ss_err}"
                )
            # Non-auth errors: pymongo may raise for cluster-state reasons. Warn but proceed.
            print(f"        WARNING: serverStatus probe returned: {ss_err}")
        mc.close()
    except Exception as e:
        current_ip = _get_current_public_ip()
        err_str = str(e).lower()
        msg = "Cannot connect to cluster via --uri.\n"
        if "authentication" in err_str or "not authorized" in err_str or "auth failed" in err_str:
            msg += "  -> Authentication failed. Verify the username and password in the URI."
        elif "timed out" in err_str or "serverselection" in err_str.replace(" ", "") or "network" in err_str:
            msg += "  -> Network / timeout. Possible causes:\n"
            msg += f"     1. Atlas Network Access list doesn't include your IP ({current_ip}).\n"
            msg += "        Add it: Atlas UI -> Project -> Security -> Network Access.\n"
            msg += "     2. Wrong hostname in URI.\n"
            msg += "     3. Cluster is paused (Atlas UI -> cluster page -> Resume)."
        else:
            msg += f"  -> Unexpected error. Your IP: {current_ip}."
        msg += f"\n\n  Actual error: {e}"
        _fail(msg)

    # Detect URI topology from hello response
    uri_via_mongos = _hello.get("msg") == "isdbgrid"
    uri_serverless = _hello.get("serverless") is True
    if uri_via_mongos:
        topology_desc = "mongos router (sharded)"
    elif uri_serverless:
        topology_desc = "serverless endpoint"
    else:
        rs_role = "primary" if _hello.get("isWritablePrimary") or _hello.get("ismaster") else "secondary"
        topology_desc = f"replica set {rs_role}"
    print(f"        OK: Connected via {topology_desc}. MongoDB version: {mongo_version_full}")

    # 5. --uri connects to same cluster as --cluster + topology cross-check
    print(f"  [4/5] Verifying --uri connects to --cluster '{args.cluster}'...")

    # Topology cross-check: catch wrong URI shape early (before wasting 30 min on wrong collstats)
    if cluster_type == "SHARDED" and not uri_via_mongos:
        _fail(
            f"--cluster '{args.cluster}' is SHARDED, but --uri connects to a single replica set member.\n"
            "  For sharded clusters, --uri must connect to a mongos router.\n"
            "  Get the SRV connection string from Atlas UI -> cluster -> Connect (do NOT set directConnection=true).\n"
            "  Example: mongodb+srv://user:pass@cluster.abc.mongodb.net/"
        )
    if cluster_type == "REPLICASET" and uri_via_mongos:
        _fail(
            f"--cluster '{args.cluster}' is a REPLICASET, but --uri connects to a mongos router (isdbgrid).\n"
            "  The URI may point to a different cluster's mongos, or the cluster name is a mismatch.\n"
            "  Verify the URI in Atlas UI -> cluster -> Connect."
        )

    cluster_map = {}
    for c in clusters_data:
        conn = c.get("connectionStrings", {}).get("standard", "")
        for part in conn.replace("mongodb://", "").split(","):
            host = part.split(":")[0].strip().rstrip("/")
            if host:
                cluster_map[host] = c["name"]
    uri_cluster = _resolve_uri_cluster(args.uri, cluster_names, cluster_map)
    if uri_cluster and uri_cluster != args.cluster:
        _fail(
            f"--uri connects to cluster '{uri_cluster}' but --cluster is '{args.cluster}'.\n"
            "  These must reference the same cluster."
        )
    if not uri_cluster:
        print(f"        WARNING: Could not auto-resolve --uri to a cluster name. Topology cross-check passed. Proceeding with --cluster='{args.cluster}'.")
    else:
        print(f"        OK: --uri resolves to '{uri_cluster}'. Topology match: {cluster_type}.")

    # 6. If --compat, verify MongoDB 5.0+
    if args.compat:
        print("  [5/5] Verifying MongoDB version for --compat (requires 5.0+)...")
        try:
            major = int(mongo_version_full.split(".")[0])
        except (ValueError, AttributeError):
            major = 0
        if major < 5:
            _fail(
                f"--compat requires MongoDB 5.0+. Detected version: {mongo_version_full}.\n"
                "  For older versions, run compat-tool separately with --directory or --file:\n"
                "    python3 compat.py --directory /path/to/source --version 8.0"
            )
        print(f"        OK: MongoDB {mongo_version_full} supports --compat.")
    else:
        print("  [5/5] --compat not requested, skipping MongoDB version check.")

    print("\nPreflight passed. Starting collection...\n")
    return client


# =============================================================================
# SOURCE: EC2 - preflight (v2.1.0 consolidation)
#
# 6-gate preflight for --source ec2. Validates:
#   1. MongoDB URI reachable + auth valid + clusterMonitor role present
#   2. Member hostname discovery + DNS resolution to private IPs
#   3. AWS credentials valid (sts:GetCallerIdentity)
#   4. IAM permissions sufficient (ec2:DescribeInstances + cloudwatch:GetMetricData)
#   5. All discovered instances in same AWS region (single-region scope)
#   6. If --compat requested, MongoDB >= 5.0
#
# Ported from ec2_metrics.py v0.2.0-dev on 2026-07-27 as part of the v2.1.0
# consolidation. All functions carry an _ec2_ prefix to avoid collision with
# the existing atlas preflight (_preflight_check) and its helpers.
# =============================================================================

# EC2 CloudWatch collection constants (used from gate 4 onwards)
_EC2_CLOUDWATCH_DAYS_DEFAULT = 14
_EC2_DELTA_INTERVAL_SECONDS = 60
_EC2_TOTAL_GATES = 6


def _ec2_detect_aws_region(cli_arg):
    """Resolve AWS region from CLI arg, environment, boto3 session config,
    or EC2 IMDSv2 (in that order). Returns the region string or None.

    boto3 usually auto-detects from IMDS on EC2, but that fails silently
    on certain systemd-managed processes (like the SSM agent's shell
    context). Doing the IMDSv2 hop ourselves guarantees resolution when
    the tool is run on an EC2 bastion - the intended customer environment.
    """
    if cli_arg:
        return cli_arg
    for k in ("AWS_REGION", "AWS_DEFAULT_REGION"):
        v = os.environ.get(k)
        if v:
            return v
    try:
        boto3, _, _, _ = _ec2_import_boto3()
        r = boto3.Session().region_name
        if r:
            return r
    except Exception:
        pass
    # IMDSv2 direct (works when running on EC2, fails cleanly otherwise)
    try:
        req = urllib.request.Request(
            "http://169.254.169.254/latest/api/token",
            method="PUT",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            token = resp.read().decode()
        req = urllib.request.Request(
            "http://169.254.169.254/latest/meta-data/placement/region",
            headers={"X-aws-ec2-metadata-token": token},
        )
        with urllib.request.urlopen(req, timeout=2) as resp:
            return resp.read().decode().strip()
    except Exception:
        return None


def _ec2_gate_fail(gate_num, gate_total, message, remediation=None, public_ip=None):
    """Print a preflight failure to stderr in the same shape as ec2_metrics.py.

    Different signature from atlas _fail() - includes gate progress markers
    (N/6) and structured remediation text. Never returns.
    """
    sys.stdout.flush()
    print(f"\n[{gate_num}/{gate_total}] FAIL: {message}", file=sys.stderr)
    if remediation:
        print(f"\nRemediation:\n{remediation}", file=sys.stderr)
    if public_ip:
        print(f"\nYour current public IP: {public_ip}", file=sys.stderr)
    _log(f"PREFLIGHT FAIL [{gate_num}/{gate_total}]: {message}", "error")
    sys.exit(2)


def _ec2_gate_ok(gate_num, gate_total, message):
    print(f"[{gate_num}/{gate_total}] OK: {message}")
    _log(f"PREFLIGHT OK [{gate_num}/{gate_total}]: {message}")


def _ec2_import_pymongo():
    """Lazy import so `--version` works without dependencies installed."""
    try:
        import pymongo  # noqa: F401
        from pymongo import MongoClient
        from pymongo.errors import (
            ConnectionFailure,
            OperationFailure,
            ServerSelectionTimeoutError,
        )
        return MongoClient, ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
    except ImportError as e:
        print(f"FATAL: pymongo not installed ({e})", file=sys.stderr)
        print("Install with: pip install -r requirements-atlas-metrics.txt", file=sys.stderr)
        sys.exit(3)


def _ec2_import_boto3():
    """Lazy import so atlas users don't need boto3 installed."""
    try:
        import boto3
        from botocore.exceptions import (
            ClientError,
            NoCredentialsError,
            PartialCredentialsError,
        )
        return boto3, ClientError, NoCredentialsError, PartialCredentialsError
    except ImportError as e:
        print(f"FATAL: boto3 not installed ({e})", file=sys.stderr)
        print("Install boto3 for --source ec2: pip install boto3", file=sys.stderr)
        sys.exit(3)


def _ec2_connect_mongo(uri, timeout_ms=10000):
    """Open a pymongo MongoClient with a short server-selection timeout.

    Returns the client on success. On failure returns None and lets the
    caller decide how to fail (usually a preflight gate).
    """
    MongoClient, ConnectionFailure, OperationFailure, ServerSelectionTimeoutError = _ec2_import_pymongo()
    try:
        client = MongoClient(uri, serverSelectionTimeoutMS=timeout_ms, appname=f"atlas_metrics/{__version__} (source=ec2)")
        client.admin.command("ping")
        return client
    except (ConnectionFailure, ServerSelectionTimeoutError, OperationFailure) as e:
        _log(f"mongo connect failed: {type(e).__name__}: {e}", "error")
        return None


def _ec2_detect_topology(client):
    """Detect topology: 'sharded' | 'replicaset' | 'standalone'.

    Uses `hello` (server info) + `isdbgrid` marker on mongos.
    """
    try:
        hello = client.admin.command("hello")
    except Exception as e:
        _log(f"detect_topology: hello failed: {e}", "error")
        return None
    msg = hello.get("msg", "")
    if msg == "isdbgrid":
        return "sharded"
    if hello.get("setName"):
        return "replicaset"
    return "standalone"


def _ec2_server_version(client):
    """Return the semantic MongoDB server version as a tuple of ints, or None."""
    try:
        bi = client.server_info()
        parts = bi.get("versionArray") or []
        if parts and len(parts) >= 3:
            return tuple(int(x) for x in parts[:3])
    except Exception as e:
        _log(f"server_version failed: {e}", "error")
    return None


def _ec2_member_hostnames(client, topology):
    """Return the list of member hostnames.

    - replicaset: from rs.status().members[].name (host:port)
    - sharded:   from sh.status() shard connection strings
    - standalone: from the URI itself (single host)
    """
    hosts = []
    if topology == "replicaset":
        try:
            status = client.admin.command("replSetGetStatus")
            for m in status.get("members", []):
                name = m.get("name", "")
                if name:
                    hosts.append(name.split(":")[0])
        except Exception as e:
            _log(f"replSetGetStatus failed: {e}", "error")
    elif topology == "sharded":
        # sh.status() shape:
        #   config db -> shards collection: { _id: "rs0", host: "rs0/n1:27017,n2:27017,..." }
        try:
            shards_cursor = client["config"]["shards"].find({}, {"host": 1})
            for shard in shards_cursor:
                host_str = shard.get("host", "")
                # Strip the "rs-name/" prefix if present
                if "/" in host_str:
                    host_str = host_str.split("/", 1)[1]
                for hp in host_str.split(","):
                    hp = hp.strip()
                    if hp:
                        hosts.append(hp.split(":")[0])
        except Exception as e:
            _log(f"config.shards read failed: {e}", "error")
    elif topology == "standalone":
        try:
            addr = client.address
            if addr and len(addr) >= 1:
                hosts.append(addr[0])
        except Exception:
            pass
    # De-dup preserving order
    seen = set()
    result = []
    for h in hosts:
        if h not in seen:
            seen.add(h)
            result.append(h)
    return result


def _ec2_resolve_to_private_ips(hostnames):
    """Resolve each hostname to an IP address.

    Returns list of {hostname, ip, resolved} tuples.
    """
    resolved = []
    for h in hostnames:
        try:
            ip = socket.gethostbyname(h)
            resolved.append({"hostname": h, "ip": ip, "resolved": True})
        except socket.gaierror as e:
            _log(f"DNS resolution failed for {h}: {e}", "warning")
            resolved.append({"hostname": h, "ip": None, "resolved": False})
    return resolved


def _ec2_discover_ec2_instances(ec2_client, private_ips):
    """Given a list of private IPs, return the EC2 instances they belong to.

    Uses ec2:DescribeInstances with a filter on `private-ip-address`.
    """
    boto3, ClientError, NoCredentialsError, PartialCredentialsError = _ec2_import_boto3()
    ips = [ip for ip in private_ips if ip]
    if not ips:
        return []
    try:
        resp = ec2_client.describe_instances(
            Filters=[{"Name": "private-ip-address", "Values": ips}]
        )
    except ClientError as e:
        _log(f"DescribeInstances failed: {e}", "error")
        return []

    instances = []
    for reservation in resp.get("Reservations", []):
        for inst in reservation.get("Instances", []):
            volumes = []
            for bdm in inst.get("BlockDeviceMappings", []):
                ebs = bdm.get("Ebs", {})
                if ebs.get("VolumeId"):
                    volumes.append({
                        "volume_id": ebs["VolumeId"],
                        "device": bdm.get("DeviceName"),
                    })
            instances.append({
                "instance_id": inst["InstanceId"],
                "private_ip": inst.get("PrivateIpAddress"),
                "region": inst["Placement"]["AvailabilityZone"][:-1] if inst.get("Placement") else None,
                "az": inst.get("Placement", {}).get("AvailabilityZone"),
                "instance_type": inst.get("InstanceType"),
                "state": inst.get("State", {}).get("Name"),
                "attached_volumes": volumes,
            })
    return instances


def _ec2_preflight_gate_1(uri):
    """Gate 1: URI reachable + auth valid + topology detected + role sufficient."""
    with _Timer("ec2_preflight_gate_1"):
        MongoClient, ConnectionFailure, OperationFailure, ServerSelectionTimeoutError = _ec2_import_pymongo()
        client = _ec2_connect_mongo(uri)
        if client is None:
            public_ip = _get_current_public_ip()
            _ec2_gate_fail(
                1, _EC2_TOTAL_GATES,
                "Could not connect to MongoDB with the provided --uri (see runtime.log for details).",
                remediation=(
                    "Verify:\n"
                    "  - URI is well-formed (mongodb:// or mongodb+srv://)\n"
                    "  - Credentials are correct and URL-encoded if they contain special chars\n"
                    "  - Network path from this host to the MongoDB nodes is open\n"
                    "  - MongoDB user has at minimum the 'clusterMonitor' role"
                ),
                public_ip=public_ip,
            )
        topology = _ec2_detect_topology(client)
        if topology is None:
            _ec2_gate_fail(1, _EC2_TOTAL_GATES,
                "Connected to MongoDB but could not detect topology (hello command failed).")
        version = _ec2_server_version(client)
        if version is None:
            _ec2_gate_fail(1, _EC2_TOTAL_GATES, "Could not read MongoDB server version.")

        # Role probe: run serverStatus. Requires clusterMonitor.
        try:
            client.admin.command("serverStatus")
        except OperationFailure as e:
            code = getattr(e, "code", None)
            code_name = getattr(e, "code_name", None)
            details = getattr(e, "details", {}) or {}
            _log(
                f"serverStatus probe failed: code={code} code_name={code_name} details={details}",
                "error",
            )
            if code == 13 or (code_name and "unauthorized" in code_name.lower()):
                _ec2_gate_fail(
                    1, _EC2_TOTAL_GATES,
                    "MongoDB user connected successfully but lacks the "
                    "'clusterMonitor' role required to collect metrics.",
                    remediation=(
                        "Grant the required roles in mongosh (run as an admin user):\n"
                        "\n"
                        "  use admin\n"
                        "  db.grantRolesToUser(\"<your-user>\", [\n"
                        "    { role: \"clusterMonitor\",  db: \"admin\" },\n"
                        "    { role: \"readAnyDatabase\", db: \"admin\" }\n"
                        "  ])\n"
                        "\n"
                        "The tool runs serverStatus, collStats, $indexStats, and\n"
                        "rs.status() / sh.status() during collection - all require\n"
                        "clusterMonitor privilege at minimum. readAnyDatabase is\n"
                        "additionally required for the compression sampling phase."
                    ),
                )
            _ec2_gate_fail(
                1, _EC2_TOTAL_GATES,
                f"MongoDB serverStatus probe failed with unexpected error "
                f"(code {code}, code_name {code_name}). See runtime.log.",
            )
        except Exception as e:
            _log(f"serverStatus probe raised {type(e).__name__}: {e}", "error")
            _ec2_gate_fail(
                1, _EC2_TOTAL_GATES,
                f"MongoDB serverStatus probe failed: {type(e).__name__}: {e}",
            )

        version_str = ".".join(str(x) for x in version)
        _ec2_gate_ok(1, _EC2_TOTAL_GATES,
            f"MongoDB {version_str} reachable - topology: {topology}, "
            f"clusterMonitor role confirmed")
        return client, topology, version


def _ec2_preflight_gate_2(client, topology):
    """Gate 2: Instance discovery via MongoDB topology -> hostnames -> private IPs."""
    with _Timer("ec2_preflight_gate_2"):
        hostnames = _ec2_member_hostnames(client, topology)
        if not hostnames:
            _ec2_gate_fail(
                2, _EC2_TOTAL_GATES,
                f"Could not extract member hostnames from topology '{topology}'.",
                remediation=(
                    "For replica sets, ensure the user has 'clusterMonitor' role to run rs.status().\n"
                    "For sharded clusters, ensure the URI connects to a mongos (not directly to a shard)."
                ),
            )
        _log(f"member hostnames: {hostnames}")
        resolved = _ec2_resolve_to_private_ips(hostnames)
        unresolved = [r["hostname"] for r in resolved if not r["resolved"]]
        if unresolved:
            _ec2_gate_fail(
                2, _EC2_TOTAL_GATES,
                f"DNS resolution failed for these MongoDB member hostnames: {unresolved}",
                remediation=(
                    "The tool needs to resolve each member hostname to a private IP so it\n"
                    "can look up the corresponding EC2 instance. Run this tool from a host\n"
                    "inside the same VPC as your MongoDB cluster, or with a DNS resolver\n"
                    "that can see the private hosted zone."
                ),
            )
        private_ips = [r["ip"] for r in resolved]
        _ec2_gate_ok(2, _EC2_TOTAL_GATES,
            f"Resolved {len(private_ips)} MongoDB member(s): {', '.join(private_ips)}")
        return hostnames, private_ips


def _ec2_preflight_gate_3(aws_region):
    """Gate 3: AWS credentials valid - sts:GetCallerIdentity."""
    with _Timer("ec2_preflight_gate_3"):
        boto3, ClientError, NoCredentialsError, PartialCredentialsError = _ec2_import_boto3()
        try:
            sts = boto3.client("sts", region_name=aws_region) if aws_region \
                else boto3.client("sts")
            identity = sts.get_caller_identity()
        except NoCredentialsError:
            _ec2_gate_fail(
                3, _EC2_TOTAL_GATES,
                "AWS credentials not found in the standard credential chain.",
                remediation=(
                    "Set credentials via one of:\n"
                    "  - Env vars: AWS_ACCESS_KEY_ID + AWS_SECRET_ACCESS_KEY (+ AWS_SESSION_TOKEN if using STS)\n"
                    "  - AWS profile: export AWS_PROFILE=<profile-name>\n"
                    "  - IAM instance profile (if running on an EC2 bastion)"
                ),
            )
        except PartialCredentialsError as e:
            _ec2_gate_fail(3, _EC2_TOTAL_GATES, f"AWS credentials are incomplete: {e}")
        except ClientError as e:
            _ec2_gate_fail(3, _EC2_TOTAL_GATES,
                  f"AWS credential validation failed: {e.response.get('Error', {}).get('Code', 'Unknown')}")
        account = identity.get("Account", "unknown")
        arn = identity.get("Arn", "unknown")
        _ec2_gate_ok(3, _EC2_TOTAL_GATES,
            f"AWS credentials valid - Account {account}, Principal {arn.split('/')[-1] if '/' in arn else arn}")
        return account, arn


def _ec2_preflight_gate_4(aws_region, instance_ids):
    """Gate 4: AWS credentials have required permissions.

    Validates by making real API calls to CloudWatch and EC2, not by IAM
    policy simulation. Any AccessDenied surfaces immediately.
    """
    with _Timer("ec2_preflight_gate_4"):
        boto3, ClientError, _, _ = _ec2_import_boto3()

        # Test 1: ec2:DescribeInstances on the specific instances we discovered.
        ec2 = boto3.client("ec2", region_name=aws_region) if aws_region \
            else boto3.client("ec2")
        try:
            ec2.describe_instances(InstanceIds=instance_ids)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code == "UnauthorizedOperation":
                _ec2_gate_fail(
                    4, _EC2_TOTAL_GATES,
                    "ec2:DescribeInstances denied by IAM policy.",
                    remediation=(
                        "Grant at least these actions: ec2:DescribeInstances, ec2:DescribeVolumes,\n"
                        "ec2:DescribeNetworkInterfaces, cloudwatch:GetMetricData."
                    ),
                )
            elif code == "InvalidInstanceID.NotFound":
                _ec2_gate_fail(
                    4, _EC2_TOTAL_GATES,
                    "Discovered EC2 instances are not visible in this AWS account/region.",
                    remediation=(
                        "Verify the AWS credentials belong to the account that owns the\n"
                        "MongoDB instances, and that --aws-region matches the region where\n"
                        "the instances live."
                    ),
                )
            else:
                _ec2_gate_fail(4, _EC2_TOTAL_GATES,
                      f"ec2:DescribeInstances failed with error code: {code}")

        # Test 2: cloudwatch:GetMetricData with a minimal query
        cw = boto3.client("cloudwatch", region_name=aws_region) if aws_region \
            else boto3.client("cloudwatch")
        end = datetime.now(timezone.utc)
        start = end - timedelta(minutes=5)
        try:
            cw.get_metric_data(
                MetricDataQueries=[{
                    "Id": "probe",
                    "MetricStat": {
                        "Metric": {
                            "Namespace": "AWS/EC2",
                            "MetricName": "CPUUtilization",
                            "Dimensions": [{"Name": "InstanceId", "Value": instance_ids[0]}],
                        },
                        "Period": 300,
                        "Stat": "Average",
                    },
                    "ReturnData": True,
                }],
                StartTime=start,
                EndTime=end,
            )
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "Unknown")
            if code in ("AccessDenied", "AccessDeniedException"):
                _ec2_gate_fail(
                    4, _EC2_TOTAL_GATES,
                    "cloudwatch:GetMetricData denied by IAM policy.",
                    remediation=(
                        "CloudWatch access is a hard requirement of --source ec2. Attach a policy\n"
                        "including cloudwatch:GetMetricData and cloudwatch:GetMetricStatistics."
                    ),
                )
            else:
                _ec2_gate_fail(4, _EC2_TOTAL_GATES,
                      f"cloudwatch:GetMetricData probe failed with error code: {code}")

        _ec2_gate_ok(4, _EC2_TOTAL_GATES,
            "AWS credentials have ec2:DescribeInstances + cloudwatch:GetMetricData permissions")
        return ec2, cw


def _ec2_preflight_gate_5(instances):
    """Gate 5: All discovered instances in same AWS region.

    Multi-region clusters are out of v1 scope. Fail loudly so we don't
    produce a report against partial data.
    """
    with _Timer("ec2_preflight_gate_5"):
        regions = {i["region"] for i in instances if i["region"]}
        if len(regions) > 1:
            _ec2_gate_fail(
                5, _EC2_TOTAL_GATES,
                f"MongoDB nodes span multiple AWS regions: {sorted(regions)}",
                remediation=(
                    "v1 of --source ec2 supports single-region clusters only. Multi-region\n"
                    "sharded clusters are a v2 scope item. For now, run the tool once per\n"
                    "region if you need coverage of all shards."
                ),
            )
        states = {i["state"] for i in instances}
        stopped = [i["instance_id"] for i in instances if i["state"] != "running"]
        if stopped:
            _log(f"non-running instances discovered: {stopped}", "warning")
            print(f"[5/{_EC2_TOTAL_GATES}] WARN: These instances are not in 'running' state: {stopped}",
                  file=sys.stderr)
        region = list(regions)[0] if regions else "unknown"
        _ec2_gate_ok(5, _EC2_TOTAL_GATES,
            f"All {len(instances)} instance(s) in region {region}, states: {sorted(states)}")
        return region


def _ec2_preflight_gate_6(server_version, do_compat):
    """Gate 6: If --compat requested, verify MongoDB >= 5.0."""
    with _Timer("ec2_preflight_gate_6"):
        if not do_compat:
            _ec2_gate_ok(6, _EC2_TOTAL_GATES, "compat scan not requested - skipped")
            return
        if server_version is None or server_version[0] < 5:
            version_str = ".".join(str(x) for x in server_version) if server_version else "unknown"
            _ec2_gate_fail(
                6, _EC2_TOTAL_GATES,
                f"--compat requires MongoDB 5.0+ (detected {version_str}).",
                remediation=(
                    "The amazon-documentdb-tools compat runner requires MongoDB 5.0+ to\n"
                    "connect. For older versions, run the compat check separately using\n"
                    "source code or log file scanning. Remove --compat to proceed without\n"
                    "the automatic compatibility scan."
                ),
            )
        _ec2_gate_ok(6, _EC2_TOTAL_GATES,
            f"MongoDB {'.'.join(str(x) for x in server_version)} - compat scan eligible")


def _ec2_preflight(args):
    """Orchestrator for the 6-gate EC2 preflight sequence.

    Returns a context dict with all handles needed for the subsequent
    collection phase: client, topology, version, hostnames, private_ips,
    instances, aws_region, ec2_client, cw_client, aws_account, aws_arn.

    Any failure exits the process with code 2 (via _ec2_gate_fail).
    """
    print(f"\n{'='*60}")
    print(f"Preflight: --source ec2 (6 gates)")
    print(f"{'='*60}\n")

    # Determine output dir early for logging
    out = args.output or f"atlas-metrics-{datetime.now().strftime('%Y%m%d-%H%M')}"
    Path(out).mkdir(parents=True, exist_ok=True)
    _init_log(out)
    _log(f"atlas_metrics.py {__version__} --source ec2 preflight starting")

    # Gate 1
    client, topology, version = _ec2_preflight_gate_1(args.uri)

    # Gate 2
    hostnames, private_ips = _ec2_preflight_gate_2(client, topology)

    # Resolve AWS region before gates 3/4
    aws_region = _ec2_detect_aws_region(getattr(args, "aws_region", None))
    if not aws_region:
        _ec2_gate_fail(
            3, _EC2_TOTAL_GATES,
            "Could not determine AWS region.",
            remediation=(
                "Pass --aws-region explicitly, set AWS_REGION or AWS_DEFAULT_REGION in the\n"
                "environment, or configure a default region via `aws configure`. When running\n"
                "on an EC2 instance, region is auto-detected from IMDSv2 - verify IMDS is\n"
                "reachable (HopLimit >= 2 in the instance metadata options)."
            ),
        )

    # Gate 3
    account, arn = _ec2_preflight_gate_3(aws_region)

    # Discover instances BEFORE gate 4 (gate 4 needs instance_ids to probe permissions)
    boto3, _, _, _ = _ec2_import_boto3()
    ec2_client_probe = boto3.client("ec2", region_name=aws_region)
    instances = _ec2_discover_ec2_instances(ec2_client_probe, private_ips)
    if not instances:
        _ec2_gate_fail(
            4, _EC2_TOTAL_GATES,
            f"No EC2 instances found for private IPs: {private_ips}",
            remediation=(
                "Verify:\n"
                "  - The MongoDB nodes are running on EC2 (not on-premises or a different cloud)\n"
                "  - The AWS account for these credentials owns those EC2 instances\n"
                "  - --aws-region matches the region where the instances live"
            ),
        )
    instance_ids = [i["instance_id"] for i in instances]

    # Gate 4
    ec2_client, cw_client = _ec2_preflight_gate_4(aws_region, instance_ids)

    # Gate 5
    region = _ec2_preflight_gate_5(instances)

    # Gate 6
    _ec2_preflight_gate_6(version, args.compat)

    print(f"\n{'='*60}")
    print(f"Preflight: all 6 gates passed")
    print(f"{'='*60}\n")

    return {
        "client": client,
        "topology": topology,
        "version": version,
        "hostnames": hostnames,
        "private_ips": private_ips,
        "instances": instances,
        "aws_region": aws_region,
        "ec2_client": ec2_client,
        "cw_client": cw_client,
        "aws_account": account,
        "aws_arn": arn,
        "output_dir": out,
    }


# =============================================================================
# SOURCE: EC2 - collection pipeline (v2.1.0 consolidation, step 5)
#
# Post-preflight collection functions for --source ec2. Ported from
# ec2_metrics.py v0.2.0-dev. All prefixed with _ec2_ to avoid namespace
# collision with atlas equivalents.
# =============================================================================

# EC2 CloudWatch metric definitions
_EC2_METRICS_LIST = [
    "CPUUtilization",
    "NetworkIn",
    "NetworkOut",
    "NetworkPacketsIn",
    "NetworkPacketsOut",
]

_EBS_METRICS_LIST = [
    "VolumeReadOps",
    "VolumeWriteOps",
    "VolumeReadBytes",
    "VolumeWriteBytes",
    "VolumeTotalReadTime",
    "VolumeTotalWriteTime",
    "VolumeQueueLength",
    "VolumeThroughputPercentage",
    "VolumeIdleTime",
    "BurstBalance",
]

# DocumentDB 8.0 unsupported operators - used for profiler cross-reference.
_EC2_DOCDB_UNSUPPORTED_OPERATORS = {
    "$facet", "$lookup", "$graphLookup", "$where", "$expr", "$function",
    "$accumulator", "$merge",
    "$dateFromString", "$dateFromParts",
    "$regexFind", "$regexFindAll",
    "$mergeObjects", "$objectToArray", "$arrayToObject",
    "$mapReduce",
    "$out",
    "$search",
    "$bucket", "$bucketAuto",
    "$documents",
    "$geoNear",
}


def _ec2_collect_mongo_sampling(client, samples=1):
    """Take N delta snapshots of serverStatus, _EC2_DELTA_INTERVAL_SECONDS apart.

    Returns dict with per-sample and aggregate opcounter/network rates.
    samples=1 -> one delta (60s wall-clock). samples=3 -> three deltas for variance.
    """
    intervals = max(1, samples)
    _log(f"collect_mongo_sampling: taking {intervals} delta(s) at "
         f"{_EC2_DELTA_INTERVAL_SECONDS}s apart each ({intervals * _EC2_DELTA_INTERVAL_SECONDS}s total)")

    def _snapshot():
        ss = client.admin.command("serverStatus")
        return {
            "ts": datetime.now(timezone.utc).isoformat(),
            "uptime": ss.get("uptime", 0),
            "opcounters": dict(ss.get("opcounters", {})),
            "network": {
                "bytesIn": ss.get("network", {}).get("bytesIn", 0),
                "bytesOut": ss.get("network", {}).get("bytesOut", 0),
                "numRequests": ss.get("network", {}).get("numRequests", 0),
            },
            "connections": {
                "current": ss.get("connections", {}).get("current", 0),
                "available": ss.get("connections", {}).get("available", 0),
                "totalCreated": ss.get("connections", {}).get("totalCreated", 0),
            },
            "wt_cache": {
                "bytes_currently_in_cache": ss.get("wiredTiger", {}).get("cache", {}).get("bytes currently in the cache", 0),
                "maximum_bytes_configured": ss.get("wiredTiger", {}).get("cache", {}).get("maximum bytes configured", 0),
                "tracked_dirty_bytes": ss.get("wiredTiger", {}).get("cache", {}).get("tracked dirty bytes in the cache", 0),
                "bytes_read_into_cache": ss.get("wiredTiger", {}).get("cache", {}).get("bytes read into cache", 0),
                "bytes_written_from_cache": ss.get("wiredTiger", {}).get("cache", {}).get("bytes written from cache", 0),
            },
            "mem": {
                "resident_mb": ss.get("mem", {}).get("resident", 0),
                "virtual_mb": ss.get("mem", {}).get("virtual", 0),
            },
        }

    def _delta(snap_a, snap_b, interval):
        d = {}
        for op in ("insert", "query", "update", "delete", "getmore", "command"):
            a = snap_a["opcounters"].get(op, 0)
            b = snap_b["opcounters"].get(op, 0)
            d[f"{op}_per_sec"] = round(max(0, b - a) / interval, 2)
        d["total_ops_per_sec"] = round(sum(d[f"{op}_per_sec"] for op in
                                           ("insert", "query", "update", "delete", "getmore", "command")), 2)
        for k in ("bytesIn", "bytesOut", "numRequests"):
            a = snap_a["network"].get(k, 0)
            b = snap_b["network"].get(k, 0)
            d[f"network_{k}_per_sec"] = round(max(0, b - a) / interval, 2)
        return d

    snapshots = [_snapshot()]
    deltas = []
    for i in range(intervals):
        with _Timer(f"mongo_sample delta {i+1}/{intervals}"):
            time.sleep(_EC2_DELTA_INTERVAL_SECONDS)
            snap = _snapshot()
            snapshots.append(snap)
            deltas.append(_delta(snapshots[-2], snapshots[-1], _EC2_DELTA_INTERVAL_SECONDS))

    aggregate = {}
    if deltas:
        for key in deltas[0].keys():
            values = sorted(d[key] for d in deltas)
            aggregate[key] = {
                "min": values[0],
                "median": values[len(values) // 2],
                "max": values[-1],
            }

    return {
        "delta_interval_seconds": _EC2_DELTA_INTERVAL_SECONDS,
        "samples": intervals,
        "snapshots": snapshots,
        "deltas": deltas,
        "aggregate": aggregate,
    }


def _ec2_pull_cloudwatch(cw_client, instances, days=None):
    """Pull `days` of CloudWatch metrics for each instance + attached EBS volumes.

    Uses batched GetMetricData with pagination. 5-min granularity matches
    the CloudWatch retention window for detailed metrics.
    """
    if days is None:
        days = _EC2_CLOUDWATCH_DAYS_DEFAULT
    end = datetime.now(timezone.utc).replace(microsecond=0)
    start = end - timedelta(days=days)
    period = 300  # 5-min

    _log(f"CloudWatch: pulling {days} days at Period={period}s "
         f"({start.isoformat()} to {end.isoformat()})")

    result = {
        "period_seconds": period,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "instances": {},
    }

    queries = []
    query_index = {}

    def _add_q(cwid, namespace, metric_name, dims):
        queries.append({
            "Id": cwid,
            "MetricStat": {
                "Metric": {
                    "Namespace": namespace,
                    "MetricName": metric_name,
                    "Dimensions": dims,
                },
                "Period": period,
                "Stat": "Average",
            },
            "ReturnData": True,
        })

    for inst in instances:
        iid = inst["instance_id"]
        result["instances"][iid] = {"ec2_metrics": {}, "ebs_metrics": {}}
        for m in _EC2_METRICS_LIST:
            cwid = f"i{len(queries):04d}"
            query_index[cwid] = (iid, "ec2", m, None)
            _add_q(cwid, "AWS/EC2", m, [{"Name": "InstanceId", "Value": iid}])
        for vol in inst.get("attached_volumes", []):
            vid = vol["volume_id"]
            for m in _EBS_METRICS_LIST:
                cwid = f"i{len(queries):04d}"
                query_index[cwid] = (iid, "ebs", m, vid)
                _add_q(cwid, "AWS/EBS", m, [{"Name": "VolumeId", "Value": vid}])

    _log(f"CloudWatch: {len(queries)} MetricDataQueries queued")

    BATCH_SIZE = 500
    all_results = []
    for i in range(0, len(queries), BATCH_SIZE):
        batch = queries[i:i + BATCH_SIZE]
        next_token = None
        while True:
            kwargs = {
                "MetricDataQueries": batch,
                "StartTime": start,
                "EndTime": end,
                "ScanBy": "TimestampAscending",
            }
            if next_token:
                kwargs["NextToken"] = next_token
            resp = cw_client.get_metric_data(**kwargs)
            all_results.extend(resp.get("MetricDataResults", []))
            next_token = resp.get("NextToken")
            if not next_token:
                break
        _log(f"CloudWatch: batch {i // BATCH_SIZE + 1} done ({len(batch)} queries)")

    for mdr in all_results:
        cwid = mdr["Id"]
        if cwid not in query_index:
            continue
        iid, kind, metric_name, resource_id = query_index[cwid]
        timestamps = [t.isoformat() if hasattr(t, "isoformat") else str(t) for t in mdr.get("Timestamps", [])]
        values = mdr.get("Values", [])
        series = {"timestamps": timestamps, "values": values, "count": len(values)}
        if kind == "ec2":
            result["instances"][iid]["ec2_metrics"][metric_name] = series
        elif kind == "ebs":
            result["instances"][iid]["ebs_metrics"].setdefault(resource_id, {})[metric_name] = series

    total_points = sum(
        s["count"]
        for inst_d in result["instances"].values()
        for s in inst_d["ec2_metrics"].values()
    ) + sum(
        s["count"]
        for inst_d in result["instances"].values()
        for vol_d in inst_d["ebs_metrics"].values()
        for s in vol_d.values()
    )
    _log(f"CloudWatch: pulled {total_points:,} data points across "
         f"{len(instances)} instance(s) over {days} days")
    return result


def _ec2_collect_profiler_data(client):
    """Read system.profile from any DB with profiler enabled, cross-reference
    query operators against DocumentDB unsupported operator list.

    NEVER enables profiler - read-only. Returns guidance if profiler not enabled.
    """
    with _Timer("collect_profiler_data"):
        result = {
            "databases_with_profiler_enabled": [],
            "databases_without_profiler": [],
            "sample_window": None,
            "top_slow_query_shapes_by_count": [],
            "top_slow_query_shapes_by_total_duration": [],
            "collections_by_query_volume": [],
            "unsupported_operator_usage": [],
            "guidance": None,
        }

        try:
            db_names = [d for d in client.list_database_names()
                        if d not in ("admin", "local", "config")]
        except Exception as e:
            _log(f"list_database_names failed: {e}", "warning")
            return result

        enabled_dbs = []
        for dbn in db_names:
            try:
                status = client[dbn].command("profile", -1)
                level = status.get("was", 0)
                slowms = status.get("slowms", 100)
                if level > 0:
                    enabled_dbs.append({"db": dbn, "level": level, "slowms": slowms})
                    result["databases_with_profiler_enabled"].append(
                        {"db": dbn, "level": level, "slowms": slowms})
                else:
                    result["databases_without_profiler"].append(dbn)
            except Exception as e:
                _log(f"profile status check failed for {dbn}: {e}", "warning")
                result["databases_without_profiler"].append(dbn)

        if not enabled_dbs:
            result["guidance"] = (
                "Profiler not enabled on any user database. For query-pattern "
                "analysis and DocumentDB operator compatibility cross-reference, "
                "run in each target database: db.setProfilingLevel(1, {slowms: 100}) "
                "then wait 24-48 hours and re-run this tool."
            )
            return result

        shape_counts = {}
        shape_totals = {}
        shape_meta = {}
        ns_counts = {}
        operator_uses = {}
        window_start = None
        window_end = None

        for entry in enabled_dbs:
            dbn = entry["db"]
            try:
                cursor = client[dbn]["system.profile"].find(
                    {}, {"ns": 1, "op": 1, "millis": 1, "ts": 1,
                         "command": 1, "originatingCommand": 1, "planSummary": 1}
                ).sort([("ts", -1)]).limit(5000)
                docs = list(cursor)
            except Exception as e:
                _log(f"read system.profile failed on {dbn}: {e}", "warning")
                continue
            _log(f"Profiler read: {dbn} -> {len(docs)} entries")

            for doc in docs:
                ts = doc.get("ts")
                if ts:
                    if window_start is None or ts < window_start:
                        window_start = ts
                    if window_end is None or ts > window_end:
                        window_end = ts
                ns = doc.get("ns", "")
                op = doc.get("op", "unknown")
                millis = doc.get("millis", 0)
                cmd = doc.get("command") or doc.get("originatingCommand") or {}
                shape_key = f"{ns}::{op}::" + ",".join(sorted(cmd.keys())[:6]) if isinstance(cmd, dict) else f"{ns}::{op}"
                shape_counts[shape_key] = shape_counts.get(shape_key, 0) + 1
                shape_totals[shape_key] = shape_totals.get(shape_key, 0) + millis
                if shape_key not in shape_meta:
                    shape_meta[shape_key] = {"ns": ns, "op": op,
                                              "sample_command_keys": sorted(cmd.keys())[:6] if isinstance(cmd, dict) else []}
                ns_counts[ns] = ns_counts.get(ns, 0) + 1

                def _scan(o):
                    if isinstance(o, dict):
                        for k, v in o.items():
                            if k.startswith("$") and k in _EC2_DOCDB_UNSUPPORTED_OPERATORS:
                                if k not in operator_uses:
                                    operator_uses[k] = {"count": 0, "sample_ns": set()}
                                operator_uses[k]["count"] += 1
                                operator_uses[k]["sample_ns"].add(ns)
                            _scan(v)
                    elif isinstance(o, list):
                        for x in o:
                            _scan(x)
                _scan(cmd)

        if window_start and window_end:
            result["sample_window"] = {
                "start": window_start.isoformat() if hasattr(window_start, "isoformat") else str(window_start),
                "end": window_end.isoformat() if hasattr(window_end, "isoformat") else str(window_end),
                "documents_analyzed": sum(shape_counts.values()),
            }

        top_by_count = sorted(shape_counts.items(), key=lambda x: -x[1])[:20]
        result["top_slow_query_shapes_by_count"] = [
            {"shape": k, "count": v, "total_millis": shape_totals.get(k, 0),
             **shape_meta.get(k, {})}
            for k, v in top_by_count
        ]

        top_by_dur = sorted(shape_totals.items(), key=lambda x: -x[1])[:20]
        result["top_slow_query_shapes_by_total_duration"] = [
            {"shape": k, "total_millis": v, "count": shape_counts.get(k, 0),
             **shape_meta.get(k, {})}
            for k, v in top_by_dur
        ]

        top_ns = sorted(ns_counts.items(), key=lambda x: -x[1])[:20]
        result["collections_by_query_volume"] = [
            {"namespace": k, "count": v} for k, v in top_ns
        ]

        result["unsupported_operator_usage"] = [
            {
                "operator": op,
                "count": info["count"],
                "sample_namespaces": sorted(list(info["sample_ns"]))[:10],
            }
            for op, info in sorted(operator_uses.items(), key=lambda x: -x[1]["count"])
        ]

        _log(f"profiler_data: {len(shape_counts)} unique shapes, "
             f"{len(operator_uses)} unsupported operator families found")
        return result


# --- index compatibility via amazon-documentdb-tools index-tool (v2.3.0) ---
#
# Design note: this module NEVER classifies an index itself. It shells out to
# index-tool twice -- once to dump (the tool decides what to dump and in what
# shape) and once to classify (the tool applies every compatibility rule).
# Any rule added upstream is picked up with no change here. The alternative,
# reconstructing mongodump metadata from our own derived fields, was rejected:
# it requires synthesizing markers such as textIndexVersion, and a marker we
# invented cannot validate a rule about that marker.

# Reserved bucket names index-tool uses at database, collection and index
# scope. Anything that is NOT one of these, at collection scope, is an index
# name. Source: DocumentDbIndexTool.find_compatibility_issues().
_INDEX_ISSUE_BUCKETS = {
    "exceeded_limits",
    "unsupported_index_options",
    "unsupported_collection_options",
    "unsupported_index_types",
    "unsupported_field_names",
}

# Populated by the collstats collectors from list_indexes() verbatim, keyed by
# "<db>.<collection>". Persisted as index_specs.json so a future rule can be
# evaluated from artifacts instead of from the customer.
_RAW_INDEX_SPECS = {}


def _record_raw_index_specs(namespace, idx_specs):
    """Store list_indexes() output verbatim. No field filtering: the point is
    that we do not decide which fields matter."""
    try:
        _RAW_INDEX_SPECS[namespace] = [dict(s) for s in idx_specs.values()]
    except Exception:
        pass


def _write_index_specs(cdir):
    """Persist raw specs. Serialized with bson.json_util so BSON types inside
    partialFilterExpression (ObjectId, Decimal128, Date) survive as Extended
    JSON instead of being flattened to str() -- otherwise a future rule that
    inspects a filter's types would read our own lossy rendering."""
    if not _RAW_INDEX_SPECS:
        return
    try:
        try:
            from bson.json_util import dumps as _bson_dumps
            payload = _bson_dumps({"namespaces": _RAW_INDEX_SPECS}, indent=2)
        except Exception:
            payload = json.dumps({"namespaces": _RAW_INDEX_SPECS}, indent=2, default=str)
        with open(Path(cdir) / "index_specs.json", "w") as f:
            f.write(payload)
    except Exception as e:
        _log(f"index_specs.json write failed: {e}", "error")


def _resolve_documentdb_tool(subpath, auto_clone=True):
    """Locate a script inside amazon-documentdb-tools, cloning the repo if it
    is not already present.

    subpath: path components under the repo root, e.g.
      ("compat-tool", "compat.py")
      ("index-tool", "migrationtools", "documentdb_index_tool.py")

    Raises RuntimeError with remediation on failure rather than returning None,
    so the caller's _Step records a real error instead of a silent skip.
    """
    script_dir = Path(__file__).parent
    for base in (script_dir.parent / "amazon-documentdb-tools",
                 script_dir / "amazon-documentdb-tools",
                 Path("amazon-documentdb-tools")):
        cand = base.joinpath(*subpath)
        if cand.exists():
            return cand

    if not auto_clone:
        raise RuntimeError(f"{'/'.join(subpath)} not found and auto_clone is off")

    clone_target = script_dir / "amazon-documentdb-tools"
    if shutil.which("git") is None:
        raise RuntimeError(
            "git is not on PATH, so amazon-documentdb-tools cannot be cloned.\n"
            "  Fix: install git (yum install -y git / apt install -y git /\n"
            "       brew install git), or pre-clone the repo manually:\n"
            f"    git clone https://github.com/awslabs/amazon-documentdb-tools.git '{clone_target}'")

    _log(f"amazon-documentdb-tools not found. Cloning to {clone_target}...")
    r = subprocess.run(
        ["git", "clone", "https://github.com/awslabs/amazon-documentdb-tools.git",
         str(clone_target)],
        capture_output=True, text=True, timeout=180)
    if r.returncode != 0:
        raise RuntimeError(
            "git clone of amazon-documentdb-tools failed: "
            f"{r.stderr.strip()[:400]}\n"
            "  Check network egress to github.com and free disk space.")
    cand = clone_target.joinpath(*subpath)
    if not cand.exists():
        raise RuntimeError(
            f"clone succeeded but {'/'.join(subpath)} is absent at {cand}. "
            "Upstream layout may have changed.")
    _log(f"Cloned amazon-documentdb-tools to {clone_target}")
    return cand


def _dumped_namespaces(meta_dir):
    """Namespaces present in a mongodump-format metadata tree. Mirrors
    index-tool's own skip list so the count is comparable."""
    skip = {"system.indexes", "system.profile", "system.users", "system.views"}
    out = set()
    for f in Path(meta_dir).rglob("*.metadata.json"):
        coll = f.name[:-len(".metadata.json")]
        if coll in skip:
            continue
        out.add(f"{f.parent.name}.{coll}")
    return sorted(out)


def _collected_namespaces(cluster_dir):
    """Namespaces this run collected via collStats. Used to detect an index
    dump that silently covered less than the cluster. Returns None when
    collstats.json is unavailable, meaning coverage cannot be verified."""
    p = Path(cluster_dir) / "collstats.json"
    if not p.exists():
        return None
    try:
        with open(p) as f:
            data = json.load(f)
        return sorted({c["namespace"] for c in data.get("collections", [])
                       if c.get("namespace")})
    except Exception:
        return None


def _flatten_index_issues(issues):
    """Walk index-tool's nested report into flat findings.

    Shape (from find_compatibility_issues):
      {db: {bucket: {...}}}                        -> database scope
      {db: {coll: {bucket: [...] | {...}}}}        -> collection scope
      {db: {coll: {index: {bucket: scalar|list|{msg: detail}}}}} -> index scope
    """
    flat = []

    def emit(scope, namespace, index, bucket, value):
        if isinstance(value, dict):
            for msg, detail in value.items():
                flat.append({"scope": scope, "namespace": namespace,
                             "index": index, "issue": msg, "detail": detail})
        else:
            flat.append({"scope": scope, "namespace": namespace,
                         "index": index, "issue": bucket, "detail": value})

    for db_name, db_body in (issues or {}).items():
        if not isinstance(db_body, dict):
            continue
        for k1, v1 in db_body.items():
            if k1 in _INDEX_ISSUE_BUCKETS:
                emit("database", db_name, None, k1, v1)
                continue
            coll = k1
            if not isinstance(v1, dict):
                continue
            for k2, v2 in v1.items():
                ns = f"{db_name}.{coll}"
                if k2 in _INDEX_ISSUE_BUCKETS:
                    emit("collection", ns, None, k2, v2)
                    continue
                if not isinstance(v2, dict):
                    continue
                for k3, v3 in v2.items():
                    emit("index", ns, k2, k3, v3)
    return flat


def _append_index_compat_to_summary(cluster_dir, target_version="8.0", max_rows=50):
    """Append an Index Compatibility section to the sizing summary markdown.

    The summary is written by _generate_sizing_summary_md during generate_report,
    which runs before the index scan (the scan needs the tools repo resolved and
    a live connection). Rather than reorder the pipeline, append afterwards.

    Rationale for putting this in the summary at all: index_compat.json already
    holds the findings, but the summary markdown is what gets read. In one
    engagement index_analysis.json carried '{"_fts": "text"}' for weeks without
    anyone noticing, because nothing surfaced it in a rendered artifact.

    No-ops when no summary exists (the --source ec2 path does not produce one)
    or when the section is already present (idempotent on re-run).
    """
    cluster_dir = Path(cluster_dir)
    compat_path = cluster_dir / "index_compat.json"
    if not compat_path.exists():
        return None

    summaries = sorted(cluster_dir.glob("*-sizing-summary.md"))
    if not summaries:
        return None

    try:
        with open(compat_path) as f:
            rep = json.load(f)
    except Exception as e:
        _log(f"index compat summary append skipped -- unreadable JSON: {e}", "error")
        return None

    heading = f"## Index Compatibility (DocumentDB {target_version})"
    findings = rep.get("findings", []) or []
    by_type = rep.get("issues_by_type", {}) or {}
    cov = rep.get("coverage", {}) or {}

    lines = ["", heading, ""]

    # Coverage first. An incomplete scan must not be read as a clean one.
    if cov.get("complete") is False:
        n = len(cov.get("missing_from_dump", []))
        lines += [
            f"> **Coverage incomplete.** {n} namespace(s) that collStats observed are",
            f"> absent from the index dump, so the findings below understate the real",
            f"> count. The usual cause is that the connecting user lacks cluster-wide",
            f"> `listDatabases`, in which case the server silently returns only the",
            f"> databases it is authorized on. Re-run with a user holding",
            f"> `clusterMonitor` + `readAnyDatabase` (or `atlasAdmin` on Atlas).",
            "",
        ]
    elif cov.get("complete") is None:
        lines += [
            "> Coverage was not verified (collStats inventory unavailable for this run).",
            "",
        ]

    if not findings:
        lines += [
            f"No unsupported index definitions found across "
            f"{cov.get('namespaces_dumped', 0)} namespaces.",
            "",
        ]
    else:
        lines += [
            f"**{len(findings)} unsupported item(s)** across "
            f"{cov.get('namespaces_dumped', 0)} namespaces. Each must be resolved "
            f"before migration -- DocumentDB rejects these definitions outright.",
            "",
            "| Issue | Count |",
            "|---|---:|",
        ]
        for issue, n in sorted(by_type.items(), key=lambda kv: (-kv[1], kv[0])):
            lines.append(f"| {issue} | {n} |")
        lines += ["", "### Findings", "",
                  "| Scope | Namespace | Index | Issue | Detail |",
                  "|---|---|---|---|---|"]

        def cell(x):
            if x is None:
                return "-"
            s = x if isinstance(x, str) else json.dumps(x, default=str)
            # Pipes would break the table; backticks keep identifiers readable.
            return s.replace("|", "\\|")

        for f in findings[:max_rows]:
            lines.append(
                f"| {cell(f.get('scope'))} | `{cell(f.get('namespace'))}` | "
                f"{('`' + cell(f.get('index')) + '`') if f.get('index') else '-'} | "
                f"{cell(f.get('issue'))} | {cell(f.get('detail'))} |")
        if len(findings) > max_rows:
            lines.append(f"| ... | *{len(findings) - max_rows} more* | | "
                         f"*see `index_compat.json`* | |")
        lines.append("")

    lines += [
        f"Classified by `{rep.get('classifier', 'index-tool')}`. "
        f"Full report in `index_compat.json`; the raw metadata dump is retained "
        f"at `index_metadata/` so this can be re-scanned after "
        f"amazon-documentdb-tools adds a rule, without re-contacting the source.",
        "",
    ]

    written = []
    for md in summaries:
        try:
            body = md.read_text()
            if heading in body:          # idempotent
                continue
            with open(md, "a") as f:
                f.write("\n".join(lines) + "\n")
            written.append(md.name)
        except Exception as e:
            _log(f"failed appending index compat section to {md.name}: {e}", "error")

    if written:
        print(f"  Index compatibility section appended to: {', '.join(written)}")
    return written


def _run_index_compat_scan(uri, cluster_dir, target_version="8.0"):
    """Run index-tool against the live cluster and persist index_compat.json.

    Raises on dump failure so _Step records it and main() exits 2. A degraded
    report is worse than none: a reader cannot tell "no incompatible indexes"
    apart from "we were unable to look".
    """
    tool = _resolve_documentdb_tool(
        ("index-tool", "migrationtools", "documentdb_index_tool.py"))

    meta_dir = Path(cluster_dir) / "index_metadata"
    meta_dir.mkdir(parents=True, exist_ok=True)

    dump = subprocess.run(
        [sys.executable, str(tool), "--dump-indexes",
         "--dir", str(meta_dir), "--uri", uri],
        capture_output=True, text=True, timeout=900)
    dumped = _dumped_namespaces(meta_dir)
    if dump.returncode != 0 or not dumped:
        raise RuntimeError(
            f"index-tool --dump-indexes produced no metadata (rc={dump.returncode}). "
            f"tail: {(dump.stdout or dump.stderr).strip()[-600:]}")

    show = subprocess.run(
        [sys.executable, str(tool), "--show-issues", "--dir", str(meta_dir)],
        capture_output=True, text=True, timeout=600)
    raw = show.stdout or ""
    if "No incompatibilities found" in raw:
        issues = {}
    elif "{" in raw:
        issues = json.loads(raw[raw.index("{"):])
    else:
        raise RuntimeError(
            f"index-tool --show-issues returned no parseable report "
            f"(rc={show.returncode}). tail: {raw.strip()[-600:]}")

    return _write_index_compat_report(issues, dumped, cluster_dir, target_version,
                                     source="index-tool --dump-indexes")


def _write_index_compat_report(issues, dumped, cluster_dir, target_version, source):
    """Shared by the live scan and --index-compat-from."""
    flat = _flatten_index_issues(issues)

    by_type = {}
    for f in flat:
        by_type[f["issue"]] = by_type.get(f["issue"], 0) + 1

    # Coverage guard. MongoDB's listDatabases does NOT error for a user without
    # cluster-wide privilege -- it returns only the databases that user is
    # authorized on, and the dump still exits 0. Verified 2026-08-24 on the lab
    # replica set: a readWrite-on-one-database user dumped 1 of 2 databases,
    # missing 9 collections carrying incompatible indexes, with no error and no
    # non-zero exit. Exit status alone therefore cannot establish coverage.
    expected = _collected_namespaces(cluster_dir)
    if expected is None:
        complete, missing = None, []
        note = "collstats.json unavailable; coverage was not verified."
    else:
        missing = sorted(set(expected) - set(dumped))
        complete = not missing
        note = ("Index dump covered every namespace collStats observed."
                if complete else
                "Index dump is MISSING namespaces that collStats observed. "
                "The most likely cause is that the connecting user lacks "
                "cluster-wide listDatabases, so the server returned only the "
                "databases it is authorized on. Findings below are INCOMPLETE.")

    report = {
        "target_version": target_version,
        "classifier": "amazon-documentdb-tools index-tool --show-issues (delegated)",
        "metadata_source": source,
        "incompatible_count": len(flat),
        "issues_by_type": by_type,
        "coverage": {
            "complete": complete,
            "namespaces_dumped": len(dumped),
            "namespaces_expected": None if expected is None else len(expected),
            "missing_from_dump": missing[:200],
            "missing_truncated": len(missing) > 200,
            "note": note,
        },
        "findings": flat,
        "raw": issues,
    }
    with open(Path(cluster_dir) / "index_compat.json", "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Surface the findings in the rendered artifact, not just the JSON.
    _append_index_compat_to_summary(cluster_dir, target_version)

    if complete is False:
        _log(f"index compat coverage INCOMPLETE: {len(missing)} namespace(s) "
             f"absent from the index dump. See index_compat.json.", "error")

    banner = (f"  INDEX COMPAT ({target_version}): {len(flat)} incompatible "
              f"item(s) across {len(dumped)} namespaces")
    if complete is False:
        banner += f"  [COVERAGE INCOMPLETE -- {len(missing)} namespaces missing]"
    print(banner)
    for issue, n in sorted(by_type.items(), key=lambda kv: -kv[1]):
        print(f"      {n:5d}  {issue}")
    return report


def _ec2_run_compat_scan(uri, output_dir, cluster_name, hostnames):
    """Run the DocumentDB 8.0 compat scan for --source ec2. Same pattern as
    atlas compat block but takes explicit hostnames (already discovered by
    preflight) instead of re-running hello.
    """
    with _Timer("run_compat_scan"):
        # Shared resolver: searches script_dir.parent, script_dir and CWD, and
        # clones into script_dir when absent. Raises with remediation on failure.
        compat_script = _resolve_documentdb_tool(("compat-tool", "compat.py"))

        from urllib.parse import urlparse as _urlparse
        creds_part = ""
        query_part = ""
        try:
            parsed = _urlparse(uri.replace("mongodb+srv://", "mongodb://"))
            if parsed.username:
                creds_part = f"{parsed.username}:{parsed.password}@"
            if parsed.query:
                params = [p for p in parsed.query.split("&")
                          if not p.lower().startswith("replicaset=")]
                if params:
                    query_part = "&" + "&".join(params)
        except Exception:
            pass

        node_uris = []
        for host in hostnames:
            node_uri = f"mongodb://{creds_part}{host}:27017/?directConnection=true{query_part}"
            node_uris.append((host, node_uri))

        cdir = Path(output_dir)
        cdir.mkdir(parents=True, exist_ok=True)
        version = "8.0"
        all_output = []
        _log(f"Running compat scan (DocumentDB {version}) against {len(node_uris)} node(s)...")

        for node_name, node_uri in node_uris:
            _log(f"compat -> {node_name}")
            try:
                result = subprocess.run(
                    [sys.executable, str(compat_script), "--uri", node_uri, "--version", version],
                    capture_output=True, text=True, timeout=300,
                )
                all_output.append(f"--- Node: {node_name} ---\n{result.stdout}")
                if result.stderr:
                    all_output.append(f"--- STDERR ({node_name}) ---\n{result.stderr}")
            except subprocess.TimeoutExpired:
                all_output.append(f"--- Node: {node_name} -- TIMEOUT after 300s ---")
                _log(f"compat scan timed out on {node_name}", "warning")
            except Exception as e:
                all_output.append(f"--- Node: {node_name} -- ERROR: {e} ---")
                _log(f"compat scan failed on {node_name}: {e}", "warning")

        compat_out = cdir / f"compat-{version}.txt"
        with open(compat_out, "w") as f:
            f.write("\n".join(all_output))
        _log(f"compat scan saved to {compat_out}")
        return {"path": str(compat_out), "nodes_scanned": len(node_uris)}


def _ec2_write_output_zip(cluster_dir_str, cluster_name):
    """Bundle the per-cluster output directory into a customer-handoff zip.

    Produces <parent>/<cluster>.zip containing everything under <cluster_dir>/.
    """
    with _Timer("write_output_zip"):
        cluster_dir = Path(cluster_dir_str)
        if not cluster_dir.is_dir():
            _log(f"write_output_zip: {cluster_dir} not a directory -- skipping", "warning")
            return None
        zip_path = cluster_dir.parent / f"{cluster_name}.zip"
        try:
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for f in cluster_dir.rglob("*"):
                    if f.is_file():
                        arcname = f.relative_to(cluster_dir.parent)
                        zf.write(f, arcname)
            size_kb = zip_path.stat().st_size / 1024
            file_count = sum(1 for _ in cluster_dir.rglob("*") if _.is_file())
            _log(f"Zip: {zip_path} ({size_kb:.1f} KB, {file_count} files)")
            return {"path": str(zip_path), "size_kb": size_kb, "file_count": file_count}
        except Exception as e:
            _log(f"write_output_zip failed: {e}", "warning")
            return None


def _ec2_collect_pipeline(args, ctx):
    """Full post-preflight collection pipeline for --source ec2.

    Reuses the atlas shared collstats collector (_collect_collstats_via_uri) which
    is already mongos-aware for sharded clusters (v2.0.2 rewrite). Everything else
    is ec2-specific (CloudWatch, mongo serverStatus sampling, profiler cross-ref).
    """
    # Set the module globals the shared collstats collector expects
    global _MONGO_URI, _URI_CLUSTER_NAME
    _MONGO_URI = args.uri
    _URI_CLUSTER_NAME = args.cluster

    output_dir = ctx["output_dir"]
    cluster_dir = Path(output_dir) / args.cluster
    cluster_dir.mkdir(parents=True, exist_ok=True)

    client = ctx["client"]

    # 1. Instances metadata + preflight context (write first - always safe)
    with open(cluster_dir / "ec2_instances.json", "w") as f:
        json.dump({
            "aws_account": ctx["aws_account"],
            "aws_region": ctx["aws_region"],
            "topology": ctx["topology"],
            "server_version": ".".join(str(x) for x in ctx["version"]),
            "hostnames": ctx["hostnames"],
            "private_ips": ctx["private_ips"],
            "instances": ctx["instances"],
        }, f, indent=2, default=str)

    # 2. MongoDB serverStatus sampling (60s delta)
    samples = getattr(args, "samples", 1)
    with _Step(f"mongo_sampling {samples} sample(s) ({samples * _EC2_DELTA_INTERVAL_SECONDS}s wall-clock)"):
        sampling = _ec2_collect_mongo_sampling(client, samples=samples)
        with open(cluster_dir / "mongo_sampling.json", "w") as f:
            json.dump(sampling, f, indent=2, default=str)

    # 3. CloudWatch pull (14 days of EC2 + EBS metrics for all instances)
    with _Step(f"pull_cloudwatch {_EC2_CLOUDWATCH_DAYS_DEFAULT} days"):
        cw = _ec2_pull_cloudwatch(ctx["cw_client"], ctx["instances"],
                                   days=_EC2_CLOUDWATCH_DAYS_DEFAULT)
        with open(cluster_dir / "cloudwatch.json", "w") as f:
            json.dump(cw, f, indent=2, default=str)

    # 4. collStats + $indexStats via shared mongos-aware collector (atlas v2.0.2 fix)
    #    Must discover namespaces first - the shared collector early-exits on empty list.
    namespaces = []
    try:
        for dbn in client.list_database_names():
            if dbn in ("admin", "local", "config"):
                continue
            for cn in client[dbn].list_collection_names():
                if not cn.startswith("system."):
                    namespaces.append(f"{dbn}.{cn}")
        _log(f"discovered {len(namespaces)} namespaces for collstats: {namespaces[:10]}"
             f"{' (...)' if len(namespaces) > 10 else ''}")
    except Exception as e:
        _log(f"namespace discovery failed: {e}", "warning")

    with _Step("collstats + indexstats (shared mongos-aware collector)"):
        # _collect_collstats_via_uri detects topology internally and dispatches to
        # either _collect_collstats_via_mongos_sharded (mongos + shards aggregation)
        # or _collect_collstats_single_node (RS/standalone)
        _collect_collstats_via_uri(namespaces=namespaces, output_dir=output_dir,
                                    cluster_name=args.cluster)

    # 5. Profiler cross-ref (read-only; guidance if profiler not enabled)
    with _Step("profiler_data (read-only cross-ref against DocDB unsupported operators)"):
        profiler = _ec2_collect_profiler_data(client)
        with open(cluster_dir / "profiler_data.json", "w") as f:
            json.dump(profiler, f, indent=2, default=str)

    # 6. Compat scan (optional)
    if args.compat:
        with _Step("compat scan (8.0) via amazon-documentdb-tools"):
            _ec2_run_compat_scan(args.uri, str(cluster_dir), args.cluster,
                                  ctx["hostnames"])

        with _Step("index compat scan (8.0) via amazon-documentdb-tools index-tool"):
            _run_index_compat_scan(args.uri, str(cluster_dir))

    # 7. Auto-zip handoff
    with _Step("write_output_zip"):
        zip_info = _ec2_write_output_zip(str(cluster_dir), args.cluster)

    print(f"\n{'='*60}")
    print(f"--source ec2 collection complete")
    print(f"{'='*60}")
    print(f"Output directory: {cluster_dir}")
    if zip_info:
        print(f"Handoff zip:      {zip_info['path']} ({zip_info['size_kb']:.1f} KB)")
    print(f"\nNote: sizing_summary.md and cost_estimator.csv are not yet generated for")
    print(f"      --source ec2. Those shared outputs land in a subsequent v2.1.0-dev commit.\n")

    return {"output_dir": str(cluster_dir), "zip": zip_info}


def _rescan_index_compat_from(meta_dir, target_version="8.0"):
    """Re-classify a previously saved index_metadata/ directory.

    The stored dump is real server output, so this carries no fidelity caveat --
    it is the same input the live path feeds to --show-issues. Use it to
    re-evaluate past runs after amazon-documentdb-tools adds a rule.
    """
    meta_dir = Path(meta_dir).resolve()
    if not meta_dir.is_dir():
        raise RuntimeError(f"--index-compat-from: not a directory: {meta_dir}")
    dumped = _dumped_namespaces(meta_dir)
    if not dumped:
        raise RuntimeError(
            f"--index-compat-from: no *.metadata.json under {meta_dir}. "
            "Expected a directory produced by index-tool --dump-indexes "
            "(layout: <dir>/<database>/<collection>.metadata.json).")

    tool = _resolve_documentdb_tool(
        ("index-tool", "migrationtools", "documentdb_index_tool.py"))
    show = subprocess.run(
        [sys.executable, str(tool), "--show-issues", "--dir", str(meta_dir)],
        capture_output=True, text=True, timeout=600)
    raw = show.stdout or ""
    if "No incompatibilities found" in raw:
        issues = {}
    elif "{" in raw:
        issues = json.loads(raw[raw.index("{"):])
    else:
        raise RuntimeError(
            f"index-tool --show-issues returned no parseable report "
            f"(rc={show.returncode}). tail: {raw.strip()[-600:]}")

    # collstats.json normally sits one level up from index_metadata/, which is
    # what lets the coverage guard still work on a re-scan.
    cluster_dir = meta_dir.parent
    print(f"Re-scanning stored index metadata: {meta_dir}")
    return _write_index_compat_report(issues, dumped, cluster_dir, target_version,
                                     source=f"stored dump at {meta_dir}")


def main():
    # Standalone re-scan mode. Handled ahead of the main parser because it needs
    # neither --uri nor --cluster, and those are required=True below.
    if "--index-compat-from" in sys.argv:
        pre = argparse.ArgumentParser(add_help=False)
        pre.add_argument("--index-compat-from", type=str, required=True)
        pre.add_argument("--target-version", default="8.0")
        known, _unknown = pre.parse_known_args()
        _rescan_index_compat_from(known.index_compat_from, known.target_version)
        return

    parser = argparse.ArgumentParser(description="Collect MongoDB metrics for DocumentDB migration assessment (Atlas by default; --source ec2 for MongoDB on EC2)")
    parser.add_argument("--version", action="version", version=f"atlas_metrics.py {__version__}")
    parser.add_argument("--source", default="atlas", choices=["atlas", "ec2", "onp"],
                        help="Source topology to collect from. 'atlas' (default): MongoDB Atlas via Atlas API. "
                             "'ec2': self-managed MongoDB on EC2 via CloudWatch (Phase 4 of consolidation - not yet implemented). "
                             "'onp': self-managed MongoDB on customer-managed hardware (planned).")
    parser.add_argument("--granularity", default="PT1M", choices=["PT10S", "PT1M", "PT5M", "PT1H", "P1D"])
    parser.add_argument("--period", default=None, help="ISO 8601 duration (e.g., P2D, P14D). Auto-set if omitted.")
    parser.add_argument("--output", default=None, help="Output directory")
    parser.add_argument("--percentile", type=int, default=95, help="Percentile for sizing (default: 95)")
    parser.add_argument("--all", action="store_true",
                        help="Standard sizing run: 14 days at 5-min granularity")
    parser.add_argument("--uri", required=True, type=str,
                        help="MongoDB connection URI for direct collStats + compression analysis. REQUIRED.")
    parser.add_argument("--compat", action="store_true",
                        help="Run compat-tool against the cluster. Checks operator/API compatibility with DocumentDB. "
                             "If amazon-documentdb-tools is not cloned locally, this flag auto-clones it from "
                             "github.com/awslabs/amazon-documentdb-tools (requires git on PATH).")
    parser.add_argument("--index-compat-from", default=None, type=str, metavar="DIR",
                        help="Re-run the index compatibility scan against a previously saved "
                             "index_metadata/ directory instead of connecting to a cluster. "
                             "Use this to re-evaluate stored runs after amazon-documentdb-tools "
                             "adds a new rule. Writes index_compat.json into DIR's parent.")
    parser.add_argument("--cluster", required=True, type=str,
                        help="Atlas cluster name to filter to. REQUIRED for --source atlas.")
    parser.add_argument("--aws-region", default=None, type=str,
                        help="AWS region for --source ec2 (auto-detected from env or IMDSv2 if omitted).")
    parser.add_argument("--samples", default=1, type=int,
                        help="Number of MongoDB serverStatus delta samples for --source ec2 (default: 1). "
                             "Each sample takes 60s wall-clock. Use 3-5 for variance analysis on quiet clusters.")
    args = parser.parse_args()

    # Source dispatch - v2.1.0 consolidation.
    # Atlas is the default. EC2 preflight + collection is implemented in v2.1.0-dev.
    if args.source == "ec2":
        ctx = _ec2_preflight(args)
        _ec2_collect_pipeline(args, ctx)
        return
    if args.source == "onp":
        raise NotImplementedError(
            "--source onp (on-premises MongoDB) is not yet implemented.\n"
            "Planned for a future release after --source ec2 is validated in production."
        )

    # From here on, args.source == "atlas" - original v2.0.3 flow preserved unchanged.

    # Preflight all prerequisites before starting the long Atlas API collection
    client = _preflight_check(args)

    # Warn if --period exceeds retention for the chosen --granularity.
    # Prevents the "silent rollup" bug where Atlas returns hourly buckets
    # presented as fine-grained data past retention window.
    if args.period and not args.all:
        max_days = GRANULARITY_MAX_DAYS.get(args.granularity)
        asked_days = _period_days(args.period)
        if max_days and asked_days and asked_days > max_days:
            print(f"\nWARNING: --period {args.period} (~{asked_days:.0f} days) exceeds retention "
                  f"for --granularity {args.granularity} (~{max_days} days).")
            print(f"         Atlas silently downsamples past retention to coarser rollups presented\n"
                  f"         as {args.granularity} buckets, biasing percentiles low.")
            print(f"         Consider --period {GRANULARITY_RETENTION[args.granularity][1]} for accurate data.\n")

    global _MONGO_URI, _URI_CLUSTER_NAME
    _MONGO_URI = args.uri
    _URI_CLUSTER_NAME = None

    # --cluster flag takes priority for filtering
    if args.cluster:
        _URI_CLUSTER_NAME = args.cluster

    # Determine output dir early for logging
    out = args.output or f"atlas-metrics-{datetime.now().strftime('%Y%m%d-%H%M')}"

    if args.all:
        runs = [
            ("PT5M",  "P14D",  "14d"),
        ]
        last_metrics, last_cs = None, None
        cached_cs = None
        for gran, period, label in runs:
            print(f"\n{'='*60}")
            print(f"Collection: {label} (granularity={gran}, period={period})")
            print(f"{'='*60}\n")
            with _Timer(f"collect_metrics {label}"):
                metrics, procs, names, cs = collect_metrics(client, gran, period, out, cached_cs=cached_cs)
            if not cached_cs and cs:
                cached_cs = cs  # Reuse collStats for subsequent windows
            with _Step(f"generate_report {label}"):
                generate_report(metrics, procs, gran, period, out, args.percentile, names, label, cs)
            last_metrics, last_cs = metrics, cs
        if args.uri and last_metrics:
            with _Step("generate_sizing_csv"):
                generate_sizing_csv(args.uri, last_cs, last_metrics, args.percentile, out)
    else:
        period = args.period or GRANULARITY_RETENTION.get(args.granularity, ("", "P2D"))[1]
        with _Timer(f"collect_metrics {args.granularity}/{period}"):
            metrics, procs, names, cs = collect_metrics(client, args.granularity, period, out)
        with _Step("generate_report"):
            generate_report(metrics, procs, args.granularity, period, out, args.percentile, names, collstats=cs)
        if args.uri:
            with _Step("generate_sizing_csv"):
                generate_sizing_csv(args.uri, cs, metrics, args.percentile, out)

    # Run compat-tool if requested
    if args.compat:
        if not args.uri:
            print("WARNING: --compat requires --uri to connect to the cluster.")
        else:
          with _Step("compat scan (8.0)"):
            import subprocess

            # Step 1: Check MongoDB version first
            try:
                import pymongo
                c = pymongo.MongoClient(host=args.uri, serverSelectionTimeoutMS=5000)
                mongo_version = c.server_info().get("version", "0")
                major = int(mongo_version.split(".")[0])
                c.close()
            except Exception as e:
                _check_auth_error(e, "--compat version check", out)
                major = 0
                mongo_version = "unknown"

            if major < 5:
                print(f"\nWARNING: MongoDB version {mongo_version} detected -- compat-tool --uri requires MongoDB 5.0+.")
                print(f"   The --uri option uses db.serverStatus() which needs MongoDB 5.0 or later.")
                print(f"   For older versions, run the compat-tool separately with --directory to scan your source code or log files:")
                print(f"")
                print(f"   python3 compat.py --directory /path/to/your/source/code --version 8.0")
                print(f"   python3 compat.py --file /path/to/mongodb-profiler.log --version 8.0")
            else:
                # Step 2: Find compat-tool
                # Locate (and clone if needed) amazon-documentdb-tools via the
                # shared resolver. It searches script_dir.parent, script_dir and
                # CWD, and clones into script_dir -- not script_dir.parent, which
                # is /home for a script in a home directory and is not writable.
                compat_script = _resolve_documentdb_tool(("compat-tool", "compat.py"))

                # Step 4: Run compat-tool via serverStatus counter sampling.
                #
                # DESIGN NOTES
                # ============
                # The upstream compat-tool has two invocation modes:
                #   --uri:  connects to a MongoDB server with directConnection=True
                #           forced, targeting parsedUri['nodelist'][0]. This fails
                #           in two important ways:
                #             (a) On Atlas PrivateLink clusters, directConnect is
                #                 rejected by the NLB with [Errno 9] Bad file
                #                 descriptor -- zero coverage.
                #             (b) On any replica set exposed via SRV, nodelist[0]
                #                 is non-deterministic; the scan can land on a
                #                 secondary and silently report zero unsupported
                #                 operators when the primary is full of them.
                #   --file: scans a text file for $operator tokens and cross-
                #           references them against the version compatibility
                #           table. Works everywhere; needs no cluster connection.
                #
                # This function uses the --file path with a synthetic input we
                # generate: pymongo connects via the customer URI (standard
                # driver behavior, works in PL/sharded/RS), runs serverStatus on
                # the primary via read preference routing, and dumps the
                # aggStageCounters + operatorCounters keys to a temp text file.
                # compat.py --file then classifies each token against the target
                # DocumentDB version. Runtime execution counts are preserved in a
                # supplementary section since --file counts file occurrences.
                if compat_script.exists():
                    import tempfile
                    import pymongo
                    compat_dir = Path(out) / _URI_CLUSTER_NAME if _URI_CLUSTER_NAME else Path(out)
                    compat_dir.mkdir(parents=True, exist_ok=True)

                    for version in ["8.0"]:
                        print(f"\n=== Sampling operators from primary serverStatus ===")
                        counters = {}
                        primary_host = None
                        is_mongos = False
                        sampling_error = None
                        try:
                            with _Timer("compat serverStatus sample"):
                                c = pymongo.MongoClient(
                                    args.uri,
                                    serverSelectionTimeoutMS=15000,
                                    read_preference=pymongo.ReadPreference.PRIMARY,
                                )
                                # Discover topology. On a sharded cluster the URI
                                # resolves to a mongos router; `hello.msg` is
                                # "isdbgrid" and `hello.primary` is not set. On a
                                # replica set, `hello.primary` returns "host:port".
                                try:
                                    hello_reply = c.admin.command("hello")
                                    is_mongos = hello_reply.get("msg") == "isdbgrid"
                                    primary_host = hello_reply.get("primary") or hello_reply.get("me")
                                except Exception:
                                    pass
                                # serverStatus is node-local; driver routes to
                                # primary because of the read preference above.
                                # For sharded, this hits a mongos.
                                ss = c.admin.command("serverStatus")
                                c.close()

                            # Flatten nested counter dicts. MongoDB nests these
                            # like aggStageCounters.$facet or operatorCounters.match.$eq.
                            def _walk(d):
                                for k, v in d.items():
                                    if k.startswith("$_"):
                                        continue
                                    if isinstance(v, dict):
                                        _walk(v)
                                    elif isinstance(v, (int, float)):
                                        counters[k] = counters.get(k, 0) + int(v)
                            metrics = ss.get("metrics", {})
                            _walk(metrics.get("aggStageCounters", {}))
                            _walk(metrics.get("operatorCounters", {}))
                            source_desc = "mongos" if is_mongos else (primary_host or "unknown")
                            print(f"  Collected {len(counters)} operators from {source_desc}")
                            if is_mongos:
                                print(f"  NOTE: sharded cluster -- serverStatus is from mongos router.")
                                print(f"  Some operators evaluated shard-side only may not surface here.")
                                print(f"  For full coverage, complement with: compat.py --directory <source>")
                        except Exception as e:
                            sampling_error = str(e)
                            _check_auth_error(e, "--compat serverStatus sampling", out)
                            print(f"  WARNING: serverStatus sampling failed: {e}")

                        # Write compat-N.txt regardless -- if sampling failed we
                        # still leave a diagnostic breadcrumb for the caller.
                        compat_out = compat_dir / f"compat-{version}.txt"
                        # Filter to operators that were actually executed. MongoDB
                        # pre-registers all operator names in serverStatus with
                        # count=0 -- if we dump those, compat.py --file will
                        # classify every pre-registered operator name as "found",
                        # producing false-positive unsupported flags for
                        # operators the application never actually used.
                        executed_counters = {op: c for op, c in counters.items() if c > 0}
                        if executed_counters:
                            # Dump one operator per line for compat.py --file
                            with tempfile.NamedTemporaryFile(
                                mode="w", suffix=".txt", delete=False, prefix="operators_"
                            ) as tf:
                                dump_path = tf.name
                                for op in sorted(executed_counters):
                                    tf.write(f"{op}\n")

                            print(f"=== Running compat-tool (DocumentDB {version}) via --file ===")
                            print(f"  Executed operators: {len(executed_counters)} of {len(counters)} known to server")
                            with _Timer(f"compat {version} --file"):
                                result = subprocess.run(
                                    [sys.executable, str(compat_script), "--file", dump_path, "--version", version],
                                    capture_output=True, text=True, timeout=300,
                                )
                            try:
                                Path(dump_path).unlink()
                            except OSError:
                                pass

                            # Parse compat.py output to extract which operators
                            # it flagged as unsupported vs supported for this
                            # target version. compat.py's output has two blocks
                            # separated by section headers:
                            #   "The following N unsupported operators were found:"
                            #   "The following N supported operators were found:"
                            # Each entry line looks like: "  $facet | found 1 time(s)"
                            unsupported_set = set()
                            supported_set = set()
                            _current_section = None
                            for line in result.stdout.splitlines():
                                stripped = line.strip()
                                if stripped.startswith("The following") and "unsupported operators" in stripped:
                                    _current_section = "unsupported"
                                    continue
                                if stripped.startswith("The following") and "supported operators" in stripped:
                                    _current_section = "supported"
                                    continue
                                # Blank line or section boundary ends the current block
                                if not stripped or stripped.startswith("Unsupported operators by"):
                                    _current_section = None
                                    continue
                                if _current_section and stripped.startswith("$"):
                                    op_name = stripped.split("|", 1)[0].strip()
                                    if _current_section == "unsupported":
                                        unsupported_set.add(op_name)
                                    elif _current_section == "supported":
                                        supported_set.add(op_name)

                            # Build structured operator_usage.json for programmatic
                            # impact analysis by downstream sizing/migration tools.
                            unsupported_details = []
                            supported_details = []
                            unclassified_details = []
                            total_unsupported_execs = 0
                            total_supported_execs = 0
                            total_unclassified_execs = 0
                            for op, count in executed_counters.items():
                                if op in unsupported_set:
                                    unsupported_details.append({"operator": op, "executions": count})
                                    total_unsupported_execs += count
                                elif op in supported_set:
                                    supported_details.append({"operator": op, "executions": count})
                                    total_supported_execs += count
                                else:
                                    # Operator executed but compat.py didn't classify it (e.g. new
                                    # operator not in compat.py's version table yet).
                                    unclassified_details.append({"operator": op, "executions": count})
                                    total_unclassified_execs += count

                            unsupported_details.sort(key=lambda x: -x["executions"])
                            supported_details.sort(key=lambda x: -x["executions"])
                            unclassified_details.sort(key=lambda x: -x["executions"])
                            total_execs = total_unsupported_execs + total_supported_execs + total_unclassified_execs

                            def _pct(part):
                                return round(100.0 * part / total_execs, 4) if total_execs > 0 else 0.0

                            operator_usage = {
                                "generated": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                                "cluster": _URI_CLUSTER_NAME or "unknown",
                                "target_version": version,
                                "topology": "sharded" if is_mongos else "replica_set",
                                "sampled_from": "mongos" if is_mongos else (primary_host or "unknown"),
                                "source": "serverStatus.metrics.aggStageCounters + operatorCounters",
                                "classifier": "amazon-documentdb-tools compat.py --file (delegated)",
                                "coverage_note": (
                                    "Sharded: serverStatus reflects mongos-level routing view. "
                                    "Operators evaluated only shard-side (e.g. some $function/$where "
                                    "on sharded collections) may not surface. Complement with "
                                    "compat.py --directory against source code for full coverage."
                                    if is_mongos else
                                    "Replica set: serverStatus sampled from primary. Coverage is "
                                    "complete for the primary's counter view."
                                ),
                                "summary": {
                                    "total_operators_executed": len(executed_counters),
                                    "total_operators_known_to_server": len(counters),
                                    "total_executions": total_execs,
                                    "unsupported_operators": len(unsupported_details),
                                    "unsupported_executions": total_unsupported_execs,
                                    "unsupported_execution_pct": _pct(total_unsupported_execs),
                                    "supported_operators": len(supported_details),
                                    "supported_executions": total_supported_execs,
                                    "supported_execution_pct": _pct(total_supported_execs),
                                    "unclassified_operators": len(unclassified_details),
                                    "unclassified_executions": total_unclassified_execs,
                                },
                                "unsupported": unsupported_details,
                                "supported": supported_details,
                                "unclassified": unclassified_details,
                            }
                            operator_usage_path = compat_dir / "operator_usage.json"
                            with open(operator_usage_path, "w") as f:
                                json.dump(operator_usage, f, indent=2)
                            print(f"  Saved: {operator_usage_path}")

                            # Human-readable compat-N.txt (retains the compat.py
                            # output plus a runtime execution counts section for
                            # inline browsing).
                            with open(compat_out, "w") as f:
                                topo_line = ("Sharded (sampled from mongos router)" if is_mongos
                                             else f"Replica set (sampled from primary: {primary_host or 'unknown'})")
                                f.write(
                                    f"# compat scan against DocumentDB {version} target\n"
                                    f"# Source: serverStatus.metrics.aggStageCounters + operatorCounters\n"
                                    f"# Topology: {topo_line}\n"
                                    f"# Filtered to {len(executed_counters)} operators with non-zero execution count\n"
                                    f"# (server registers {len(counters)} operators total; zero-count entries omitted)\n"
                                    f"# Method: dump operator names to text file, delegate classification to compat.py --file\n"
                                    f"# See operator_usage.json for structured / machine-readable output.\n"
                                )
                                if is_mongos:
                                    f.write(
                                        "#\n"
                                        "# COVERAGE NOTE (sharded): serverStatus reflects mongos-level routing view.\n"
                                        "# Operators evaluated only shard-side (some $function/$accumulator/$where on\n"
                                        "# sharded collections) may not surface here. For full coverage complement with:\n"
                                        "#   python3 compat.py --directory /path/to/source-code --version 8.0\n"
                                    )
                                f.write("\n")
                                f.write(result.stdout)
                                if result.stderr:
                                    f.write(f"\n--- compat.py STDERR ---\n{result.stderr}")
                                f.write("\n\n=== Runtime execution counts on primary ===\n\n")
                                f.write("(--file mode reports file-occurrence counts; the numbers below are\n")
                                f.write("the actual serverStatus counter values from the primary node.\n")
                                f.write("These represent CUMULATIVE executions since the primary last restarted,\n")
                                f.write("which is why some counts can look large -- divide by cluster uptime to\n")
                                f.write("estimate per-second rates.)\n\n")
                                if unsupported_details:
                                    f.write(f"UNSUPPORTED IN DOCUMENTDB {version} ({total_unsupported_execs:,} total executions, "
                                            f"{_pct(total_unsupported_execs)}% of workload):\n")
                                    for item in unsupported_details:
                                        f.write(f"  {item['operator']}: {item['executions']:,} executions\n")
                                    f.write("\n")
                                if supported_details:
                                    f.write(f"SUPPORTED IN DOCUMENTDB {version} ({total_supported_execs:,} total executions, "
                                            f"{_pct(total_supported_execs)}% of workload):\n")
                                    for item in supported_details:
                                        f.write(f"  {item['operator']}: {item['executions']:,} executions\n")
                                    f.write("\n")
                                if unclassified_details:
                                    f.write(f"UNCLASSIFIED BY compat.py ({total_unclassified_execs:,} total executions):\n")
                                    for item in unclassified_details:
                                        f.write(f"  {item['operator']}: {item['executions']:,} executions\n")
                        else:
                            with open(compat_out, "w") as f:
                                f.write(
                                    f"# compat scan against DocumentDB {version} target -- NO USAGE\n\n"
                                )
                                if sampling_error:
                                    f.write(f"serverStatus sampling from primary failed: {sampling_error}\n\n")
                                elif counters:
                                    f.write(f"Server registers {len(counters)} operator names but none have been executed.\n")
                                    f.write("This is normal for a freshly-provisioned cluster with no traffic.\n\n")
                                else:
                                    f.write("No operator counters returned from serverStatus.\n\n")
                                f.write("For static-analysis coverage, run compat.py against source or profiler:\n")
                                f.write("  python3 compat.py --directory /path/to/source-code --version 8.0\n")
                                f.write("  python3 compat.py --file /path/to/mongodb-profiler.log --version 8.0\n")
                        print(f"  Saved: {compat_out}")

    # Index compatibility. Runs after the operator compat scan so both land in
    # the same handoff zip. Delegates entirely to index-tool -- see
    # _run_index_compat_scan.
    if args.compat and args.uri:
        with _Step("index compat scan (8.0) via amazon-documentdb-tools index-tool"):
            _idx_dir = Path(out) / _URI_CLUSTER_NAME if _URI_CLUSTER_NAME else Path(out)
            _run_index_compat_scan(args.uri, str(_idx_dir))

    # Zip the per-cluster output directory for easy handoff.
    # Produces <output>/<cluster>.zip alongside the <output>/<cluster>/ folder.
    # Customer can email/upload the single zip file instead of tarring manually.
    with _Step("auto-zip output"):
        import zipfile
        cluster_dir = Path(out) / _URI_CLUSTER_NAME if _URI_CLUSTER_NAME else Path(out)
        if cluster_dir.is_dir():
            zip_path = Path(out) / f"{_URI_CLUSTER_NAME}.zip" if _URI_CLUSTER_NAME else Path(out) / f"{Path(out).name}.zip"
            print(f"\n=== Zipping output for handoff ===")
            with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
                for f in cluster_dir.rglob("*"):
                    if f.is_file():
                        # Preserve the <cluster>/... prefix so unzip creates a clean top-level folder
                        arcname = f.relative_to(cluster_dir.parent)
                        zf.write(f, arcname)
            size_kb = zip_path.stat().st_size / 1024
            print(f"  Saved: {zip_path} ({size_kb:.1f} KB)")
            print(f"  Contains {sum(1 for _ in cluster_dir.rglob('*') if _.is_file())} files from {cluster_dir}")

    _log("atlas_metrics.py finished")

    # Emit a partial-success summary if any pipeline step failed. Foundational
    # steps (collect_metrics) still hard-fail via _Timer -- if you reach this
    # point they succeeded, and any failures below are strictly downstream
    # artifact-generation steps. Exit non-zero (2) so shell scripts / CI can
    # distinguish "completed cleanly" from "completed with missing artifacts".
    if _Step.any_failed():
        failed = _Step.failures()
        print(f"\n{'='*60}")
        print(f"COMPLETED WITH PARTIAL SUCCESS -- {len(failed)} step(s) failed:")
        for label, err in failed:
            print(f"  * {label}: {err}")
        print(f"{'='*60}")
        print(f"The raw sizing-report.json and per-node metric JSONs were still")
        print(f"produced (or the run would have aborted much earlier). See runtime.log")
        print(f"for the full stack traces and rerun after addressing the failures.")
        print("\nDone (partial success).")
        sys.exit(2)

    print("\nDone!")


if __name__ == "__main__":
    main()
