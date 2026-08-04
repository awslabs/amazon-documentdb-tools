"""WA Checks — Sustainability Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Sustainability
IDs are stable — do not rename (SUST1, SUST2).
"""
import logging
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)

GRAVITON_FAMILIES = ("r6g", "r7g", "r8g", "t4g", "r6gd")


@register_check("SUST1", "Sustainability", "Graviton processor",
                source="infrastructure", per_instance=True, priority=10)
def check_graviton(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    itype = inst["DBInstanceClass"]
    family = itype.replace("db.", "").split(".")[0] if itype.startswith("db.") else ""
    is_graviton = family in GRAVITON_FAMILIES
    return [{"pillar": "Sustainability", "id": "SUST1",
             "label": f"{iid} {'uses' if is_graviton else 'is not'} Graviton ({itype})",
             "status": "pass" if is_graviton else "warn",
             "detail": "" if is_graviton else "Migrate to Graviton (r6g/r7g/r8g) for better price-performance"}]


@register_check("SUST2", "Sustainability", "Compression enabled",
                source="database", requires_analysis=True, priority=20)
def check_compression(ctx):
    if not ctx.analysis_data:
        return []
    uncompressed = []
    total = 0
    for db_name, collections in ctx.analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll, data in collections.items():
            if isinstance(data, dict):
                total += 1
                compression = data.get("compression", {})
                if not compression.get("enabled", False):
                    uncompressed.append(f"{db_name}.{coll}")
    if not total:
        return []
    if uncompressed:
        return [{"pillar": "Sustainability", "id": "SUST2",
                 "label": f"{len(uncompressed)}/{total} collection(s) without compression",
                 "status": "warn",
                 "detail": f"Enable compression to reduce storage and I/O: {', '.join(uncompressed[:5])}"}]
    return [{"pillar": "Sustainability", "id": "SUST2",
             "label": f"All {total} collections have compression enabled",
             "status": "pass", "detail": ""}]


# ── Migrated from tabs/wa_v2/sustainability.py (Phase 3C) ────────────────────

@register_check("SUST3a", "Sustainability", "Idle reader detection",
                source="cloudwatch", priority=30)
def check_idle_readers_sust(ctx):
    """SUST 3 — Idle readers waste compute and energy."""
    from datetime import datetime, timedelta
    end = datetime.utcnow()
    start = end - timedelta(days=7)
    idle = []

    # Identify writer
    writer_ids = set()
    for m in ctx.cluster.get("DBClusterMembers", []):
        if m.get("IsClusterWriter"):
            writer_ids.add(m.get("DBInstanceIdentifier"))

    for inst in ctx.instances:
        iid = inst["DBInstanceIdentifier"]
        if iid in writer_ids:
            continue
        try:
            conn_resp = ctx.cw_client.get_metric_statistics(
                Namespace="AWS/DocDB", MetricName="DatabaseConnections",
                Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
                StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
            iops_resp = ctx.cw_client.get_metric_statistics(
                Namespace="AWS/DocDB", MetricName="ReadIOPS",
                Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
                StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
            avg_conn = (sum(d["Average"] for d in conn_resp.get("Datapoints", [])) /
                        max(len(conn_resp.get("Datapoints", [])), 1)
                        if conn_resp.get("Datapoints") else 0)
            avg_iops = (sum(d["Average"] for d in iops_resp.get("Datapoints", [])) /
                        max(len(iops_resp.get("Datapoints", [])), 1)
                        if iops_resp.get("Datapoints") else 0)
            if avg_conn < 2 and avg_iops < 5:
                idle.append(iid)
        except Exception:
            pass

    if idle:
        return [{"pillar": "Sustainability", "id": "SUST3a",
                 "label": f"{len(idle)} idle reader(s): {', '.join(idle)}",
                 "status": "warn",
                 "detail": "Idle replicas consume compute and energy with no workload benefit — "
                           "consider removing them to reduce resource waste"}]
    return [{"pillar": "Sustainability", "id": "SUST3a",
             "label": "No idle reader instances detected", "status": "pass", "detail": ""}]


@register_check("SUST4a", "Sustainability", "Storage I/O efficiency",
                source="infrastructure", priority=31)
def check_storage_io_efficiency(ctx):
    """SUST 4 — I/O-Optimized storage evaluation from cluster snapshot cost analysis."""
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            snap_data = _snap.get("data")
        cost_data = snap_data.get("cost_data") if snap_data else None

        if ctx.cluster.get("StorageType", "standard") == "iopt1":
            return [{"pillar": "Sustainability", "id": "SUST4a",
                     "label": "I/O-Optimized storage active", "status": "pass",
                     "detail": "Reduces I/O amplification and improves energy efficiency for "
                               "write-heavy workloads"}]
        if cost_data:
            sc = cost_data["standard_costs"]
            ic = cost_data["iopt_costs"]
            savings = cost_data.get("potential_savings", 0)
            cur_type = cost_data.get("current_type", "standard")
            is_std = cur_type == "standard"
            std_wins = sc["total"] <= ic["total"]

            if (is_std and std_wins) or (not is_std and not std_wins):
                return [{"pillar": "Sustainability", "id": "SUST4a",
                         "label": "Standard storage optimal for current I/O profile",
                         "status": "pass",
                         "detail": f"Standard: ${sc['total']:,.2f}/mo  ·  I/O-Optimized: "
                                   f"${ic['total']:,.2f}/mo (30-day analysis). I/O-Optimized would "
                                   "not reduce cost or I/O amplification for this workload"}]
            target = "I/O-Optimized" if is_std else "Standard"
            return [{"pillar": "Sustainability", "id": "SUST4a",
                     "label": f"Switch to {target} storage to cut I/O amplification "
                              f"(save ${savings:,.2f}/mo)",
                     "status": "warn",
                     "detail": f"Standard: ${sc['total']:,.2f}/mo  ·  I/O-Optimized: "
                               f"${ic['total']:,.2f}/mo (30-day analysis). {target} storage "
                               "reduces I/O amplification and improves energy efficiency for "
                               "this workload's I/O profile"}]
        return [{"pillar": "Sustainability", "id": "SUST4a",
                 "label": "Storage I/O efficiency — cost analysis pending",
                 "status": "info",
                 "detail": "Load Cluster Overview to run the 30-day storage cost analysis and "
                           "determine whether I/O-Optimized storage would reduce I/O amplification"}]
    except Exception as e:
        return [{"pillar": "Sustainability", "id": "SUST4a",
                 "label": f"Cannot evaluate storage I/O efficiency: {e}", "status": "warn", "detail": str(e)}]


@register_check("SUST5a", "Sustainability", "Backup retention efficiency",
                source="infrastructure", priority=32)
def check_backup_retention_sust(ctx):
    """SUST 5 — Over-retention wastes storage."""
    retention = ctx.cluster.get("BackupRetentionPeriod", 1)
    if retention > 35:
        return [{"pillar": "Sustainability", "id": "SUST5a",
                 "label": f"Backup retention {retention} days — review if necessary",
                 "status": "warn",
                 "detail": "Excessively long retention increases storage consumption; "
                           "align with actual RTO/RPO requirements"}]
    return [{"pillar": "Sustainability", "id": "SUST5a",
             "label": f"Backup retention {retention} days — within sustainable range",
             "status": "pass", "detail": ""}]
