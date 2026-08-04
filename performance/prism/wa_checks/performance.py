"""WA Checks — Performance Efficiency Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Performance Efficiency
IDs are stable — do not rename (PERF5, PERF6, PERF8, PERF9, PERF11, PERF12, PERF13, PERF15, PERF1b).
"""
import logging
from datetime import datetime, timedelta
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)

CONN_LIMITS = {
    "db.t3.medium": 1000, "db.t4g.medium": 1000,
    "db.r5.large": 3400, "db.r6g.large": 3400, "db.r6gd.large": 3400, "db.r8g.large": 3400,
    "db.r5.xlarge": 7000, "db.r6g.xlarge": 7000, "db.r6gd.xlarge": 7000, "db.r8g.xlarge": 7000,
    "db.r5.2xlarge": 14200, "db.r6g.2xlarge": 14200, "db.r6gd.2xlarge": 14200, "db.r8g.2xlarge": 14200,
    "db.r5.4xlarge": 28400, "db.r6g.4xlarge": 28400, "db.r6gd.4xlarge": 28400, "db.r8g.4xlarge": 28400,
    "db.r5.8xlarge": 60000, "db.r6g.8xlarge": 60000, "db.r6gd.8xlarge": 60000, "db.r8g.8xlarge": 60000,
    "db.r5.12xlarge": 60000, "db.r6g.12xlarge": 60000, "db.r6gd.12xlarge": 60000, "db.r8g.12xlarge": 60000,
    "db.r5.16xlarge": 60000, "db.r6g.16xlarge": 60000, "db.r6gd.16xlarge": 60000, "db.r8g.16xlarge": 60000,
    "db.r5.24xlarge": 60000,
}

INSTANCE_RAM_GIB = {
    "db.t3.medium": 4, "db.t4g.medium": 4,
    "db.r5.large": 16, "db.r6g.large": 16, "db.r6gd.large": 16, "db.r8g.large": 16,
    "db.r5.xlarge": 32, "db.r6g.xlarge": 32, "db.r6gd.xlarge": 32, "db.r8g.xlarge": 32,
    "db.r5.2xlarge": 64, "db.r6g.2xlarge": 64, "db.r6gd.2xlarge": 64, "db.r8g.2xlarge": 64,
    "db.r5.4xlarge": 128, "db.r6g.4xlarge": 128, "db.r6gd.4xlarge": 128, "db.r8g.4xlarge": 128,
    "db.r5.8xlarge": 256, "db.r6g.8xlarge": 256, "db.r6gd.8xlarge": 256, "db.r8g.8xlarge": 256,
    "db.r5.12xlarge": 384, "db.r6g.12xlarge": 384, "db.r6gd.12xlarge": 384, "db.r8g.12xlarge": 384,
    "db.r5.16xlarge": 512, "db.r6g.16xlarge": 512, "db.r6gd.16xlarge": 512, "db.r8g.16xlarge": 512,
    "db.r5.24xlarge": 768,
}


@register_check("PERF5", "Performance Efficiency", "Connection utilization",
                source="cloudwatch", per_instance=True, priority=10)
def check_connections(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    itype = inst["DBInstanceClass"]
    limit = CONN_LIMITS.get(itype, 0)
    if not limit:
        return []
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="DatabaseConnections",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Maximum"])
        dps = [d["Maximum"] for d in resp.get("Datapoints", [])]
        if dps:
            max_conn = max(dps)
            pct = max_conn / limit * 100
            status = "pass" if pct < 70 else "warn" if pct < 90 else "fail"
            detail = "Consider upsizing or connection pooling" if pct >= 70 else ""
            return [{"pillar": "Performance Efficiency", "id": "PERF5",
                     "label": f"Peak connections for {iid} ({int(max_conn)}/{limit} = {pct:.0f}%)",
                     "status": status, "detail": detail}]
        return []
    except Exception:
        return []


@register_check("PERF6", "Performance Efficiency", "Buffer cache hit ratio",
                source="cloudwatch", per_instance=True, priority=11)
def check_buffer_cache(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="BufferCacheHitRatio",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
        dps = [d["Average"] for d in resp.get("Datapoints", [])]
        if dps:
            avg = sum(dps) / len(dps)
            status = "pass" if avg >= 99 else "warn" if avg >= 95 else "fail"
            detail = "Working set may not fit in memory" if avg < 95 else ""
            return [{"pillar": "Performance Efficiency", "id": "PERF6",
                     "label": f"Buffer cache hit ratio for {iid} ({avg:.1f}%)",
                     "status": status, "detail": detail}]
        return []
    except Exception:
        return []


@register_check("PERF11", "Performance Efficiency", "FreeableMemory",
                source="cloudwatch", per_instance=True, priority=12)
def check_freeable_memory(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    itype = inst["DBInstanceClass"]
    ram_gib = INSTANCE_RAM_GIB.get(itype, 0)
    if not ram_gib:
        return []
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="FreeableMemory",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Minimum"])
        dps = [d["Minimum"] for d in resp.get("Datapoints", [])]
        if dps:
            min_free = min(dps)
            free_pct = min_free / (ram_gib * 1024**3) * 100
            status = "fail" if free_pct < 5 else "warn" if free_pct < 10 else "pass"
            detail = "Instance under memory pressure" if free_pct < 10 else ""
            return [{"pillar": "Performance Efficiency", "id": "PERF11",
                     "label": f"FreeableMemory min for {iid}: {min_free / (1024**3):.1f} GiB ({free_pct:.0f}%)",
                     "status": status, "detail": detail}]
        return []
    except Exception:
        return []


@register_check("PERF12", "Performance Efficiency", "Swap usage",
                source="cloudwatch", per_instance=True, priority=13)
def check_swap(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="SwapUsage",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Maximum"])
        dps = [d["Maximum"] for d in resp.get("Datapoints", [])]
        if dps and max(dps) > 0:
            return [{"pillar": "Performance Efficiency", "id": "PERF12",
                     "label": f"SwapUsage on {iid}: {max(dps) / (1024**2):.0f} MB",
                     "status": "fail",
                     "detail": "Instance is swapping — critically undersized"}]
        elif dps:
            return [{"pillar": "Performance Efficiency", "id": "PERF12",
                     "label": f"No swap usage on {iid}",
                     "status": "pass", "detail": ""}]
        return []
    except Exception:
        return []


@register_check("PERF13", "Performance Efficiency", "Disk queue depth",
                source="cloudwatch", per_instance=True, priority=14)
def check_disk_queue(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="DiskQueueDepth",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
        dps = [d["Average"] for d in resp.get("Datapoints", [])]
        if dps:
            avg = sum(dps) / len(dps)
            return [{"pillar": "Performance Efficiency", "id": "PERF13",
                     "label": f"DiskQueueDepth avg for {iid}: {avg:.1f}",
                     "status": "warn" if avg > 5 else "pass",
                     "detail": "I/O backing up — evaluate I/O-Optimized or upsizing" if avg > 5 else ""}]
        return []
    except Exception:
        return []


@register_check("PERF8", "Performance Efficiency", "Index-to-data ratio",
                source="database", requires_analysis=True, priority=30)
def check_index_ratio(ctx):
    if not ctx.analysis_data:
        return []
    total_index = 0
    total_data = 0
    for db_name, collections in ctx.analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll, data in collections.items():
            if isinstance(data, dict):
                total_index += data.get("total_index_size", 0)
                total_data += data.get("size", 0)
    if total_data > 0:
        ratio = total_index / total_data * 100
        return [{"pillar": "Performance Efficiency", "id": "PERF8",
                 "label": f"Index-to-data ratio: {ratio:.0f}%",
                 "status": "warn" if ratio > 50 else "pass",
                 "detail": "High index overhead — review for unused indexes" if ratio > 50 else ""}]
    return []


@register_check("PERF9", "Performance Efficiency", "Storage bloat",
                source="database", requires_analysis=True, priority=31)
def check_bloat(ctx):
    if not ctx.analysis_data:
        return []
    bloated = []
    for db_name, collections in ctx.analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll, data in collections.items():
            if isinstance(data, dict) and data.get("bloat_pct", 0) > 30:
                bloated.append(f"{db_name}.{coll}")
    if bloated:
        return [{"pillar": "Performance Efficiency", "id": "PERF9",
                 "label": f"{len(bloated)} collection(s) with >30% storage bloat",
                 "status": "warn",
                 "detail": f"Consider compaction: {', '.join(bloated[:5])}. Also review "
                           "per-index bloat in the Database and Index overview — rebuild "
                           "or drop bloated indexes to reclaim storage and reduce write I/O."}]
    return [{"pillar": "Performance Efficiency", "id": "PERF9",
             "label": "No significant storage bloat detected",
             "status": "pass",
             "detail": "Also check per-index bloat in the Database and Index overview — "
                       "individual indexes can be bloated even when collections are not."}]


@register_check("PERF15", "Performance Efficiency", "Large collections without indexes",
                source="database", requires_analysis=True, priority=33)
def check_no_indexes(ctx):
    if not ctx.analysis_data:
        return []
    flagged = []
    for db_name, collections in ctx.analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll, data in collections.items():
            if isinstance(data, dict):
                count = data.get("count", 0)
                indexes = data.get("indexes", [])
                # Only _id index = no secondary indexes
                if count > 100000 and len(indexes) <= 1:
                    flagged.append(f"{db_name}.{coll}")
    if flagged:
        return [{"pillar": "Performance Efficiency", "id": "PERF15",
                 "label": f"{len(flagged)} large collection(s) with no secondary indexes",
                 "status": "warn",
                 "detail": f"Every query = full scan: {', '.join(flagged[:5])}"}]
    return []


# ── Migrated from tabs/wa_v2/performance.py (Phase 3B) ───────────────────────

@register_check("PERF2a", "Performance Efficiency", "Query Planner version",
                source="infrastructure", priority=35)
def check_query_planner(ctx):
    """PERF 2 — Query Planner version based on engine version."""
    engine_ver = ctx.cluster.get("EngineVersion", "")
    major = int(engine_ver.split(".")[0]) if engine_ver else 0
    if major >= 8:
        return [{"pillar": "Performance Efficiency", "id": "PERF2a",
                 "label": f"Query Planner v3 available (engine {engine_ver})", "status": "pass",
                 "detail": "DocumentDB 8.0 includes Query Planner v3 with improved index selection"}]
    elif major >= 5:
        return [{"pillar": "Performance Efficiency", "id": "PERF2a",
                 "label": f"Query Planner v2 (engine {engine_ver})", "status": "info",
                 "detail": "Upgrade to 8.0 for Query Planner v3 with better multi-key and compound index selection"}]
    return [{"pillar": "Performance Efficiency", "id": "PERF2a",
             "label": f"Legacy Query Planner (engine {engine_ver})", "status": "warn",
             "detail": "Upgrade to DocumentDB 5.0+ for Query Planner v2, or 8.0 for v3"}]


@register_check("PERF2b", "Performance Efficiency", "CPUUtilization alarm",
                source="cloudwatch", priority=36)
def check_cpu_alarm(ctx):
    """PERF 2/7 — CW alarm for CPUUtilization."""
    try:
        all_alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        iids = {i["DBInstanceIdentifier"] for i in ctx.instances}
        alarm_metrics = {a["MetricName"] for a in all_alarms
                         if any(d.get("Value") in iids or ctx.cluster_id in str(d)
                                for d in a.get("Dimensions", []))}
        if "CPUUtilization" in alarm_metrics:
            return [{"pillar": "Performance Efficiency", "id": "PERF2b",
                     "label": "CPUUtilization alarm configured", "status": "pass", "detail": ""}]
        return [{"pillar": "Performance Efficiency", "id": "PERF2b",
                 "label": "CPUUtilization alarm not configured", "status": "warn",
                 "detail": "Recommended threshold: 80% sustained — indicates need to scale or optimise queries"}]
    except Exception as e:
        return [{"pillar": "Performance Efficiency", "id": "PERF2b",
                 "label": f"Cannot check alarms: {e}", "status": "warn", "detail": str(e)}]


@register_check("PERF7a", "Performance Efficiency", "DatabaseConnections alarm",
                source="cloudwatch", priority=37)
def check_connections_alarm(ctx):
    """PERF 7 — CW alarm for DatabaseConnections."""
    try:
        all_alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        iids = {i["DBInstanceIdentifier"] for i in ctx.instances}
        alarm_metrics = {a["MetricName"] for a in all_alarms
                         if any(d.get("Value") in iids or ctx.cluster_id in str(d)
                                for d in a.get("Dimensions", []))}
        if "DatabaseConnections" in alarm_metrics:
            return [{"pillar": "Performance Efficiency", "id": "PERF7a",
                     "label": "DatabaseConnections alarm configured", "status": "pass", "detail": ""}]
        return [{"pillar": "Performance Efficiency", "id": "PERF7a",
                 "label": "DatabaseConnections alarm not configured", "status": "warn",
                 "detail": "Target below 80% of instance connection limit — "
                           "set alarm when approaching the limit to prevent connection exhaustion"}]
    except Exception as e:
        return [{"pillar": "Performance Efficiency", "id": "PERF7a",
                 "label": f"Cannot check alarms: {e}", "status": "warn", "detail": str(e)}]
