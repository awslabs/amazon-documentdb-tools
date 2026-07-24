"""WA Review v2 — Cost Optimization pillar extra checks.

Most checks migrated to wa_checks/cost_optimization.py (Phase 3B + 3C).
This module retains only the long-running CloudWatch Logs COLLSCAN checks
(COST2, PERF1d) which poll for up to 30s and cannot run in the synchronous
plugin pipeline without blocking all other checks.
"""
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

SKILL         = "cost_optimization"
SKILL_VERSION = "1.0"
PILLAR        = "Cost Optimization"


def run_checks(cluster_id, region, conn_str=None, *, cluster=None, instances=None):
    results = []

    def _add(cid, label, status, detail=""):
        results.append({"pillar": PILLAR, "id": cid,
                        "label": label, "status": status, "detail": detail})

    def _add_perf(cid, label, status, detail=""):
        results.append({"pillar": "Performance Efficiency", "id": cid,
                        "label": label, "status": status, "detail": detail})

    docdb = boto3.client("docdb", region_name=region)

    # Resolve cluster metadata (use pre-fetched if available)
    cl = cluster
    if cl is None:
        try:
            cl = docdb.describe_db_clusters(
                DBClusterIdentifier=cluster_id)["DBClusters"][0]
        except Exception as e:
            _add("COST2a", f"Cannot describe cluster: {e}", "warn")
            _add_perf("PERF1d", f"Cannot describe cluster: {e}", "warn")
            return results

    # ── COST2a: COLLSCAN detection from profiler logs (30s poll) ───────────────
    try:
        logs = boto3.client("logs", region_name=region)
        log_exports = cl.get("EnabledCloudwatchLogsExports", [])
        if "profiler" not in log_exports:
            _add("COST2a", "Profiler logs not enabled — COLLSCAN detection unavailable", "info",
                 "Enable profiler logging to detect COLLSCAN queries that increase I/O costs")
        else:
            log_group = f"/aws/docdb/{cluster_id}/profiler"
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            try:
                resp = logs.start_query(
                    logGroupName=log_group,
                    startTime=int(start_time.timestamp()),
                    endTime=int(end_time.timestamp()),
                    queryString="fields @message | filter @message like /COLLSCAN/ | stats count() as collscans",
                    limit=1,
                )
                import time
                qid = resp["queryId"]
                for _ in range(15):
                    time.sleep(2)
                    res = logs.get_query_results(queryId=qid)
                    if res["status"] == "Complete":
                        break
                rows = res.get("results", [])
                count = int(rows[0][0]["value"]) if rows and rows[0] else 0
                if count > 0:
                    _add("COST2a", f"{count} COLLSCAN queries detected in last 24h", "warn",
                         "Collection scans consume I/O for every document read. "
                         "Add indexes to avoid COLLSCAN and reduce I/O costs")
                else:
                    _add("COST2a", "No COLLSCAN queries detected in last 24h", "pass")
            except Exception:
                _add("COST2a", "Profiler enabled — COLLSCAN check requires log group access", "info",
                     f"Query log group {log_group} for COLLSCAN patterns")
    except Exception as e:
        _add("COST2a", f"Cannot check COLLSCAN: {e}", "warn")

    # ── PERF1d: Same COLLSCAN detection for Performance pillar ────────────────
    try:
        logs = boto3.client("logs", region_name=region)
        log_exports = cl.get("EnabledCloudwatchLogsExports", [])
        if "profiler" not in log_exports:
            _add_perf("PERF1d", "Profiler logs not enabled — COLLSCAN detection unavailable", "info",
                      "Enable profiler logging to detect full collection scans that degrade query performance")
        else:
            log_group = f"/aws/docdb/{cluster_id}/profiler"
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=24)
            try:
                resp = logs.start_query(
                    logGroupName=log_group,
                    startTime=int(start_time.timestamp()),
                    endTime=int(end_time.timestamp()),
                    queryString="fields @message | filter @message like /COLLSCAN/ | stats count() as collscans",
                    limit=1,
                )
                import time
                qid = resp["queryId"]
                for _ in range(15):
                    time.sleep(2)
                    res = logs.get_query_results(queryId=qid)
                    if res["status"] == "Complete":
                        break
                rows = res.get("results", [])
                count = int(rows[0][0]["value"]) if rows and rows[0] else 0
                if count > 0:
                    _add_perf("PERF1d", f"{count} COLLSCAN queries detected in last 24h", "warn",
                              "Collection scans read every document — add indexes to avoid COLLSCAN "
                              "and reduce query latency")
                else:
                    _add_perf("PERF1d", "No COLLSCAN queries detected in last 24h", "pass")
            except Exception:
                _add_perf("PERF1d", "Profiler enabled — COLLSCAN check requires log group access", "info",
                          f"Query log group {log_group} for COLLSCAN patterns")
    except Exception as e:
        _add_perf("PERF1d", f"Cannot check COLLSCAN: {e}", "warn")

    return results
