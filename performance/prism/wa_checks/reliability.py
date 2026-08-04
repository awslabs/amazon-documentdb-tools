"""WA Checks — Reliability Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Reliability

Display-id scheme (each id matches its question heading number):
  REL 1 RPO/RTO        → REL1a/b/c
  REL 2 Exception      → REL2a
  REL 3 Connection     → REL3a (retryWrites), REL3b (connection config)
  REL 4 Cursor         → REL4a (timeout metric), REL4b (cursor config)
  REL 5 High Avail.    → REL5a/b/c
  REL 6 Monitoring     → REL6d (failover), REL6e (MVCC); REL6a/b/c are CW alarms (wa_v2)
"""
import logging
from datetime import datetime, timedelta
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)


@register_check("REL1a", "Reliability", "Backup retention period",
                source="infrastructure", priority=10)
def check_backup_retention(ctx):
    retention = ctx.cluster.get("BackupRetentionPeriod", 1)
    status = "pass" if retention >= 7 else "warn" if retention >= 3 else "fail"
    detail = "Recommended: 7+ days for production" if retention < 7 else ""
    return [{"pillar": "Reliability", "id": "REL1a",
             "label": f"Backup retention period ({retention} days)",
             "status": status, "detail": detail}]


@register_check("REL1b", "Reliability", "AWS Backup plan",
                source="infrastructure", priority=11)
def check_aws_backup(ctx):
    import boto3
    try:
        backup = boto3.client("backup", region_name=ctx.region)
        plans = backup.list_backup_plans()["BackupPlansList"]
        cluster_arn = ctx.cluster.get("DBClusterArn", "")
        covered = False
        cross_region = False

        for plan in plans:
            plan_id = plan["BackupPlanId"]
            try:
                selections = backup.list_backup_selections(BackupPlanId=plan_id)["BackupSelectionsList"]
                for sel in selections:
                    sel_detail = backup.get_backup_selection(
                        BackupPlanId=plan_id, SelectionId=sel["SelectionId"])["BackupSelection"]
                    resources = sel_detail.get("Resources", [])
                    if not resources or any(cluster_arn in r or "*" in r for r in resources):
                        covered = True
                plan_detail = backup.get_backup_plan(BackupPlanId=plan_id)["BackupPlan"]
                for rule in plan_detail.get("Rules", []):
                    if rule.get("CopyActions"):
                        cross_region = True
            except Exception:
                pass

        if covered:
            detail = "Cross-region copy configured" if cross_region else \
                     "Consider adding cross-region copy for DR"
            return [{"pillar": "Reliability", "id": "REL1b",
                     "label": "AWS Backup plan covers this cluster",
                     "status": "pass", "detail": detail}]
        else:
            return [{"pillar": "Reliability", "id": "REL1b",
                     "label": "No AWS Backup plan found for this cluster",
                     "status": "warn",
                     "detail": "Use AWS Backup for centralized backup governance"}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL1b",
                 "label": f"Cannot check AWS Backup: {e}",
                 "status": "warn", "detail": ""}]


@register_check("REL1c", "Reliability", "Global cluster",
                source="infrastructure", priority=12)
def check_global_cluster(ctx):
    try:
        gc_resp = ctx.docdb_client.describe_global_clusters()
        cluster_arn = ctx.cluster.get("DBClusterArn", "")
        for gc in gc_resp.get("GlobalClusters", []):
            member_arns = [m.get("DBClusterArn", "") for m in gc.get("GlobalClusterMembers", [])]
            if cluster_arn in member_arns:
                gc_id = gc.get("GlobalClusterIdentifier")
                return [{"pillar": "Reliability", "id": "REL1c",
                         "label": f"Global cluster: configured ({gc_id})",
                         "status": "pass", "detail": ""}]
        return [{"pillar": "Reliability", "id": "REL1c",
                 "label": "Global cluster: not configured",
                 "status": "warn",
                 "detail": "Consider global clusters for cross-region DR with < 1 min RTO"}]
    except Exception:
        return [{"pillar": "Reliability", "id": "REL1c",
                 "label": "Global cluster: not configured",
                 "status": "info", "detail": ""}]


@register_check("REL5a", "Reliability", "Instance count",
                source="infrastructure", priority=20)
def check_instance_count(ctx):
    n = len(ctx.instances)
    status = "pass" if n >= 2 else "fail"
    detail = "Minimum 2 instances required for auto failover" if n < 2 else ""
    return [{"pillar": "Reliability", "id": "REL5a",
             "label": f"Instance count ({n})",
             "status": status, "detail": detail}]


@register_check("REL5b", "Reliability", "Multi-AZ deployment",
                source="infrastructure", priority=21)
def check_multi_az(ctx):
    azs = set(i.get("AvailabilityZone", "") for i in ctx.instances) - {""}
    n_azs = len(azs)
    az_list = ", ".join(sorted(azs)) if azs else "—"
    status = "pass" if n_azs >= 2 else "fail"
    detail = "" if n_azs >= 2 else "Deploy instances across 2+ AZs for automatic failover"
    return [{"pillar": "Reliability", "id": "REL5b",
             "label": f"Instances across {n_azs} AZ(s): {az_list}",
             "status": status, "detail": detail}]


@register_check("REL5c", "Reliability", "Replica lag",
                source="cloudwatch", priority=22)
def check_replica_lag(ctx):
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="DBClusterReplicaLagMaximum",
            Dimensions=[{"Name": "DBClusterIdentifier", "Value": ctx.cluster_id}],
            StartTime=start, EndTime=end, Period=3600,
            Statistics=["Average", "Maximum"])
        dps = resp.get("Datapoints", [])
        if dps:
            max_ms = max(d["Maximum"] for d in dps)  # Already in milliseconds
            avg_ms = sum(d["Average"] for d in dps) / len(dps)
            status = "pass" if max_ms < 100 else "warn" if max_ms < 1000 else "fail"
            detail = "" if max_ms < 100 else \
                     "Replica lag > 1s — alarm if > 1000 ms" if max_ms >= 1000 else \
                     "Replica lag elevated — healthy clusters typically < 20 ms"
            return [{"pillar": "Reliability", "id": "REL5c",
                     "label": f"Replica lag — avg {avg_ms:.0f} ms, max {max_ms:.0f} ms",
                     "status": status, "detail": detail}]
        return [{"pillar": "Reliability", "id": "REL5c",
                 "label": "No replica lag data (single instance or no readers)",
                 "status": "info", "detail": ""}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL5c",
                 "label": f"Cannot check replica lag: {e}",
                 "status": "warn", "detail": ""}]


@register_check("REL6d", "Reliability", "Failover events",
                source="infrastructure", priority=30)
def check_failover_events(ctx):
    try:
        evt_end = datetime.utcnow()
        evt_start = evt_end - timedelta(days=13)
        events = ctx.docdb_client.describe_events(
            SourceIdentifier=ctx.cluster_id, SourceType="db-cluster",
            StartTime=evt_start, EndTime=evt_end)
        failover_events = [e for e in events.get("Events", [])
                          if "failover" in e.get("Message", "").lower()
                          or "failover" in ",".join(e.get("EventCategories", [])).lower()]
        if failover_events:
            return [{"pillar": "Reliability", "id": "REL6d",
                     "label": f"{len(failover_events)} failover event(s) in last 13 days",
                     "status": "warn",
                     "detail": f"Most recent: {failover_events[-1].get('Message', '')[:120]}"}]
        # Silent when no failovers
        return []
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL6d",
                 "label": f"Cannot check events: {e}",
                 "status": "warn", "detail": ""}]


@register_check("REL4a", "Reliability", "Cursor timeouts",
                source="cloudwatch", per_instance=True, priority=31)
def check_cursor_timeouts(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="DatabaseCursorsTimedOut",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=86400, Statistics=["Sum"])
        dps = resp.get("Datapoints", [])
        total = sum(d["Sum"] for d in dps) if dps else 0
        if total > 0:
            return [{"pillar": "Reliability", "id": "REL4a",
                     "label": f"Cursor timeouts on {iid}: {int(total)} in 7 days",
                     "status": "warn",
                     "detail": "Application not closing cursors properly"}]
        # Silent when no timeouts
        return []
    except Exception:
        return []


@register_check("REL6e", "Reliability", "MVCC ID availability",
                source="cloudwatch", per_instance=True, writer_only=True, priority=32)
def check_mvcc_ids(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    MAX_MVCC = 1_400_000_000
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="AvailableMVCCIds",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Minimum"])
        dps = resp.get("Datapoints", [])
        if dps:
            min_ids = min(d["Minimum"] for d in dps)
            pct = min_ids / MAX_MVCC * 100
            status = "pass" if pct >= 50 else "warn" if pct >= 25 else "fail"
            detail = "" if pct >= 50 else \
                     "MVCC IDs critically low — long-running queries blocking GC" if pct < 25 else \
                     "MVCC IDs declining — check for long-running queries"
            return [{"pillar": "Reliability", "id": "REL6e",
                     "label": f"MVCC IDs on writer: {pct:.0f}% available",
                     "status": status, "detail": detail}]
        return []
    except Exception:
        return []


@register_check("REL2a", "Reliability", "Exception handling & retry logic",
                source="infrastructure", priority=40)
def check_exception_handling(ctx):
    """App-side guidance (REL 2 — Exception Handling). Always emitted as info.

    Not detectable via AWS API, so this runs in the plugin path (which always
    executes) rather than relying on the optional v2 extras thread — that keeps
    the "REL 2" group present in both the UI and the PDF on every run.
    """
    return [{"pillar": "Reliability", "id": "REL2a",
             "label": "Exception handling & retry logic — app-side configuration",
             "status": "info",
             "detail": "Implement exponential backoff with jitter for transient "
                       "errors. Connect via the cluster endpoint with replicaSet=rs0. "
                       "Not detectable via AWS API — verify in application code."}]


@register_check("REL3a", "Reliability", "retryWrites configuration",
                source="infrastructure", priority=39)
def check_retry_writes(ctx):
    """App-side reminder (REL 3 — Connection Management). Always info.

    DocumentDB does not support retryable writes; the driver must set
    retryWrites=false. Not detectable via AWS API.
    """
    return [{"pillar": "Reliability", "id": "REL3a",
             "label": "retryWrites=false required — driver configuration",
             "status": "info",
             "detail": "DocumentDB does not support retryable writes. Set "
                       "retryWrites=false in your connection string/driver options. "
                       "Not detectable via AWS API — verify in application code."}]


@register_check("REL3b", "Reliability", "Connection management",
                source="infrastructure", priority=41)
def check_connection_management(ctx):
    """App-side guidance (REL 3 — Connection Management). Always info."""
    return [{"pillar": "Reliability", "id": "REL3b",
             "label": "Connection management — app-side configuration",
             "status": "info",
             "detail": "Configure maxPoolSize and maxIdleTimeMS in your driver to "
                       "bound and recycle connections. "
                       "Not detectable via AWS API — verify in application code."}]


@register_check("REL3c", "Reliability", "Connection & query handling",
                source="infrastructure", priority=43)
def check_cluster_endpoint(ctx):
    """App-side guidance (REL 3 — Connection Management). Always info."""
    return [{"pillar": "Reliability", "id": "REL3c",
             "label": "Connection & query handling — app-side configuration",
             "status": "info",
             "detail": "Connect via the cluster endpoint with replicaSet=rs0 so the "
                       "driver discovers the writer and reader topology. Use "
                       "allowDiskUse:true for large aggregations and terminate "
                       "abandoned queries via db.killOp(). "
                       "Not detectable via AWS API — verify in application code."}]


@register_check("REL4b", "Reliability", "Cursor management",
                source="infrastructure", priority=42)
def check_cursor_management(ctx):
    """App-side guidance (REL 4 — Cursor Management). Always info."""
    return [{"pillar": "Reliability", "id": "REL4b",
             "label": "Cursor management — app-side configuration",
             "status": "info",
             "detail": "Configure the correct batchSize() in your application and "
                       "set a cursor timeoutMS in your driver configuration. "
                       "Not detectable via AWS API — verify in application code."}]


# ── Migrated from tabs/wa_v2/reliability.py (Phase 3B) ────────────────────────

@register_check("REL1f", "Reliability", "Cross-region snapshot copies",
                source="infrastructure", priority=13)
def check_cross_region_snapshots(ctx):
    """REL 1 — Cross-region snapshot copies for DR."""
    try:
        snaps = ctx.docdb_client.describe_db_cluster_snapshots(
            DBClusterIdentifier=ctx.cluster_id,
            SnapshotType="manual")["DBClusterSnapshots"]
        cross_region = [s for s in snaps
                        if s.get("SourceDBClusterSnapshotArn", "") and
                        ctx.region not in s.get("SourceDBClusterSnapshotArn", "")]
        if cross_region:
            return [{"pillar": "Reliability", "id": "REL1f",
                     "label": f"{len(cross_region)} cross-region snapshot copy(s) found",
                     "status": "pass",
                     "detail": "Cross-region copies support DR with flexible RTO"}]
        return [{"pillar": "Reliability", "id": "REL1f",
                 "label": "No cross-region snapshot copies found",
                 "status": "info",
                 "detail": "Copy snapshots to another region for DR scenarios "
                           "where sub-minute RTO is not required"}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL1f",
                 "label": f"Cannot check cross-region snapshots: {e}",
                 "status": "warn", "detail": str(e)}]


@register_check("REL6a", "Reliability", "DatabaseConnections alarm",
                source="cloudwatch", priority=50)
def check_alarm_connections(ctx):
    """REL 6 — CW alarm for DatabaseConnections."""
    try:
        all_alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        alarm_metrics = {a["MetricName"] for a in all_alarms
                         if any(ctx.cluster_id in str(d)
                                for d in a.get("Dimensions", []))}
        if "DatabaseConnections" in alarm_metrics:
            return [{"pillar": "Reliability", "id": "REL6a",
                     "label": "DatabaseConnections alarm configured", "status": "pass",
                     "detail": ""}]
        return [{"pillar": "Reliability", "id": "REL6a",
                 "label": "DatabaseConnections alarm not configured", "status": "warn",
                 "detail": "Alarm at 80% of instance connection limit (max 30,000)"}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL6a",
                 "label": f"Cannot check alarms: {e}", "status": "warn", "detail": str(e)}]


@register_check("REL6b", "Reliability", "DatabaseCursorsTimedOut alarm",
                source="cloudwatch", priority=51)
def check_alarm_cursors_timed_out(ctx):
    """REL 6 — CW alarm for DatabaseCursorsTimedOut."""
    try:
        all_alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        alarm_metrics = {a["MetricName"] for a in all_alarms
                         if any(ctx.cluster_id in str(d)
                                for d in a.get("Dimensions", []))}
        if "DatabaseCursorsTimedOut" in alarm_metrics:
            return [{"pillar": "Reliability", "id": "REL6b",
                     "label": "DatabaseCursorsTimedOut alarm configured", "status": "pass",
                     "detail": ""}]
        return [{"pillar": "Reliability", "id": "REL6b",
                 "label": "DatabaseCursorsTimedOut alarm not configured", "status": "warn",
                 "detail": "Alarm when cursors time out — indicates the app is not closing cursors"}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL6b",
                 "label": f"Cannot check alarms: {e}", "status": "warn", "detail": str(e)}]


@register_check("REL6c", "Reliability", "DatabaseCursors alarm",
                source="cloudwatch", priority=52)
def check_alarm_cursors(ctx):
    """REL 6 — CW alarm for DatabaseCursors."""
    try:
        all_alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        alarm_metrics = {a["MetricName"] for a in all_alarms
                         if any(ctx.cluster_id in str(d)
                                for d in a.get("Dimensions", []))}
        if "DatabaseCursors" in alarm_metrics:
            return [{"pillar": "Reliability", "id": "REL6c",
                     "label": "DatabaseCursors alarm configured", "status": "pass",
                     "detail": ""}]
        return [{"pillar": "Reliability", "id": "REL6c",
                 "label": "DatabaseCursors alarm not configured", "status": "warn",
                 "detail": "Alarm at 80% of cursor limit (max 4,560)"}]
    except Exception as e:
        return [{"pillar": "Reliability", "id": "REL6c",
                 "label": f"Cannot check alarms: {e}", "status": "warn", "detail": str(e)}]
