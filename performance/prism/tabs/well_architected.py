"""Well-Architected Review tab — DocumentDB cluster assessment."""
import json
import os
import re
import logging
import threading
import boto3
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update
from tabs.ui_helpers import metric_card, section_title

logger = logging.getLogger(__name__)

# ── Load WA advisor prompt from SKILL file ────────────────────────────────────
_WA_ADVISOR_PROMPT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "documentdb-well-architected-review", "wa-advisor-prompt.md"
)


def _load_wa_advisor_prompt():
    """Load the WA advisor system prompt from documentdb-well-architected-review/wa-advisor-prompt.md.

    Strips YAML frontmatter if present. Falls back to a minimal prompt if file missing.
    """
    try:
        with open(_WA_ADVISOR_PROMPT_PATH) as f:
            text = f.read()
        # Strip YAML frontmatter
        if text.startswith("---"):
            end = text.find("---", 3)
            if end > 0:
                text = text[end + 3:].strip()
        return text
    except FileNotFoundError:
        logger.warning("WA advisor prompt file not found: %s", _WA_ADVISOR_PROMPT_PATH)
        return ("You are an Amazon DocumentDB Well-Architected advisor. "
                "Provide actionable recommendations based on failing checks. "
                "Return JSON: {pillar: [{action, why, impact, priority}]}")


_wa = {"results": [], "running": False, "done": False, "error": None, "ai_md": None}
_wa_lock = threading.Lock()

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
GRAVITON_FAMILIES = ("r6g", "r7g", "r8g", "t4g", "r6gd")

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


def _add(results, pillar, check_id, label, status, detail=""):
    results.append({"pillar": pillar, "id": check_id, "label": label, "status": status, "detail": detail})


# ── Plugin-based WA checks (new system) ──────────────────────────────────────

def _run_wa_checks_via_plugins(cluster_id, region, analysis_data=None):
    """Run WA checks using the wa_checks plugin registry.

    Returns list of check result dicts, or None if plugin system unavailable.
    """
    try:
        from wa_checks import run_checks
    except ImportError:
        logger.debug("wa_checks plugin system not available, using legacy path")
        return None

    try:
        docdb = boto3.client("docdb", region_name=region)
        cw = boto3.client("cloudwatch", region_name=region)
        ec2 = boto3.client("ec2", region_name=region)

        cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        insts = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]

        results = run_checks(
            cluster_id=cluster_id,
            region=region,
            cluster=cl,
            instances=insts,
            analysis_data=analysis_data,
            docdb_client=docdb,
            cw_client=cw,
            ec2_client=ec2,
        )
        logger.info("Plugin WA checks complete: %d results", len(results))
        return results
    except Exception as e:
        logger.warning("Plugin WA checks failed, falling back to legacy: %s", e)
        return None


def _run_wa_checks(cluster_id, region, analysis_data=None):
    """Run all Well-Architected checks. Called in background thread.

    Uses plugin system (wa_checks/) when available, falls back to legacy inline checks.
    """
    with _wa_lock:
        _wa.update(results=[], running=True, done=False, error=None, ai_md=None)

    # ── Try plugin system first ──────────────────────────────────────────────
    plugin_results = _run_wa_checks_via_plugins(cluster_id, region, analysis_data)
    if plugin_results is not None:
        # Plugin system succeeded — add database-level checks and AI recs
        results = plugin_results

        # Run database-level checks (these are still in legacy _run_db_checks for now)
        try:
            db_checks = _run_db_checks(analysis_data)
            results.extend(db_checks)
        except Exception as e:
            logger.warning("Database-level checks failed: %s", e)

        if not analysis_data:
            _add(results, "Other", "INFO",
                 "Database-level checks skipped — run Analyze on a database first", "info")

        # Set done immediately so UI can render check results
        with _wa_lock:
            _wa.update(results=results, running=False, done=True, ai_md=None)
        logger.info("WA review complete (plugin path): %d checks", len(results))

        # AI recommendations (async — updates ai_md when ready, UI will pick up on next poll)
        ai_md = None
        try:
            ai_md = _generate_ai_recommendations(results, cluster_id, region, analysis_data)
            with _wa_lock:
                _wa["ai_md"] = ai_md
        except Exception as e:
            logger.warning("AI recommendations failed: %s", e)

        # Persist
        try:
            from agent_memory import save_wa_results
            save_wa_results(cluster_id, results)
        except Exception:
            pass
        return

    # ── Fallback: legacy inline checks ───────────────────────────────────────
    logger.info("Using legacy WA check path")

    results = []
    try:
        docdb = boto3.client("docdb", region_name=region)
        cw = boto3.client("cloudwatch", region_name=region)
        ec2 = boto3.client("ec2", region_name=region)

        # Describe cluster
        try:
            cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        except Exception as e:
            _add(results, "Other", "ERR", f"Cannot describe cluster: {e}", "fail")
            with _wa_lock:
                _wa.update(results=results, running=False, done=True)
            return

        # Describe instances
        try:
            insts = docdb.describe_db_instances(
                Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]
        except Exception as e:
            insts = []
            _add(results, "Other", "ERR", f"Cannot describe instances: {e}", "fail")

        # ── RELIABILITY ──────────────────────────────────────────────────────
        # REL1a: Backup retention (sub-point 1)
        retention = cl.get("BackupRetentionPeriod", 1)
        _add(results, "Reliability", "REL1a",
             f"Backup retention period ({retention} days)",
             "pass" if retention >= 7 else "warn" if retention >= 3 else "fail",
             "Recommended: 7+ days for production" if retention < 7 else "")

        # REL1b: AWS Backup plan (sub-points REL1.2 + REL2.2)
        try:
            backup = boto3.client("backup", region_name=region)
            plans = backup.list_backup_plans()["BackupPlansList"]
            # Check if any plan has a selection covering this cluster
            cluster_arn = cl.get("DBClusterArn", "")
            covered = False
            cross_region = False
            for plan in plans:
                plan_id = plan["BackupPlanId"]
                try:
                    selections = backup.list_backup_selections(BackupPlanId=plan_id)["BackupSelectionsList"]
                    for sel in selections:
                        sel_detail = backup.get_backup_selection(
                            BackupPlanId=plan_id,
                            SelectionId=sel["SelectionId"])["BackupSelection"]
                        resources = sel_detail.get("Resources", [])
                        if not resources or any(cluster_arn in r or "*" in r for r in resources):
                            covered = True
                    # Check for cross-region copy actions
                    plan_detail = backup.get_backup_plan(BackupPlanId=plan_id)["BackupPlan"]
                    for rule in plan_detail.get("Rules", []):
                        if rule.get("CopyActions"):
                            cross_region = True
                except Exception:
                    pass
            if covered:
                _add(results, "Reliability", "REL1b",
                     f"AWS Backup plan covers this cluster", "pass",
                     "Cross-region copy configured" if cross_region else
                     "Consider adding cross-region copy for DR (REL2)")
            else:
                _add(results, "Reliability", "REL1b",
                     "No AWS Backup plan found for this cluster", "warn",
                     "Use AWS Backup for centralized backup governance across clusters and regions")
        except Exception as e:
            _add(results, "Reliability", "REL1b", f"Cannot check AWS Backup: {e}", "warn")

        # REL1c: Global cluster (covers REL1.3 + REL2.1 — low RTO)
        is_global = False
        global_cluster_id = None
        try:
            gc_resp = docdb.describe_global_clusters()
            for gc in gc_resp.get("GlobalClusters", []):
                member_arns = [m.get("DBClusterArn", "") for m in gc.get("GlobalClusterMembers", [])]
                if cl.get("DBClusterArn") in member_arns:
                    is_global = True
                    global_cluster_id = gc.get("GlobalClusterIdentifier")
                    break
        except Exception:
            pass
        _add(results, "Reliability", "REL1c",
             f"Global cluster: {'configured (' + global_cluster_id + ')' if is_global else 'not configured'}",
             "pass" if is_global else "warn",
             "" if is_global else
             "Consider global clusters for cross-region DR with < 1 min RTO (REL1.3, REL2.1)")

        # REL1d: Manual snapshots for compliance/archival (REL1.4)
        try:
            snaps = docdb.describe_db_cluster_snapshots(
                DBClusterIdentifier=cluster_id,
                SnapshotType="manual")["DBClusterSnapshots"]
            if snaps:
                _add(results, "Reliability", "REL1d",
                     f"{len(snaps)} manual snapshot(s) exist", "pass",
                     "Ensure lifecycle policies are defined via AWS Backup to avoid unbounded storage costs")
            else:
                _add(results, "Reliability", "REL1d",
                     "No manual snapshots found", "info",
                     "Create manual snapshots for compliance or archival requirements")
        except Exception as e:
            _add(results, "Reliability", "REL1d", f"Cannot check manual snapshots: {e}", "warn")

        # REL2b: DR testing note (REL2.4 — not detectable, always info)
        _add(results, "Reliability", "REL2",
             "DR procedures: test failover and restore regularly", "info",
             "Use managed Global Cluster Failover to simulate regional failover in non-prod environments")

        del_prot = cl.get("DeletionProtection", False)
        _add(results, "Security", "SEC8a",
             f"Deletion protection ({'enabled' if del_prot else 'disabled'})",
             "pass" if del_prot else "fail",
             "" if del_prot else "Enable deletion protection for production clusters")

        # REL2a: Exception handling — app-side note (REL 2 — Exception Handling)
        _add(results, "Reliability", "REL2a",
             "Exception handling & retry logic — app-side configuration", "info",
             "Implement exponential backoff + jitter for transient errors. "
             "Connect via cluster endpoint with replicaSet=rs0. "
             "Not detectable via AWS API — verify in application code.")

        # REL3a: retryWrites reminder — app-side note (REL 3 — Connection Management)
        _add(results, "Reliability", "REL3a",
             "retryWrites=false required — driver configuration", "info",
             "DocumentDB does not support retryable writes. Set retryWrites=false "
             "in your connection string/driver options. "
             "Not detectable via AWS API — verify in application code.")

        # REL3b: Connection management — app-side note (REL 3 — Connection Management)
        _add(results, "Reliability", "REL3b",
             "Connection management — app-side configuration", "info",
             "Configure maxPoolSize and maxIdleTimeMS in your driver to bound and "
             "recycle connections. "
             "Not detectable via AWS API — verify in application code.")

        # REL3c: Cluster endpoint connection — app-side note (REL 3 — Connection Management)
        _add(results, "Reliability", "REL3c",
             "Connection & query handling — app-side configuration", "info",
             "Connect via the cluster endpoint with replicaSet=rs0 so the driver "
             "discovers the writer and reader topology. Use allowDiskUse:true for "
             "large aggregations and terminate abandoned queries via db.killOp(). "
             "Not detectable via AWS API — verify in application code.")

        # REL4b: Cursor management — app-side note (REL 4 — Cursor Management)
        _add(results, "Reliability", "REL4b",
             "Cursor management — app-side configuration", "info",
             "Configure the correct batchSize() in your application and set a cursor "
             "timeoutMS in your driver configuration. "
             "Not detectable via AWS API — verify in application code.")

        n_inst = len(insts)
        _add(results, "Reliability", "REL5a",
             f"Instance count ({n_inst})",
             "pass" if n_inst >= 2 else "fail",
             "Minimum 2 instances required for auto failover" if n_inst < 2 else "")

        azs = set(i.get("AvailabilityZone", "") for i in insts) - {""}
        n_azs = len(azs)
        az_list = ", ".join(sorted(azs)) if azs else "—"
        _add(results, "Reliability", "REL5b",
             f"Instances across {n_azs} AZ(s): {az_list}",
             "pass" if n_azs >= 2 else "fail",
             "" if n_azs >= 2 else "Deploy instances across 2+ AZs for automatic failover")

        # ── SECURITY ─────────────────────────────────────────────────────────
        encrypted = cl.get("StorageEncrypted", False)
        _add(results, "Security", "SEC1a",
             f"Encryption at rest ({'enabled' if encrypted else 'disabled'})",
             "pass" if encrypted else "fail",
             "" if encrypted else "Enable encryption at rest (requires new cluster)")

        tls_val = "unknown"
        try:
            pg_name = cl.get("DBClusterParameterGroup", "")
            if pg_name:
                params = docdb.describe_db_cluster_parameters(DBClusterParameterGroupName=pg_name)
                for p in params.get("Parameters", []):
                    if p.get("ParameterName") == "tls":
                        tls_val = p.get("ParameterValue", "enabled")
                        break
        except Exception as e:
            tls_val = f"check failed: {e}"
        _add(results, "Security", "SEC1b",
             f"TLS ({tls_val})",
             "pass" if tls_val == "enabled" else "fail",
             "" if tls_val == "enabled" else "TLS should be enabled")

        # Security groups
        vpc_sgs = cl.get("VpcSecurityGroups", [])
        sg_open = False
        sg_checked = 0
        sg_flagged = set()
        for vsg in vpc_sgs:
            sg_id = vsg.get("VpcSecurityGroupId", "")
            if not sg_id:
                continue
            try:
                sg_detail = ec2.describe_security_groups(GroupIds=[sg_id])["SecurityGroups"][0]
                sg_checked += 1
                for rule in sg_detail.get("IpPermissions", []):
                    for ip_range in rule.get("IpRanges", []):
                        if ip_range.get("CidrIp") in ("0.0.0.0/0", "::/0") and sg_id not in sg_flagged:
                            sg_open = True
                            sg_flagged.add(sg_id)
                            _add(results, "Security", "SEC2a",
                                 f"Security group {sg_id} open to 0.0.0.0/0", "fail",
                                 "Restrict to specific CIDR ranges")
            except Exception as e:
                _add(results, "Security", "SEC2a",
                     f"Cannot check SG {sg_id}: {e}", "warn")
        if not sg_open and sg_checked > 0:
            _add(results, "Security", "SEC2a",
                 f"Security groups properly restricted ({sg_checked} checked)", "pass")

        logs = cl.get("EnabledCloudwatchLogsExports", [])
        audit_enabled = "audit" in logs
        profiler_enabled = "profiler" in logs
        _add(results, "Security", "SEC5",
             f"Audit logging ({'enabled' if audit_enabled else 'disabled'})",
             "pass" if audit_enabled else "info",
             "" if audit_enabled else "Optional — enable if compliance or access tracking is required")

        # ── OPERATIONAL EXCELLENCE ───────────────────────────────────────────
        sg_name = cl.get("DBSubnetGroup", "")
        try:
            if sg_name:
                sg = docdb.describe_db_subnet_groups(DBSubnetGroupName=sg_name)["DBSubnetGroups"][0]
                sg_azs = set(s["SubnetAvailabilityZone"]["Name"] for s in sg.get("Subnets", []))
                _add(results, "Operational Excellence", "OPS2",
                     f"Subnet group spans {len(sg_azs)} AZ(s)",
                     "pass" if len(sg_azs) >= 3 else "warn",
                     "Recommended: 3 AZs for failover flexibility" if len(sg_azs) < 3 else "")
        except Exception as e:
            _add(results, "Operational Excellence", "OPS2",
                 f"Cannot check subnet group: {e}", "warn")

        _add(results, "Operational Excellence", "OPS5a",
             f"Profiler logging ({'enabled' if profiler_enabled else 'disabled'})",
             "pass" if profiler_enabled else "warn",
             "" if profiler_enabled else "Enable profiler for slow query analysis")

        try:
            alarms = cw.describe_alarms(AlarmNamePrefix=cluster_id)
            n_alarms = len(alarms.get("MetricAlarms", []))
            _add(results, "Operational Excellence", "OPS5b",
                 f"CloudWatch alarms ({n_alarms} configured)",
                 "pass" if n_alarms >= 3 else "warn" if n_alarms > 0 else "fail",
                 "Recommended: alarms for CPU, FreeableMemory, DatabaseConnections" if n_alarms < 3 else "")
        except Exception as e:
            _add(results, "Operational Excellence", "OPS5b",
                 f"Cannot check alarms: {e}", "warn")

        # ── COST OPTIMIZATION ────────────────────────────────────────────────
        try:
            tags_resp = docdb.list_tags_for_resource(ResourceName=cl["DBClusterArn"])
            n_tags = len(tags_resp.get("TagList", []))
            _add(results, "Cost Optimization", "COST6",
                 f"Cost allocation tags ({n_tags} tags)",
                 "pass" if n_tags >= 2 else "warn",
                 "Add cost allocation tags for expense tracking" if n_tags < 2 else "")
        except Exception as e:
            _add(results, "Cost Optimization", "COST6", f"Cannot check tags: {e}", "warn")

        end = datetime.utcnow()
        start = end - timedelta(days=7)

        for inst in insts:
            iid = inst["DBInstanceIdentifier"]
            itype = inst["DBInstanceClass"]
            dim = [{"Name": "DBInstanceIdentifier", "Value": iid}]

            # CPU utilization
            try:
                resp = cw.get_metric_statistics(Namespace="AWS/DocDB", MetricName="CPUUtilization",
                    Dimensions=dim, StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
                dps = [d["Average"] for d in resp.get("Datapoints", [])]
                if dps:
                    avg_cpu = sum(dps) / len(dps)
                    p95_cpu = sorted(dps)[int(len(dps) * 0.95)]
                    _add(results, "Cost Optimization", "COST1",
                         f"CPU for {iid} (avg {avg_cpu:.1f}%, P95 {p95_cpu:.1f}%)",
                         "warn" if p95_cpu < 10 else "pass",
                         f"Instance {itype} may be oversized" if p95_cpu < 10 else "")
                else:
                    _add(results, "Cost Optimization", "COST1",
                         f"No CPU data for {iid}", "warn", "No datapoints in last 7 days")
            except Exception as e:
                _add(results, "Cost Optimization", "COST1", f"Cannot check CPU for {iid}: {e}", "warn")

            # Graviton
            family = itype.replace("db.", "").split(".")[0] if itype.startswith("db.") else ""
            _add(results, "Sustainability", "SUST1",
                 f"{iid} {'uses' if family in GRAVITON_FAMILIES else 'is not'} Graviton ({itype})",
                 "pass" if family in GRAVITON_FAMILIES else "warn",
                 "" if family in GRAVITON_FAMILIES else "Migrate to Graviton (r6g/r7g/r8g) for better price-performance")

            # Buffer cache hit ratio
            try:
                resp = cw.get_metric_statistics(Namespace="AWS/DocDB", MetricName="BufferCacheHitRatio",
                    Dimensions=dim, StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
                dps = [d["Average"] for d in resp.get("Datapoints", [])]
                if dps:
                    avg_cache = sum(dps) / len(dps)
                    _add(results, "Performance Efficiency", "PERF6",
                         f"Buffer cache hit ratio for {iid} ({avg_cache:.1f}%)",
                         "pass" if avg_cache >= 99 else "warn" if avg_cache >= 95 else "fail",
                         "Working set may not fit in memory" if avg_cache < 95 else "")
                else:
                    _add(results, "Performance Efficiency", "PERF6",
                         f"No cache data for {iid}", "warn")
            except Exception as e:
                _add(results, "Performance Efficiency", "PERF6", f"Cannot check cache for {iid}: {e}", "warn")

            # Connections vs limits
            try:
                resp = cw.get_metric_statistics(Namespace="AWS/DocDB", MetricName="DatabaseConnections",
                    Dimensions=dim, StartTime=start, EndTime=end, Period=3600, Statistics=["Maximum"])
                dps = [d["Maximum"] for d in resp.get("Datapoints", [])]
                limit = CONN_LIMITS.get(itype, 0)
                if dps and limit:
                    max_conn = max(dps)
                    pct = max_conn / limit * 100
                    _add(results, "Performance Efficiency", "PERF5",
                         f"Peak connections for {iid} ({int(max_conn)}/{limit} = {pct:.0f}%)",
                         "pass" if pct < 70 else "warn" if pct < 90 else "fail",
                         "Consider upsizing or connection pooling" if pct >= 70 else "")
                elif dps:
                    _add(results, "Performance Efficiency", "PERF5",
                         f"Peak connections for {iid} ({int(max(dps))}), unknown limit for {itype}", "info")
            except Exception as e:
                _add(results, "Performance Efficiency", "PERF5", f"Cannot check connections for {iid}: {e}", "warn")

        # OPS8: Engine version — informational, not a major concern
        engine_ver = cl.get("EngineVersion", "unknown")
        major = engine_ver.split(".")[0] if engine_ver != "unknown" else ""
        if major in ("3", "4"):
            _add(results, "Operational Excellence", "OPS8",
                 f"Engine version {engine_ver} — end-of-life", "warn",
                 "v3.6 reached EOL Sep 2024 — plan upgrade to 5.0 or 8.0")
        elif major == "5":
            _add(results, "Operational Excellence", "OPS8",
                 f"Engine version {engine_ver}", "info",
                 "8.0 available — offers Zstandard compression and Query Planner v3")
        else:
            _add(results, "Operational Excellence", "OPS8",
                 f"Engine version {engine_ver}", "info", "")

        # REL5c: Replica lag (Doug REL7 — monitor reliability)
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/DocDB", MetricName="DBClusterReplicaLagMaximum",
                Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
                StartTime=end - timedelta(days=7), EndTime=end,
                Period=3600, Statistics=["Average", "Maximum"])
            dps = resp.get("Datapoints", [])
            if dps:
                avg_lag = sum(d["Average"] for d in dps) / len(dps)
                max_lag = max(d["Maximum"] for d in dps)
                avg_ms = avg_lag / 1000
                max_ms = max_lag / 1000
                _add(results, "Reliability", "REL5c",
                     f"Replica lag — avg {avg_ms:.0f} ms, max {max_ms:.0f} ms (7 days)",
                     "pass" if max_ms < 100 else "warn" if max_ms < 1000 else "fail",
                     "" if max_ms < 100 else
                     "Replica lag > 1s detected — alarm if DBClusterReplicaLagMaximum > 1000 ms"
                     if max_ms >= 1000 else
                     "Replica lag elevated — healthy clusters typically < 20 ms")
            else:
                _add(results, "Reliability", "REL5c",
                     "No replica lag data (single instance or no readers)", "info")
        except Exception as e:
            _add(results, "Reliability", "REL5c", f"Cannot check replica lag: {e}", "warn")

        # REL6: IaC deployment detection (Doug REL6 — automate database deployments)
        try:
            cfn = boto3.client("cloudformation", region_name=region)
            stacks = cfn.describe_stacks()["Stacks"]
            matching = [s["StackName"] for s in stacks
                        if any(cluster_id.lower() in str(v).lower()
                               for v in [s.get("StackName", ""),
                                         str(s.get("Parameters", "")),
                                         str(s.get("Tags", ""))])]
            if matching:
                _add(results, "Reliability", "REL6",
                     f"IaC deployment detected: {matching[0]}", "pass",
                     "Cluster managed via CloudFormation")
            else:
                _add(results, "Reliability", "REL6",
                     "No CloudFormation stack detected for this cluster", "warn",
                     "Use CloudFormation, CDK, or Terraform for consistent, repeatable deployments")
        except Exception as e:
            _add(results, "Reliability", "REL6",
                 f"Cannot check IaC deployment: {e}", "warn")

        # REL7: Recent failover events (13 days) — only surface if failovers occurred
        try:
            evt_end = datetime.utcnow()
            evt_start = evt_end - timedelta(days=13)
            events = docdb.describe_events(
                SourceIdentifier=cluster_id, SourceType="db-cluster",
                StartTime=evt_start, EndTime=evt_end)
            failover_events = [e for e in events.get("Events", [])
                               if "failover" in e.get("Message", "").lower()
                               or "failover" in ",".join(e.get("EventCategories", [])).lower()]
            if failover_events:
                _add(results, "Reliability", "REL6d",
                     f"{len(failover_events)} failover event(s) in last 13 days", "warn",
                     f"Most recent: {failover_events[-1].get('Message', '')[:120]}")
        except Exception as e:
            _add(results, "Reliability", "REL6d", f"Cannot check events: {e}", "warn")

        # SEC6: TLS minimum version
        tls_version_val = "unknown"
        try:
            pg_name_sec = cl.get("DBClusterParameterGroup", "")
            if pg_name_sec:
                params_sec = docdb.describe_db_cluster_parameters(DBClusterParameterGroupName=pg_name_sec)
                for p in params_sec.get("Parameters", []):
                    if p.get("ParameterName") == "tls_version":
                        tls_version_val = p.get("ParameterValue", "unknown")
                        break
        except Exception:
            pass
        if tls_version_val != "unknown":
            is_tls12 = "1.2" in tls_version_val and "1.0" not in tls_version_val and "1.1" not in tls_version_val
            _add(results, "Security", "SEC1d",
                 f"TLS minimum version: {tls_version_val}",
                 "pass" if is_tls12 else "warn",
                 "" if is_tls12 else "Set tls_version to TLSv1.2 to disable older protocols")

        # SEC3: Secrets Manager
        cluster_endpoint = cl.get("Endpoint", "")
        try:
            sm = boto3.client("secretsmanager", region_name=region)
            found_secret = False
            for page in sm.get_paginator("list_secrets").paginate():
                for s in page.get("SecretList", []):
                    name_lower = (s.get("Name", "") or "").lower()
                    desc_lower = (s.get("Description", "") or "").lower()
                    if (cluster_id.lower() in name_lower
                            or cluster_id.lower() in desc_lower
                            or cluster_endpoint.lower() in desc_lower):
                        found_secret = True
                        break
                if found_secret:
                    break
            _add(results, "Security", "SEC3",
                 f"Secrets Manager {'references' if found_secret else 'does not reference'} this cluster",
                 "pass" if found_secret else "warn",
                 "" if found_secret else "Store credentials in Secrets Manager instead of application config")
        except Exception as e:
            _add(results, "Security", "SEC3", f"Cannot check Secrets Manager: {e}", "warn")

        # OPS5c: Custom parameter group
        pg_name_ops = cl.get("DBClusterParameterGroup", "")
        is_default_pg = pg_name_ops.startswith("default.")
        _add(results, "Operational Excellence", "OPS5c",
             f"Parameter group: {pg_name_ops}",
             "warn" if is_default_pg else "pass",
             "Use a custom parameter group for workload-specific tuning" if is_default_pg else "")

        # OPS7: Maintenance window
        maint_window = cl.get("PreferredMaintenanceWindow", "not set")
        _add(results, "Operational Excellence", "OPS7",
             f"Maintenance window: {maint_window}", "info",
             "Verify this window aligns with your lowest-traffic period")

        # COST7: Storage type
        storage_type = cl.get("StorageType", "standard")
        _add(results, "Cost Optimization", "COST7",
             f"Storage type: {storage_type}", "info",
             "Evaluate I/O-Optimized for write-heavy workloads (>25% I/O cost)"
             if storage_type != "iopt1" else "I/O-Optimized active")

        # Per-instance new checks
        for inst in insts:
            iid = inst["DBInstanceIdentifier"]
            itype = inst["DBInstanceClass"]
            dim = [{"Name": "DBInstanceIdentifier", "Value": iid}]
            is_writer = any(
                m.get("DBInstanceIdentifier") == iid and m.get("IsClusterWriter", False)
                for m in cl.get("DBClusterMembers", []))

            # PERF11: FreeableMemory
            ram_gib = INSTANCE_RAM_GIB.get(itype, 0)
            if ram_gib:
                try:
                    resp = cw.get_metric_statistics(
                        Namespace="AWS/DocDB", MetricName="FreeableMemory",
                        Dimensions=dim, StartTime=start, EndTime=end,
                        Period=3600, Statistics=["Minimum"])
                    dps = [d["Minimum"] for d in resp.get("Datapoints", [])]
                    if dps:
                        min_free = min(dps)
                        free_pct = min_free / (ram_gib * 1024**3) * 100
                        _add(results, "Performance Efficiency", "PERF11",
                             f"FreeableMemory min for {iid}: {min_free / (1024**3):.1f} GiB ({free_pct:.0f}% of {ram_gib} GiB)",
                             "fail" if free_pct < 5 else "warn" if free_pct < 10 else "pass",
                             "Instance under memory pressure" if free_pct < 10 else "")
                except Exception:
                    pass

            # PERF12: SwapUsage
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="SwapUsage",
                    Dimensions=dim, StartTime=start, EndTime=end,
                    Period=3600, Statistics=["Maximum"])
                dps = [d["Maximum"] for d in resp.get("Datapoints", [])]
                if dps and max(dps) > 0:
                    _add(results, "Performance Efficiency", "PERF12",
                         f"SwapUsage on {iid}: {max(dps) / (1024**2):.0f} MB", "fail",
                         "Instance is swapping — critically undersized")
                elif dps:
                    _add(results, "Performance Efficiency", "PERF12",
                         f"No swap usage on {iid}", "pass")
            except Exception:
                pass

            # PERF13: DiskQueueDepth
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="DiskQueueDepth",
                    Dimensions=dim, StartTime=start, EndTime=end,
                    Period=3600, Statistics=["Average"])
                dps = [d["Average"] for d in resp.get("Datapoints", [])]
                if dps:
                    avg_dqd = sum(dps) / len(dps)
                    _add(results, "Performance Efficiency", "PERF13",
                         f"DiskQueueDepth avg for {iid}: {avg_dqd:.1f}",
                         "warn" if avg_dqd > 5 else "pass",
                         "I/O backing up — evaluate I/O-Optimized or upsizing" if avg_dqd > 5 else "")
            except Exception:
                pass

            # PERF14: IndexBufferCacheHitRatio
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="IndexBufferCacheHitRatio",
                    Dimensions=dim, StartTime=start, EndTime=end,
                    Period=3600, Statistics=["Average"])
                dps = [d["Average"] for d in resp.get("Datapoints", [])]
                if dps:
                    avg_idx_cache = sum(dps) / len(dps)
                    _add(results, "Performance Efficiency", "PERF14",
                         f"IndexBufferCacheHitRatio for {iid}: {avg_idx_cache:.1f}%",
                         "pass" if avg_idx_cache >= 99 else "warn" if avg_idx_cache >= 95 else "fail",
                         "Indexes do not fit in memory" if avg_idx_cache < 95 else "")
            except Exception:
                pass

            # REL8: DatabaseCursorsTimedOut — only surface if timeouts occurred
            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="DatabaseCursorsTimedOut",
                    Dimensions=dim, StartTime=start, EndTime=end,
                    Period=86400, Statistics=["Sum"])
                dps = [d["Sum"] for d in resp.get("Datapoints", [])]
                total_timedout = sum(dps) if dps else 0
                if total_timedout > 0:
                    _add(results, "Reliability", "REL4a",
                         f"{int(total_timedout)} cursor(s) timed out on {iid} in last 7 days", "warn",
                         "Application may not be closing cursors properly")
            except Exception:
                pass

            # REL9: AvailableMVCCIds (writer only)
            if is_writer:
                try:
                    resp = cw.get_metric_statistics(
                        Namespace="AWS/DocDB", MetricName="AvailableMVCCIds",
                        Dimensions=dim, StartTime=start, EndTime=end,
                        Period=3600, Statistics=["Minimum"])
                    dps = [d["Minimum"] for d in resp.get("Datapoints", [])]
                    if dps:
                        min_mvcc = min(dps)
                        pct = min_mvcc / 1_400_000_000 * 100
                        _add(results, "Reliability", "REL6e",
                             f"AvailableMVCCIds min: {min_mvcc:,.0f} ({pct:.0f}%)",
                             "fail" if pct < 25 else "warn" if pct < 50 else "pass",
                             "MVCC ID exhaustion risk" if pct < 50 else "")
                except Exception:
                    pass

            # COST9: Idle reader detection
            if not is_writer:
                try:
                    conn_resp = cw.get_metric_statistics(
                        Namespace="AWS/DocDB", MetricName="DatabaseConnections",
                        Dimensions=dim, StartTime=start, EndTime=end,
                        Period=3600, Statistics=["Average"])
                    io_resp = cw.get_metric_statistics(
                        Namespace="AWS/DocDB", MetricName="ReadIOPS",
                        Dimensions=dim, StartTime=start, EndTime=end,
                        Period=3600, Statistics=["Average"])
                    avg_conn = sum(d["Average"] for d in conn_resp.get("Datapoints", [])) / max(len(conn_resp.get("Datapoints", [])), 1) if conn_resp.get("Datapoints") else 0
                    avg_iops = sum(d["Average"] for d in io_resp.get("Datapoints", [])) / max(len(io_resp.get("Datapoints", [])), 1) if io_resp.get("Datapoints") else 0
                    if avg_conn < 2 and avg_iops < 5:
                        _add(results, "Cost Optimization", "COST9",
                             f"Reader {iid} appears idle (avg {avg_conn:.0f} connections, {avg_iops:.0f} ReadIOPS)", "warn",
                             "Consider removing this replica to reduce cost")
                    else:
                        _add(results, "Cost Optimization", "COST9",
                             f"Reader {iid} is active (avg {avg_conn:.0f} connections, {avg_iops:.0f} ReadIOPS)", "pass")
                except Exception:
                    pass

        # Database-level checks (from cached analysis_data)
        db_checks = _run_db_checks(analysis_data)
        results.extend(db_checks)

        if not analysis_data:
            _add(results, "Other", "INFO",
                 "Database-level checks skipped — run Analyze on a database first", "info")

        # AI-powered recommendations
        ai_md = None
        try:
            ai_md = _generate_ai_recommendations(results, cluster_id, region, analysis_data)
        except Exception as e:
            logger.warning("AI recommendations failed: %s", e)

        with _wa_lock:
            _wa.update(results=results, running=False, done=True, ai_md=ai_md)
        logger.info("WA review complete: %d checks", len(results))

        # Persist to disk
        try:
            from agent_memory import save_wa_results
            save_wa_results(cluster_id, results)
        except Exception:
            pass

    except Exception as e:
        logger.error("WA review failed: %s", e, exc_info=True)
        with _wa_lock:
            _wa.update(results=results, running=False, done=True, error=str(e))




# ── Database-level checks (from analysis_data, no DB calls) ──────────────────

def _run_db_checks(analysis_data):
    """Derive checks from cached analysis_data. No extra DB calls."""
    results = []
    if not analysis_data:
        return results

    total_indexes = 0
    unused_indexes = 0
    redundant = 0
    low_cardinality = 0
    total_data_size = 0
    total_index_size = 0
    total_unused_bytes = 0
    bloated_colls = []
    over_indexed_colls = []
    compression_disabled = []
    collscan_candidates = []

    for db_name, collections in analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll_name, stats in collections.items():
            if not isinstance(stats, dict) or "error" in stats:
                continue
            indexes = stats.get("indexes", [])
            total_indexes += len(indexes)

            for idx in indexes:
                if idx.get("usage", {}).get("potential_unused"):
                    unused_indexes += 1
                if idx.get("cardinality", {}).get("is_low"):
                    low_cardinality += 1

            # Redundant (prefix subset)
            ordered = [tuple(idx.get("ordered_fields", [])) for idx in indexes]
            for i, a in enumerate(ordered):
                for j, b in enumerate(ordered):
                    if i != j and len(a) < len(b) and b[:len(a)] == a:
                        redundant += 1
                        break

            total_data_size += stats.get("size", 0)
            for idx in indexes:
                total_index_size += idx.get("size", 0)

            unused_pct = stats.get("unusedStorageSize", {}).get("unusedPercent", 0.0)
            total_unused_bytes += stats.get("unusedStorageSize", {}).get("unusedBytes", 0)
            if unused_pct > 30:
                bloated_colls.append(f"{db_name}.{coll_name} ({unused_pct:.0f}%)")
            if len(indexes) > 10:
                over_indexed_colls.append(f"{db_name}.{coll_name} ({len(indexes)} indexes)")
            if not stats.get("compression", {}).get("enabled", False):
                compression_disabled.append(f"{db_name}.{coll_name}")
            doc_count = stats.get("count", 0)
            non_id = [idx for idx in indexes if idx.get("name") != "_id_"]
            if doc_count > 100000 and len(non_id) == 0:
                collscan_candidates.append(f"{db_name}.{coll_name} ({doc_count:,} docs)")

    # Unused-index guidance is emitted as a static info note (COST4a) by
    # wa_checks/cost_optimization.py under "COST 4 — Index Efficiency". The
    # previous COST3 emission here was removed: it collided with "COST 3 —
    # Storage" and duplicated the plugin note.
    _unused_indexes_note_moved_to_COST4a = True

    if redundant > 0:
        _add(results, "Performance Efficiency", "PERF1b",
             f"{redundant} redundant index(es) (prefix subsets)", "warn",
             "Covered by compound indexes and can be dropped")
    else:
        _add(results, "Performance Efficiency", "PERF1b",
             "No redundant indexes detected", "pass")

    if bloated_colls:
        unused_mb = total_unused_bytes / (1024 * 1024)
        _add(results, "Performance Efficiency", "PERF9",
             f"{len(bloated_colls)} collection(s) with >30% bloat ({unused_mb:,.0f} MB reclaimable)", "warn",
             "Run compact to reclaim storage. " + ", ".join(bloated_colls[:3])
             + ". Also review per-index bloat in the Database and Index overview "
               "(indexes with high unused %) — rebuild or drop bloated indexes to "
               "reclaim storage and reduce write I/O.")
    else:
        _add(results, "Performance Efficiency", "PERF9",
             "No significant collection storage bloat", "pass",
             "Also check per-index bloat in the Database and Index overview — "
             "individual indexes can be bloated even when collections are not.")

    if over_indexed_colls:
        _add(results, "Performance Efficiency", "PERF10",
             f"{len(over_indexed_colls)} collection(s) with >10 indexes", "warn",
             ", ".join(over_indexed_colls[:3]))
    else:
        _add(results, "Performance Efficiency", "PERF10",
             "No over-indexed collections", "pass")

    if compression_disabled:
        _add(results, "Sustainability", "SUST2",
             f"Compression disabled on {len(compression_disabled)} collection(s)", "warn",
             "Enable compression to reduce storage and I/O")
    else:
        _add(results, "Sustainability", "SUST2", "Compression enabled on all collections", "pass")

    if collscan_candidates:
        _add(results, "Performance Efficiency", "PERF15",
             f"{len(collscan_candidates)} large collection(s) with no secondary indexes", "warn",
             "Every query does a full collection scan. " + ", ".join(collscan_candidates[:3]))
    else:
        _add(results, "Performance Efficiency", "PERF15",
             "All large collections have secondary indexes", "pass")

    if total_data_size > 0:
        ratio = total_index_size / total_data_size * 100
        _add(results, "Performance Efficiency", "PERF8",
             f"Index-to-data ratio: {ratio:.0f}%",
             "warn" if ratio > 50 else "pass",
             "Indexes exceed 50% of data size" if ratio > 50 else "")

    # PERF1: Large average document size
    large_docs = []
    low_card_names = []
    ttl_colls = []
    write_amp_colls = []
    for db_name, collections in analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll_name, stats in collections.items():
            if not isinstance(stats, dict) or "error" in stats:
                continue
            avg_obj = stats.get("avgObjSize", 0)
            if avg_obj > 8192:
                large_docs.append(f"{db_name}.{coll_name} ({avg_obj:,} bytes)")
            for idx in stats.get("indexes", []):
                if idx.get("cardinality", {}).get("is_low"):
                    low_card_names.append(f"{db_name}.{coll_name}.{idx['name']}")
                if idx.get("expireAfterSeconds") is not None:
                    if f"{db_name}.{coll_name}" not in ttl_colls:
                        ttl_colls.append(f"{db_name}.{coll_name}")
            coll_data = stats.get("size", 0)
            coll_idx = sum(i.get("size", 0) for i in stats.get("indexes", []))
            if coll_data > 0 and coll_idx > 2 * coll_data:
                write_amp_colls.append(
                    f"{db_name}.{coll_name} (index {coll_idx/coll_data:.1f}x data)")

    if large_docs:
        _add(results, "Performance Efficiency", "PERF1",
             f"{len(large_docs)} collection(s) with avg doc size > 8 KB", "warn",
             ", ".join(large_docs[:5]))
    else:
        _add(results, "Performance Efficiency", "PERF1",
             "All collections have avg doc size \u2264 8 KB", "pass")

    # PERF1c: Low cardinality indexes
    if low_card_names:
        _add(results, "Performance Efficiency", "PERF1c",
             f"{len(low_card_names)} low cardinality index(es) detected", "warn",
             ", ".join(low_card_names[:5]))
    else:
        _add(results, "Performance Efficiency", "PERF1c",
             "No low cardinality indexes detected", "pass")

    # COST4: TTL indexes
    if ttl_colls:
        _add(results, "Cost Optimization", "COST4",
             f"TTL indexes found on {len(ttl_colls)} collection(s)", "pass",
             ", ".join(ttl_colls[:5]))
    else:
        _add(results, "Cost Optimization", "COST4",
             "No TTL indexes found", "warn",
             "Consider TTL indexes for automatic data expiration")

    # PERF16: Per-collection write amplification
    if write_amp_colls:
        _add(results, "Performance Efficiency", "PERF16",
             f"{len(write_amp_colls)} collection(s) with index size > 2x data size", "warn",
             "High write amplification — review index necessity. "
             + ", ".join(write_amp_colls[:5]))
    else:
        _add(results, "Performance Efficiency", "PERF16",
             "No collections with excessive index-to-data ratio", "pass")

    return results


# ── AI-powered recommendations (Bedrock) ─────────────────────────────────────

def _generate_ai_recommendations(check_results, cluster_id, region, analysis_data):
    """Call Bedrock with DocumentDB-specific system prompt. Returns {pillar: [rec, ...]} or None."""
    import json as _json

    # ── Failing / warning checks (remapped IDs matching UI) ───────────────────
    try:
        from tabs.wa_v2.base import ID_REMAP, DETAIL_OVERRIDES
        def _display_id(raw):
            r = ID_REMAP.get(raw, raw)
            return r if r is not None else raw
        failing = [
            {
                "pillar":   c["pillar"],
                "check_id": _display_id(c["id"]),
                "label":    c["label"],
                "status":   c["status"],
                "detail":   DETAIL_OVERRIDES.get(_display_id(c["id"]), c.get("detail", "")),
            }
            for c in check_results
            if c["status"] in ("fail", "warn")
            and not (ID_REMAP.get(c["id"]) is None and c["id"] in ID_REMAP)
        ]
    except Exception:
        failing = [
            {"pillar": c["pillar"], "check_id": c["id"],
             "label": c["label"], "status": c["status"], "detail": c.get("detail", "")}
            for c in check_results if c["status"] in ("fail", "warn")
        ]

    if not failing:
        return None

    # ── Cluster metadata ──────────────────────────────────────────────────────
    cluster_meta = {"cluster_id": cluster_id, "region": region}
    try:
        docdb = boto3.client("docdb", region_name=region)
        cl    = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        insts = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}]
        )["DBInstances"]
        cluster_meta.update({
            "engine_version":    cl.get("EngineVersion", "unknown"),
            "instance_types":    list({i["DBInstanceClass"] for i in insts}),
            "instance_count":    len(insts),
            "writer_count":      sum(1 for m in cl.get("DBClusterMembers", []) if m.get("IsClusterWriter")),
            "reader_count":      sum(1 for m in cl.get("DBClusterMembers", []) if not m.get("IsClusterWriter")),
            "storage_type":      cl.get("StorageType", "standard"),
            "backup_retention":  cl.get("BackupRetentionPeriod", 1),
            "log_exports":       cl.get("EnabledCloudwatchLogsExports", []),
            "deletion_protection": cl.get("DeletionProtection", False),
            "storage_encrypted": cl.get("StorageEncrypted", False),
            "multi_az":          len({i.get("AvailabilityZone") for i in insts}) >= 2,
        })
    except Exception as e:
        logger.debug("Could not enrich cluster metadata for AI prompt: %s", e)

    # ── Key metrics from snapshot (if available) ──────────────────────────────
    metrics_summary = {}
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            snap = _snap.get("data")
        if snap:
            inst_metrics = snap.get("instances", [])
            cpu_avgs  = [i["cpu_avg"]  for i in inst_metrics if i.get("cpu_avg")  is not None]
            bchr_vals = [i["buffer_cache_hit_ratio"] for i in inst_metrics
                         if i.get("buffer_cache_hit_ratio") is not None]
            conn_maxs = [i["conn_max"] for i in inst_metrics if i.get("conn_max") is not None]
            if cpu_avgs:
                metrics_summary["avg_cpu_pct"] = round(sum(cpu_avgs) / len(cpu_avgs), 1)
            if bchr_vals:
                metrics_summary["buffer_cache_hit_ratio"] = round(min(bchr_vals), 1)
            if conn_maxs:
                metrics_summary["peak_connections"] = int(max(conn_maxs))
            if snap.get("storage_gb"):
                metrics_summary["storage_gb"] = round(snap["storage_gb"], 1)
    except Exception:
        pass

    pillars = sorted(set(c["pillar"] for c in failing))

    # ── System prompt (loaded from documentdb-advisor/wa-advisor-prompt.md) ───
    system_prompt = _load_wa_advisor_prompt()

    # ── User message ──────────────────────────────────────────────────────────
    user_msg = (
        f"Review the following Amazon DocumentDB Well-Architected check results "
        f"and provide recommendations.\n\n"
        f"## Cluster Context\n{_json.dumps(cluster_meta, default=str)}\n\n"
        f"## Key Metrics (recent averages)\n{_json.dumps(metrics_summary, default=str)}\n\n"
        f"## Failing / Warning Checks\n{_json.dumps(failing, default=str)[:4000]}\n\n"
        f"Pillars to address: {pillars}"
    )

    for model_id in ("us.anthropic.claude-sonnet-4-20250514-v1:0",
                     "us.anthropic.claude-haiku-4-5-20251001-v1:0",
                     "anthropic.claude-3-haiku-20240307-v1:0"):
        try:
            bedrock = boto3.client("bedrock-runtime", region_name=region)
            resp = bedrock.converse(
                modelId=model_id,
                messages=[{"role": "user", "content": [{"text": user_msg}]}],
                system=[{"text": system_prompt}],
                inferenceConfig={"maxTokens": 3000, "temperature": 0.2},
            )
            text = resp["output"]["message"]["content"][0]["text"].strip()
            text = re.sub(r"^```[a-z]*\s*", "", text)
            text = re.sub(r"\s*```$", "", text).strip()
            match = re.search(r"\{.*\}", text, re.DOTALL)
            if match:
                text = match.group()
            result = json.loads(text)
            if isinstance(result, dict):
                return result
            logger.warning("Bedrock %s returned non-dict JSON: %s", model_id, type(result))
        except Exception as e:
            logger.warning("Bedrock %s failed for AI recs: %s", model_id, e)
            continue
    return None


# ── AWS Well-Architected pillar icons (inline SVG, official colors) ───────────
def _wa_icon(svg_path, color="#232F3E", size=20):
    """Build an inline SVG icon for a WA pillar."""
    from dash import html
    return html.Div(
        html.Img(src=f"data:image/svg+xml,{svg_path}",
                 style={"width": f"{size}px", "height": f"{size}px"}),
        style={"display": "inline-flex", "alignItems": "center", "marginRight": ".4rem"}
    )

# Official AWS WA pillar colors
_WA_COLORS = {
    "Reliability":            "#d45b07",  # orange
    "Security":               "#dd344c",  # red
    "Operational Excellence": "#5e6b7a",  # grey
    "Performance Efficiency": "#8c4fff",  # purple
    "Cost Optimization":      "#067f68",  # green
    "Sustainability":         "#0972d3",  # blue
}

# Minimal SVG paths for each pillar (AWS-style simplified)
_WA_SVGS = {
    "Reliability": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M12 2L3 7v6c0 5.25 3.83 10.15 9 11.35C17.17 23.15 21 18.25 21 13V7L12 2zm0 2.18l7 3.89v5.93c0 4.23-3.08 8.18-7 9.14-3.92-.96-7-4.91-7-9.14V8.07l7-3.89z' fill='%23d45b07'/%3E%3Cpath d='M11 15.5l-3-3 1.41-1.41L11 12.67l4.59-4.58L17 9.5l-6 6z' fill='%23d45b07'/%3E%3C/svg%3E",
    "Security": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M18 8h-1V6c0-2.76-2.24-5-5-5S7 3.24 7 6v2H6c-1.1 0-2 .9-2 2v10c0 1.1.9 2 2 2h12c1.1 0 2-.9 2-2V10c0-1.1-.9-2-2-2zM9 6c0-1.66 1.34-3 3-3s3 1.34 3 3v2H9V6zm9 14H6V10h12v10zm-6-3c1.1 0 2-.9 2-2s-.9-2-2-2-2 .9-2 2 .9 2 2 2z' fill='%23dd344c'/%3E%3C/svg%3E",
    "Operational Excellence": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M19.14 12.94c.04-.31.06-.63.06-.94 0-.31-.02-.63-.06-.94l2.03-1.58a.49.49 0 00.12-.61l-1.92-3.32a.49.49 0 00-.59-.22l-2.39.96c-.5-.38-1.03-.7-1.62-.94l-.36-2.54a.484.484 0 00-.48-.41h-3.84c-.24 0-.43.17-.47.41l-.36 2.54c-.59.24-1.13.57-1.62.94l-2.39-.96a.49.49 0 00-.59.22L2.74 8.87c-.12.21-.08.47.12.61l2.03 1.58c-.04.31-.06.63-.06.94s.02.63.06.94l-2.03 1.58a.49.49 0 00-.12.61l1.92 3.32c.12.22.37.29.59.22l2.39-.96c.5.38 1.03.7 1.62.94l.36 2.54c.05.24.24.41.48.41h3.84c.24 0 .44-.17.47-.41l.36-2.54c.59-.24 1.13-.56 1.62-.94l2.39.96c.22.08.47 0 .59-.22l1.92-3.32c.12-.22.07-.47-.12-.61l-2.01-1.58zM12 15.6A3.6 3.6 0 1115.6 12 3.6 3.6 0 0112 15.6z' fill='%235e6b7a'/%3E%3C/svg%3E",
    "Performance Efficiency": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M20.38 8.57l-1.23 1.85a8 8 0 01-.22 7.58H5.07A8 8 0 0115.58 6.85l1.85-1.23A10 10 0 003.35 19a2 2 0 001.72 1h13.85a2 2 0 001.74-1 10 10 0 00-.27-10.44z' fill='%238c4fff'/%3E%3Cpath d='M10.59 15.41a2 2 0 002.83 0l5.66-8.49-8.49 5.66a2 2 0 000 2.83z' fill='%238c4fff'/%3E%3C/svg%3E",
    "Cost Optimization": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M12 2C6.48 2 2 6.48 2 12s4.48 10 10 10 10-4.48 10-10S17.52 2 12 2zm0 18c-4.41 0-8-3.59-8-8s3.59-8 8-8 8 3.59 8 8-3.59 8-8 8zm.31-8.86c-1.77-.45-2.34-.94-2.34-1.67 0-.84.79-1.43 2.1-1.43 1.38 0 1.9.66 1.94 1.64h1.71c-.05-1.34-.87-2.57-2.49-2.97V5H11.5v1.69c-1.51.32-2.72 1.3-2.72 2.81 0 1.79 1.49 2.69 3.66 3.21 1.95.46 2.34 1.15 2.34 1.87 0 .53-.39 1.39-2.1 1.39-1.6 0-2.23-.72-2.32-1.64H8.65c.1 1.7 1.36 2.66 2.85 2.97V19h1.72v-1.67c1.52-.29 2.72-1.16 2.72-2.74 0-2.22-1.86-2.97-3.63-3.45z' fill='%23067f68'/%3E%3C/svg%3E",
    "Sustainability": "%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M6.05 8.05a7.001 7.001 0 009.9 0C17.32 6.68 18 5.05 18 3.35V3c-3.69.02-5.37.78-7 2.42C9.37 3.78 7.69 3.02 4 3v.35c0 1.7.68 3.33 2.05 4.7zM12 7.83c1.2-1.63 2.88-2.57 5.47-2.77-.2 1.3-.76 2.48-1.67 3.39a5.002 5.002 0 01-7.07 0c-.91-.91-1.47-2.09-1.67-3.39C9.65 5.26 10.8 6.2 12 7.83zM12 21c3.31 0 6-2.69 6-6v-2h-2v2c0 2.21-1.79 4-4 4s-4-1.79-4-4v-2H6v2c0 3.31 2.69 6 6 6z' fill='%230972d3'/%3E%3C/svg%3E",
}

PILLAR_META = {
    "Reliability":             {"icon": "reliability",    "order": 1},
    "Security":                {"icon": "security",       "order": 2},
    "Operational Excellence":  {"icon": "opex",           "order": 3},
    "Performance Efficiency":  {"icon": "performance",    "order": 4},
    "Cost Optimization":       {"icon": "cost",           "order": 5},
    "Sustainability":          {"icon": "sustainability", "order": 6},
}

STATUS_BADGE = {
    "pass": ("✅ Pass", "success"),
    "warn": ("⚠️ Warning", "warning"),
    "fail": ("❌ Fail", "danger"),
    "info": ("ℹ️ Info", "info"),
}



# Table styles for pillar panels
_P_TH = {"padding": ".35rem .5rem", "fontSize": ".65rem", "fontWeight": "700",
          "textTransform": "uppercase", "letterSpacing": ".5px",
          "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
          "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_P_TD = {"padding": ".3rem .5rem", "fontSize": ".82rem", "color": "var(--text-body)",
         "borderBottom": "1px solid var(--border-default)", "verticalAlign": "top"}


_STATUS_META = {
    "fail": ("Fail",    "danger",  "var(--accent-red)",    "#fff0f0",               "var(--accent-red)"),
    "warn": ("Warning", "warning", "var(--color-warning)", "#fffbf0",               "var(--color-warning)"),
    "pass": ("Pass",    "success", "var(--accent-green)",  "#f0fff4",               "var(--accent-green)"),
    "info": ("Info",    "info",    "var(--text-muted)",    "var(--bg-surface-alt)", "var(--text-muted)"),
}


def _check_group(checks, status_filter):
    """Render one status group — coloured subheader with count, black label text."""
    label, badge_color, hdr_color, bg, border = _STATUS_META[status_filter]
    rows = [c for c in checks if c["status"] == status_filter]
    if not rows:
        return None

    # Subheader: coloured badge label + count
    subheader = html.Div([
        dbc.Badge(label, color=badge_color, style={"fontSize": ".65rem"}),
        html.Span(f" ({len(rows)})",
                  style={"fontSize": ".68rem", "color": hdr_color,
                         "fontWeight": "700", "marginLeft": ".3rem"}),
    ], style={"padding": ".3rem .5rem", "marginTop": ".3rem",
              "display": "flex", "alignItems": "center"})

    # Rows: black label, muted detail
    items = []
    for c in rows:
        items.append(html.Div([
            html.Div([
                html.Span(c["id"], style={"fontSize": ".68rem", "fontWeight": "700",
                                          "color": "var(--text-muted)", "fontFamily": "monospace",
                                          "marginRight": ".4rem", "flexShrink": "0"}),
                html.Span(c["label"], style={"fontWeight": "600", "fontSize": ".82rem",
                                             "color": "var(--text-body)"}),
            ], style={"display": "flex", "alignItems": "baseline"}),
            html.Div(c["detail"],
                     style={"fontSize": ".75rem", "color": "var(--text-muted)",
                            "marginTop": ".1rem", "paddingLeft": "3rem"}) if c.get("detail") else None,
        ], style={"padding": ".35rem .5rem",
                  "borderBottom": "1px solid var(--border-default)"}))

    return html.Div([subheader] + items)


def _pillar_panel(pillar_name, checks):
    """Build a single pillar panel with checks grouped by subsection (collapsible)."""
    from tabs.wa_v2.base import WA_QUESTIONS

    meta = PILLAR_META.get(pillar_name, {"icon": "📋", "order": 99})
    p_pass = sum(1 for c in checks if c["status"] == "pass")
    p_warn = sum(1 for c in checks if c["status"] == "warn")
    p_fail = sum(1 for c in checks if c["status"] == "fail")

    header_color = "var(--border-default)"

    svg_data = _WA_SVGS.get(pillar_name, "")
    pillar_color = _WA_COLORS.get(pillar_name, "#232F3E")
    icon_el = (html.Img(src=f"data:image/svg+xml,{svg_data}",
                        style={"width": "18px", "height": "18px", "marginRight": ".4rem"})
               if svg_data else
               html.Span(meta["icon"], style={"fontSize": "1rem", "marginRight": ".4rem"}))

    header = html.Div([
        html.Div([icon_el,
                  html.Span(pillar_name, style={"fontWeight": "700", "fontSize": ".88rem",
                                               "color": pillar_color})],
                 style={"display": "flex", "alignItems": "center"}),
        html.Div([
            html.Span(f"✅ {p_pass}", style={"fontSize": ".72rem", "color": "var(--accent-green)",
                      "fontWeight": "700", "marginLeft": ".3rem"}) if p_pass else None,
            html.Span(f"⚠️ {p_warn}", style={"fontSize": ".72rem", "color": "var(--color-warning)",
                      "fontWeight": "700", "marginLeft": ".3rem"}) if p_warn else None,
            html.Span(f"❌ {p_fail}", style={"fontSize": ".72rem", "color": "var(--accent-red)",
                      "fontWeight": "700", "marginLeft": ".3rem"}) if p_fail else None,
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
              "padding": ".45rem .6rem", "borderBottom": f"2px solid {header_color}",
              "background": "var(--bg-surface-alt)", "borderRadius": "8px 8px 0 0"})

    # Group checks by subsection using WA_QUESTIONS mapping
    from collections import OrderedDict
    subsections = OrderedDict()
    for c in checks:
        subsection_title = WA_QUESTIONS.get(c["id"], "Other")
        if subsection_title not in subsections:
            subsections[subsection_title] = []
        subsections[subsection_title].append(c)

    # Build collapsible subsection panels
    body_children = []
    for idx, (subsection_title, sub_checks) in enumerate(subsections.items()):
        sub_pass = sum(1 for c in sub_checks if c["status"] == "pass")
        sub_warn = sum(1 for c in sub_checks if c["status"] == "warn")
        sub_fail = sum(1 for c in sub_checks if c["status"] == "fail")

        # Status icons for subsection header
        status_icons = []
        if sub_fail:
            status_icons.append(html.Span(f"❌{sub_fail}", style={"fontSize": ".68rem", "color": "var(--accent-red)", "marginLeft": ".3rem"}))
        if sub_warn:
            status_icons.append(html.Span(f"⚠️{sub_warn}", style={"fontSize": ".68rem", "color": "var(--color-warning)", "marginLeft": ".3rem"}))
        if sub_pass:
            status_icons.append(html.Span(f"✓{sub_pass}", style={"fontSize": ".68rem", "color": "var(--accent-green)", "marginLeft": ".3rem"}))

        # Subsection header (clickable via Details/Summary)
        check_items = []
        for c in sub_checks:
            status_icon = {"pass": "✓", "fail": "✗", "warn": "!", "info": "ℹ"}.get(c["status"], "•")
            status_color = {"pass": "var(--accent-green)", "fail": "var(--accent-red)",
                           "warn": "var(--color-warning)", "info": "var(--text-muted)"}.get(c["status"], "var(--text-body)")
            check_items.append(html.Div([
                html.Div([
                    html.Span(status_icon, style={"fontSize": ".78rem", "fontWeight": "700",
                                                  "color": status_color, "width": "1.2rem",
                                                  "display": "inline-block", "textAlign": "center"}),
                    html.Span(c["id"], style={"fontSize": ".68rem", "fontWeight": "700",
                                              "color": "var(--text-muted)", "fontFamily": "monospace",
                                              "marginRight": ".4rem", "marginLeft": ".2rem"}),
                    html.Span(c["label"], style={"fontWeight": "500", "fontSize": ".82rem",
                                                 "color": "var(--text-body)"}),
                ], style={"display": "flex", "alignItems": "baseline"}),
                html.Div(c["detail"],
                         style={"fontSize": ".75rem", "color": "var(--text-muted)",
                                "marginTop": ".1rem", "paddingLeft": "2.8rem"}) if c.get("detail") else None,
            ], style={"padding": ".3rem .5rem",
                      "borderBottom": "1px solid var(--border-default)"}))

        # Use HTML details/summary for native collapsible without callbacks
        subsection_el = html.Details([
            html.Summary([
                html.Span(subsection_title, style={"fontWeight": "600", "fontSize": ".82rem",
                                                    "color": "var(--text-heading)"}),
                *status_icons,
            ], style={"padding": ".4rem .5rem", "cursor": "pointer",
                      "background": "var(--bg-surface-alt)", "borderRadius": "4px",
                      "borderBottom": "1px solid var(--border-default)",
                      "display": "flex", "alignItems": "center", "gap": ".2rem",
                      "listStyle": "none"}),
            html.Div(check_items, style={"paddingLeft": ".3rem"}),
        ], open=True if (sub_fail or sub_warn) else False,
           style={"marginTop": ".3rem"})

        body_children.append(subsection_el)

    return dbc.Card([header, dbc.CardBody(html.Div(body_children), style={"padding": ".2rem .4rem"})],
                    style={"borderRadius": "8px", "border": "1px solid var(--border-default)",
                           "height": "100%"})


def _render_results(results, ai_md=None):
    """Render WA results as 2-column pillar panels (3 rows x 2 cols)."""
    if not results:
        return dbc.Alert("No results.", color="info")

    n_pass = sum(1 for r in results if r["status"] == "pass")
    n_warn = sum(1 for r in results if r["status"] == "warn")
    n_fail = sum(1 for r in results if r["status"] == "fail")
    total = len(results)
    score = int((n_pass / total) * 100) if total else 0

    score_color = "var(--accent-green)" if score >= 80 else "var(--color-warning)" if score >= 60 else "var(--accent-red)"

    # Summary bar
    summary = html.Div([
        html.Div([
            html.Span(f"{score}%", style={"fontSize": "1.4rem", "fontWeight": "800", "color": score_color}),
            html.Span(" Health Score", style={"fontSize": ".82rem", "color": "var(--text-muted)",
                       "marginLeft": ".4rem"}),
        ]),
        html.Div([
            html.Span(f"\u2705 {n_pass}", style={"fontSize": ".82rem", "marginRight": ".8rem",
                       "color": "var(--accent-green)", "fontWeight": "600"}),
            html.Span(f"\u26a0\ufe0f {n_warn}", style={"fontSize": ".82rem", "marginRight": ".8rem",
                       "color": "var(--color-warning)", "fontWeight": "600"}),
            html.Span(f"\u274c {n_fail}", style={"fontSize": ".82rem",
                       "color": "var(--accent-red)", "fontWeight": "600"}),
            html.Span(f"  \u00b7  {total} checks", style={"fontSize": ".78rem",
                       "color": "var(--text-muted)", "marginLeft": ".5rem"}),
        ]),
    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
              "padding": ".6rem .8rem", "borderRadius": "8px", "marginBottom": ".75rem",
              "border": "1px solid var(--border-default)", "background": "var(--bg-surface-alt)"})

    # Group by pillar
    pillars = {}
    for r in results:
        p = r["pillar"]
        if p not in pillars:
            pillars[p] = []
        pillars[p].append(r)

    sorted_pillars = sorted(pillars.items(), key=lambda x: PILLAR_META.get(x[0], {}).get("order", 99))

    # Build 2-column grid (3 rows x 2 cols)
    rows = []
    for i in range(0, len(sorted_pillars), 2):
        cols = []
        for j in range(2):
            if i + j < len(sorted_pillars):
                pname, checks = sorted_pillars[i + j]
                cols.append(dbc.Col(_pillar_panel(pname, checks), md=6, className="mb-3"))
            else:
                cols.append(dbc.Col(md=6))
        rows.append(dbc.Row(cols))

    # AI Recommendations section — grouped by pillar
    if ai_md:
        ai_data = ai_md if isinstance(ai_md, dict) else {}
        pillar_order = ["Reliability", "Security", "Operational Excellence",
                        "Performance Efficiency", "Cost Optimization", "Sustainability"]
        ordered_pillars = [p for p in pillar_order if p in ai_data] + \
                          [p for p in ai_data if p not in pillar_order]

        pri_color = {"Critical": "danger", "High": "danger",
                     "Medium": "warning", "Low": "secondary"}

        pillar_cards = []
        for pillar in ordered_pillars:
            recs = ai_data.get(pillar, [])
            if not recs:
                continue
            pcolor = _WA_COLORS.get(pillar, "#232F3E")
            rec_rows = []
            for r in recs:
                pri = r.get("priority", "Medium")
                rec_rows.append(html.Div([
                    html.Div([
                        dbc.Badge(pri, color=pri_color.get(pri, "secondary"),
                                  className="me-2", style={"fontSize": ".62rem"}),
                        html.Span(r.get("action", ""),
                                  style={"fontWeight": "600", "fontSize": ".82rem"}),
                    ], style={"marginBottom": ".15rem"}),
                    html.Div([
                        html.Span("Why: ", style={"fontWeight": "700", "color": "var(--text-muted)",
                                                  "fontSize": ".75rem"}),
                        html.Span(r.get("why", ""),
                                  style={"fontSize": ".75rem", "color": "var(--text-muted)"}),
                    ]) if r.get("why") else None,
                    html.Div([
                        html.Span("Impact: ", style={"fontWeight": "700", "color": "var(--text-muted)",
                                                     "fontSize": ".75rem"}),
                        html.Span(r.get("impact", ""),
                                  style={"fontSize": ".75rem", "color": "var(--text-muted)"}),
                    ]) if r.get("impact") else None,
                ], style={"padding": ".35rem .5rem",
                          "borderBottom": "1px solid var(--border-default)"}))

            pillar_cards.append(dbc.Col(
                dbc.Card([
                    html.Div([
                        html.Span(pillar, style={"fontWeight": "700", "fontSize": ".82rem",
                                                 "color": pcolor}),
                        dbc.Badge(str(len(recs)), color="secondary", className="ms-2",
                                  style={"fontSize": ".6rem"}),
                    ], style={"padding": ".4rem .6rem",
                              "borderBottom": f"2px solid {pcolor}",
                              "background": "var(--bg-surface-alt)",
                              "borderRadius": "8px 8px 0 0"}),
                    html.Div(rec_rows),
                ], style={"borderRadius": "8px",
                          "border": "1px solid var(--border-default)",
                          "height": "100%"}),
                md=6, className="mb-3"
            ))

        if pillar_cards:
            ai_section = html.Div([
                html.Div("AI-Powered Recommendations",
                         style={"fontWeight": "700", "fontSize": ".88rem",
                                "color": "var(--text-heading)", "marginBottom": ".5rem"}),
                dbc.Row(pillar_cards),
            ])
            rows.append(ai_section)
    elif ai_md is None:
        rows.append(dbc.Alert([
            dbc.Spinner(size="sm", spinner_class_name="me-2"),
            html.Span("Generating AI recommendations… This may take a few minutes; "
                      "revisit in a little while."),
        ], color="info", className="mt-2 d-flex align-items-center",
           style={"fontSize": ".85rem"}))

    return html.Div([summary] + rows + _build_wa_trend())


def _build_wa_trend():
    """Build a vertical stacked bar chart — checks by pillar (6 pillars only)."""
    try:
        with _wa_lock:
            current_results = list(_wa["results"]) if _wa["results"] else []
        if not current_results:
            return []

        import plotly.graph_objects as go

        PILLARS = ["Reliability", "Security", "Operational Excellence",
                   "Performance Efficiency", "Cost Optimization", "Sustainability"]
        SHORT = ["Reliab.", "Security", "Ops Excel.", "Perform.", "Cost Opt.", "Sustain."]

        def _stats(results):
            s = {p: {"pass": 0, "warn": 0, "fail": 0} for p in PILLARS}
            for r in results:
                p = r.get("pillar", "")
                if p in s:
                    st = r.get("status", "")
                    if st in s[p]:
                        s[p][st] += 1
            return s

        curr = _stats(current_results)

        c_pass = [curr[p]["pass"] for p in PILLARS]
        c_warn = [curr[p]["warn"] for p in PILLARS]
        c_fail = [curr[p]["fail"] for p in PILLARS]

        fig = go.Figure()
        fig.add_trace(go.Bar(
            name="Pass", x=SHORT, y=c_pass, marker_color="#1D8102",
            text=[str(v) if v else "" for v in c_pass],
            textposition="inside", textfont=dict(size=10, color="white"),
        ))
        fig.add_trace(go.Bar(
            name="Warn", x=SHORT, y=c_warn, marker_color="#F2C94C",
            text=[str(v) if v else "" for v in c_warn],
            textposition="inside", textfont=dict(size=10, color="#5e4b00"),
        ))
        fig.add_trace(go.Bar(
            name="Fail", x=SHORT, y=c_fail, marker_color="#D13212",
            text=[str(v) if v else "" for v in c_fail],
            textposition="inside", textfont=dict(size=10, color="white"),
        ))

        fig.update_layout(
            barmode="stack",
            height=200,
            template="plotly_white",
            font_family="sans-serif",
            margin=dict(t=8, b=40, l=28, r=12),
            xaxis=dict(tickfont=dict(size=9), tickangle=0, type="category"),
            yaxis=dict(title="", showticklabels=False, showgrid=False, zeroline=False),
            legend=dict(orientation="h", y=-0.25, x=0.5, xanchor="center",
                        font=dict(size=9)),
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.3,
        )

        elements = [
            html.Div("Pillar Drift — Current Run", style={"fontSize": ".78rem", "fontWeight": "700",
                                                  "color": "var(--text-muted)", "marginTop": ".5rem"}),
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
        ]
        return elements
    except Exception:
        return []

