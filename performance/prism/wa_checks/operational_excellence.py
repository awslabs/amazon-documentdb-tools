"""WA Checks — Operational Excellence Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Operational Excellence
IDs are stable — do not rename (OPS2, OPS5a, OPS5b, OPS5c, OPS7, OPS8).
"""
import logging
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)


@register_check("OPS2", "Operational Excellence", "Subnet group AZ span",
                source="infrastructure", priority=10)
def check_subnet_group(ctx):
    sg_name = ctx.cluster.get("DBSubnetGroup", "")
    if not sg_name:
        return []
    try:
        sg = ctx.docdb_client.describe_db_subnet_groups(
            DBSubnetGroupName=sg_name)["DBSubnetGroups"][0]
        azs = set(s["SubnetAvailabilityZone"]["Name"] for s in sg.get("Subnets", []))
        n = len(azs)
        return [{"pillar": "Operational Excellence", "id": "OPS2",
                 "label": f"Subnet group spans {n} AZ(s)",
                 "status": "pass" if n >= 3 else "warn",
                 "detail": "Recommended: 3 AZs for failover flexibility" if n < 3 else ""}]
    except Exception as e:
        return [{"pillar": "Operational Excellence", "id": "OPS2",
                 "label": f"Cannot check subnet group: {e}",
                 "status": "warn", "detail": ""}]


@register_check("OPS5a", "Operational Excellence", "Profiler logging",
                source="infrastructure", priority=20)
def check_profiler_logging(ctx):
    logs = ctx.cluster.get("EnabledCloudwatchLogsExports", [])
    enabled = "profiler" in logs
    return [{"pillar": "Operational Excellence", "id": "OPS5a",
             "label": f"Profiler logging ({'enabled' if enabled else 'disabled'})",
             "status": "pass" if enabled else "warn",
             "detail": "" if enabled else "Enable profiler for slow query analysis"}]


@register_check("OPS5b", "Operational Excellence", "CloudWatch alarms",
                source="infrastructure", priority=21)
def check_cloudwatch_alarms(ctx):
    try:
        alarms = ctx.cw_client.describe_alarms(AlarmNamePrefix=ctx.cluster_id)
        n = len(alarms.get("MetricAlarms", []))
        status = "pass" if n >= 3 else "warn" if n > 0 else "fail"
        detail = "Recommended: alarms for CPU, FreeableMemory, DatabaseConnections" if n < 3 else ""
        return [{"pillar": "Operational Excellence", "id": "OPS5b",
                 "label": f"CloudWatch alarms ({n} configured)",
                 "status": status, "detail": detail}]
    except Exception as e:
        return [{"pillar": "Operational Excellence", "id": "OPS5b",
                 "label": f"Cannot check alarms: {e}",
                 "status": "warn", "detail": ""}]


@register_check("OPS5c", "Operational Excellence", "Custom parameter group",
                source="infrastructure", priority=22)
def check_parameter_group(ctx):
    pg_name = ctx.cluster.get("DBClusterParameterGroup", "")
    is_default = pg_name.startswith("default.")
    return [{"pillar": "Operational Excellence", "id": "OPS5c",
             "label": f"Parameter group: {pg_name}",
             "status": "warn" if is_default else "pass",
             "detail": "Use a custom parameter group for workload-specific tuning" if is_default else ""}]


@register_check("OPS7", "Operational Excellence", "Maintenance window",
                source="infrastructure", priority=50)
def check_maintenance_window(ctx):
    window = ctx.cluster.get("PreferredMaintenanceWindow", "not set")
    return [{"pillar": "Operational Excellence", "id": "OPS7",
             "label": f"Maintenance window: {window}",
             "status": "info",
             "detail": "Verify this window aligns with your lowest-traffic period"}]



# ── Migrated from tabs/wa_v2/ops.py (Phase 3B) ───────────────────────────────

@register_check("OPS1a", "Operational Excellence", "IaC deployment detection",
                source="infrastructure", priority=5)
def check_iac(ctx):
    """OPS 1 — Detect CloudFormation stack managing this cluster."""
    import boto3
    try:
        cfn = boto3.client("cloudformation", region_name=ctx.region)
        stacks = cfn.describe_stacks()["Stacks"]
        matching = [s["StackName"] for s in stacks
                    if any(ctx.cluster_id.lower() in str(v).lower()
                           for v in [s.get("StackName", ""),
                                     str(s.get("Parameters", "")),
                                     str(s.get("Tags", ""))])]
        if matching:
            return [{"pillar": "Operational Excellence", "id": "OPS1a",
                     "label": f"IaC stack detected: {matching[0]}", "status": "pass",
                     "detail": "Cluster managed via CloudFormation"}]
        return [{"pillar": "Operational Excellence", "id": "OPS1a",
                 "label": "No CloudFormation stack detected for this cluster", "status": "warn",
                 "detail": "Use CloudFormation, CDK, or Terraform for consistent, "
                           "repeatable deployments across regions and accounts"}]
    except Exception as e:
        return [{"pillar": "Operational Excellence", "id": "OPS1a",
                 "label": f"Cannot check IaC deployment: {e}", "status": "warn", "detail": str(e)}]


@register_check("OPS2a", "Operational Excellence", "Failover playbooks",
                source="infrastructure", priority=12)
def check_failover_playbooks(ctx):
    """OPS 2 — Failover playbooks info note."""
    return [{"pillar": "Operational Excellence", "id": "OPS2a",
             "label": "Failover playbooks — review recommended", "status": "info",
             "detail": "Document operational playbooks for cluster failover, replica lag, "
                       "connection exhaustion, and backup/restore. Test during scheduled "
                       "maintenance windows and update based on lessons learned."}]


@register_check("OPS3a", "Operational Excellence", "EventBridge Config rules",
                source="infrastructure", priority=23)
def check_eventbridge_config(ctx):
    """OPS 3 — EventBridge rules for AWS Config compliance events."""
    import boto3
    try:
        events = boto3.client("events", region_name=ctx.region)
        rules = events.list_rules()["Rules"]
        config_rules = [r for r in rules
                        if "config" in r.get("Name", "").lower() or
                        "compliance" in r.get("Description", "").lower()]
        if config_rules:
            return [{"pillar": "Operational Excellence", "id": "OPS3a",
                     "label": f"EventBridge rules for Config events: {len(config_rules)} found",
                     "status": "pass", "detail": ""}]
        return [{"pillar": "Operational Excellence", "id": "OPS3a",
                 "label": "No EventBridge rules for Config compliance events", "status": "warn",
                 "detail": "Configure EventBridge to respond to AWS Config compliance changes "
                           "and trigger SNS notifications or Lambda remediation"}]
    except Exception as e:
        return [{"pillar": "Operational Excellence", "id": "OPS3a",
                 "label": f"Cannot check EventBridge rules: {e}", "status": "warn", "detail": str(e)}]


@register_check("OPS3b", "Operational Excellence", "Pre-production benchmarking",
                source="infrastructure", priority=24)
def check_benchmarking(ctx):
    """OPS 3 — Benchmark info note."""
    return [{"pillar": "Operational Excellence", "id": "OPS3b",
             "label": "Pre-production benchmarking — review recommended", "status": "info",
             "detail": "Benchmark with representative workload patterns before production. "
                       "Record baseline values for BufferCacheHitRatio, IndexBufferCacheHitRatio, "
                       "VolumeReadIOPS, VolumeWriteIOPS, and DBClusterReplicaLagMaximum."}]


@register_check("OPS4a", "Operational Excellence", "Operational readiness",
                source="infrastructure", priority=30)
def check_operational_readiness(ctx):
    """OPS 4 — Operational readiness info note."""
    return [{"pillar": "Operational Excellence", "id": "OPS4a",
             "label": "Operational readiness — review recommended", "status": "info",
             "detail": "Maintain playbooks for failover, performance degradation, connection "
                       "exhaustion, and security incidents. Run readiness tests during maintenance "
                       "windows. Ensure operations personnel are trained on DocumentDB best practices."}]


@register_check("OPS4b", "Operational Excellence", "Maintenance window",
                source="infrastructure", priority=31)
def check_maintenance_window_v2(ctx):
    """OPS 4 — Maintenance window alignment."""
    window = ctx.cluster.get("PreferredMaintenanceWindow", "not set")
    return [{"pillar": "Operational Excellence", "id": "OPS4b",
             "label": f"Maintenance window: {window}", "status": "info",
             "detail": "Verify this window aligns with your lowest-traffic period"}]


@register_check("OPS5d", "Operational Excellence", "Performance Insights",
                source="infrastructure", priority=25)
def check_performance_insights(ctx):
    """OPS 5 — Performance Insights enabled on all instances."""
    disabled = [i["DBInstanceIdentifier"] for i in ctx.instances
                if not i.get("PerformanceInsightsEnabled", False)]
    if not disabled:
        return [{"pillar": "Operational Excellence", "id": "OPS5d",
                 "label": "Performance Insights enabled on all instances", "status": "pass",
                 "detail": ""}]
    return [{"pillar": "Operational Excellence", "id": "OPS5d",
             "label": f"Performance Insights disabled on: {', '.join(disabled)}", "status": "warn",
             "detail": "Enable Performance Insights for visual database load analysis "
                       "via average active sessions (AAS)"}]


@register_check("OPS5e", "Operational Excellence", "DML audit logging",
                source="infrastructure", priority=26)
def check_dml_audit(ctx):
    """OPS 5 — DML audit logging via audit_filter parameter."""
    try:
        pg_name = ctx.cluster.get("DBClusterParameterGroup", "")
        dml_enabled = False
        if pg_name:
            params = ctx.docdb_client.describe_db_cluster_parameters(
                DBClusterParameterGroupName=pg_name)["Parameters"]
            for p in params:
                if p.get("ParameterName") == "audit_filter":
                    val = p.get("ParameterValue", p.get("DefaultValue", ""))
                    dml_enabled = bool(val and val.lower() not in ("none", "disabled", ""))
                    break
        return [{"pillar": "Operational Excellence", "id": "OPS5e",
                 "label": f"DML audit logging {'enabled' if dml_enabled else 'not enabled'}",
                 "status": "pass" if dml_enabled else "info",
                 "detail": "" if dml_enabled else
                 "Enable DML auditing to log create, read, update, and delete operations "
                 "for compliance and access visibility"}]
    except Exception as e:
        return [{"pillar": "Operational Excellence", "id": "OPS5e",
                 "label": f"Cannot check DML audit: {e}", "status": "warn", "detail": str(e)}]
