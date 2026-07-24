"""WA Checks — Cost Optimization Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Cost Optimization
IDs are stable — do not rename (COST1, COST3, COST6, COST7, COST9).
"""
import logging
from datetime import datetime, timedelta
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)


@register_check("COST1", "Cost Optimization", "CPU utilization",
                source="cloudwatch", per_instance=True, priority=10)
def check_cpu_utilization(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    itype = inst["DBInstanceClass"]
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="CPUUtilization",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
        dps = [d["Average"] for d in resp.get("Datapoints", [])]
        if dps:
            avg = sum(dps) / len(dps)
            p95 = sorted(dps)[int(len(dps) * 0.95)]
            status = "warn" if p95 < 10 else "pass"
            detail = f"Instance {itype} may be oversized" if p95 < 10 else ""
            return [{"pillar": "Cost Optimization", "id": "COST1",
                     "label": f"CPU for {iid} (avg {avg:.1f}%, P95 {p95:.1f}%)",
                     "status": status, "detail": detail}]
        return [{"pillar": "Cost Optimization", "id": "COST1",
                 "label": f"No CPU data for {iid}",
                 "status": "warn", "detail": "No datapoints in last 7 days"}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST1",
                 "label": f"Cannot check CPU for {iid}: {e}",
                 "status": "warn", "detail": ""}]


@register_check("COST4a", "Cost Optimization", "Unused indexes",
                source="infrastructure", priority=20)
def check_unused_indexes(ctx):
    """Unused-index cost guidance (COST 4 — Index Efficiency). Static info note.

    Emitted unconditionally as info so it always appears under "COST 4 — Index
    Efficiency" regardless of whether database analysis data is present. Index
    detection logic is intentionally not performed here — this is guidance only.
    Uses id COST4a (not COST3*) so it does NOT group under "COST 3 — Storage".
    """
    return [{"pillar": "Cost Optimization", "id": "COST4a",
             "label": "Unused indexes — review and drop to lower storage cost",
             "status": "info",
             "detail": "Drop unused indexes for lower storage cost. Unused indexes "
                       "consume storage and add write I/O. Use $indexStats to identify "
                       "indexes with zero operations, and drop them after confirming "
                       "they are not needed for periodic/reporting queries. "
                       "Check the Database Overview section for unused indexes "
                       "detected on this cluster."}]


@register_check("COST6", "Cost Optimization", "Cost allocation tags",
                source="infrastructure", priority=30)
def check_tags(ctx):
    try:
        tags_resp = ctx.docdb_client.list_tags_for_resource(
            ResourceName=ctx.cluster.get("DBClusterArn", ""))
        n = len(tags_resp.get("TagList", []))
        return [{"pillar": "Cost Optimization", "id": "COST6",
                 "label": f"Cost allocation tags ({n} tags)",
                 "status": "pass" if n >= 2 else "warn",
                 "detail": "Add cost allocation tags for expense tracking" if n < 2 else ""}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST6",
                 "label": f"Cannot check tags: {e}",
                 "status": "warn", "detail": ""}]


@register_check("COST7", "Cost Optimization", "Storage type",
                source="infrastructure", priority=40)
def check_storage_type(ctx):
    storage_type = ctx.cluster.get("StorageType", "standard")
    return [{"pillar": "Cost Optimization", "id": "COST7",
             "label": f"Storage type: {storage_type}",
             "status": "info",
             "detail": "Evaluate I/O-Optimized for write-heavy workloads (>25% I/O cost)"
             if storage_type != "iopt1" else "I/O-Optimized active"}]


@register_check("COST9", "Cost Optimization", "Idle reader detection",
                source="cloudwatch", per_instance=True, reader_only=True, priority=50)
def check_idle_readers(ctx):
    inst = ctx.current_instance
    iid = inst["DBInstanceIdentifier"]
    try:
        # Do not recommend removing readers if cluster has fewer than 3 instances
        total_instances = len(ctx.instances)

        end = datetime.utcnow()
        start = end - timedelta(days=7)

        # Check connections
        conn_resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="DatabaseConnections",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
        conn_dps = [d["Average"] for d in conn_resp.get("Datapoints", [])]
        avg_conn = sum(conn_dps) / len(conn_dps) if conn_dps else 0

        # Check ReadIOPS
        iops_resp = ctx.cw_client.get_metric_statistics(
            Namespace="AWS/DocDB", MetricName="ReadIOPS",
            Dimensions=[{"Name": "DBInstanceIdentifier", "Value": iid}],
            StartTime=start, EndTime=end, Period=3600, Statistics=["Average"])
        iops_dps = [d["Average"] for d in iops_resp.get("Datapoints", [])]
        avg_iops = sum(iops_dps) / len(iops_dps) if iops_dps else 0

        is_idle = avg_conn < 2 and avg_iops < 5
        if is_idle:
            if total_instances < 3:
                # Don't recommend removal — use for read scaling instead
                return [{"pillar": "Cost Optimization", "id": "COST9",
                         "label": f"Reader {iid} has low activity (avg {avg_conn:.0f} conns, {avg_iops:.0f} IOPS)",
                         "status": "pass",
                         "detail": "Reader retained for HA and read scaling. "
                                   "Route read traffic using readPreference=secondaryPreferred to utilise this reader."}]
            return [{"pillar": "Cost Optimization", "id": "COST9",
                     "label": f"Reader {iid} appears idle (avg {avg_conn:.0f} conns, {avg_iops:.0f} IOPS)",
                     "status": "warn",
                     "detail": "Consider routing read traffic to this reader using readPreference=secondaryPreferred "
                               "to utilise it for read scaling before considering removal."}]
        return [{"pillar": "Cost Optimization", "id": "COST9",
                 "label": f"Reader {iid} active (avg {avg_conn:.0f} conns, {avg_iops:.0f} IOPS)",
                 "status": "pass", "detail": ""}]
    except Exception:
        return []


# ── Migrated from tabs/wa_v2/cost.py (Phase 3C) ──────────────────────────────

@register_check("COST3a", "Cost Optimization", "Storage type cost analysis",
                source="infrastructure", priority=41)
def check_storage_cost_analysis(ctx):
    """COST 3 — Storage type optimality based on cluster snapshot cost analysis."""
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            snap_data = _snap.get("data")

        cost_data = snap_data.get("cost_data") if snap_data else None
        cluster_info = snap_data.get("cluster", {}) if snap_data else {}
        storage_type = cluster_info.get("storage_type", "Standard")

        if cost_data:
            sc = cost_data["standard_costs"]
            ic = cost_data["iopt_costs"]
            savings = cost_data["potential_savings"]
            cur_type = cost_data.get("current_type", "standard")
            is_std = cur_type == "standard"
            std_wins = sc["total"] <= ic["total"]

            if (is_std and std_wins) or (not is_std and not std_wins):
                return [{"pillar": "Cost Optimization", "id": "COST3a",
                         "label": f"Storage type: {storage_type} — optimal for current workload",
                         "status": "pass",
                         "detail": f"Standard: ${sc['total']:,.2f}/mo  ·  I/O-Optimized: ${ic['total']:,.2f}/mo. "
                                   "Current storage type is the most cost-effective choice based on your I/O pattern"}]
            target = "I/O-Optimized" if is_std else "Standard"
            return [{"pillar": "Cost Optimization", "id": "COST3a",
                     "label": f"Storage type: {storage_type} — consider switching to {target} (save ${savings:,.2f}/mo)",
                     "status": "warn",
                     "detail": f"Standard: ${sc['total']:,.2f}/mo  ·  I/O-Optimized: ${ic['total']:,.2f}/mo. "
                               f"Switching to {target} storage would save approximately ${savings:,.2f}/month "
                               "based on your actual I/O usage over the last 30 days"}]
        return [{"pillar": "Cost Optimization", "id": "COST3a",
                 "label": f"Storage type: {storage_type} — cost analysis pending",
                 "status": "info",
                 "detail": "Load Cluster Overview to run storage cost analysis and determine "
                           "whether Standard or I/O-Optimized storage is optimal for your workload"}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST3a",
                 "label": f"Cannot check storage type: {e}", "status": "warn", "detail": str(e)}]


@register_check("COST3b", "Cost Optimization", "Default compression",
                source="infrastructure", priority=42)
def check_default_compression(ctx):
    """COST 3 — Default collection compression parameter."""
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            snap_data = _snap.get("data")
        cluster_info = snap_data.get("cluster", {}) if snap_data else {}
        compression = str(cluster_info.get("compression", "None"))
        comp_l = compression.lower()

        if not snap_data:
            return [{"pillar": "Cost Optimization", "id": "COST3b",
                     "label": "Compression — load Cluster Overview to check",
                     "status": "info",
                     "detail": "Enable default collection compression (zstd or lz4) to reduce storage size and I/O costs"}]
        if comp_l == "none":
            return [{"pillar": "Cost Optimization", "id": "COST3b",
                     "label": "Default collection compression unavailable on this engine version",
                     "status": "info",
                     "detail": "default_collection_compression requires DocumentDB 5.0+. Upgrade to enable "
                               "cluster-level default compression and reduce storage/I/O costs"}]
        if comp_l == "disabled":
            return [{"pillar": "Cost Optimization", "id": "COST3b",
                     "label": "Default compression not enabled", "status": "warn",
                     "detail": "Set default_collection_compression in the cluster parameter group (5.0: enabled; "
                               "8.0: zstd or lz4) to reduce storage footprint and lower I/O costs"}]
        return [{"pillar": "Cost Optimization", "id": "COST3b",
                 "label": f"Default compression enabled: {compression.upper()}", "status": "pass",
                 "detail": "Compression reduces storage size and I/O — ensure all collections use the cluster default"}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST3b",
                 "label": f"Cannot check compression: {e}", "status": "warn", "detail": str(e)}]


@register_check("COST4b", "Cost Optimization", "Index count per collection",
                source="infrastructure", priority=21)
def check_index_count(ctx):
    """COST 4 — Index count guidance (static info note)."""
    return [{"pillar": "Cost Optimization", "id": "COST4b",
             "label": "Index count per collection — requires database analysis",
             "status": "info",
             "detail": "Maintain 5 or fewer indexes per collection. Each index adds write I/O. "
                       "Run Analyze on a database to check index counts per collection."}]


@register_check("COST5", "Cost Optimization", "Stale manual snapshots",
                source="infrastructure", priority=45)
def check_stale_snapshots(ctx):
    """COST 5 — Manual snapshots older than retention period."""
    try:
        retention = ctx.cluster.get("BackupRetentionPeriod", 7)
        cutoff = datetime.utcnow() - timedelta(days=retention)
        snaps = ctx.docdb_client.describe_db_cluster_snapshots(
            DBClusterIdentifier=ctx.cluster_id,
            SnapshotType="manual")["DBClusterSnapshots"]
        stale = [s["DBClusterSnapshotIdentifier"] for s in snaps
                 if s.get("SnapshotCreateTime") and
                 s["SnapshotCreateTime"].replace(tzinfo=None) < cutoff]
        if stale:
            return [{"pillar": "Cost Optimization", "id": "COST5",
                     "label": f"{len(stale)} manual snapshot(s) older than {retention}-day retention",
                     "status": "warn",
                     "detail": f"Manual snapshots are billed indefinitely. "
                               f"Delete or archive: {', '.join(stale[:3])}"
                               + (" and more" if len(stale) > 3 else "")}]
        return [{"pillar": "Cost Optimization", "id": "COST5",
                 "label": "No stale manual snapshots found", "status": "pass", "detail": ""}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST5",
                 "label": f"Cannot check manual snapshots: {e}", "status": "warn", "detail": str(e)}]


@register_check("COST10", "Cost Optimization", "Serverless evaluation",
                source="cloudwatch", priority=55)
def check_serverless_eval(ctx):
    """COST 1 — Evaluate Serverless suitability based on utilization patterns."""
    try:
        end = datetime.utcnow()
        start = end - timedelta(days=7)
        idle_count = 0
        for inst in ctx.instances:
            iid = inst["DBInstanceIdentifier"]
            dim = [{"Name": "DBInstanceIdentifier", "Value": iid}]
            try:
                resp = ctx.cw_client.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="CPUUtilization",
                    Dimensions=dim, StartTime=start, EndTime=end,
                    Period=3600, Statistics=["Average"])
                dps = [d["Average"] for d in resp.get("Datapoints", [])]
                if dps:
                    avg = sum(dps) / len(dps)
                    idle_pct = sum(1 for d in dps if d < 10) / len(dps) * 100
                    if avg < 20 and idle_pct > 40:
                        idle_count += 1
            except Exception:
                pass

        if idle_count > 0:
            return [{"pillar": "Cost Optimization", "id": "COST10",
                     "label": f"{idle_count} instance(s) show low/variable utilization",
                     "status": "warn",
                     "detail": "Evaluate Amazon DocumentDB Serverless for variable or unpredictable "
                               "workloads — up to 90% cost savings vs provisioning for peak capacity"}]
        return [{"pillar": "Cost Optimization", "id": "COST10",
                 "label": "Instance utilization consistent — provisioned sizing appropriate",
                 "status": "pass",
                 "detail": "Serverless is best suited for intermittent or unpredictable workloads"}]
    except Exception as e:
        return [{"pillar": "Cost Optimization", "id": "COST10",
                 "label": f"Cannot evaluate Serverless suitability: {e}", "status": "warn", "detail": str(e)}]
