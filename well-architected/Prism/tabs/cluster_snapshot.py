"""Cluster Overview — cluster config, instances, metrics, instance recommendations."""
import logging
import threading
import boto3
from datetime import datetime, timedelta
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ctx, ALL
import pandas as pd
import plotly.express as px
from tabs.ui_helpers import coffee_animation, wire_animation, section_title
from aws_discovery import get_cluster_databases
from instance_recommender import analyze_workload_statistics, recommend_instance_type
from storage_cost_analyzer import analyze_storage_costs
import plotly.graph_objects as go

logger = logging.getLogger(__name__)

_snap = {"data": None, "running": False, "done": False, "error": None, "fetched_at": None, "phase": 0, "rendered_phase": None}
_snap_lock = threading.Lock()


def start_snapshot_if_needed(cluster_id, region, connection_string, db_count=None):
    """Start background snapshot if not already running/done. Safe to call multiple times."""
    if not cluster_id:
        return
    with _snap_lock:
        if _snap["running"] or _snap["done"]:
            return
    threading.Thread(target=_run_snapshot,
                     args=(cluster_id, region, connection_string, db_count),
                     daemon=True).start()


def reset_snapshot():
    """Reset snapshot state (e.g. when switching clusters)."""
    with _snap_lock:
        _snap.update(data=None, running=False, done=False, error=None, fetched_at=None, phase=0, rendered_phase=None)


def _run_snapshot(cluster_id, region, connection_string, known_db_count=None):
    """Gather all cluster-level data in background."""
    with _snap_lock:
        _snap.update(data=None, running=True, done=False, error=None, fetched_at=datetime.utcnow(), phase=0, rendered_phase=None)

    try:
        docdb = boto3.client("docdb", region_name=region)
        cw = boto3.client("cloudwatch", region_name=region)

        # ═══ PHASE 1: Cluster config + instance list (fast, ~1s) ════════════
        cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]

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

        # Last failover event (last 90 days)
        last_failover_time = None
        try:
            evt_resp = docdb.describe_events(
                SourceIdentifier=cluster_id, SourceType="db-cluster",
                StartTime=datetime.utcnow() - timedelta(days=90),
                EndTime=datetime.utcnow())
            failover_events = [
                e for e in evt_resp.get("Events", [])
                if "failover" in e.get("Message", "").lower()
                or "failover" in ",".join(e.get("EventCategories", [])).lower()
            ]
            if failover_events:
                last_failover_time = max(e["Date"] for e in failover_events)
        except Exception:
            pass

        insts = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]

        # Compression + storage type (fast param group lookup)
        # default_collection_compression exists only on DocumentDB 5.0+.
        #   5.0 → "enabled" | "disabled"
        #   8.0 → "zstd" | "lz4" | "disabled"  (defaults to zstd)
        #   3.6 / 4.0 → parameter absent → report "None"
        compression_enabled = "None"
        cs_retention_sec = 0
        profiler_enabled = "Unknown"
        profiler_threshold_ms = None
        pg_name = cl.get("DBClusterParameterGroup", "")
        if pg_name:
            try:
                params = docdb.describe_db_cluster_parameters(DBClusterParameterGroupName=pg_name)
                for p in params.get("Parameters", []):
                    if p.get("ParameterName") == "default_collection_compression":
                        val = p.get("ParameterValue", p.get("DefaultValue", "")) or ""
                        compression_enabled = val.lower() if val else "disabled"
                    if p.get("ParameterName") == "change_stream_log_retention_duration":
                        cs_val = p.get("ParameterValue", p.get("DefaultValue", "0"))
                        try:
                            cs_retention_sec = int(cs_val)
                        except (ValueError, TypeError):
                            cs_retention_sec = 0
                    if p.get("ParameterName") == "profiler":
                        prof_val = p.get("ParameterValue", p.get("DefaultValue", "disabled"))
                        profiler_enabled = prof_val if prof_val else "disabled"
                    if p.get("ParameterName") == "profiler_threshold_ms":
                        pt_val = p.get("ParameterValue", p.get("DefaultValue"))
                        try:
                            profiler_threshold_ms = int(pt_val)
                        except (ValueError, TypeError):
                            profiler_threshold_ms = None
            except Exception:
                pass

        if insts and any("iopt1" in inst.get("DBInstanceClass", "").lower() or
                         inst.get("StorageType", "") == "iopt1" for inst in insts):
            storage_type = "I/O Optimized"
        else:
            storage_type = "Standard"

        # Build skeleton instance list (no metrics yet)
        inst_skeleton = []
        for inst in insts:
            iid = inst["DBInstanceIdentifier"]
            itype = inst["DBInstanceClass"]
            az = inst.get("AvailabilityZone", "—")
            role = "Writer" if any(
                m.get("DBInstanceIdentifier") == iid and m.get("IsClusterWriter", False)
                for m in cl.get("DBClusterMembers", [])) else "Reader"
            inst_skeleton.append({
                "id": iid, "type": itype, "az": az, "role": role,
                "endpoint": inst.get("Endpoint", {}).get("Address", ""),
                "port": inst.get("Endpoint", {}).get("Port", 27017),
                "status": inst.get("DBInstanceStatus", "—"),
                "cpu_avg": None, "cpu_max": None,
                "conn_avg": None, "conn_max": None,
                "mem_avg_gb": None, "cpu_datapoints": [],
                "buffer_cache_hit_ratio": None,
            })

        # Publish Phase 1 — overview table + observations render immediately
        data = {
            "cluster": {
                "id": cl.get("DBClusterIdentifier", "—"),
                "engine_version": cl.get("EngineVersion", "—"),
                "status": cl.get("Status", "—"),
                "storage_encrypted": cl.get("StorageEncrypted", False),
                "deletion_protection": cl.get("DeletionProtection", False),
                "backup_retention": cl.get("BackupRetentionPeriod", 0),
                "logs_exports": cl.get("EnabledCloudwatchLogsExports", []),
                "audit_log": "audit" in cl.get("EnabledCloudwatchLogsExports", []),
                "profiler_log": "profiler" in cl.get("EnabledCloudwatchLogsExports", []),
                "profiler_enabled": profiler_enabled,
                "profiler_threshold_ms": profiler_threshold_ms,
                "latest_restorable_time": cl.get("LatestRestorableTime"),
                "is_global": is_global,
                "global_cluster_id": global_cluster_id,
                "compression": compression_enabled,
                "storage_type": storage_type,
                "last_failover_time": last_failover_time.isoformat() if last_failover_time else None,
            },
            "cost_data": None,
            "instances": inst_skeleton,
            "storage_gb": 0,
            "db_count": None,
            "change_streams": [],
            "cs_retention_hours": round(cs_retention_sec / 3600, 1) if cs_retention_sec > 0 else 0,
        }
        with _snap_lock:
            _snap.update(data=dict(data), phase=1)

        # ═══ PHASE 2: CloudWatch metrics per instance (slow, ~3-8s) ═══════
        end = datetime.utcnow()
        start_7d = end - timedelta(days=7)
        start_48h = end - timedelta(hours=48)

        for idx, inst in enumerate(insts):
            iid = inst["DBInstanceIdentifier"]
            dim = [{"Name": "DBInstanceIdentifier", "Value": iid}]
            m = inst_skeleton[idx]  # mutate in place

            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="CPUUtilization",
                    Dimensions=dim, StartTime=start_7d, EndTime=end,
                    Period=3600, Statistics=["Average", "Maximum"])
                dps = resp.get("Datapoints", [])
                if dps:
                    m["cpu_avg"] = sum(d["Average"] for d in dps) / len(dps)
                    m["cpu_max"] = max(d["Maximum"] for d in dps)
            except Exception as e:
                logger.warning("CPU metrics failed for %s: %s", iid, e)

            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="CPUUtilization",
                    Dimensions=dim, StartTime=start_48h, EndTime=end,
                    Period=300, Statistics=["Average", "Maximum", "Minimum"])
                dps = sorted(resp.get("Datapoints", []), key=lambda x: x["Timestamp"])
                m["cpu_datapoints"] = [d["Average"] for d in dps]
            except Exception:
                pass

            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="DatabaseConnections",
                    Dimensions=dim, StartTime=start_7d, EndTime=end,
                    Period=3600, Statistics=["Average", "Maximum"])
                dps = resp.get("Datapoints", [])
                if dps:
                    m["conn_avg"] = sum(d["Average"] for d in dps) / len(dps)
                    m["conn_max"] = max(d["Maximum"] for d in dps)
            except Exception:
                pass

            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="FreeableMemory",
                    Dimensions=dim, StartTime=start_7d, EndTime=end,
                    Period=3600, Statistics=["Average"])
                dps = resp.get("Datapoints", [])
                if dps:
                    m["mem_avg_gb"] = sum(d["Average"] for d in dps) / len(dps) / (1024**3)
            except Exception:
                pass

            try:
                resp = cw.get_metric_statistics(
                    Namespace="AWS/DocDB", MetricName="BufferCacheHitRatio",
                    Dimensions=dim, StartTime=start_48h, EndTime=end,
                    Period=3600, Statistics=["Average"])
                dps = resp.get("Datapoints", [])
                if dps:
                    m["buffer_cache_hit_ratio"] = sum(d["Average"] for d in dps) / len(dps)
            except Exception:
                pass

        # Storage (cluster level)
        storage_bytes = 0
        try:
            resp = cw.get_metric_statistics(
                Namespace="AWS/DocDB", MetricName="VolumeBytesUsed",
                Dimensions=[{"Name": "DBClusterIdentifier", "Value": cluster_id}],
                StartTime=start_7d, EndTime=end, Period=86400, Statistics=["Maximum"])
            dps = resp.get("Datapoints", [])
            if dps:
                storage_bytes = max(d["Maximum"] for d in dps)
        except Exception:
            pass

        # Database count (pymongo connect is slow — kept out of Phase 1)
        db_count = known_db_count
        if db_count is None:
            if connection_string:
                try:
                    dbs = get_cluster_databases(connection_string)
                    db_count = len(dbs) if dbs else 0
                except Exception:
                    db_count = 0

        # Query change streams configuration via $listChangeStreams
        change_streams = []
        if connection_string:
            try:
                import pymongo
                cs_client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000, appname='DocDB-Prism')
                cs_cursor = cs_client.admin.aggregate([{"$listChangeStreams": 1}])
                for cs in cs_cursor:
                    db_name = cs.get("database", "")
                    coll_name = cs.get("collection", "")
                    if db_name == "" and coll_name == "":
                        change_streams.append({"scope": "all databases"})
                    elif coll_name == "":
                        change_streams.append({"scope": "database", "database": db_name})
                    else:
                        change_streams.append({"scope": "collection", "database": db_name, "collection": coll_name})
                cs_client.close()
            except Exception as e:
                logger.debug("Change streams query failed: %s", e)

        # Publish Phase 2 — instances table + recommendations now have data
        data["instances"] = inst_skeleton
        data["storage_gb"] = storage_bytes / (1024**3)
        data["db_count"] = db_count
        data["change_streams"] = change_streams
        with _snap_lock:
            _snap.update(data=dict(data), phase=2)

        # ═══ PHASE 3: Storage cost analysis (slowest, pricing API) ═══════
        cost_data = None
        try:
            cost_data = analyze_storage_costs(cluster_id, region)
        except Exception as e:
            logger.warning("Storage cost analysis failed: %s", e)

        data["cost_data"] = cost_data

        with _snap_lock:
            _snap.update(data=data, running=False, done=True, fetched_at=datetime.utcnow(), phase=3)
        logger.info("Cluster snapshot complete: %d instances, %.2f GB storage, %s databases",
                     len(inst_skeleton), data["storage_gb"], data.get("db_count") or "?")

    except Exception as e:
        logger.error("Cluster snapshot failed: %s", e, exc_info=True)
        with _snap_lock:
            _snap.update(running=False, done=True, error=str(e))


def render_cluster_snapshot(cluster_id, region, connection_string):
    """Render the cluster snapshot tab."""
    if not cluster_id:
        return html.Div([
            html.Span("Cluster Overview", className="section-title"),
            dbc.Alert("Select a cluster to view snapshot.", color="warning"),
        ])

    with _snap_lock:
        cached = _snap["data"]
        cached_done = _snap["done"]
        running = _snap["running"]

    initial_results = ""
    initial_loading = ""
    poll_disabled = True
    if running and not cached_done:
        initial_loading = html.Div(
            coffee_animation("Connecting to cluster...", pct=10),
            className="d-flex align-items-center justify-content-center py-3")
        poll_disabled = False
    elif cached and cached_done:
        initial_results = _render_snapshot(cached)

    # Elapsed time label for reload button
    btn_label = "Load Snapshot"
    if cached and cached_done:
        fetched_at = _snap.get("fetched_at")
        if fetched_at:
            elapsed = int((datetime.utcnow() - fetched_at).total_seconds())
            btn_label = f"Reload  ·  {elapsed}s ago"
        else:
            btn_label = "Reload Snapshot"

    return html.Div([
        html.Div([
            html.Div([
                html.Span("Cluster Overview", className="section-title",
                           style={"marginBottom": "0", "borderBottom": "none", "paddingBottom": "0"}),
                html.Span("  ·  ", style={"color": "var(--text-muted)", "margin": "0 .3rem"}),
                html.Span(cluster_id, style={"fontSize": ".88rem", "fontWeight": "600",
                                              "color": "var(--text-body)", "fontFamily": "monospace"}),
                html.Span(f"  ({region})", style={"fontSize": ".8rem", "color": "var(--text-muted)"}),
            ], style={"display": "flex", "alignItems": "baseline", "flexWrap": "wrap", "gap": ".1rem"}),
            dbc.Button(btn_label, id="snap-run-btn", color="warning",
                       size="sm", style={"fontWeight": "600", "whiteSpace": "nowrap"}),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                  "marginBottom": ".75rem", "paddingBottom": ".4rem",
                  "borderBottom": "2px solid var(--color-primary)"}),
        dcc.Store(id="snap-meta", data={"cluster_id": cluster_id, "region": region,
                                         "connection_string": connection_string,
                                         "db_count": _snap.get("data", {}).get("db_count") if _snap.get("data") else None}),
        dcc.Store(id="snap-sections-state", data={"instances": False, "storage_detail": False}),
        html.Div(id="snap-loading", children=initial_loading),
        html.Div(id="snap-results", children=initial_results),
        dcc.Interval(id="snap-poll", interval=800, disabled=poll_disabled),
        dcc.Interval(id="snap-age-timer", interval=30000, disabled=not (cached and cached_done)),
    ])


@callback(
    Output("snap-loading", "children", allow_duplicate=True),
    Output("snap-results", "children", allow_duplicate=True),
    Output("snap-poll", "disabled", allow_duplicate=True),
    Output("snap-run-btn", "children", allow_duplicate=True),
    Input("snap-run-btn", "n_clicks"),
    State("snap-meta", "data"),
    prevent_initial_call=True,
)
def cb_snap_start(n, meta):
    if not meta:
        return no_update, no_update, True, no_update
    threading.Thread(target=_run_snapshot,
                     args=(meta["cluster_id"], meta["region"], meta.get("connection_string"),
                           meta.get("db_count")),
                     daemon=True).start()
    return html.Div(
        coffee_animation("Reloading cluster data...", pct=10),
        className="d-flex align-items-center justify-content-center py-3"), "", False, "Reloading..."


@callback(
    Output("snap-loading", "children"),
    Output("snap-results", "children"),
    Output("snap-poll", "disabled"),
    Output("snap-run-btn", "children"),
    Input("snap-poll", "n_intervals"),
    prevent_initial_call=True,
)
def cb_snap_poll(n):
    with _snap_lock:
        done = _snap["done"]
        error = _snap["error"]
        data = _snap["data"]
        phase = _snap.get("phase", 0)
        fetched_at = _snap.get("fetched_at")

    if error:
        return html.Span(f"Error: {error[:80]}", style={"color": "#d91515", "fontSize": ".85rem"}), "", True, "Reload Snapshot"

    if not data:
        return no_update, no_update, False, no_update

    # Flicker guard: this poll fires every 800ms, but the snapshot data only
    # changes at phase boundaries (1→2→3) and on completion. Re-rendering the
    # (large) results subtree on every tick replaces the whole DOM and makes the
    # Overview visibly flicker until the slow final phase finishes. Only rebuild
    # when the phase actually advanced or we're done; otherwise leave the DOM
    # untouched and just keep polling.
    with _snap_lock:
        last_rendered = _snap.get("rendered_phase")
    if not done and phase == last_rendered:
        return no_update, no_update, False, no_update

    # Compute button label from fetched_at
    if fetched_at and done:
        elapsed = int((datetime.utcnow() - fetched_at).total_seconds())
        btn_label = f"Reload  ·  {elapsed}s ago"
    else:
        btn_label = "Reloading..."

    # Render whatever we have so far
    results = _render_snapshot(data)
    with _snap_lock:
        _snap["rendered_phase"] = phase

    if done:
        return "", results, True, btn_label

    # Still loading — show phase indicator + partial results
    phase_labels = {1: "Fetching CloudWatch metrics...", 2: "Analyzing storage costs..."}
    loading = html.Div(
        coffee_animation(phase_labels.get(phase, "Loading..."), pct=min(30 + phase * 25, 90)),
        className="d-flex align-items-center justify-content-center py-3")
    loading_old = html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span(phase_labels.get(phase, "Loading..."),
                  style={"fontSize": ".84rem", "color": "var(--text-muted)"}),
    ], className="d-flex align-items-center mb-2")
    return loading, results, False, btn_label


# ── Section collapse toggle ──────────────────────────────────────────────────
@callback(
    Output("snap-sections-state", "data"),
    Output({"type": "snap-collapse", "section": ALL}, "is_open"),
    Output({"type": "snap-chevron", "section": ALL}, "children"),
    Input({"type": "snap-section-btn", "section": ALL}, "n_clicks"),
    State("snap-sections-state", "data"),
    prevent_initial_call=True,
)
def cb_snap_toggle_section(clicks, state):
    if not ctx.triggered_id or not any(c for c in clicks if c):
        from dash import no_update
        return no_update, [no_update] * len(clicks), [no_update] * len(clicks)

    section = ctx.triggered_id["section"]
    state = dict(state) if state else {"instances": False, "storage_detail": False}
    state[section] = not state.get(section, False)

    sections_order = ["instances", "storage_detail"]
    is_open = [state.get(s, False) for s in sections_order]
    chevrons = ["▾" if o else "▸" for o in is_open]
    return state, is_open, chevrons


def _collapsible_header(title, section_key, is_open=False, count=None):
    """Build a clickable collapsible section header."""
    chevron = "▾" if is_open else "▸"
    badge = ""
    if count is not None:
        badge = dbc.Badge(str(count), color="secondary", className="ms-2", style={"fontSize": ".7rem"})
    return html.Div([
        html.Div([
            html.Span(chevron, id={"type": "snap-chevron", "section": section_key},
                       style={"fontSize": ".85rem", "marginRight": ".5rem", "color": "var(--text-muted)", "width": "1rem", "display": "inline-block"}),
            html.Span(title, style={"fontWeight": "700", "fontSize": ".92rem"}),
            badge,
        ], className="d-flex align-items-center"),
    ], id={"type": "snap-section-btn", "section": section_key},
       style={"cursor": "pointer", "padding": ".6rem .8rem", "borderRadius": "8px",
              "background": "var(--bg-surface-alt)", "border": "1px solid var(--border-default)",
              "marginBottom": ".3rem", "userSelect": "none"},
       className="snap-section-header")


_REC_ICONS = {'serverless': '✅', 'nvme': '🚀', 'standard_downgrade': '🔽',
              'standard_upgrade': '🔼', 'no_change': '✅'}

# ── Shared table styling ─────────────────────────────────────────────────────
_TH = {"padding": ".45rem .7rem", "fontSize": ".7rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".5px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_TD = {"padding": ".4rem .7rem", "fontSize": ".84rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "verticalAlign": "middle"}
_TD_VAL = {**_TD, "fontWeight": "600", "color": "var(--text-heading)"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)"}


def _status_dot(ok, label_ok="Enabled", label_bad="Disabled", optional=False):
    """Inline colored status text. Optional settings show muted when disabled, not red."""
    if ok:
        color = "var(--accent-green)"
    elif optional:
        color = "var(--text-muted)"
    else:
        color = "var(--accent-red)"
    text = label_ok if ok else label_bad
    return html.Span(text, style={"color": color, "fontWeight": "600"})


def _loading_val(prefix=""):
    """Inline loading placeholder for values not yet available."""
    parts = []
    if prefix:
        parts.append(html.Span(prefix, style={"marginRight": ".3rem"}))
    parts.append(html.Span("⏳", style={"animation": "pulse 1.5s ease-in-out infinite",
                                         "fontSize": ".8rem"}))
    return html.Span(parts)


def _fmt_failover(ts_str):
    """Format last failover timestamp as a human-readable string."""
    if not ts_str:
        return html.Span("None in last 90 days", style={"color": "var(--accent-green)", "fontWeight": "600"})
    try:
        from datetime import timezone
        ts = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        elapsed = datetime.now(timezone.utc) - ts
        days = elapsed.days
        age = f"{days}d ago" if days > 0 else "today"
        label = ts.strftime("%d %b %Y %H:%M UTC") + f"  ·  {age}"
        color = "var(--accent-red)" if days < 7 else "var(--color-warning)" if days < 30 else "var(--text-body)"
        return html.Span(label, style={"color": color, "fontWeight": "600", "fontFamily": "monospace", "fontSize": ".82rem"})
    except Exception:
        return ts_str


def _render_snapshot(data):
    """Build the full snapshot UI — executive-grade tables, no cards."""
    cl = data["cluster"]
    insts = data["instances"]
    storage_gb = data["storage_gb"]
    db_count = data["db_count"]
    cost_data = data.get("cost_data")


    # Change streams info
    cs_list = data.get("change_streams", [])
    cs_retention = data.get("cs_retention_hours", 0)
    if cs_list:
        cs_all = any(cs.get("scope") == "all databases" for cs in cs_list)
        if cs_all:
            cs_label = "All databases"
        else:
            cs_dbs = [cs.get("database", "") for cs in cs_list if cs.get("scope") == "database"]
            cs_colls = [cs for cs in cs_list if cs.get("scope") == "collection"]
            parts = []
            if cs_dbs:
                parts.append(f"{len(cs_dbs)} database(s)")
            if cs_colls:
                parts.append(f"{len(cs_colls)} collection(s)")
            cs_label = ", ".join(parts)
        if cs_retention:
            cs_label += f" \u00b7 {cs_retention}h retention"
    else:
        cs_label = "Not enabled"

    n_writers = sum(1 for i in insts if i["role"] == "Writer")
    n_readers = sum(1 for i in insts if i["role"] == "Reader")
    azs = set(i["az"] for i in insts)
    compression = str(cl.get("compression", "None"))
    comp_l = compression.lower()
    # Active compression: 5.0 "enabled", or 8.0 "zstd"/"lz4". "none" = parameter
    # absent (older versions); "disabled" = present but turned off.
    comp_ok = comp_l in ("enabled", "zstd", "lz4")
    if comp_ok:
        comp_label = "Enabled" if comp_l == "enabled" else compression.upper()
    elif comp_l == "none":
        comp_label = "None"          # parameter not present (DocumentDB < 5.0)
    else:
        comp_label = "Disabled"
    stype = cl.get("storage_type", "Standard")

    # Profiler (slow query logging) — from cluster parameter group
    prof_enabled_raw = str(cl.get("profiler_enabled", "Unknown")).lower()
    prof_on = prof_enabled_raw == "enabled"
    prof_threshold = cl.get("profiler_threshold_ms")
    if prof_on and prof_threshold is not None:
        profiler_val = _status_dot(True, f"Enabled  ·  {prof_threshold} ms threshold")
    elif prof_on:
        profiler_val = _status_dot(True, "Enabled")
    elif prof_enabled_raw == "disabled":
        profiler_val = _status_dot(False, "Disabled", optional=True)
    else:
        profiler_val = "Unknown"

    children = []

    # ── 1. Cluster Overview — single dense table ─────────────────────────
    overview_rows = [
        ("Status",          cl["status"].capitalize(),
         "Engine",          cl["engine_version"],
         "Instances",       f"{len(insts)}  ({n_writers}W / {n_readers}R)"),
        ("Storage",         _loading_val(f"{storage_gb:.2f} GB") if storage_gb == 0 and db_count is None else f"{storage_gb:.2f} GB",
         "Storage Type",    stype,
         "Databases",       _loading_val() if db_count is None else str(db_count)),
        ("Encryption",      _status_dot(cl["storage_encrypted"]),
         "Deletion Protection", _status_dot(cl["deletion_protection"]),
         "Availability Zones", f"{len(azs)}  {'✓ Multi-AZ' if len(azs) >= 2 else '⚠ Single-AZ'}"),
        ("Compression",     _status_dot(comp_ok, comp_label, comp_label, optional=True),
         "Profiler Log",    _status_dot(cl.get("profiler_log", False), optional=True),
         "Audit Log",       _status_dot(cl.get("audit_log", False), optional=True)),
        ("Slow Query Profiler", profiler_val,
         "Backup Retention", f"{cl['backup_retention']} days",
         "Global Cluster",  cl.get("global_cluster_id", "—") if cl.get("is_global") else "No"),
        ("Change Streams",  cs_label,
         "Last Failover",   _fmt_failover(cl.get("last_failover_time")),
         "", ""),
    ]

    tbody = []
    for row in overview_rows:
        if not row[0]:
            continue
        cells = []
        for idx in range(0, 6, 2):
            if row[idx]:
                cells.append(html.Td(row[idx], style={**_TD, "color": "var(--text-muted)", "width": "13%"}))
                cells.append(html.Td(row[idx+1], style={**_TD_VAL, "width": "20%"}))
            else:
                cells.append(html.Td("", style=_TD))
                cells.append(html.Td("", style=_TD))
        tbody.append(html.Tr(cells))

    children.append(html.Table(html.Tbody(tbody), style=_TABLE, className="mb-3"))

    # ── 2. Side-by-side: RPO/RTO (left) + Monthly Cost Comparison (right) ─
    rpo_panel = _build_rpo_rto_row(cl, azs)
    cost_panel = _build_cost_comparison_panel(cost_data)

    children.append(html.Div([
        html.Div(rpo_panel, style={"flex": "1", "minWidth": "0", "border": "1px solid var(--border-default)",
                                    "borderRadius": "8px", "padding": ".6rem"}),
        html.Div(cost_panel, style={"flex": "1", "minWidth": "0", "border": "1px solid var(--border-default)",
                                     "borderRadius": "8px", "padding": ".6rem"}),
    ], style={"display": "flex", "gap": "1rem", "marginBottom": ".75rem"}))

    # ── 3. Instances & Metrics — collapsible table + chart ───────────────
    # ── 3. Instances — merged metrics & recommendations ────────────────
    inst_header = html.Tr([html.Th(h, style=_TH) for h in
        ["Instance", "Type", "AZ", "CPU Avg/Max", "Conns (Avg/Max)", "Free Mem", "Cache Hit",
         "Pattern", "Recommendation"]])
    inst_body = []
    for i in insts:
        bchr = i.get("buffer_cache_hit_ratio")
        bchr_style = {**_TD_VAL}
        if bchr is not None and bchr < 85:
            bchr_style["color"] = "var(--accent-red)"
        cpu_style = {**_TD_VAL}
        if i["cpu_avg"] is not None and i["cpu_avg"] < 5:
            cpu_style["color"] = "var(--accent-blue)"

        # Role suffix on instance ID
        role_tag = "(W)" if i["role"] == "Writer" else "(R)"
        role_color = "var(--color-primary)" if i["role"] == "Writer" else "var(--text-muted)"

        # Recommendation data
        dp = i.get("cpu_datapoints", [])
        wa = analyze_workload_statistics(dp)
        pattern_text = "—"
        rec_text = "—"
        rec_color = "var(--text-muted)"

        if wa["pattern"] != "insufficient_data":
            pattern_text = wa["pattern"].replace("_", " ").title()
            rec = recommend_instance_type(i["type"], wa, bchr)
            ro = rec.get("recommendation")
            if ro and isinstance(ro, dict) and "type" in ro:
                icon = _REC_ICONS.get(ro["type"], "")
                rec_text = f"{icon} {ro['instance']}"
                rec_color = {"serverless": "var(--accent-green)", "nvme": "var(--color-primary)",
                             "standard_downgrade": "var(--accent-blue)", "standard_upgrade": "var(--accent-red)",
                             "no_change": "var(--accent-green)"}.get(ro["type"], "var(--text-body)")

        inst_body.append(html.Tr([
            html.Td([
                html.Span(i["id"], style={"fontWeight": "600", "fontFamily": "monospace", "fontSize": ".8rem"}),
                html.Span(f" {role_tag}", style={"fontWeight": "600", "fontSize": ".75rem", "color": role_color}),
            ], style=_TD),
            html.Td(i["type"], style=_TD),
            html.Td(i["az"], style=_TD),
            html.Td(f"{i['cpu_avg']:.1f}% / {i['cpu_max']:.1f}%" if i["cpu_avg"] is not None else "—", style=cpu_style),
            html.Td(f"{i['conn_avg']:.0f} / {i['conn_max']:.0f}" if i["conn_avg"] is not None else "—", style=_TD),
            html.Td(f"{i['mem_avg_gb']:.1f} GB" if i["mem_avg_gb"] is not None else "—", style=_TD),
            html.Td(f"{bchr:.1f}%" if bchr is not None else "—", style=bchr_style),
            html.Td(pattern_text, style={**_TD, "fontSize": ".8rem"}),
            html.Td(rec_text, style={**_TD_VAL, "color": rec_color}),
        ]))

    inst_table = html.Table([html.Thead(inst_header), html.Tbody(inst_body)], style=_TABLE)
    inst_content = [inst_table]

    children.append(_collapsible_header("🖥️  Instances & Recommendations (7-day)", "instances", count=len(insts)))
    children.append(dbc.Collapse(
        html.Div(inst_content, className="mt-2 mb-2"),
        id={"type": "snap-collapse", "section": "instances"}, is_open=False,
    ))

    # ── 5. Storage Detail — collapsible chart + metrics ─────────────────
    storage_detail = _build_storage_detail(cost_data)
    children.append(_collapsible_header("Storage & Cost Detail", "storage_detail"))
    children.append(dbc.Collapse(
        html.Div(storage_detail, className="mt-2 mb-2"),
        id={"type": "snap-collapse", "section": "storage_detail"}, is_open=False,
    ))

    return html.Div(children)


def _build_recommendation_table(insts):
    """Build recommendations as a single sleek table."""
    header = html.Tr([html.Th(h, style=_TH) for h in
        ["Instance", "Current", "Pattern", "CPU Avg / Max / P95", "Efficiency", "Recommendation", "Confidence"]])
    rows = []
    for i in insts:
        dp = i.get("cpu_datapoints", [])
        itype = i["type"]
        wa = analyze_workload_statistics(dp)

        if wa["pattern"] == "insufficient_data":
            rows.append(html.Tr([
                html.Td(i["id"], style={**_TD, "fontFamily": "monospace", "fontSize": ".8rem", "fontWeight": "600"}),
                html.Td(itype, style=_TD),
                html.Td("—", style=_TD), html.Td("—", style=_TD), html.Td("—", style=_TD),
                html.Td("Insufficient data", style={**_TD, "color": "var(--color-warning)", "fontStyle": "italic"}),
                html.Td("—", style=_TD),
            ]))
            continue

        rec = recommend_instance_type(itype, wa, i.get("buffer_cache_hit_ratio"))
        st = wa["stats"]
        eff = rec.get("current_efficiency", {})
        ro = rec.get("recommendation")

        rec_text, rec_color = "—", "var(--text-muted)"
        if ro and isinstance(ro, dict) and "type" in ro:
            icon = _REC_ICONS.get(ro["type"], "")
            rec_text = f"{icon} {ro['instance']}"
            rec_color = {"serverless": "var(--accent-green)", "nvme": "var(--color-primary)",
                         "standard_downgrade": "var(--accent-blue)", "standard_upgrade": "var(--accent-red)",
                         "no_change": "var(--accent-green)"}.get(ro["type"], "var(--text-body)")

        conf = rec.get("confidence", 0)
        conf_color = "var(--accent-green)" if conf >= 70 else "var(--color-warning)" if conf >= 40 else "var(--text-muted)"

        rows.append(html.Tr([
            html.Td(i["id"], style={**_TD, "fontFamily": "monospace", "fontSize": ".8rem", "fontWeight": "600"}),
            html.Td(itype, style=_TD),
            html.Td(wa["pattern"].replace("_", " ").title(), style={**_TD, "fontSize": ".8rem"}),
            html.Td(f"{st.get('cpu_mean',0):.1f}% / {st.get('cpu_max',0):.1f}% / {st.get('cpu_p95',0):.1f}%", style=_TD),
            html.Td(f"{eff.get('overall_efficiency',0)*100:.0f}%", style=_TD_VAL),
            html.Td(rec_text, style={**_TD_VAL, "color": rec_color}),
            html.Td(f"{conf:.0f}%", style={**_TD_VAL, "color": conf_color}),
        ]))

    return html.Table([html.Thead(header), html.Tbody(rows)], style=_TABLE)


# ── Self-registration ────────────────────────────────────────────────────────
# ── Age timer: enable when poll completes ────────────────────────────────────
@callback(
    Output("snap-age-timer", "disabled"),
    Input("snap-poll", "disabled"),
    prevent_initial_call=True,
)
def cb_snap_enable_age_timer(poll_disabled):
    return not poll_disabled  # start timer when poll stops


@callback(
    Output("snap-run-btn", "children", allow_duplicate=True),
    Input("snap-age-timer", "n_intervals"),
    prevent_initial_call=True,
)
def cb_snap_age_tick(n):
    with _snap_lock:
        fetched_at = _snap.get("fetched_at")
        done = _snap.get("done", False)
    if not fetched_at or not done:
        return no_update
    elapsed = int((datetime.utcnow() - fetched_at).total_seconds())
    if elapsed < 60:
        age = f"{elapsed}s ago"
    elif elapsed < 3600:
        age = f"{elapsed // 60}m ago"
    else:
        age = f"{elapsed // 3600}h ago"
    return f"Reload  ·  {age}"


from tabs.registry import register_tab
register_tab("snapshot", "", "Cluster Overview", "cluster", render_cluster_snapshot)


def _build_rpo_rto_row(cl, azs):
    """Build RPO/RTO as a compact table (used in left panel)."""
    from datetime import datetime, timezone
    is_global = cl.get("is_global", False)
    latest_restore = cl.get("latest_restorable_time")
    single_az = len(azs) < 2

    if latest_restore:
        if isinstance(latest_restore, str):
            latest_restore = datetime.fromisoformat(latest_restore.replace("Z", "+00:00"))
        if latest_restore.tzinfo is None:
            latest_restore = latest_restore.replace(tzinfo=timezone.utc)
        delta = datetime.now(timezone.utc) - latest_restore
        rpo_seconds = int(delta.total_seconds())
        rpo_str = f"{rpo_seconds}s" if rpo_seconds < 60 else f"{rpo_seconds // 60}m {rpo_seconds % 60}s"
    else:
        rpo_str = "Unknown"
        rpo_seconds = None

    rto_str = "~10 min" if single_az else "~30 sec"
    rto_note = "Single-AZ" if single_az else "Multi-AZ failover"
    rpo_ok = rpo_seconds is not None and rpo_seconds < 300
    rto_ok = not single_az

    rows_data = [
        ("In-Region RPO", rpo_str, rpo_ok, latest_restore, False),
        ("In-Region RTO", rto_str, rto_ok, rto_note, True),
    ]
    if is_global:
        rows_data.append(("Cross-Region RPO", "< 5 sec", True, "Async replication", True))
        rows_data.append(("Cross-Region RTO", "~1 min", True, "Managed failover", True))

    _TH_SM = {**_TH, "padding": ".35rem .5rem", "fontSize": ".65rem"}
    _TD_SM = {**_TD, "padding": ".3rem .5rem", "fontSize": ".82rem"}
    _TD_SM_VAL = {**_TD_SM, "fontWeight": "600", "color": "var(--text-heading)"}

    header = html.Tr([html.Th(h, style=_TH_SM) for h in ["⏱️ RPO / RTO", "Value", "Note"]])
    body = []
    for label, val, ok, note_or_ts, is_text in rows_data:
        color = "var(--accent-green)" if ok else "var(--color-warning)"
        if is_text:
            note_cell = html.Td(note_or_ts, style={**_TD_SM, "color": "var(--text-muted)"})
        else:
            ts = note_or_ts
            ts_str = ""
            if ts:
                try:
                    ts_str = ts.strftime("%d %b %Y %H:%M UTC")
                except Exception:
                    ts_str = str(ts)[:16]
            note_cell = html.Td(
                html.Span(ts_str, style={"fontSize": ".75rem", "fontFamily": "monospace",
                                          "color": "var(--text-muted)", "whiteSpace": "nowrap"}),
                style=_TD_SM)
        body.append(html.Tr([
            html.Td(label, style={**_TD_SM, "fontWeight": "600"}),
            html.Td(val, style={**_TD_SM_VAL, "color": color}),
            note_cell,
        ]))

    elements = [html.Table([html.Thead(header), html.Tbody(body)],
                            style=_TABLE)]
    if not is_global:
        elements.append(html.Div("ℹ️  Consider Global Clusters for cross-region DR",
                     style={"fontSize": ".72rem", "color": "var(--text-muted)",
                            "marginTop": ".3rem", "paddingLeft": ".2rem"}))
    return html.Div(elements)


def _build_cost_comparison_panel(cost_data):
    """Build monthly cost comparison as a compact table (used in right panel)."""
    if not cost_data:
        return html.Div("Cost data analysis — in progress",
                        style={"fontSize": ".82rem", "color": "var(--text-muted)", "padding": ".5rem"})

    sc, ic = cost_data["standard_costs"], cost_data["iopt_costs"]
    n = cost_data["instance_count"]
    savings = cost_data["potential_savings"]

    _TH_SM = {**_TH, "padding": ".35rem .5rem", "fontSize": ".65rem"}
    _TD_SM = {**_TD, "padding": ".3rem .5rem", "fontSize": ".82rem"}
    _TD_SM_VAL = {**_TD_SM, "fontWeight": "600", "color": "var(--text-heading)"}

    days = cost_data.get("actual_days", 30)
    current_type = cost_data.get("current_type", "standard")
    is_current_std = current_type == "standard"

    header = html.Tr([html.Th(h, style=_TH_SM) for h in
        [f"Cluster cost comparison (30-day)", "Standard", "I/O Optimized"]])
    cost_rows = [
        (f"Compute ({n} inst.)", sc["compute"], ic["compute"]),
        ("I/O Operations", sc["io"], ic["io"]),
        ("Storage", sc["storage"], ic["storage"]),
        ("Backup", sc["backup"], ic["backup"]),
    ]
    body = []
    for label, std, iopt in cost_rows:
        body.append(html.Tr([
            html.Td(label, style={**_TD_SM, "color": "var(--text-muted)"}),
            html.Td(f"${std:,.2f}", style=_TD_SM_VAL),
            html.Td(f"${iopt:,.2f}", style=_TD_SM_VAL),
        ]))
    # Total row — highlight the cheaper option in green
    std_better = sc["total"] <= ic["total"]
    body.append(html.Tr([
        html.Td("Total", style={**_TD_SM, "fontWeight": "700"}),
        html.Td(f"${sc['total']:,.2f}", style={**_TD_SM_VAL,
                "color": "var(--accent-green)" if std_better else "var(--text-heading)"}),
        html.Td(f"${ic['total']:,.2f}", style={**_TD_SM_VAL,
                "color": "var(--accent-green)" if not std_better else "var(--text-heading)"}),
    ]))

    elements = [html.Table([html.Thead(header), html.Tbody(body)], style=_TABLE)]

    # Recommendation
    if savings > 0:
        if (is_current_std and not std_better) or (not is_current_std and std_better):
            # Should switch
            target = "I/O Optimized" if not std_better else "Standard"
            reason = ("I/O charges are high enough that the bundled I/O model saves money"
                      if target == "I/O Optimized"
                      else "I/O volume is low — Standard per-operation pricing is cheaper")
            elements.append(html.Div(
                f"💡 Switch to {target} — save ${savings:,.2f}/mo. {reason}",
                style={"fontSize": ".76rem", "color": "var(--accent-green)", "fontWeight": "600",
                       "marginTop": ".3rem", "paddingLeft": ".2rem"}))
        else:
            elements.append(html.Div(
                f"✅ Current storage configuration ({current_type.title()}) is already the optimal choice",
                style={"fontSize": ".76rem", "color": "var(--accent-green)", "fontWeight": "600",
                       "marginTop": ".3rem", "paddingLeft": ".2rem"}))
    else:
        elements.append(html.Div(
            f"✅ Current storage configuration ({current_type.title()}) is already the optimal choice",
            style={"fontSize": ".76rem", "color": "var(--accent-green)", "fontWeight": "600",
                   "marginTop": ".3rem", "paddingLeft": ".2rem"}))
    return html.Div(elements)


def _build_storage_detail(cost_data):
    """Build storage detail panel with chart and usage metrics."""
    if not cost_data:
        return html.Div("Cost data analysis — in progress", style={"fontSize": ".82rem", "color": "var(--text-muted)"})

    sc, ic = cost_data["standard_costs"], cost_data["iopt_costs"]

    # Usage metrics inline in a mini table
    usage_header = html.Tr([html.Th(h, style=_TH) for h in ["Metric", "Value"]])
    usage_body = [
        html.Tr([html.Td("Storage Size", style={**_TD, "color": "var(--text-muted)"}),
                 html.Td(f"{cost_data['storage_gb']:.2f} GB", style=_TD_VAL)]),
        html.Tr([html.Td("Backup Size", style={**_TD, "color": "var(--text-muted)"}),
                 html.Td(f"{cost_data['backup_gb']:.2f} GB", style=_TD_VAL)]),
        html.Tr([html.Td("Monthly I/O Ops", style={**_TD, "color": "var(--text-muted)"}),
                 html.Td(f"{cost_data['monthly_iops']:,.0f}", style=_TD_VAL)]),
        html.Tr([html.Td("Analysis Period", style={**_TD, "color": "var(--text-muted)"}),
                 html.Td(f"{cost_data['actual_days']} days", style=_TD_VAL)]),
    ]

    # Cost breakdown chart
    cats = ['Compute', 'I/O Ops', 'Storage', 'Backup']
    std_vals = [sc['compute'], sc['io'], sc['storage'], sc['backup']]
    iopt_vals = [ic['compute'], ic['io'], ic['storage'], ic['backup']]
    fig = go.Figure([
        go.Bar(name='Standard', x=cats, y=std_vals, marker_color='#0972d3',
               text=[f"${v:,.0f}" for v in std_vals], textposition='outside',
               textfont=dict(size=10)),
        go.Bar(name='I/O Optimized', x=cats, y=iopt_vals, marker_color='#ff9900',
               text=[f"${v:,.0f}" for v in iopt_vals], textposition='outside',
               textfont=dict(size=10)),
    ])
    fig.update_layout(barmode='group', yaxis_title='Monthly ($)',
                      title=dict(text='30-day cost breakdown — Standard vs I/O Optimized',
                                 font=dict(size=12, color='#5f6b7a'), x=0.5),
                      height=280, template="plotly_white", font_family="sans-serif",
                      margin=dict(t=32, b=24, l=40, r=12),
                      legend=dict(orientation="h", y=-0.15))

    return html.Div([
        html.Div([
            html.Div(
                html.Table([html.Thead(usage_header), html.Tbody(usage_body)], style=_TABLE),
                style={"flex": "1", "minWidth": "0"}),
            html.Div(
                dcc.Graph(figure=fig, config={"displayModeBar": False}),
                style={"flex": "2", "minWidth": "0"}),
        ], style={"display": "flex", "gap": "1rem", "alignItems": "flex-start"}),
    ])


def _build_observations_table(cl, insts, azs):
    """Build observations as a severity-sorted table."""
    obs = []  # (severity, icon, message)
    if not cl["storage_encrypted"]:
        obs.append(("Critical", "❌", "Encryption at rest is disabled"))
    if len(azs) < 2:
        obs.append(("High", "⚠️", "All instances in a single AZ — no failover protection"))
    if not cl["deletion_protection"]:
        obs.append(("High", "⚠️", "Deletion protection is disabled"))
    if cl["backup_retention"] < 7:
        obs.append(("Medium", "⚠️", f"Backup retention is only {cl['backup_retention']} days (recommend 7+)"))
    if not cl.get("profiler_log"):
        obs.append(("Medium", "⚠️", "Profiler log export is not enabled"))
    if not cl.get("audit_log"):
        obs.append(("Medium", "⚠️", "Audit log export is not enabled"))
    comp = str(cl.get("compression", "None")).lower()
    if comp == "disabled":
        obs.append(("Low", "💡", "Default compression is not enabled — consider enabling for storage savings"))
    elif comp == "none":
        obs.append(("Low", "💡", "Default collection compression is unavailable on this engine version — upgrade to 5.0+ for storage savings"))
    for i in insts:
        if i["cpu_avg"] is not None and i["cpu_avg"] < 5:
            obs.append(("Low", "💰", f"{i['id']} avg CPU is {i['cpu_avg']:.1f}% — may be oversized"))
        bchr = i.get("buffer_cache_hit_ratio")
        if bchr is not None and bchr < 85:
            obs.append(("Medium", "⚠️", f"{i['id']} buffer cache hit ratio is {bchr:.1f}% (target >90%)"))

    if not obs:
        return {"count": 0, "element": html.Div("✅  No issues — cluster looks healthy",
                style={"fontSize": ".85rem", "color": "var(--accent-green)", "fontWeight": "600",
                       "padding": ".4rem 0"})}

    sev_colors = {"Critical": "var(--accent-red)", "High": "var(--accent-red)",
                  "Medium": "var(--color-warning)", "Low": "var(--accent-blue)"}
    sev_order = {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}
    obs.sort(key=lambda x: sev_order.get(x[0], 9))

    header = html.Tr([html.Th(h, style=_TH) for h in ["Severity", "", "Finding"]])
    body = []
    for sev, icon, msg in obs:
        body.append(html.Tr([
            html.Td(sev, style={**_TD, "fontWeight": "700", "color": sev_colors.get(sev, "var(--text-body)"),
                                 "fontSize": ".78rem", "width": "80px"}),
            html.Td(icon, style={**_TD, "width": "30px", "textAlign": "center"}),
            html.Td(msg, style=_TD),
        ]))

    return {"count": len(obs),
            "element": html.Table([html.Thead(header), html.Tbody(body)], style=_TABLE)}
