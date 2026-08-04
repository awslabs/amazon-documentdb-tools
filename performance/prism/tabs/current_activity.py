"""Current Activity tab — multi-instance support with instance selector."""
import logging
import time
import pymongo
import boto3
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ctx
from tabs.ui_helpers import section_title, data_table

logger = logging.getLogger(__name__)

_last_refresh = {"ts": 0}

# Table styles
_TH = {"padding": ".35rem .5rem", "fontSize": ".65rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".4px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_TD = {"padding": ".3rem .5rem", "fontSize": ".8rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "verticalAlign": "middle"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)"}


def _check_tunnel_health(connection_string):
    """Check if tunnel is alive before attempting connection. Returns (ok, error_msg)."""
    if not _is_tunnel_mode(connection_string):
        return True, None  # Direct mode — no tunnel to check
    try:
        from ssh_tunnel import is_tunnel_active, ensure_tunnel
        if is_tunnel_active():
            return True, None
        # Try auto-reconnect
        logger.warning("Tunnel dead — attempting auto-reconnect")
        if ensure_tunnel():
            return True, None
        return False, "SSH tunnel lost. Click Reconnect to restore."
    except Exception as e:
        return False, f"Tunnel check failed: {e}"


def _fetch_current_ops(connection_string):
    """Fetch operations via $currentOp aggregation with fallbacks."""
    # Pre-check tunnel health to avoid flooding errors
    if _is_tunnel_mode(connection_string):
        ok, err = _check_tunnel_health(connection_string)
        if not ok:
            return [], err

    try:
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000, appname='DocDB-Prism')
        db = client.admin
        logger.info("Fetching currentOp via $currentOp aggregation")
        try:
            pipeline = [{"$currentOp": {"allUsers": True, "idleConnections": True}}]
            ops = list(db.aggregate(pipeline))
            logger.info("$currentOp returned %d operations", len(ops))
            if ops:
                client.close()
                return ops, None
        except Exception as e1:
            logger.warning("$currentOp failed: %s, trying command", e1)
        try:
            result = db.command("currentOp", True)
            ops = result.get("inprog", [])
            client.close()
            return ops, None
        except Exception as e2:
            logger.warning("currentOp command failed: %s", e2)
        result = db.command({"currentOp": 1, "allUsers": True, "idleConnections": True})
        ops = result.get("inprog", [])
        client.close()
        return ops, None
    except Exception as e:
        logger.error("All currentOp methods failed: %s", e)
        return [], str(e)


def _discover_instances(cluster_id, region):
    """Get instance endpoints from AWS API."""
    try:
        docdb = boto3.client("docdb", region_name=region)
        cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
        insts = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]
        result = []
        for inst in insts:
            iid = inst["DBInstanceIdentifier"]
            endpoint = inst.get("Endpoint", {}).get("Address", "")
            port = inst.get("Endpoint", {}).get("Port", 27017)
            role = "Writer" if any(
                m.get("DBInstanceIdentifier") == iid and m.get("IsClusterWriter", False)
                for m in cl.get("DBClusterMembers", [])) else "Reader"
            result.append({
                "id": iid, "endpoint": endpoint, "port": port,
                "role": role, "type": inst.get("DBInstanceClass", ""),
            })
        return result
    except Exception as e:
        logger.warning("Instance discovery failed: %s", e)
        return []



def _is_tunnel_mode(conn_str):
    """Detect if connection goes through SSH tunnel (localhost)."""
    return "localhost" in conn_str or "127.0.0.1" in conn_str


def _get_tunnel_instance_conn(instance_id, base_conn_str):
    """In tunnel mode, get per-instance connection string via ssh_tunnel.
    Falls back to base_conn_str if no dedicated tunnel exists for this instance."""
    try:
        from ssh_tunnel import get_instance_connection_string, _get_credentials
        u, p, tls = _get_credentials()
        if u and p:
            conn = get_instance_connection_string(instance_id, u, p, tls)
            if conn:
                return conn
    except Exception:
        pass
    return base_conn_str
def _build_instance_conn_str(base_conn_str, instance_endpoint, port):
    """Build a direct connection string to a specific instance."""
    # Parse credentials from base connection string
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(base_conn_str)
        user = parsed.username or ""
        pwd = parsed.password or ""
        params = parsed.query
        # Remove replicaSet and add directConnection
        parts = [p for p in params.split("&") if not p.startswith("replicaSet")]
        if not any(p.startswith("directConnection") for p in parts):
            parts.append("directConnection=true")
        param_str = "&".join(p for p in parts if p)
        return f"mongodb://{user}:{pwd}@{instance_endpoint}:{port}/?{param_str}"
    except Exception:
        return base_conn_str


def _fmt_us(microsecs):
    if not microsecs:
        return "\u2014"
    try:
        microsecs = int(microsecs)
    except (ValueError, TypeError):
        return "\u2014"
    if microsecs >= 1_000_000:
        return f"{microsecs / 1_000_000:.1f}s"
    if microsecs >= 1_000:
        return f"{microsecs / 1_000:.1f}ms"
    return f"{microsecs}\u00b5s"


def _get_user(op):
    users = op.get("effectiveUsers", [])
    return users[0].get("user", "\u2014") if users else "\u2014"


def _get_app(op):
    meta = op.get("clientMetaData", op.get("clientMetadata", {})) or {}
    app_info = meta.get("application", {}) or {}
    return app_info.get("name", "") or "\u2014"


def _build_msg_cell(msg, opid):
    """Build message table cell — short preview + View button if long."""
    if not msg:
        return html.Td("\u2014", style={**_TD, "fontSize": ".72rem", "color": "var(--text-muted)"})

    is_long = len(msg) > 60
    if not is_long:
        return html.Td(msg, style={**_TD, "fontSize": ".72rem", "color": "var(--text-muted)"})

    preview = msg[:60] + "…"
    return html.Td([
        html.Span(preview, style={"fontSize": ".72rem", "color": "var(--text-muted)"}),
        dbc.Button("View", size="sm", outline=True, color="secondary",
                   id={"type": "activity-msg-btn", "index": str(opid)},
                   style={"fontSize": ".6rem", "padding": "0 .4rem",
                          "marginLeft": ".4rem", "verticalAlign": "middle",
                          "lineHeight": "1.4", "borderRadius": "4px"}),
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle(
                f"Message — PID {opid}",
                style={"fontSize": ".9rem"}), close_button=True),
            dbc.ModalBody(
                html.Pre(msg,
                         style={"fontSize": ".82rem", "whiteSpace": "pre-wrap",
                                "wordBreak": "break-all", "margin": "0",
                                "padding": ".8rem", "borderRadius": "6px",
                                "background": "var(--bg-surface-alt)",
                                "border": "1px solid var(--border-default)"}),
            ),
        ], id={"type": "activity-msg-modal", "index": str(opid)},
           is_open=False, centered=True, size="lg"),
    ], style={**_TD, "maxWidth": "220px"})


def _render_connection_lost(error_msg):
    """Render a connection-lost banner with Reconnect button."""
    return html.Div([
        html.Div([
            html.Span("\u26a0\ufe0f", style={"fontSize": "1.5rem", "marginRight": ".6rem"}),
            html.Div([
                html.Div("Connection Lost", style={"fontWeight": "700", "fontSize": ".95rem",
                          "color": "var(--accent-red)"}),
                html.Div(error_msg or "SSH tunnel disconnected",
                         style={"fontSize": ".78rem", "color": "var(--text-muted)",
                                "marginTop": ".2rem"}),
            ]),
        ], style={"display": "flex", "alignItems": "center"}),
        html.Div([
            dbc.Button("\U0001f504  Reconnect", id="activity-refresh-btn", color="warning",
                       size="sm", style={"fontWeight": "600"}),
            html.Span("Auto-refresh paused",
                      style={"fontSize": ".72rem", "color": "var(--text-muted)",
                             "marginLeft": ".6rem"}),
        ], style={"marginTop": ".6rem"}),
    ], style={"padding": "1.2rem", "border": "1px solid var(--accent-red)",
              "borderRadius": "8px", "background": "rgba(209,50,18,.04)",
              "marginTop": ".5rem"})


def _build_instance_summary(ops, instance_id, role):
    """Build summary for one instance."""
    active = [op for op in ops if op.get("active", False)
              and op.get("desc", "") not in ("TTLMonitor", "featureCompatibilityVersion")]
    idle = [op for op in ops if not op.get("active", False)
            and op.get("desc", "") not in ("TTLMonitor", "featureCompatibilityVersion")]
    internal = len(ops) - len(active) - len(idle)

    active.sort(key=lambda x: x.get("microsecs_running", 0), reverse=True)
    longest = _fmt_us(active[0].get("microsecs_running", 0)) if active else "\u2014"

    return {
        "instance_id": instance_id, "role": role,
        "total": len(ops), "active": len(active), "idle": len(idle),
        "internal": internal, "longest": longest,
        "top_active": active[:5], "top_idle": idle[:5],
    }


def _render_multi_instance(all_results, selected_instance="all", total_instances=0, max_per_node=5):
    """Render a single unified table of active operations, ordered by duration (longest first).
    Max `max_per_node` ops per node in the default view. Node dropdown filters."""
    if not all_results:
        return dbc.Alert("No activity data available.", color="info")

    # Cluster-wide summary
    total_all = sum(r["total"] for r in all_results)
    active_all = sum(r["active"] for r in all_results)
    idle_all = sum(r["idle"] for r in all_results)

    def _kpi(label, value, color=None):
        v_style = {"fontSize": ".88rem", "fontWeight": "700", "lineHeight": "1"}
        if color:
            v_style["color"] = color
        return html.Div([
            html.Div(str(value), style=v_style),
            html.Div(label, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                      "textTransform": "uppercase", "letterSpacing": ".3px"}),
        ], style={"textAlign": "center", "flex": "1"})

    summary_bar = html.Div([
        _kpi("Instances", f"{len(all_results)}/{total_instances}" if total_instances > len(all_results) else str(len(all_results))),
        _kpi("Total Ops", total_all),
        _kpi("Active", active_all, "var(--accent-red)" if active_all > 10 else None),
        _kpi("Idle", idle_all),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)", "borderRadius": "8px",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # Collect all active ops across all instances, tag with instance info
    all_active_ops = []
    for r in all_results:
        # Filter by selected instance
        if selected_instance != "all" and r["instance_id"] != selected_instance:
            continue
        # Take top N per node
        for op in r["top_active"][:max_per_node]:
            all_active_ops.append({
                "op": op,
                "instance_id": r["instance_id"],
                "role": r["role"],
            })

    # Sort all ops by duration (longest first)
    all_active_ops.sort(key=lambda x: x["op"].get("microsecs_running", 0), reverse=True)

    # Build single unified table
    if all_active_ops:
        header = html.Tr([
            html.Th("PID", style=_TH),
            html.Th("Instance", style=_TH),
            html.Th("Duration", style=_TH),
            html.Th("Operation", style=_TH),
            html.Th("Namespace", style=_TH),
            html.Th("Query / Command", style={**_TH, "minWidth": "180px"}),
            html.Th("User", style=_TH),
            html.Th("App", style=_TH),
            html.Th("Status", style=_TH),
            html.Th("Message", style={**_TH, "minWidth": "120px"}),
        ])
        rows = []
        for item in all_active_ops:
            op = item["op"]
            us = op.get("microsecs_running", 0)
            dur_style = {**_TD, "fontWeight": "700", "fontFamily": "monospace"}
            if us > 5_000_000:
                dur_style["color"] = "var(--accent-red)"
            elif us > 1_000_000:
                dur_style["color"] = "var(--color-warning)"
            else:
                dur_style["color"] = "var(--text-body)"

            blocked = op.get("waitingForLock", False)
            status_text = "blocked" if blocked else "running"
            status_color = "var(--accent-red)" if blocked else "var(--accent-green)"

            role_badge_color = "var(--color-primary)" if item["role"] == "Writer" else "var(--text-muted)"
            instance_cell = html.Td([
                html.Span(item["instance_id"].split("-")[-1] if "-" in item["instance_id"] else item["instance_id"],
                          style={"fontFamily": "monospace", "fontSize": ".76rem"}),
                html.Span(f" {item['role'][0]}",
                          style={"fontSize": ".65rem", "fontWeight": "700",
                                 "color": role_badge_color, "marginLeft": ".2rem"}),
            ], style=_TD)

            # Extract query/command details
            query_doc = op.get("command", op.get("query", {})) or {}
            if isinstance(query_doc, dict):
                # Remove noisy fields for display
                display_query = {k: v for k, v in query_doc.items()
                                 if k not in ("lsid", "$clusterTime", "$db", "$readPreference",
                                              "readPreference", "shardVersion")}
                import json as _json
                try:
                    query_str = _json.dumps(display_query, default=str, indent=2)
                except Exception:
                    query_str = str(display_query)
            else:
                query_str = str(query_doc)

            # Build query cell: short preview + "View" button if long
            is_long = len(query_str) > 80
            query_preview = query_str[:80] + "…" if is_long else query_str
            if is_long:
                query_cell = html.Td([
                    html.Span(query_preview,
                              style={"fontFamily": "monospace", "fontSize": ".72rem",
                                     "display": "inline-block", "maxWidth": "220px",
                                     "overflow": "hidden", "textOverflow": "ellipsis",
                                     "whiteSpace": "nowrap", "verticalAlign": "middle"}),
                    dbc.Button("View", size="sm", outline=True, color="secondary",
                               id={"type": "activity-query-btn", "index": str(op.get("opid", id(op)))},
                               style={"fontSize": ".6rem", "padding": "0 .4rem",
                                      "marginLeft": ".4rem", "verticalAlign": "middle",
                                      "lineHeight": "1.4", "borderRadius": "4px"}),
                    dbc.Modal([
                        dbc.ModalHeader(dbc.ModalTitle(
                            f"Query — PID {op.get('opid', '—')} · {op.get('ns', '')}",
                            style={"fontSize": ".9rem"}), close_button=True),
                        dbc.ModalBody(
                            html.Pre(query_str,
                                     style={"fontSize": ".78rem", "whiteSpace": "pre-wrap",
                                            "wordBreak": "break-all", "margin": "0",
                                            "background": "var(--bg-code)", "color": "var(--text-code)",
                                            "padding": ".8rem", "borderRadius": "6px",
                                            "maxHeight": "60vh", "overflow": "auto"}),
                        ),
                    ], id={"type": "activity-query-modal", "index": str(op.get("opid", id(op)))},
                       is_open=False, centered=True, size="lg"),
                ], style={**_TD, "overflow": "hidden"})
            else:
                query_cell = html.Td(
                    query_preview,
                    style={**_TD, "fontFamily": "monospace", "fontSize": ".72rem"})

            # Message field (e.g. "waiting for lock", plan summary, etc.)
            msg = op.get("msg", op.get("desc", "")) or ""

            rows.append(html.Tr([
                html.Td(str(op.get("opid", "\u2014")),
                        style={**_TD, "fontFamily": "monospace", "fontSize": ".76rem"}),
                instance_cell,
                html.Td(_fmt_us(us), style=dur_style),
                html.Td(op.get("op", "\u2014"), style={**_TD, "fontWeight": "600"}),
                html.Td(op.get("ns", "\u2014"), style={**_TD, "fontFamily": "monospace", "fontSize": ".76rem"}),
                query_cell,
                html.Td(_get_user(op), style=_TD),
                html.Td(_get_app(op), style={**_TD, "fontSize": ".76rem"}),
                html.Td(status_text, style={**_TD, "color": status_color, "fontWeight": "600"}),
                _build_msg_cell(msg, op.get("opid", id(op))),
            ]))

        table = html.Table([html.Thead(header), html.Tbody(rows)],
                           style={**_TABLE, "tableLayout": "auto"})
    else:
        table = html.Div("\u2705  No active operations across cluster",
                         style={"fontSize": ".88rem", "color": "var(--accent-green)",
                                "fontWeight": "600", "padding": ".8rem .5rem",
                                "border": "1px solid var(--border-default)",
                                "borderRadius": "8px", "textAlign": "center"})

    children = [summary_bar]
    if total_instances > len(all_results):
        children.append(html.Div(
            f"Showing {len(all_results)} of {total_instances} instances (tunnel mode \u2014 only writer reachable)",
            style={"fontSize": ".72rem", "color": "var(--text-muted)", "fontStyle": "italic",
                   "marginBottom": ".4rem", "paddingLeft": ".2rem"}))

    # Table title
    filter_label = f"on {selected_instance}" if selected_instance != "all" else "across all nodes"
    children.append(html.Div([
        html.Span(f"Active Operations {filter_label}",
                  style={"fontSize": ".82rem", "fontWeight": "700", "color": "var(--text-heading)"}),
        html.Span(f"  \u00b7  max {max_per_node} per node, ordered by duration",
                  style={"fontSize": ".72rem", "color": "var(--text-muted)"}),
    ], style={"marginBottom": ".4rem"}))

    children.append(table)
    return html.Div(children)


def render_current_activity(connection_string, cluster_id="", region="us-east-1"):
    """Render the tab — discovers instances and fetches from all."""
    if not connection_string:
        return html.Div([
            section_title("Current Activity"),
            dbc.Alert("Connection required to view live activity.", color="warning"),
        ])

    # Discover instances
    instances = []
    if cluster_id:
        instances = _discover_instances(cluster_id, region)

    # Build instance selector options
    inst_options = [{"label": "All Instances", "value": "all"}]
    for inst in instances:
        inst_options.append({
            "label": f"{inst['id']} ({inst['role']})",
            "value": inst["id"],
        })

    # Fetch from all instances (or just cluster endpoint if no instances found)
    all_results = []
    if instances:
        for inst in instances:
            if inst["endpoint"]:
                if _is_tunnel_mode(connection_string):
                    inst_conn = _get_tunnel_instance_conn(inst["id"], connection_string)
                else:
                    inst_conn = _build_instance_conn_str(connection_string, inst["endpoint"], inst["port"])
                ops, err = _fetch_current_ops(inst_conn)
                if not err:
                    all_results.append(_build_instance_summary(ops, inst["id"], inst["role"]))
    if not all_results:
        # Fallback: use cluster endpoint
        ops, err = _fetch_current_ops(connection_string)
        if not err and ops:
            writer_id = next((i["id"] for i in instances if i.get("role") == "Writer"), "cluster"); all_results.append(_build_instance_summary(ops, writer_id, "Writer"))

    _last_refresh["ts"] = time.time()
    initial_results = _render_multi_instance(all_results, total_instances=len(instances))

    return html.Div([
        section_title("Current Activity"),
        dbc.Card(dbc.CardBody(
            dbc.Row([
                dbc.Col([
                    html.Small("Live view of active operations across all instances",
                               className="text-muted", style={"fontSize": ".85rem"}),
                    html.Span(id="activity-last-refresh",
                              style={"fontSize": ".78rem", "marginLeft": ".75rem"}),
                ], className="d-flex align-items-center"),
                dbc.Col(html.Div([
                    dcc.Dropdown(id="activity-instance-select", options=inst_options,
                                 value="all", clearable=False,
                                 style={"width": "200px", "fontSize": ".78rem"}),
                    dcc.Dropdown(id="activity-max-per-node",
                                 options=[{"label": f"{n} per node", "value": n}
                                          for n in [5, 10, 15, 20, 30]],
                                 value=5, clearable=False,
                                 style={"width": "130px", "fontSize": ".78rem"}),
                    dbc.Button("\U0001f504  Refresh", id="activity-refresh-btn", color="warning",
                               size="sm", style={"fontWeight": "600"}),
                ], style={"display": "flex", "gap": ".4rem", "alignItems": "center"}),
                    width="auto"),
            ], align="center"),
        ), className="mb-3", style={"borderRadius": "10px"}),
        dcc.Store(id="activity-conn-store", data=connection_string),
        dcc.Store(id="activity-cluster-meta", data={
            "cluster_id": cluster_id, "region": region,
            "instances": instances,
        }),
        dcc.Store(id="activity-refresh-ts", data=_last_refresh["ts"]),
        dcc.Interval(id="activity-timer", interval=10_000, disabled=True),
        html.Div(id="activity-results", children=initial_results),
    ])


@callback(
    Output("activity-results", "children"),
    Output("activity-refresh-ts", "data"),
    Output("activity-cluster-meta", "data", allow_duplicate=True),
    Output("activity-timer", "disabled"),
    Input("activity-refresh-btn", "n_clicks"),
    State("activity-conn-store", "data"),
    State("activity-cluster-meta", "data"),
    State("activity-instance-select", "value"),
    State("activity-max-per-node", "value"),
    prevent_initial_call=True,
)
def cb_refresh_activity(n, conn_str, meta, selected, max_per_node):
    if not conn_str:
        return dbc.Alert("No connection available.", color="warning"), no_update, no_update, True

    # Check tunnel health first
    if _is_tunnel_mode(conn_str):
        ok, err = _check_tunnel_health(conn_str)
        if not ok:
            return _render_connection_lost(err), no_update, no_update, True

    cluster_id = (meta or {}).get("cluster_id", "")
    region = (meta or {}).get("region", "us-east-1")

    # Re-discover instances on refresh (catches new instances)
    instances = []
    if cluster_id:
        instances = _discover_instances(cluster_id, region)
    if not instances:
        instances = (meta or {}).get("instances", [])

    # Fetch from all instances
    all_results = []
    conn_error = None
    if instances:
        for inst in instances:
            if inst.get("endpoint"):
                if _is_tunnel_mode(conn_str):
                    inst_conn = _get_tunnel_instance_conn(inst["id"], conn_str)
                else:
                    inst_conn = _build_instance_conn_str(conn_str, inst["endpoint"], inst["port"])
                ops, err = _fetch_current_ops(inst_conn)
                if err:
                    conn_error = err
                elif ops:
                    all_results.append(_build_instance_summary(ops, inst["id"], inst["role"]))
    if not all_results:
        ops, err = _fetch_current_ops(conn_str)
        if err:
            conn_error = err
        elif ops:
            writer_id = next((i["id"] for i in instances if i.get("role") == "Writer"), "cluster")
            all_results.append(_build_instance_summary(ops, writer_id, "Writer"))

    # If all fetches failed, show connection lost
    if not all_results and conn_error:
        return _render_connection_lost(conn_error), no_update, no_update, True

    now = time.time()
    _last_refresh["ts"] = now

    updated_meta = dict(meta or {})
    updated_meta["instances"] = instances

    return (_render_multi_instance(all_results, selected or "all",
                                   total_instances=len(instances),
                                   max_per_node=max_per_node or 5),
            now, updated_meta, False)


@callback(
    Output("activity-results", "children", allow_duplicate=True),
    Output("activity-timer", "disabled", allow_duplicate=True),
    Input("activity-instance-select", "value"),
    Input("activity-max-per-node", "value"),
    State("activity-conn-store", "data"),
    State("activity-cluster-meta", "data"),
    prevent_initial_call=True,
)
def cb_filter_instance(selected, max_per_node, conn_str, meta):
    """Re-fetch and filter by selected instance / max per node. Fires immediately on dropdown change."""
    if not conn_str:
        return no_update, no_update

    # Check tunnel health first
    if _is_tunnel_mode(conn_str):
        ok, err = _check_tunnel_health(conn_str)
        if not ok:
            return _render_connection_lost(err), True

    instances = (meta or {}).get("instances", [])
    all_results = []
    conn_error = None
    if instances:
        for inst in instances:
            if inst.get("endpoint"):
                if _is_tunnel_mode(conn_str):
                    inst_conn = _get_tunnel_instance_conn(inst["id"], conn_str)
                else:
                    inst_conn = _build_instance_conn_str(conn_str, inst["endpoint"], inst["port"])
                ops, err = _fetch_current_ops(inst_conn)
                if err:
                    conn_error = err
                elif ops:
                    all_results.append(_build_instance_summary(ops, inst["id"], inst["role"]))
    if not all_results:
        ops, err = _fetch_current_ops(conn_str)
        if err:
            conn_error = err
        elif ops:
            writer_id = next((i["id"] for i in instances if i.get("role") == "Writer"), "cluster")
            all_results.append(_build_instance_summary(ops, writer_id, "Writer"))

    if not all_results and conn_error:
        return _render_connection_lost(conn_error), True

    return (_render_multi_instance(all_results, selected or "all",
                                   total_instances=len(instances),
                                   max_per_node=max_per_node or 5), False)


@callback(
    Output("activity-last-refresh", "children"),
    Input("activity-timer", "n_intervals"),
    Input("activity-refresh-ts", "data"),
    prevent_initial_call=True,
)
def cb_update_timer(n, ts):
    if not ts:
        return ""
    elapsed = time.time() - ts
    if elapsed < 60:
        text = f"{int(elapsed)}s ago"
    elif elapsed < 3600:
        text = f"{int(elapsed / 60)}m ago"
    else:
        text = f"{int(elapsed / 3600)}h ago"
    return html.Span(f"\u23f0 Last refreshed: {text}",
                     style={"color": "#8d99a8", "fontWeight": "500"})


# ── Query modal toggle ────────────────────────────────────────────────────────
from dash import ALL

@callback(
    Output({"type": "activity-query-modal", "index": ALL}, "is_open"),
    Input({"type": "activity-query-btn", "index": ALL}, "n_clicks"),
    State({"type": "activity-query-modal", "index": ALL}, "is_open"),
    prevent_initial_call=True,
)
def cb_toggle_query_modal(clicks, is_open_list):
    """Toggle the query detail modal when View button is clicked."""
    if not clicks or not any(c for c in clicks if c):
        return [no_update] * len(is_open_list)
    triggered = ctx.triggered_id
    if not triggered:
        return [no_update] * len(is_open_list)
    idx = triggered.get("index", "")
    return [not o if is_open_list[i] is not None and
            ctx.outputs_list[i]["id"]["index"] == idx else no_update
            for i, o in enumerate(is_open_list)]


@callback(
    Output({"type": "activity-msg-modal", "index": ALL}, "is_open"),
    Input({"type": "activity-msg-btn", "index": ALL}, "n_clicks"),
    State({"type": "activity-msg-modal", "index": ALL}, "is_open"),
    prevent_initial_call=True,
)
def cb_toggle_msg_modal(clicks, is_open_list):
    """Toggle the message detail modal when View button is clicked."""
    if not clicks or not any(c for c in clicks if c):
        return [no_update] * len(is_open_list)
    triggered = ctx.triggered_id
    if not triggered:
        return [no_update] * len(is_open_list)
    idx = triggered.get("index", "")
    return [not o if is_open_list[i] is not None and
            ctx.outputs_list[i]["id"]["index"] == idx else no_update
            for i, o in enumerate(is_open_list)]


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_tab
register_tab("activity", "", "Activity", "cluster", render_current_activity)
