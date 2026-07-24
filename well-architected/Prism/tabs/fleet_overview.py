"""Fleet Overview — landing page with fleetwide instance summary.

This is the first screen users see. It discovers all DocumentDB clusters
in a region, shows distribution charts, and lets users click through
to connect to an individual cluster.
"""
import logging
import threading
import time
import boto3
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ctx, ALL
import plotly.express as px
from aws_discovery import get_aws_regions
from tabs.ui_helpers import data_table, wire_animation, coffee_animation

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 300  # 5 minutes

_fleet = {"data": None, "running": False, "done": False, "error": None, "region": None, "timestamp": 0}
_fleet_lock = threading.Lock()

PAGE_SIZE = 10


def _is_cache_valid():
    """Check if cached fleet data is still fresh."""
    if not _fleet["done"] or not _fleet["data"]:
        return False
    return (time.time() - _fleet["timestamp"]) < CACHE_TTL_SECONDS


def reset_fleet():
    """Force-reset fleet state."""
    with _fleet_lock:
        _fleet.update(data=None, running=False, done=False, error=None, region=None, timestamp=0)


def start_fleet_if_needed(region):
    """Start background fleet discovery if not already running/done or cache is valid."""
    if not region:
        return
    with _fleet_lock:
        if _fleet["running"]:
            return
        if _is_cache_valid() and _fleet["region"] == region:
            return
    threading.Thread(target=_fetch_fleet, args=(region,), daemon=True).start()


def _fetch_fleet(region):
    """Gather all DocumentDB clusters and their instances in the region."""
    with _fleet_lock:
        _fleet.update(data=None, running=True, done=False, error=None, region=region, timestamp=0)

    try:
        docdb = boto3.client("docdb", region_name=region)
        clusters_resp = docdb.describe_db_clusters()
        clusters = [c for c in clusters_resp.get("DBClusters", []) if c.get("Engine") == "docdb"]

        # Fetch global clusters
        global_clusters_count = 0
        try:
            global_resp = docdb.describe_global_clusters()
            global_clusters_count = len(global_resp.get("GlobalClusters", []))
        except Exception:
            pass  # Global clusters API may not be available in all regions

        instances_resp = docdb.describe_db_instances(
            Filters=[{"Name": "engine", "Values": ["docdb"]}]
        )
        all_instances = instances_resp.get("DBInstances", [])

        cluster_data = []
        pi_enabled = 0
        pi_disabled = 0

        for cl in clusters:
            cid = cl["DBClusterIdentifier"]
            members = {m["DBInstanceIdentifier"] for m in cl.get("DBClusterMembers", [])}
            writer_ids = {
                m["DBInstanceIdentifier"]
                for m in cl.get("DBClusterMembers", [])
                if m.get("IsClusterWriter")
            }
            cl_instances = [i for i in all_instances if i["DBInstanceIdentifier"] in members]

            inst_list = []
            for i in cl_instances:
                pi = i.get("PerformanceInsightsEnabled", False)
                if pi:
                    pi_enabled += 1
                else:
                    pi_disabled += 1
                inst_list.append({
                    "id": i["DBInstanceIdentifier"],
                    "type": i["DBInstanceClass"],
                    "status": i.get("DBInstanceStatus", "—"),
                    "az": i.get("AvailabilityZone", "—"),
                    "role": "Writer" if i["DBInstanceIdentifier"] in writer_ids else "Reader",
                    "engine_version": i.get("EngineVersion", "—"),
                    "pi_enabled": pi,
                    # Per-instance endpoint/port — required so tunnel mode can open
                    # a dedicated tunnel per instance (reader queries, cluster-wide
                    # index usage). Without these, open_tunnel falls back to a
                    # cluster-endpoint-only (writer) tunnel.
                    "endpoint": i.get("Endpoint", {}).get("Address", ""),
                    "port": i.get("Endpoint", {}).get("Port", 27017),
                })

            cluster_data.append({
                "cluster_id": cid,
                "status": cl.get("Status", "—"),
                "engine_version": cl.get("EngineVersion", "—"),
                "endpoint": cl.get("Endpoint", "—"),
                "port": cl.get("Port", 27017),
                "log_group": f"/aws/docdb/{cid}/profiler",
                "instance_count": len(cl_instances),
                "instances": inst_list,
            })

        total_instances = sum(c["instance_count"] for c in cluster_data)
        type_counts = {}
        version_counts = {}
        for c in cluster_data:
            v = c["engine_version"]
            version_counts[v] = version_counts.get(v, 0) + 1
            for inst in c["instances"]:
                t = inst["type"]
                type_counts[t] = type_counts.get(t, 0) + 1

        data = {
            "clusters": cluster_data,
            "total_clusters": len(cluster_data),
            "total_instances": total_instances,
            "global_clusters": global_clusters_count,
            "type_counts": type_counts,
            "version_counts": version_counts,
            "pi_enabled": pi_enabled,
            "pi_disabled": pi_disabled,
            "region": region,
        }

        with _fleet_lock:
            _fleet.update(data=data, running=False, done=True, timestamp=time.time())
        logger.info("Fleet overview complete: %d clusters, %d instances", len(cluster_data), total_instances)

    except Exception as e:
        logger.error("Fleet overview failed: %s", e, exc_info=True)
        with _fleet_lock:
            _fleet.update(running=False, done=True, error=str(e), timestamp=0)


# ── Rendering ────────────────────────────────────────────────────────────────

def _make_donut(names, values, title, colors=None):
    """Create a styled donut chart — values only inside slices, legend has labels."""
    fig = px.pie(names=names, values=values, hole=0.45, title=f"<b>{title}</b>",
                 color_discrete_sequence=colors)
    fig.update_traces(textposition="inside", textinfo="value",
                      hovertemplate="%{label}: %{value}<extra></extra>")
    fig.update_layout(
        height=300, template="plotly_white", font_family="sans-serif",
        margin=dict(t=45, b=10, l=10, r=10),
        legend=dict(orientation="h", yanchor="bottom", y=-0.3,
                    xanchor="center", x=0.5, font=dict(size=11)),
        title_font=dict(size=13, color="#414d5c", family="Helvetica, Arial, sans-serif"),
    )
    return fig


def _build_cluster_table(clusters, sort_by="name", page=0):
    """Build the cluster table rows for the given sort and page."""
    # Sort
    if sort_by == "version":
        sorted_clusters = sorted(clusters, key=lambda c: c["engine_version"])
    elif sort_by == "status":
        sorted_clusters = sorted(clusters, key=lambda c: c["status"])
    elif sort_by == "instances":
        sorted_clusters = sorted(clusters, key=lambda c: c["instance_count"], reverse=True)
    else:
        sorted_clusters = sorted(clusters, key=lambda c: c["cluster_id"])

    total = len(sorted_clusters)
    start = page * PAGE_SIZE
    end = min(start + PAGE_SIZE, total)
    page_clusters = sorted_clusters[start:end]
    total_pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)

    COL_CLUSTER  = {"flex": "2", "minWidth": "160px", "padding": "0 .75rem 0 0", "overflow": "hidden"}
    COL_VERSION  = {"width": "72px",  "flexShrink": "0", "textAlign": "center"}
    COL_INST     = {"width": "96px",  "flexShrink": "0", "textAlign": "center"}
    COL_TYPES    = {"width": "180px", "flexShrink": "0", "overflow": "hidden"}
    COL_STATUS   = {"width": "90px",  "flexShrink": "0", "textAlign": "center"}
    COL_ACTIONS  = {"width": "190px", "flexShrink": "0", "textAlign": "right"}

    # Table header
    header = html.Div([
        html.Span("Cluster",        style=COL_CLUSTER),
        html.Span("Version",        style=COL_VERSION),
        html.Span("Instances",      style=COL_INST),
        html.Span("Types",          style=COL_TYPES),
        html.Span("Status",         style=COL_STATUS),
        html.Span("",               style=COL_ACTIONS),
    ], className="fleet-table-header")

    rows = []
    for cl in page_clusters:
        n_w = sum(1 for i in cl["instances"] if i["role"] == "Writer")
        n_r = sum(1 for i in cl["instances"] if i["role"] == "Reader")
        types_in_cluster = sorted({i["type"] for i in cl["instances"]})
        type_chips = [
            html.Span(
                f"{sum(1 for i in cl['instances'] if i['type'] == t)}× {t}",
                className="fleet-type-chip")
            for t in types_in_cluster
        ]
        if not type_chips:
            type_chips = [html.Span("—", style={"color": "var(--text-muted)", "fontSize": ".8rem"})]

        status_cls = "fleet-status-dot fleet-status-dot--ok" if cl["status"] == "available" \
            else "fleet-status-dot fleet-status-dot--warn"

        rows.append(html.Div([
            html.Span([
                html.Span("", style={"fontSize": ".85rem", "marginRight": ".3rem", "flexShrink": "0"}),
                html.Span(cl["cluster_id"], className="fleet-row-name"),
            ], style={**COL_CLUSTER, "display": "flex", "alignItems": "center"}),
            html.Span(
                dbc.Badge(cl["engine_version"], className="fleet-badge-version",
                          style={"fontSize": ".72rem"}),
                style=COL_VERSION),
            html.Span(
                f"{cl['instance_count']} ({n_w}W/{n_r}R)" if cl["instance_count"] else "0",
                style={**COL_INST, "fontSize": ".8rem", "color": "var(--text-body)"}),
            html.Span(
                html.Div(type_chips, className="fleet-type-chips"),
                style=COL_TYPES),
            html.Span(
                html.Div([
                    html.Span(className=status_cls),
                    html.Span(cl["status"], style={"fontSize": ".78rem"}),
                ], className="d-flex align-items-center gap-1 justify-content-center"),
                style=COL_STATUS),
            html.Span(
                html.Div([
                    dbc.Button([
                        html.Img(src="/assets/Well-Architected-Tool.svg",
                                 style={"width": "14px", "height": "14px", "marginRight": ".3rem"}),
                        "Review",
                    ],
                               id={"type": "fleet-wa", "idx": cl["cluster_id"]},
                               size="sm", outline=True, color="secondary",
                               style={"fontSize": ".72rem", "borderRadius": "6px",
                                      "fontWeight": "600", "marginRight": ".4rem",
                                      "whiteSpace": "nowrap"}),
                    dbc.Button("Connect →",
                               id={"type": "fleet-connect", "idx": cl["cluster_id"]},
                               className="fleet-connect-btn-sm"),
                ], className="d-flex align-items-center justify-content-end"),
                style=COL_ACTIONS),
        ], className="fleet-table-row"))

    # Pagination footer
    pagination = html.Div([
        html.Span(f"Showing {start+1}–{end} of {total} clusters",
                  style={"fontSize": ".78rem", "color": "var(--text-muted)"}),
        html.Div([
            dbc.Button("← Prev", id="fleet-page-prev", size="sm", outline=True,
                       color="secondary", disabled=(page == 0),
                       style={"fontSize": ".75rem", "borderRadius": "6px"}),
            html.Span(f"{page+1} / {total_pages}",
                       style={"fontSize": ".8rem", "fontWeight": "600",
                              "padding": "0 .6rem", "color": "var(--text-body)"}),
            dbc.Button("Next →", id="fleet-page-next", size="sm", outline=True,
                       color="secondary", disabled=(end >= total),
                       style={"fontSize": ".75rem", "borderRadius": "6px"}),
        ], className="d-flex align-items-center gap-1"),
    ], className="d-flex justify-content-between align-items-center",
       style={"padding": ".6rem 1.25rem", "borderTop": "1px solid var(--border-default)"})

    return html.Div([html.Div([header] + rows, className="fleet-table"), pagination])


def _render_fleet(data):
    """Build the fleet dashboard content."""
    clusters = data["clusters"]
    type_counts = data["type_counts"]
    version_counts = data["version_counts"]
    pi_enabled = data.get("pi_enabled", 0)
    pi_disabled = data.get("pi_disabled", 0)

    children = []

    # ── Summary metrics ──────────────────────────────────────────────────
    children.append(html.Div([
        html.Div([
            html.Div(str(data["total_clusters"]), className="fleet-metric-value"),
            html.Div("Clusters", className="fleet-metric-label"),
        ], className="fleet-metric"),
        html.Div([
            html.Div(str(data.get("global_clusters", 0)), className="fleet-metric-value"),
            html.Div("Global Clusters", className="fleet-metric-label"),
        ], className="fleet-metric"),
        html.Div([
            html.Div(str(data["total_instances"]), className="fleet-metric-value"),
            html.Div("Total Instances", className="fleet-metric-label"),
        ], className="fleet-metric"),
        html.Div([
            html.Div(str(len(type_counts)), className="fleet-metric-value"),
            html.Div("Instance Types", className="fleet-metric-label"),
        ], className="fleet-metric"),
        html.Div([
            html.Div(str(len(version_counts)), className="fleet-metric-value"),
            html.Div("Engine Versions", className="fleet-metric-label"),
        ], className="fleet-metric"),
    ], className="fleet-metrics"))

    # ── Distribution charts (3 donuts) ───────────────────────────────────
    chart_items = []

    if type_counts:
        types_sorted = sorted(type_counts.items(), key=lambda x: -x[1])
        fig = _make_donut([t for t, _ in types_sorted], [c for _, c in types_sorted],
                          "Instance Types")
        chart_items.append(html.Div(
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
            className="fleet-chart-card"))

    if version_counts:
        versions_sorted = sorted(version_counts.items())
        fig = _make_donut([v for v, _ in versions_sorted], [c for _, c in versions_sorted],
                          "Engine Versions")
        chart_items.append(html.Div(
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
            className="fleet-chart-card"))

    if pi_enabled + pi_disabled > 0:
        fig = _make_donut(
            ["Enabled", "Disabled"], [pi_enabled, pi_disabled],
            "Performance Insights",
            colors=["#037f0c", "#d91515"])
        chart_items.append(html.Div(
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
            className="fleet-chart-card"))

    if chart_items:
        children.append(html.Div(chart_items, className="fleet-charts"))

    # ── Sort controls + cluster table ────────────────────────────────────
    if clusters:
        children.append(html.Div([
            html.Div("Clusters", className="fleet-section-title"),
            html.Div([
                html.Span("Sort by:", style={"fontSize": ".78rem", "color": "var(--text-muted)",
                                             "marginRight": ".4rem", "fontWeight": "600"}),
                dbc.RadioItems(
                    id="fleet-sort",
                    options=[
                        {"label": "Name", "value": "name"},
                        {"label": "Version", "value": "version"},
                        {"label": "Status", "value": "status"},
                        {"label": "Instances", "value": "instances"},
                    ],
                    value="name", inline=True,
                    className="fleet-sort-radios",
                    inputClassName="me-1",
                    labelClassName="fleet-sort-label",
                ),
            ], className="d-flex align-items-center"),
        ], className="d-flex justify-content-between align-items-end mb-2"))

        children.append(html.Div(
            _build_cluster_table(clusters, sort_by="name", page=0),
            id="fleet-table-container"))
    else:
        children.append(dbc.Alert("No DocumentDB clusters found in this region.", color="info"))

    return html.Div(children)


# ── Landing page layout ─────────────────────────────────────────────────────

def render_fleet_landing():
    """Render the fleet overview as the app landing page with rich UI."""
    # Check for cached fleet data or running state
    with _fleet_lock:
        cached = _fleet["data"] if _is_cache_valid() else None
        cached_region = _fleet["region"] or "us-east-1"
        running = _fleet["running"]

    initial_loading = ""
    initial_results = ""
    poll_disabled = True

    if not running and not cached:
        # Auto-discover on first load
        start_fleet_if_needed(cached_region)
        running = True

    if running:
        initial_loading = html.Div(
            wire_animation(
                phases=["Connecting to AWS", "Discovering clusters", "Fetching instances"],
                active_phase=0,
            ), className="d-flex align-items-center justify-content-center py-3")
        poll_disabled = False
    elif cached:
        remaining = int(CACHE_TTL_SECONDS - (time.time() - _fleet["timestamp"]))
        initial_loading = html.Span(
            f"✅ Fleet data loaded (cached · expires in {remaining}s)",
            style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})
        initial_results = _render_fleet(cached)

    return html.Div([
        # Hidden dummy elements for removed buttons (Dash requires Input elements to exist)
        html.Div([
            html.Button(id="fleet-run-btn", style={"display": "none"}),
            html.Button(id="fleet-skip-btn", style={"display": "none"}),
            dcc.Dropdown(id="fleet-region", style={"display": "none"}, value=cached_region),
        ], style={"display": "none"}),

        html.Div([
            dcc.Store(id="fleet-meta", data={"region": cached_region}),
            html.Div(id="fleet-loading", children=initial_loading, className="fleet-status-bar"),
            html.Div(id="fleet-results", children=initial_results),
            dcc.Interval(id="fleet-poll", interval=800, disabled=poll_disabled),
        ], className="fleet-body"),

        # ── Connect modal (full auth form) ──────────────────────────────
        dbc.Modal([
            dbc.ModalHeader(
                dbc.ModalTitle("Connect to your cluster"),
                close_button=True),
            dbc.ModalBody([
                # Step 1: Region & Cluster
                html.Div([
                    html.Div([
                        html.Span("1", className="auth-step-num"),
                        html.Span("Cluster", className="auth-step-label"),
                    ], className="auth-step-header"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("AWS Region", className="form-label"),
                            dcc.Dropdown(
                                id="auth-region",
                                options=[{"label": r, "value": r} for r in get_aws_regions()],
                                value="us-east-1", clearable=False,
                            ),
                        ], md=4),
                        dbc.Col([
                            html.Label("Cluster", className="form-label"),
                            dbc.Row([
                                dbc.Col(
                                    dbc.Button("Discover", id="auth-discover-btn",
                                               color="secondary", size="sm", outline=True,
                                               className="w-100"),
                                    width=4),
                                dbc.Col(
                                    dcc.Dropdown(id="auth-cluster-dropdown",
                                                 placeholder="Select cluster...",
                                                 style={"fontSize": ".88rem"}),
                                    width=8),
                            ], className="g-2"),
                            html.Div(id="auth-discover-status", className="mt-1"),
                        ], md=8),
                    ], className="mb-3"),
                    html.Div(id="auth-cluster-info"),
                ], className="auth-section"),

                html.Hr(className="my-3"),

                # Step 2: Connection Mode
                html.Div([
                    html.Div([
                        html.Span("2", className="auth-step-num"),
                        html.Span("Connection", className="auth-step-label"),
                    ], className="auth-step-header"),
                    dbc.RadioItems(
                        id="auth-conn-mode",
                        options=[
                            {"label": "  SSH Tunnel", "value": "tunnel"},
                            {"label": "  Direct", "value": "direct"},
                        ],
                        value="tunnel", inline=True, className="mb-3",
                    ),
                    html.Div(id="auth-ssh-fields", children=[
                        dbc.Row([
                            dbc.Col([
                                html.Label("Bastion Host IP", className="form-label"),
                                dbc.Input(id="auth-bastion", placeholder="10.0.1.50", size="sm"),
                            ], md=4),
                            dbc.Col([
                                html.Label("SSH User", className="form-label"),
                                dbc.Input(id="auth-ssh-user", value="ec2-user", size="sm"),
                            ], md=4),
                            dbc.Col([
                                html.Label("SSH Key Path (.pem)", className="form-label"),
                                dbc.Input(id="auth-ssh-key", placeholder="/path/to/key.pem", size="sm"),
                            ], md=4),
                        ], className="mb-2"),
                    ]),
                    html.Div(id="auth-direct-fields", children=[
                        # Option B: when a fleet cluster is selected its host is
                        # known — the URI is built internally from host +
                        # username/password (Step 3). The manual URI field is
                        # only shown when no cluster is selected (manual host).
                        html.Div(id="auth-manual-uri-wrap", children=[
                            html.Label("Connection String (manual host)",
                                       className="form-label"),
                            dbc.Input(id="auth-manual-conn",
                                      placeholder="mongodb://user:pass@host:27017/?...", size="sm"),
                            html.Small(
                                "Only needed for a host not in the fleet. For a "
                                "selected cluster, leave blank — the URI is built "
                                "from the host plus your username and password.",
                                className="text-muted", style={"fontSize": ".72rem"}),
                        ]),
                        html.Div(id="auth-direct-hint", children=[
                            html.Small(
                                "Connecting to the selected cluster — just enter "
                                "your username and password below.",
                                className="text-muted", style={"fontSize": ".72rem"}),
                        ], style={"display": "none"}),
                    ], style={"display": "none"}),
                ], className="auth-section"),

                html.Hr(className="my-3"),

                # Step 3: Credentials
                html.Div([
                    html.Div([
                        html.Span("3", className="auth-step-num"),
                        html.Span("Credentials", className="auth-step-label"),
                    ], className="auth-step-header"),
                    dbc.Row([
                        dbc.Col([
                            html.Label("Username", className="form-label"),
                            dbc.Input(id="auth-username", placeholder="docdb-user", size="sm"),
                        ], md=4),
                        dbc.Col([
                            html.Label("Password", className="form-label"),
                            dbc.Input(id="auth-password", type="password",
                                      placeholder="Password", size="sm"),
                        ], md=4),
                        dbc.Col([
                            html.Label("\u00a0", className="form-label"),
                            dbc.Checklist(
                                id="auth-tls",
                                options=[{"label": "  TLS / SSL", "value": "ssl"}],
                                value=[], className="mt-1",
                            ),
                        ], md=4),
                    ]),
                ], className="auth-section"),

                html.Hr(className="my-3"),

                # Connect button + status
                html.Div([
                    html.Button("Connect & Load Databases", id="auth-connect-btn",
                                className="btn btn-warning w-100",
                                style={"fontWeight": "700", "fontSize": "1rem", "padding": ".6rem",
                                       "borderRadius": "6px", "border": "none"}),
                    html.Div(id="auth-connect-status", className="mt-2"),
                ]),

                # Hidden stores needed by auth callbacks
                dcc.Store(id="auth-clusters-store"),
            ]),
        ], id="fleet-connect-modal", is_open=False, centered=True, size="xl",
           className="fleet-auth-modal"),

        # ── Well-Architected Review modal ────────────────────────────────
        dbc.Modal([
            dbc.ModalHeader(dbc.ModalTitle([
                html.Img(src="/assets/Well-Architected-Tool.svg",
                         style={"width": "22px", "height": "22px", "marginRight": ".5rem"}),
                "Well-Architected Review",
            ]), close_button=True),
            dbc.ModalBody([
                html.Div([
                    html.Div(id="fleet-wa-cluster-info"),
                    dbc.Button("Download PDF", id="fleet-wa-download-btn",
                               size="sm", outline=True, color="secondary",
                               style={"fontSize": ".75rem", "borderRadius": "6px",
                                      "fontWeight": "600"},
                               disabled=True),
                ], className="d-flex justify-content-between align-items-center mb-3"),
                html.Div(id="fleet-wa-loading"),
                html.Div(id="fleet-wa-results"),
                dcc.Store(id="fleet-wa-meta"),
                dcc.Interval(id="fleet-wa-poll", interval=800, disabled=True),
                dcc.Download(id="fleet-wa-pdf-download"),
            ]),
        ], id="fleet-wa-modal", is_open=False, centered=True, size="xl",
           className="fleet-auth-modal"),

        html.Div([
            html.Small("Amazon DocumentDB Agent v2.0  ·  Built by Amazon DocumentDB SSA Team",
                       style={"color": "var(--text-muted)"}),
        ], className="text-center", style={"padding": "1rem 0"}),
    ], className="fleet-page")


# ── Callbacks ────────────────────────────────────────────────────────────────

@callback(
    Output("fleet-meta", "data"),
    Output("fleet-loading", "children", allow_duplicate=True),
    Output("fleet-results", "children", allow_duplicate=True),
    Output("fleet-poll", "disabled", allow_duplicate=True),
    Input("topnav-region-selector", "value"),
    prevent_initial_call=True,
)
def cb_fleet_region(region):
    """When region changes in topnav, auto-trigger fleet discovery."""
    region = region or "us-east-1"
    with _fleet_lock:
        _fleet.update(data=None, running=False, done=False, error=None, region=None, timestamp=0)
    threading.Thread(target=_fetch_fleet, args=(region,), daemon=True).start()
    loading = html.Div(
        wire_animation(
            phases=["Connecting to AWS", "Discovering clusters", "Fetching instances"],
            active_phase=0,
        ), className="d-flex align-items-center justify-content-center py-3")
    return {"region": region}, loading, "", False


@callback(
    Output("fleet-loading", "children", allow_duplicate=True),
    Output("fleet-results", "children", allow_duplicate=True),
    Output("fleet-poll", "disabled", allow_duplicate=True),
    Output("fleet-clusters-store", "data", allow_duplicate=True),
    Input("fleet-run-btn", "n_clicks"),
    State("fleet-meta", "data"),
    prevent_initial_call=True,
)
def cb_fleet_start(n, meta):
    # Button removed from UI; kept as no-op for Dash callback registration
    return no_update, no_update, no_update, no_update


@callback(
    Output("fleet-loading", "children"),
    Output("fleet-results", "children"),
    Output("fleet-poll", "disabled"),
    Output("fleet-clusters-store", "data"),
    Input("fleet-poll", "n_intervals"),
    prevent_initial_call=True,
)
def cb_fleet_poll(n):
    with _fleet_lock:
        done = _fleet["done"]
        running = _fleet["running"]
        error = _fleet["error"]
        data = _fleet["data"]

    # If not actively running, stop polling (stale state from previous run)
    if not running and not done:
        return no_update, no_update, True, no_update

    if not done:
        return no_update, no_update, False, no_update

    if error:
        return (html.Span(f"Error: {error[:120]}", style={"color": "#d91515", "fontSize": ".85rem"}),
                "", True, no_update)

    if not data:
        return "Done", dbc.Alert("No data returned.", color="info"), True, no_update

    done_msg = html.Span("✅ Fleet data loaded",
                         style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})
    return done_msg, _render_fleet(data), True, data["clusters"]


# ── Sort / Pagination callbacks ──────────────────────────────────────────────

@callback(
    Output("fleet-table-container", "children"),
    Output("fleet-page-store", "data", allow_duplicate=True),
    Input("fleet-sort", "value"),
    State("fleet-clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_fleet_sort(sort_by, clusters_data):
    if not clusters_data:
        return no_update, no_update
    return _build_cluster_table(clusters_data, sort_by=sort_by, page=0), 0


@callback(
    Output("fleet-table-container", "children", allow_duplicate=True),
    Output("fleet-page-store", "data", allow_duplicate=True),
    Input("fleet-page-prev", "n_clicks"),
    State("fleet-page-store", "data"),
    State("fleet-sort", "value"),
    State("fleet-clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_fleet_prev(n, page, sort_by, clusters_data):
    if not n or not clusters_data:
        return no_update, no_update
    new_page = max(0, (page or 0) - 1)
    return _build_cluster_table(clusters_data, sort_by=sort_by or "name", page=new_page), new_page


@callback(
    Output("fleet-table-container", "children", allow_duplicate=True),
    Output("fleet-page-store", "data"),
    Input("fleet-page-next", "n_clicks"),
    State("fleet-page-store", "data"),
    State("fleet-sort", "value"),
    State("fleet-clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_fleet_next(n, page, sort_by, clusters_data):
    if not n or not clusters_data:
        return no_update, no_update
    total_pages = max(1, (len(clusters_data) + PAGE_SIZE - 1) // PAGE_SIZE)
    new_page = min((page or 0) + 1, total_pages - 1)
    return _build_cluster_table(clusters_data, sort_by=sort_by or "name", page=new_page), new_page


# ── Clear error message when user fills in fields ────────────────────────────

@callback(
    Output("auth-connect-status", "children", allow_duplicate=True),
    Input("auth-cluster-dropdown", "value"),
    Input("auth-username", "value"),
    Input("auth-password", "value"),
    Input("auth-bastion", "value"),
    Input("auth-ssh-key", "value"),
    Input("auth-conn-mode", "value"),
    prevent_initial_call=True,
)
def cb_clear_connect_error(*_):
    """Clear the error message as soon as user changes any input field."""
    return ""


# ── Connect / Skip callbacks ────────────────────────────────────────────────

@callback(
    Output("fleet-connect-modal", "is_open", allow_duplicate=True),
    Output("auth-clusters-store", "data", allow_duplicate=True),
    Output("auth-cluster-dropdown", "options", allow_duplicate=True),
    Output("auth-cluster-dropdown", "value", allow_duplicate=True),
    Output("auth-region", "value", allow_duplicate=True),
    Output("auth-discover-status", "children", allow_duplicate=True),
    Input({"type": "fleet-connect", "idx": ALL}, "n_clicks"),
    State("fleet-clusters-store", "data"),
    State("fleet-meta", "data"),
    prevent_initial_call=True,
)
def cb_fleet_connect(clicks, clusters_data, meta):
    """Open modal with cluster pre-selected from the table row."""
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update, no_update, no_update, no_update, no_update, no_update
    cluster_id = ctx.triggered_id["idx"]
    region = (meta or {}).get("region", "us-east-1")

    # Build dropdown options from fleet data
    opts = []
    selected = None
    if clusters_data:
        opts = [{"label": f"{c['cluster_id']} ({c['status']})", "value": i}
                for i, c in enumerate(clusters_data)]
        for i, c in enumerate(clusters_data):
            if c["cluster_id"] == cluster_id:
                selected = i
                break

    status = dbc.Alert("Cluster pre-selected from fleet overview",
                       color="success", className="py-1 mb-0",
                       style={"fontSize": ".82rem"})
    return True, clusters_data, opts, selected, region, status


@callback(
    Output("fleet-connect-modal", "is_open"),
    Output("auth-clusters-store", "data", allow_duplicate=True),
    Output("auth-cluster-dropdown", "options", allow_duplicate=True),
    Output("auth-region", "value", allow_duplicate=True),
    Output("auth-discover-status", "children", allow_duplicate=True),
    Input("fleet-skip-btn", "n_clicks"),
    State("fleet-clusters-store", "data"),
    State("fleet-meta", "data"),
    prevent_initial_call=True,
)
def cb_fleet_skip(n, clusters_data, meta):
    # Button removed from UI; kept as no-op for Dash callback registration
    return no_update, no_update, no_update, no_update, no_update


# ── Fleet Well-Architected Review ────────────────────────────────────────────
_fleet_wa = {"results": [], "running": False, "done": False, "error": None}
_fleet_wa_lock = threading.Lock()


@callback(
    Output("fleet-wa-modal", "is_open"),
    Output("fleet-wa-meta", "data"),
    Output("fleet-wa-cluster-info", "children"),
    Output("fleet-wa-loading", "children", allow_duplicate=True),
    Output("fleet-wa-results", "children", allow_duplicate=True),
    Output("fleet-wa-poll", "disabled", allow_duplicate=True),
    Input({"type": "fleet-wa", "idx": ALL}, "n_clicks"),
    State("fleet-meta", "data"),
    prevent_initial_call=True,
)
def cb_fleet_wa_start(clicks, meta):
    """Open WA modal and start review for the selected cluster."""
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update, no_update, no_update, no_update, no_update, no_update

    cluster_id = ctx.triggered_id["idx"]
    region = (meta or {}).get("region", "us-east-1")

    with _fleet_wa_lock:
        _fleet_wa.update(results=[], running=True, done=False, error=None)

    def _run():
        try:
            from tabs.well_architected import _run_wa_checks, _wa, _wa_lock
            _run_wa_checks(cluster_id, region)
            with _wa_lock:
                results = list(_wa["results"])
                error = _wa.get("error")
            with _fleet_wa_lock:
                _fleet_wa.update(results=results, running=False, done=True,
                                 error=error)
        except Exception as e:
            with _fleet_wa_lock:
                _fleet_wa.update(results=[], running=False, done=True, error=str(e))

    threading.Thread(target=_run, daemon=True).start()

    info = html.Div([
        html.Small("Cluster: ", className="text-muted"),
        html.Strong(cluster_id, style={"fontSize": ".9rem"}),
        html.Small(f"  ·  {region}", className="text-muted"),
    ])
    loading = html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span("Running Well-Architected checks…",
                  style={"fontSize": ".88rem", "color": "#5f6b7a"}),
    ], className="d-flex align-items-center mb-2")

    return True, {"cluster_id": cluster_id, "region": region}, info, loading, "", False


@callback(
    Output("fleet-wa-loading", "children"),
    Output("fleet-wa-results", "children"),
    Output("fleet-wa-poll", "disabled"),
    Output("fleet-wa-download-btn", "disabled"),
    Input("fleet-wa-poll", "n_intervals"),
    prevent_initial_call=True,
)
def cb_fleet_wa_poll(n):
    with _fleet_wa_lock:
        done = _fleet_wa["done"]
        error = _fleet_wa["error"]
        results = list(_fleet_wa["results"])

    if not done:
        return no_update, no_update, False, no_update

    if error:
        return html.Span(f"❌ {error[:80]}",
                         style={"color": "#d91515", "fontSize": ".85rem"}), "", True, True

    from tabs.well_architected import _render_results
    done_msg = html.Span(f"✅ Review complete — {len(results)} checks",
                         style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})
    return done_msg, _render_results(results), True, False


# ── PDF Export ───────────────────────────────────────────────────────────────

def _generate_wa_pdf(results, cluster_id, region):
    """Generate a Well-Architected Review PDF report."""
    from fpdf import FPDF
    from datetime import datetime

    def _safe(text):
        """Replace Unicode characters unsupported by built-in PDF fonts."""
        return (str(text)
                .replace("\u2014", "-").replace("\u2013", "-")   # em/en dash
                .replace("\u2018", "'").replace("\u2019", "'")   # smart quotes
                .replace("\u201c", '"').replace("\u201d", '"')
                .replace("\u2026", "...").replace("\u00b7", "-") # ellipsis, middle dot
                .replace("\u2705", "[OK]").replace("\u274c", "[X]")
                .replace("\u26a0", "[!]").replace("\u2139", "[i]")
                .encode("latin-1", errors="replace").decode("latin-1"))

    pdf = FPDF()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 18)
    pdf.cell(0, 12, "Well-Architected Review", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, _safe(f"Cluster: {cluster_id}  |  Region: {region}  |  {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # Summary
    n_pass = sum(1 for r in results if r["status"] == "pass")
    n_warn = sum(1 for r in results if r["status"] == "warn")
    n_fail = sum(1 for r in results if r["status"] == "fail")
    total = len(results)
    score = int((n_pass / total) * 100) if total else 0

    pdf.set_text_color(0, 0, 0)
    pdf.set_font("Helvetica", "B", 12)
    pdf.cell(0, 8, _safe(f"Score: {score}%  |  {n_pass} Passed  |  {n_warn} Warnings  |  {n_fail} Failed  |  {total} Total"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # Separator
    pdf.set_draw_color(200, 200, 200)
    pdf.line(10, pdf.get_y(), 200, pdf.get_y())
    pdf.ln(4)

    # Group by pillar
    pillar_order = ["Reliability", "Security", "Operational Excellence",
                    "Performance Efficiency", "Cost Optimization", "Sustainability", "Other"]
    pillars = {}
    for r in results:
        p = r["pillar"]
        if p not in pillars:
            pillars[p] = []
        pillars[p].append(r)

    status_symbols = {"pass": "[PASS]", "warn": "[WARN]", "fail": "[FAIL]", "info": "[INFO]"}

    for pillar_name in pillar_order:
        if pillar_name not in pillars:
            continue
        checks = pillars[pillar_name]
        p_pass = sum(1 for c in checks if c["status"] == "pass")

        # Pillar header
        pdf.set_font("Helvetica", "B", 12)
        pdf.set_text_color(35, 47, 62)
        pdf.cell(0, 8, _safe(f"{pillar_name}  ({p_pass}/{len(checks)} passed)"),
                 new_x="LMARGIN", new_y="NEXT")

        for check in checks:
            sym = status_symbols.get(check["status"], "[?]")

            # Status + label
            pdf.set_font("Helvetica", "B", 9)
            if check["status"] == "pass":
                pdf.set_text_color(3, 127, 12)
            elif check["status"] == "warn":
                pdf.set_text_color(141, 102, 5)
            elif check["status"] == "fail":
                pdf.set_text_color(217, 21, 21)
            else:
                pdf.set_text_color(9, 114, 211)

            pdf.cell(18, 5, sym)
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Helvetica", "", 9)
            pdf.cell(0, 5, _safe(check["label"][:100]), new_x="LMARGIN", new_y="NEXT")

            # Detail
            if check.get("detail"):
                pdf.set_font("Helvetica", "I", 8)
                pdf.set_text_color(120, 120, 120)
                pdf.cell(18, 4, "")
                pdf.cell(0, 4, _safe(check["detail"][:120]), new_x="LMARGIN", new_y="NEXT")

        pdf.ln(3)

    return pdf.output()


@callback(
    Output("fleet-wa-pdf-download", "data"),
    Input("fleet-wa-download-btn", "n_clicks"),
    State("fleet-wa-meta", "data"),
    prevent_initial_call=True,
)
def cb_fleet_wa_download(n, meta):
    if not n or not meta:
        return no_update

    with _fleet_wa_lock:
        results = list(_fleet_wa["results"])

    if not results:
        return no_update

    cluster_id = meta.get("cluster_id", "unknown")
    region = meta.get("region", "us-east-1")
    try:
        pdf_bytes = _generate_wa_pdf(results, cluster_id, region)
        return dcc.send_bytes(lambda buf: buf.write(bytes(pdf_bytes)), f"WAR-{cluster_id}.pdf")
    except ImportError:
        logger.warning("fpdf2 not installed — PDF export unavailable. Run: pip install fpdf2")
        return no_update
    except Exception as e:
        logger.error("PDF generation failed: %s", e)
        return no_update
