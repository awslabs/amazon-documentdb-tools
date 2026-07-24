import json
import threading
import time
import logging
import dash
from dash import html, dcc, Input, Output, State, callback, no_update, ctx, clientside_callback
import dash_bootstrap_components as dbc

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

from aws_discovery import (
    discover_documentdb_clusters, get_cluster_databases,
    get_aws_regions, build_connection_string,
)
from db_analyzer import get_documentdb_stats
from ssh_tunnel import open_tunnel, close_tunnel, is_tunnel_active, get_tunnel_info, get_tunnel_connection_string

# Tab modules — imported so their register_tab() / register_db_tab() calls execute.
import tabs.cluster_snapshot        # noqa: F401
import tabs.well_architected        # noqa: F401
import tabs.wa_v2                   # noqa: F401
# import tabs.storage_config  # merged into cluster_snapshot
import tabs.current_activity        # noqa: F401
import tabs.cluster_slow_queries    # noqa: F401
import tabs.chat_panel              # noqa: F401
import tabs.database_dashboard      # noqa: F401  (triggers DB-tab imports)
import tabs.recommended_actions     # noqa: F401

from tabs.registry import get_tabs, get_db_tabs
from tabs.cluster_snapshot import start_snapshot_if_needed, reset_snapshot
from tabs.auth_page import render_auth_page
from tabs.fleet_overview import start_fleet_if_needed, render_fleet_landing, reset_fleet
from tabs.cluster_tree import build_cluster_tree
from tabs.collection_detail import render_collection_detail
from tabs.database_dashboard import render_database_dashboard
from tabs.index_detail import render_index_detail
from tabs.code_review_panel import render_code_review_panel


def _aws_topnav(breadcrumb=None, region=None, cluster_id=None, show_region_selector=False,
                show_logout=True):
    """AWS Console-style top navigation bar, shared across all views."""
    crumb_items = []
    if breadcrumb:
        for i, (label, is_active) in enumerate(breadcrumb):
            if i > 0:
                crumb_items.append(
                    html.Span("›", className="aws-topnav-breadcrumb-sep")
                )
            crumb_items.append(
                html.Span(label,
                          className="aws-topnav-breadcrumb-item aws-topnav-breadcrumb-item--active"
                          if is_active else "aws-topnav-breadcrumb-item")
            )

    right_items = []
    if show_region_selector:
        right_items.append(
            html.Div([
                dcc.Dropdown(
                    id="topnav-region-selector",
                    options=[{"label": r, "value": r} for r in get_aws_regions()],
                    value=region or "us-east-1",
                    clearable=False,
                    style={"width": "180px", "fontSize": ".82rem"},
                    className="topnav-region-dropdown",
                ),
            ], style={"display": "flex", "alignItems": "center"})
        )
    elif region:
        right_items.append(html.Span(region, className="aws-topnav-region-badge"))
    if cluster_id:
        right_items.append(
            html.Span(cluster_id, className="aws-topnav-user",
                      style={"borderLeft": "1px solid rgba(255,255,255,.1)",
                             "paddingLeft": ".5rem", "marginLeft": ".25rem"})
        )
    if show_logout:
        right_items.append(
            html.A("Sign out", href="/logout",
                   className="aws-topnav-user",
                   style={"borderLeft": "1px solid rgba(255,255,255,.1)",
                          "paddingLeft": ".6rem", "marginLeft": ".4rem",
                          "color": "#fff", "textDecoration": "none",
                          "cursor": "pointer", "fontSize": ".82rem",
                          "fontWeight": "600"})
        )

    return html.Div([
        # Logo area
        html.Div([
            html.Img(src="/assets/prism-icon-dark.svg",
                     style={"width": "24px", "height": "24px", "borderRadius": "4px",
                            "flexShrink": "0"}),
            html.Div([
                html.Div("Prism", className="aws-topnav-logo-text"),
                html.Div("for Amazon DocumentDB", className="aws-topnav-logo-sub"),
            ]),
        ], className="aws-topnav-logo"),

        # Breadcrumb
        html.Div(crumb_items, className="aws-topnav-breadcrumb"),

        # Right side: region + cluster
        html.Div(right_items, className="aws-topnav-actions"),
    ], className="aws-topnav")

# ── Global progress state (thread-safe) ─────────────────────────────────────
_progress = {"phase": "", "detail": "", "pct": 0, "running": False, "done": False, "error": None, "result": None, "conn_meta": None}
_lock = threading.Lock()

# ── Server-side connection state (credentials never sent to browser) ─────────
_conn_state = {"connection_string": None}
_conn_lock = threading.Lock()


def get_connection_string():
    """Get the active connection string (server-side only)."""
    with _conn_lock:
        return _conn_state["connection_string"]


def set_connection_string(conn_str):
    """Store connection string server-side."""
    with _conn_lock:
        _conn_state["connection_string"] = conn_str


def _progress_callback(phase, detail, pct):
    with _lock:
        _progress["phase"] = phase
        _progress["detail"] = detail
        _progress["pct"] = min(pct, 100)


# ── Lazy load state (thread-safe) ─────────────────────────────────────────────
_lazy_load = {"pending": {}, "done": False}
_lazy_lock = threading.Lock()


def _fetch_db_tree(client, db_name):
    """Fetch collection names only — just enough to render the sidebar tree."""
    db_obj = client[db_name]
    colls = {}
    try:
        for coll_name in sorted(db_obj.list_collection_names()):
            colls[coll_name] = {"indexes": [], "count": 0}
    except Exception:
        pass
    return colls


def _start_lazy_load(conn_str, remaining_dbs, delay):
    """Start background thread to load remaining databases."""
    with _lazy_lock:
        _lazy_load["pending"] = {}
        _lazy_load["done"] = False

    def _run():
        import pymongo
        try:
            client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000, appname='DocDB-Prism')
            for db_name in remaining_dbs:
                data = _fetch_db_tree(client, db_name)
                with _lazy_lock:
                    _lazy_load["pending"][db_name] = data
                time.sleep(delay)
            client.close()
        except Exception as e:
            logging.getLogger(__name__).warning("Lazy load failed: %s", e)
        finally:
            with _lazy_lock:
                _lazy_load["done"] = True

    threading.Thread(target=_run, daemon=True).start()


# ── App ──────────────────────────────────────────────────────────────────────
# CSS is auto-loaded from assets/style.css

app = dash.Dash(
    __name__,
    external_stylesheets=[dbc.themes.BOOTSTRAP],
    suppress_callback_exceptions=True,
    title="Amazon DocumentDB Prism",
)
server = app.server

# ── Application-login gate (local SQLite store) ──────────────────────────────
# A stable secret key keeps login sessions valid across restarts; the user
# store lives in a local SQLite file (see auth_store.py / create_user.py).
import flask
import auth_store

auth_store.init_db()
server.secret_key = auth_store.get_or_create_secret_key()
# Session security hardening
server.config.update(
    SESSION_PERMANENT=True,
    PERMANENT_SESSION_LIFETIME=900,         # 15 min server-side idle timeout
    SESSION_COOKIE_HTTPONLY=True,            # Not readable by JS (XSS defense)
    SESSION_COOKIE_SAMESITE="Lax",          # CSRF defense
    SESSION_COOKIE_SECURE=False,            # Set True when behind HTTPS/ALB
)


def _is_authenticated():
    """Authoritative server-side auth check (browser state is not trusted)."""
    return bool(flask.session.get("authenticated"))


@server.before_request
def _refresh_session():
    """Refresh session lifetime on each request (sliding window expiry)."""
    flask.session.modified = True


@server.route("/logout")
def _logout():
    """Clear the session and return to the sign-in page."""
    flask.session.clear()
    return flask.redirect("/")

# ── Sidebar ──────────────────────────────────────────────────────────────────

def _stepper(step=0):
    """Build the 3-step visual stepper. step: 0=none, 1=cluster active, 2=creds active, 3=db active."""
    labels = ["Cluster", "Credentials", "Database"]
    icons  = ["1", "2", "3"]
    parts  = []
    for i in range(3):
        s = i + 1
        if s < step:
            dot_cls = "stepper-dot stepper-dot--done"
            step_cls = "stepper-step stepper-step--done"
            icon = "✓"
        elif s == step:
            dot_cls = "stepper-dot stepper-dot--active"
            step_cls = "stepper-step stepper-step--active"
            icon = icons[i]
        else:
            dot_cls = "stepper-dot"
            step_cls = "stepper-step"
            icon = icons[i]
        parts.append(html.Div([
            html.Div(icon, className=dot_cls),
            html.Div(labels[i], className="stepper-label"),
        ], className=step_cls))
        if i < 2:
            line_cls = "stepper-line stepper-line--done" if s < step else "stepper-line"
            parts.append(html.Div(className=line_cls))
    return html.Div(parts, className="stepper")


def _build_sidebar(conn_data=None):
    dbs = conn_data.get('databases', []) if conn_data else []
    db_opts = [{'label': d, 'value': d} for d in dbs]
    first_db = dbs[0] if dbs else None
    return html.Div([
    dcc.Store(id="theme-store", data="light", storage_type="local"),

    # ── Back to connect
    dbc.Button("← Back to Fleet", id="back-to-connect-btn", color="secondary",
               size="sm", outline=True,
               style={"fontSize": ".78rem", "margin": ".5rem .75rem .25rem",
                      "display": "block", "width": "calc(100% - 1.5rem)",
                      "color": "#8d99a8", "borderColor": "#8d99a8"}),

    # ── Database selector (hidden, used by callbacks)
    dcc.Dropdown(id="db-dropdown", placeholder="Select database…",
                 options=db_opts, value=first_db,
                 style={"display": "none"}),

    html.Hr(style={"borderColor": "#d5dbdb", "margin": ".4rem 0"}),

    # ── Cluster section
    html.Div([
        html.Small("CLUSTER", className="sb-section-label"),
        html.Div(html.Span("Overview", style={"fontSize": ".82rem"}),
                 className="tree-node", id={"type": "tree-tool", "tool": "snapshot"}),
        html.Div(html.Span("Well-Architected", style={"fontSize": ".82rem"}),
                 className="tree-node", id={"type": "tree-tool", "tool": "wa"}),
        html.Div(html.Span("Activity", style={"fontSize": ".82rem"}),
                 className="tree-node", id={"type": "tree-tool", "tool": "activity"}),
        html.Div(html.Span("Slow Queries", style={"fontSize": ".82rem"}),
                 className="tree-node", id={"type": "tree-tool", "tool": "cluster_slowquery"}),
        html.Div(html.Span("Recommended Actions", style={"fontSize": ".82rem"}),
                 className="tree-node", id={"type": "tree-tool", "tool": "rec_actions"}),
    ], className="cluster-tree mb-1"),

    html.Hr(style={"borderColor": "#d5dbdb", "margin": ".4rem 0"}),

    # ── AI Advisor — own subsection
    html.Div([
        html.Div([
            html.Span("AI Advisor", style={"fontSize": ".82rem", "fontWeight": "700",
                      "color": "var(--color-primary-hover)"}),
        ], className="tree-node", id={"type": "tree-tool", "tool": "chat"}),
    ], className="cluster-tree mb-1"),

    html.Hr(style={"borderColor": "#d5dbdb", "margin": ".4rem 0"}),

    # ── Agent Status Bar
    html.Div(id="agent-status-bar", className="mb-1"),

    # ── DATABASES header + Analyze + Refresh buttons
    html.Div([
        html.Small("DATABASES", className="sb-section-label",
                   style={"padding": "0"}),
        html.Div([
            dbc.Button("↻", id="sidebar-refresh-btn", color="link",
                       size="sm", title="Refresh status",
                       style={"fontSize": ".85rem", "padding": ".1rem .3rem",
                              "color": "var(--text-sidebar-muted)", "lineHeight": "1"}),
            dbc.Button("Analyze", id="analyze-btn", color="warning",
                       size="sm", style={"fontWeight": "700", "fontSize": ".7rem",
                                         "padding": ".15rem .5rem", "lineHeight": "1.3"}),
        ], style={"display": "flex", "gap": ".3rem", "alignItems": "center"}),
    ], className="d-flex justify-content-between align-items-center",
       style={"padding": ".3rem .5rem"}),
    html.Div(id="sidebar-analysis-status"),

    # ── Cluster Explorer Tree (databases)
    html.Div(id="sidebar-tree", className="mb-2",
             style={"overflowY": "auto", "overflowX": "hidden", "flex": "1"}),

    html.Hr(style={"borderColor": "#d5dbdb", "margin": ".4rem 0"}),

    # ── Code Review section
    html.Div([
        html.Small("CODE REVIEW", className="sb-section-label"),
        html.Div([
            html.Span("Application Code Review", style={"fontSize": ".82rem"}),
        ], className="tree-node", id={"type": "tree-tool", "tool": "code_review"}),
    ], className="cluster-tree mb-1"),

    # ── Hidden elements needed by existing callbacks
    dcc.Store(id="clusters-store"),
    dcc.Store(id="analysis-store"),
    dcc.Store(id="conn-store"),
    dcc.Store(id="analysis-trigger", data=0),
    dcc.Store(id="active-nav", data="snapshot"),
    dcc.Store(id="tree-selection", data={"level": "cluster", "db": "", "collection": "", "index": ""}),
    dcc.Store(id="tree-expanded", data={"databases": []}),
    dcc.Store(id="sidebar-collapsed", data=False),
    dcc.Interval(id="progress-interval", interval=400, disabled=True),
    dcc.Interval(id="lazy-load-interval", interval=2000, disabled=True),
    # Agent stores
    dcc.Store(id="agent-store", data=None, storage_type="session"),
    dcc.Interval(id="agent-poll", interval=4000, disabled=True),
    dcc.Download(id="agent-report-download"),
    # Hidden inputs that old callbacks reference (keep them to avoid errors)
    html.Div([
        dcc.Dropdown(id="cluster-dropdown", style={"display": "none"}),
        dcc.Dropdown(id="region-dropdown", options=[], style={"display": "none"}),
        dbc.Input(id="username-input", style={"display": "none"}),
        dbc.Input(id="password-input", style={"display": "none"}),
        dbc.Input(id="manual-conn", style={"display": "none"}),
        dbc.Input(id="manual-log", type="hidden"),
        dbc.Checklist(id="ssl-check", options=[], value=[], style={"display": "none"}),
        dbc.RadioItems(id="conn-mode", options=[], value="direct", style={"display": "none"}),
        dbc.Input(id="bastion-host", style={"display": "none"}),
        dbc.Input(id="ssh-user", style={"display": "none"}),
        dbc.Input(id="ssh-key-path", style={"display": "none"}),
        dbc.Button(id="discover-btn", style={"display": "none"}),
        dbc.Button(id="load-db-btn", style={"display": "none"}),
        html.Div(id="discover-status"),
        html.Div(id="cluster-info"),
        html.Div(id="load-db-status"),
        html.Div(id="tunnel-status"),
        html.Div(id="conn-status-pill"),
        html.Div(id="stepper-area"),
        html.Div(id="sec-cluster"),
        html.Div(id="sec-creds"),
        html.Div(id="sec-db"),
        html.Div(id="ssh-fields"),
        html.Div(id="direct-fields"),
        html.Div(id="sec-cluster-header"),
        html.Div(id="sec-creds-header"),
        html.Div(id="sec-db-header"),
        html.Div(id="sidebar-toggle"),
    ], style={"display": "none"}),
], id="sidebar-container", className="sidebar")

# ── Main content ─────────────────────────────────────────────────────────────
header = html.Div([
    dbc.Row([
        dbc.Col(html.Span("⚙️", style={"fontSize": "1.4rem"}), width="auto", className="pe-2"),
        dbc.Col([
            html.H2("Amazon DocumentDB Prism", className="mb-0"),
            html.P("Intelligent cluster analysis & optimization", className="mb-0"),
        ]),
    ], align="center"),
], className="app-header")

main_content = html.Div([
    html.Div(id="progress-area"),
    html.Div(id="results-area"),
    html.Div(id="nav-content", style={"display": "none"}),
    html.Div(id="db-tab-content", style={"display": "none"}),
    html.Div([
        dbc.Row([
            dbc.Col(html.Span(["Built by ", html.Strong("Amazon DocumentDB SSA Team")]), width="auto"),
            dbc.Col(html.Span("Amazon DocumentDB Prism v2.0"), className="text-end"),
        ]),
    ], className="app-footer"),
], style={"padding": "1.25rem 1.5rem", "overflowY": "auto", "height": "100vh"})

app.layout = html.Div([
    dcc.Store(id="app-view", data="fleet", storage_type="session"),
    dcc.Store(id="fleet-clusters-store", storage_type="session"),
    dcc.Store(id="fleet-page-store", data=0, storage_type="session"),
    dcc.Store(id="app-conn-data", storage_type="session"),
    # Idle-logout: redirect to /logout after IDLE_TIMEOUT_MS of no real user
    # activity. Uses genuine activity (mouse/key/scroll/touch) rather than the
    # app's background polling, which would otherwise keep the session alive.
    dcc.Interval(id="idle-check", interval=15_000),
    html.Div(id="idle-dummy", style={"display": "none"}),
    html.Div(id="app-view-container"),
], id="app-root")


# Client-side idle timer: records the last real user interaction and redirects
# to /logout once the idle threshold is exceeded. Runs on every 15s tick.
IDLE_TIMEOUT_MS = 5 * 60 * 1000  # 5 minutes
clientside_callback(
    f"""
    function(n_intervals) {{
        var TIMEOUT = {IDLE_TIMEOUT_MS};
        if (!window.__prismIdleInit) {{
            window.__prismIdleInit = true;
            window.__prismLastActivity = Date.now();
            var bump = function() {{ window.__prismLastActivity = Date.now(); }};
            ['mousemove','mousedown','keydown','scroll','touchstart','click'].forEach(
                function(ev) {{ window.addEventListener(ev, bump, {{passive: true}}); }}
            );
        }}
        if (Date.now() - (window.__prismLastActivity || Date.now()) > TIMEOUT) {{
            window.location.href = '/logout';
        }}
        return '';
    }}
    """,
    Output("idle-dummy", "children"),
    Input("idle-check", "n_intervals"),
)


# ── Login view ───────────────────────────────────────────────────────────────

def render_login_page(message=None):
    """Render the application-login gate shown before any app content."""
    alert = ""
    if message:
        alert = dbc.Alert(message, color="danger", className="py-2",
                          style={"fontSize": ".85rem"})
    return html.Div([
        _aws_topnav(breadcrumb=[("Sign in", True)], show_logout=False),
        html.Div([
            html.Div([
                html.Img(src="/assets/prism-icon-light.svg",
                         style={"width": "64px", "height": "64px",
                                "display": "block", "margin": "0 auto 1rem auto",
                                "borderRadius": "12px"}),
                html.H4("Sign in to Prism", className="mb-1",
                        style={"fontWeight": "700", "textAlign": "center"}),
                html.P("Enter your application credentials to continue.",
                       className="text-muted mb-3", style={"fontSize": ".85rem",
                                                           "textAlign": "center"}),
                html.Div(id="login-alert", children=alert),
                html.Label("Username", className="form-label"),
                dbc.Input(id="login-username", placeholder="username",
                          className="mb-2", autoFocus=True),
                html.Label("Password", className="form-label"),
                dbc.Input(id="login-password", type="password",
                          placeholder="password", className="mb-3",
                          n_submit=0),
                html.Button("Sign in", id="login-btn",
                            className="btn btn-warning w-100",
                            style={"fontWeight": "700", "padding": ".5rem",
                                   "borderRadius": "6px", "border": "none"}),
            ], style={"maxWidth": "380px", "margin": "8vh auto", "padding": "2rem",
                      "background": "#fff", "borderRadius": "10px",
                      "boxShadow": "0 2px 16px rgba(0,0,0,.08)"}),
        ], style={"minHeight": "80vh"}),
    ])


# ── View router ──────────────────────────────────────────────────────────────
@callback(
    Output("app-view-container", "children"),
    Input("app-view", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=False,
)
def cb_route_view(view, conn_data):
    # Gate everything behind the login: no valid session → show sign-in only.
    if not _is_authenticated():
        return render_login_page()
    if view == "fleet":
        fleet_page = render_fleet_landing()
        return html.Div([
            _aws_topnav(
                breadcrumb=[("Clusters", True)],
                show_region_selector=True,
            ),
            fleet_page,
        ])
    if view == "explore" and conn_data:
        # If tunnel mode, verify tunnel is still alive — auto-reconnect if dead
        if conn_data.get("conn_mode") == "tunnel" and not is_tunnel_active():
            from ssh_tunnel import ensure_tunnel
            if not ensure_tunnel():
                # Tunnel dead, show fleet page instead
                fleet_page = render_fleet_landing()
                return html.Div([
                    _aws_topnav(
                        breadcrumb=[("Clusters", True)],
                        show_region_selector=True,
                    ),
                    fleet_page,
                ])
        # Auto-trigger snapshot on first load
        cluster_id = conn_data.get("cluster_id")
        region = conn_data.get("region", "us-east-1")
        conn_str = get_connection_string() or ""
        start_snapshot_if_needed(cluster_id, region, conn_str,
                                db_count=len(conn_data.get("databases", [])))
        return html.Div([
            _aws_topnav(
                breadcrumb=[
                    ("Clusters", False),
                    (cluster_id or "Cluster", True),
                ],
                region=region,
                cluster_id=cluster_id,
            ),
            html.Div([
                _build_sidebar(conn_data),
                html.Div(main_content, id="main-wrap", className="main-wrap"),
            ], className="app-shell"),
        ])
    # Default: show fleet page
    fleet_page = render_fleet_landing()
    return html.Div([
        _aws_topnav(
            breadcrumb=[("Clusters", True)],
            show_region_selector=True,
        ),
        fleet_page,
    ])


# ── Login: verify credentials and open a server-side session ─────────────────
@callback(
    Output("app-view-container", "children", allow_duplicate=True),
    Output("login-alert", "children"),
    Input("login-btn", "n_clicks"),
    Input("login-password", "n_submit"),
    State("login-username", "value"),
    State("login-password", "value"),
    prevent_initial_call=True,
)
def cb_login(n_clicks, n_submit, username, password):
    if not (n_clicks or n_submit):
        return no_update, no_update
    if auth_store.verify_user(username, password):
        flask.session["authenticated"] = True
        flask.session["username"] = (username or "").strip()
        # Render the fleet landing directly so no page reload is required.
        fleet_page = render_fleet_landing()
        return html.Div([
            _aws_topnav(breadcrumb=[("Clusters", True)], show_region_selector=True),
            fleet_page,
        ]), no_update
    return no_update, dbc.Alert("Invalid username or password.", color="danger",
                                className="py-2", style={"fontSize": ".85rem"})


# ── Auth page: discover clusters ─────────────────────────────────────────────
@callback(
    Output("auth-clusters-store", "data", allow_duplicate=True),
    Output("auth-cluster-dropdown", "options", allow_duplicate=True),
    Output("auth-discover-status", "children", allow_duplicate=True),
    Input("auth-discover-btn", "n_clicks"),
    State("auth-region", "value"),
    prevent_initial_call=True,
)
def cb_auth_discover(n, region):
    from aws_discovery import discover_documentdb_clusters
    clusters = discover_documentdb_clusters(region)
    if not clusters:
        return None, [], dbc.Alert("No clusters found", color="warning", className="py-1 mb-0",
                                    style={"fontSize": ".82rem"})
    opts = [{"label": f"{c['cluster_id']} ({c['status']})", "value": i} for i, c in enumerate(clusters)]
    return clusters, opts, dbc.Alert(f"Found {len(clusters)} cluster(s)", color="success",
                                      className="py-1 mb-0", style={"fontSize": ".82rem"})


# ── Auth page: show cluster info ─────────────────────────────────────────────
@callback(
    Output("auth-cluster-info", "children"),
    Input("auth-cluster-dropdown", "value"),
    State("auth-clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_auth_cluster_info(idx, clusters):
    if idx is None or not clusters:
        return ""
    c = clusters[idx]
    return html.Small(
        f"Endpoint: {c['endpoint']}:{c['port']}  \u00b7  Log: {c['log_group']}",
        className="text-muted", style={"fontSize": ".78rem", "wordBreak": "break-all"},
    )


# ── Auth page: toggle SSH/Direct fields ──────────────────────────────────────
@callback(
    Output("auth-ssh-fields", "style"),
    Output("auth-direct-fields", "style"),
    Input("auth-conn-mode", "value"),
    prevent_initial_call=True,
)
def cb_auth_mode_toggle(mode):
    if mode == "tunnel":
        return {"display": "block"}, {"display": "none"}
    return {"display": "none"}, {"display": "block"}


# ── Auth page: in Direct mode, hide the manual URI when a fleet cluster is
# selected (URI is built internally from host + user/pass). Show it only when
# no cluster is selected (connecting to a host not in the fleet). ─────────────
@callback(
    Output("auth-manual-uri-wrap", "style"),
    Output("auth-direct-hint", "style"),
    Input("auth-cluster-dropdown", "value"),
    Input("auth-conn-mode", "value"),
    prevent_initial_call=True,
)
def cb_auth_direct_uri_toggle(cluster_idx, mode):
    # Only relevant in direct mode; tunnel mode hides the whole direct block.
    cluster_selected = cluster_idx is not None
    if mode == "direct" and cluster_selected:
        # Host known → build URI internally; hide manual URI, show hint.
        return {"display": "none"}, {"display": "block"}
    # No cluster selected (or not direct) → allow manual URI, hide hint.
    return {"display": "block"}, {"display": "none"}


# ── Auth page: Connect button ────────────────────────────────────────────────
@callback(
    Output("app-view", "data", allow_duplicate=True),
    Output("app-conn-data", "data", allow_duplicate=True),
    Output("auth-connect-status", "children"),
    Input("auth-connect-btn", "n_clicks"),
    State("auth-clusters-store", "data"),
    State("auth-cluster-dropdown", "value"),
    State("auth-conn-mode", "value"),
    State("auth-bastion", "value"),
    State("auth-ssh-user", "value"),
    State("auth-ssh-key", "value"),
    State("auth-username", "value"),
    State("auth-password", "value"),
    State("auth-tls", "value"),
    State("auth-manual-conn", "value"),
    State("auth-region", "value"),
    prevent_initial_call=True,
)
def cb_auth_connect(n, clusters, cidx, conn_mode, bastion, ssh_user, ssh_key,
                    username, password, tls, manual_conn, region):

    use_tls = "ssl" in (tls or [])

    # Resolve cluster from store — cidx can be int index or None
    cluster = None
    if clusters and cidx is not None:
        try:
            cluster = clusters[int(cidx)]
        except (IndexError, ValueError, TypeError):
            pass

    # ── Direct mode ──────────────────────────────────────────────────────
    if conn_mode == "direct":
        # Option B: when a fleet cluster is selected, its host is known — build
        # the URI internally from host + username/password and ignore any
        # (hidden, possibly stale) manual URI field. The manual URI is only used
        # when NO cluster is selected (connecting to a host not in the fleet).
        if cluster and username and password:
            from aws_discovery import build_connection_string
            conn_str = build_connection_string(cluster, username, password, use_tls)
            log_group = cluster["log_group"]
            cluster_id = cluster["cluster_id"]
        elif manual_conn:
            conn_str = manual_conn
            log_group = cluster["log_group"] if cluster else ""
            cluster_id = cluster["cluster_id"] if cluster else None
        else:
            missing = []
            if not cluster and not manual_conn:
                missing.append("cluster (or manual connection string)")
            if not username:
                missing.append("username")
            if not password:
                missing.append("password")
            return no_update, no_update, dbc.Alert(
                f"Missing: {', '.join(missing)}",
                color="warning", style={"fontSize": ".85rem"})

    # ── Tunnel mode ──────────────────────────────────────────────────────
    elif conn_mode == "tunnel":
        missing = []
        if not cluster:
            missing.append("cluster")
        if not bastion:
            missing.append("bastion host")
        if not ssh_key:
            missing.append("SSH key path")
        if not username:
            missing.append("username")
        if not password:
            missing.append("password")
        if missing:
            return no_update, no_update, dbc.Alert(
                f"Missing: {', '.join(missing)}",
                color="warning", style={"fontSize": ".85rem"})
        from ssh_tunnel import open_tunnel, get_tunnel_connection_string
        result = open_tunnel(bastion, ssh_user or "ec2-user", ssh_key,
                             cluster["endpoint"], cluster["port"],
                             instances=cluster.get("instances", []))
        if not result["ok"]:
            return no_update, no_update, dbc.Alert(
                f"Tunnel failed: {result['error']}", color="danger", style={"fontSize": ".85rem"})
        conn_str = get_tunnel_connection_string(username, password, use_tls)
        from ssh_tunnel import save_credentials
        save_credentials(username, password, use_tls)
        log_group = cluster["log_group"]
        cluster_id = cluster["cluster_id"]
    else:
        return no_update, no_update, dbc.Alert(
            "Select a connection mode", color="warning", style={"fontSize": ".85rem"})

    # Test connection and fetch tree structure.
    # Use a longer selection/connect timeout here (vs the 5s used elsewhere) so
    # the *real* failure surfaces on the auth page. With a 5s selection timeout
    # the driver often reports the ambiguous "No servers found yet ... rtt: None"
    # with no captured error, because server selection aborts before the TCP/TLS
    # attempt finishes. Matching connectTimeoutMS to the selection window lets a
    # blocked port (security group) or slow handshake produce an actionable
    # message such as "connection timed out" or a TLS error.
    try:
        import pymongo
        client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=20000,
                                     connectTimeoutMS=20000, appname='DocDB-Prism')
        client.admin.command("ping")
        _all_dbs = [d for d in client.list_databases()
                   if d["name"] not in ("admin", "local")]
        db_names = [d["name"] for d in _all_dbs]
        db_sizes = {d["name"]: d.get("sizeOnDisk", 0) for d in _all_dbs}

        # Fetch only first N databases immediately; rest loaded in background
        from prism_cfg import get_config
        _ll_cfg = get_config().get("database_tree", {})
        initial_batch = _ll_cfg.get("initial_batch", 10)

        tree_data = {}
        for db_name in db_names[:initial_batch]:
            tree_data[db_name] = _fetch_db_tree(client, db_name)

        client.close()

        # Kick off background thread for remaining databases
        remaining = db_names[initial_batch:]
        if remaining:
            delay = _ll_cfg.get("delay_between_seconds", 2)
            _start_lazy_load(conn_str, remaining, delay)
    except Exception as e:
        return no_update, no_update, dbc.Alert(
            f"Connection failed: {e}", color="danger", style={"fontSize": ".85rem"})

    # Success — store connection data (credentials kept server-side only)
    set_connection_string(conn_str)

    conn_data = {
        "cluster_id": cluster_id,
        "log_group": log_group,
        "region": region,
        "databases": db_names,
        "db_sizes": db_sizes,
        "conn_mode": conn_mode,
        "tree_data": tree_data,
    }

    # Start autonomous agent (conn_str passed server-side only)
    try:
        from agent_orchestrator import start_agent
        agent_data = dict(conn_data)
        agent_data["connection_string"] = conn_str
        start_agent(agent_data)
    except Exception as e:
        logging.getLogger(__name__).warning("Agent start failed: %s", e)

    return "explore", conn_data, no_update


# ── Connect button: disable on click, re-enable when status updates ──────────
# Two SEPARATE clientside callbacks with DIFFERENT triggers avoid the
# "two callbacks writing same output from same trigger" conflict.

# 1) On click → disable + "Connecting..."
clientside_callback(
    """
    function(n_clicks) {
        if (!n_clicks) { return window.dash_clientside.no_update; }
        return [true, "Connecting..."];
    }
    """,
    Output("auth-connect-btn", "disabled", allow_duplicate=True),
    Output("auth-connect-btn", "children", allow_duplicate=True),
    Input("auth-connect-btn", "n_clicks"),
    prevent_initial_call=True,
)

# 2) When the status div changes (server responded with an error) → re-enable
clientside_callback(
    """
    function(status_children) {
        if (status_children) {
            return [false, "Connect & Load Databases"];
        }
        return window.dash_clientside.no_update;
    }
    """,
    Output("auth-connect-btn", "disabled", allow_duplicate=True),
    Output("auth-connect-btn", "children", allow_duplicate=True),
    Input("auth-connect-status", "children"),
    prevent_initial_call=True,
)


# ── Theme toggle on auth page (reuse same clientside pattern) ────────────────



# ═══════════════════════════════════════════════════════════════════════════════
# CALLBACKS
# ═══════════════════════════════════════════════════════════════════════════════

# ── Sidebar collapse toggle ──────────────────────────────────────────────────
@callback(
    Output("sidebar-collapsed", "data"),
    Output("sidebar-container", "className"),
    Output("main-wrap", "className"),
    Input("sidebar-toggle", "n_clicks"),
    State("sidebar-collapsed", "data"),
    prevent_initial_call=True,
)
def cb_sidebar_toggle(n, collapsed):
    new = not collapsed
    sb_cls = "sidebar sidebar--collapsed" if new else "sidebar"
    main_cls = "main-wrap main-wrap--expanded" if new else "main-wrap"
    return new, sb_cls, main_cls


# ── Stepper + accordion state ────────────────────────────────────────────────
@callback(
    Output("stepper-area", "children"),
    Output("sec-cluster", "className"),
    Output("sec-creds", "className"),
    Output("sec-db", "className"),
    Output("conn-status-pill", "children"),
    Input("cluster-dropdown", "value"),
    Input("db-dropdown", "value"),
    Input("sec-cluster-header", "n_clicks"),
    Input("sec-creds-header", "n_clicks"),
    Input("sec-db-header", "n_clicks"),
    State("username-input", "value"),
    State("password-input", "value"),
    State("manual-conn", "value"),
    State("clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_stepper(cluster_val, db_val,
              click_cluster, click_creds, click_db,
              user, pwd, manual, clusters):
    triggered = ctx.triggered_id

    # Determine completion state
    has_cluster = cluster_val is not None or bool(manual)
    has_creds = bool(manual) or (bool(user) and bool(pwd))
    has_db = bool(db_val)

    # Determine which section to open
    if triggered == "sec-cluster-header":
        active = 1
    elif triggered == "sec-creds-header":
        active = 2
    elif triggered == "sec-db-header":
        active = 3
    else:
        # Auto-advance to the next incomplete step
        if not has_cluster:
            active = 1
        elif not has_creds:
            active = 2
        else:
            active = 3

    # Stepper step number (highest completed + 1)
    step = 1
    if has_cluster:
        step = 2
    if has_cluster and has_creds:
        step = 3
    if has_cluster and has_creds and has_db:
        step = 4  # all done

    sec_base = "sb-section"
    sec_cls = [sec_base, sec_base, sec_base]
    sec_cls[active - 1] += " sb-section--active"

    # Connection pill
    if has_cluster and has_creds:
        cluster_name = ""
        if clusters and cluster_val is not None:
            cluster_name = clusters[cluster_val].get("cluster_id", "cluster")
        elif manual:
            cluster_name = "manual"
        pill = html.Div([
            html.Div(className="conn-dot"),
            html.Span(f"Connected · {cluster_name}" if cluster_name else "Ready"),
        ], className="conn-pill conn-pill--on")
    else:
        pill = html.Div([
            html.Div(className="conn-dot"),
            html.Span("Not connected"),
        ], className="conn-pill conn-pill--off")

    return _stepper(min(step, 4)), sec_cls[0], sec_cls[1], sec_cls[2], pill


# ── Discover clusters ────────────────────────────────────────────────────────
@callback(
    Output("clusters-store", "data"),
    Output("cluster-dropdown", "options"),
    Output("discover-status", "children"),
    Input("discover-btn", "n_clicks"),
    State("region-dropdown", "value"),
    prevent_initial_call=True,
)
def cb_discover(n, region):
    clusters = discover_documentdb_clusters(region)
    if not clusters:
        return None, [], dbc.Alert("No clusters found", color="warning", className="py-1 mb-0", style={"fontSize": ".82rem"})
    opts = [{"label": f"{c['cluster_id']} ({c['status']})", "value": i} for i, c in enumerate(clusters)]
    return clusters, opts, dbc.Alert(f"✅ Found {len(clusters)} cluster(s)", color="success", className="py-1 mb-0", style={"fontSize": ".82rem"})


# ── Cluster info ─────────────────────────────────────────────────────────────
@callback(
    Output("cluster-info", "children"),
    Output("manual-log", "value"),
    Input("cluster-dropdown", "value"),
    State("clusters-store", "data"),
    prevent_initial_call=True,
)
def cb_cluster_info(idx, clusters):
    if idx is None or not clusters:
        return "", no_update
    c = clusters[idx]
    info = html.Div([
        html.Div(f"📡  {c['endpoint']}:{c['port']}", style={"fontSize": ".73rem", "color": "#8d99a8", "wordBreak": "break-all"}),
        html.Div(f"📋  {c['log_group']}", style={"fontSize": ".73rem", "color": "#8d99a8", "wordBreak": "break-all"}),
    ], className="mt-1")
    return info, c['log_group']


# ── Load databases (auto-triggered when connection details are ready) ────────
# ── Connection mode toggle (show/hide SSH vs Direct fields) ──────────────────
@callback(
    Output("ssh-fields", "style"),
    Output("direct-fields", "style"),
    Input("conn-mode", "value"),
    prevent_initial_call=True,
)
def cb_conn_mode_toggle(mode):
    if mode == "tunnel":
        return {"display": "block"}, {"display": "none"}
    return {"display": "none"}, {"display": "block"}


# (tunnel close moved to back-to-connect button)


def _build_conn(clusters, cidx, user, pwd, ssl, manual, conn_mode="direct",
                bastion=None, ssh_user=None, key_path=None):
    """Build connection string. In tunnel mode, opens SSH tunnel first."""
    use_tls = "ssl" in (ssl or [])

    if conn_mode == "tunnel":
        if not (clusters and cidx is not None and user and pwd and bastion and key_path):
            return None, None, None, None
        c = clusters[cidx]
        # Open tunnel
        result = open_tunnel(bastion, ssh_user or "ec2-user", key_path,
                             c["endpoint"], c["port"])
        if not result["ok"]:
            return None, None, None, result["error"]
        conn = get_tunnel_connection_string(user, pwd, use_tls)
        return conn, c["log_group"], c["cluster_id"], None

    # Direct mode
    if manual:
        return manual, None, None, None
    if clusters and cidx is not None and user and pwd:
        c = clusters[cidx]
        return build_connection_string(c, user, pwd, use_tls), c["log_group"], c["cluster_id"], None
    return None, None, None, None


@callback(
    Output("db-dropdown", "options"),
    Output("load-db-status", "children"),
    Output("tunnel-status", "children", allow_duplicate=True),
    Input("load-db-btn", "n_clicks"),
    State("cluster-dropdown", "value"),
    State("username-input", "value"),
    State("password-input", "value"),
    State("ssl-check", "value"),
    State("manual-conn", "value"),
    State("clusters-store", "data"),
    State("conn-mode", "value"),
    State("bastion-host", "value"),
    State("ssh-user", "value"),
    State("ssh-key-path", "value"),
    prevent_initial_call=True,
)
def cb_load_dbs(n_clicks, cidx, user, pwd, ssl, manual, clusters, conn_mode, bastion, ssh_user, key_path):
    conn, _, _, tunnel_err = _build_conn(clusters, cidx, user, pwd, ssl, manual,
                                          conn_mode, bastion, ssh_user, key_path)
    if tunnel_err:
        return [], dbc.Alert(f"Tunnel error: {tunnel_err[:80]}", color="danger", className="py-1 mb-0", style={"fontSize": ".78rem"}), dbc.Alert(f"❌ {tunnel_err[:60]}", color="danger", className="py-1 mb-0 mt-1", style={"fontSize": ".75rem"})
    if not conn:
        return [], "", ""
    # Avoid connecting with partial credentials (fires on every keystroke)
    if not manual and (not user or not pwd or cidx is None):
        return [], ""
    try:
        dbs = get_cluster_databases(conn)
    except Exception:
        dbs = []
    if not dbs:
        return [], dbc.Alert("No databases found", color="warning", className="py-1 mb-0", style={"fontSize": ".82rem"}), ""
    tunnel_msg = ""
    if conn_mode == "tunnel" and is_tunnel_active():
        tunnel_msg = html.Small("✅ Tunnel active", style={"color": "#037f0c", "fontSize": ".75rem"})
    return [{"label": d, "value": d} for d in dbs], dbc.Alert(f"✅ {len(dbs)} database(s)", color="success", className="py-1 mb-0", style={"fontSize": ".82rem"}), tunnel_msg


# ── Start analysis (kick off background thread) ─────────────────────────────
@callback(
    Output("analysis-trigger", "data"),
    Output("progress-interval", "disabled"),
    Output("progress-area", "children", allow_duplicate=True),
    Input("analyze-btn", "n_clicks"),
    State("db-dropdown", "value"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_start_analysis(n, db_name, conn_data):
    if not db_name:
        return no_update, True, dbc.Alert("⚠️ Select a database first", color="warning")
    if not conn_data:
        return no_update, True, dbc.Alert("⚠️ Not connected. Go back and connect first.", color="warning")
    conn = get_connection_string()
    if not conn:
        return no_update, True, dbc.Alert("⚠️ Connection lost. Reconnect to cluster.", color="warning")
    log_group = conn_data.get("log_group", "")
    cluster_id = conn_data.get("cluster_id")
    region = conn_data.get("region", "us-east-1")

    # Reset progress
    with _lock:
        _progress.update(phase="Initializing", detail="Starting analysis…", pct=0,
                         running=True, done=False, error=None, result=None,
                         conn_meta={"connection_string": conn, "log_group": log_group,
                                    "region": region, "cluster_id": cluster_id,
                                    "database": db_name, "databases": conn_data.get("databases", [])})

    def _run():
        try:
            result = get_documentdb_stats({"connection_string": conn, "database_name": db_name},
                                          progress_callback=_progress_callback)
            with _lock:
                if "error" in result:
                    _progress.update(running=False, done=True, error=result["error"])
                else:
                    _progress.update(running=False, done=True, result=result)
        except Exception as e:
            with _lock:
                _progress.update(running=False, done=True, error=str(e))

    threading.Thread(target=_run, daemon=True).start()

    # Return initial progress UI and enable interval
    return (n or 1), False, _build_progress_ui("Initializing", "Starting analysis…", 0)


def _build_progress_ui(phase, detail, pct):
    return html.Div([
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Div(f"🔍  {phase}", className="progress-phase"),
                    html.Div(detail, className="progress-detail mt-1"),
                ]),
                dbc.Col(html.Div(f"{pct}%", className="progress-pct"), width="auto"),
            ], align="center"),
            dbc.Progress(value=pct, color="warning", className="mt-2",
                         style={"height": "6px", "borderRadius": "3px", "background": "rgba(255,255,255,.1)"}),
        ], className="progress-panel"),
    ], className="mb-3")


# ── Poll progress ────────────────────────────────────────────────────────────
@callback(
    Output("progress-area", "children"),
    Output("progress-interval", "disabled", allow_duplicate=True),
    Output("analysis-store", "data", allow_duplicate=True),
    Output("conn-store", "data", allow_duplicate=True),
    Output("sidebar-analysis-status", "children"),
    Input("progress-interval", "n_intervals"),
    State("analysis-store", "data"),
    prevent_initial_call=True,
)
def cb_poll_progress(n, existing_analysis):
    with _lock:
        phase = _progress["phase"]
        detail = _progress["detail"]
        pct = _progress["pct"]
        done = _progress["done"]
        error = _progress["error"]
        result = _progress["result"]
        conn_meta = _progress["conn_meta"]

    if not done:
        return _build_progress_ui(phase, detail, pct), False, no_update, no_update, no_update

    # Done — clear main progress area, show compact status in sidebar
    if error:
        sb_msg = dbc.Alert(f"❌ {error[:80]}", color="danger", className="py-1 mb-0 mt-2", style={"fontSize": ".78rem"}, duration=5000)
        return "", True, no_update, no_update, sb_msg

    sb_msg = dbc.Alert("✅ Analysis complete", color="success", className="py-1 mb-0 mt-2", style={"fontSize": ".78rem"}, duration=3000)
    # Merge new results with existing analysis data (preserve previously analyzed databases)
    merged = dict(existing_analysis) if existing_analysis else {}
    if isinstance(result, dict):
        merged.update(result)
    return "", True, merged, conn_meta, sb_msg


# ── Nav items definition (built from registry) ──────────────────────────────
def _build_nav_items():
    """Build NAV_ITEMS list from the tab registry."""
    items = []
    # DB-level tabs first
    for tid, info in get_db_tabs().items():
        items.append((tid, info["icon"], info["label"], "db"))
    # Cluster / tool-level tabs
    for tid, info in get_tabs().items():
        items.append((tid, info["icon"], info["label"], info["group"]))
    return items


def _build_vnav(active="overview"):
    """Build the vertical icon navigation rail."""
    nav_items = _build_nav_items()
    items = []
    prev_group = None
    for nid, icon, tip, group in nav_items:
        if prev_group and group != prev_group:
            items.append(html.Div(className="vnav-divider"))
        prev_group = group
        cls = "vnav-btn vnav-btn--active" if nid == active else "vnav-btn"
        items.append(
            html.Div(
                html.Div(icon, className="vnav-icon"),
                className=cls, id={"type": "vnav-btn", "index": nid},
                title=tip,
            )
        )
    return html.Div(items, className="vnav")


# ── Post-analysis: update tree state and mark db as analyzed ───────────────
@callback(
    Output("tree-selection", "data", allow_duplicate=True),
    Output("active-nav", "data", allow_duplicate=True),
    Output("tree-expanded", "data", allow_duplicate=True),
    Output("app-conn-data", "data", allow_duplicate=True),
    Input("analysis-store", "data"),
    State("conn-store", "data"),
    State("tree-expanded", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_post_analysis(analysis_data, conn_meta, expanded, conn_data):
    import logging as _log
    _log.getLogger("app").info("cb_post_analysis: conn_meta_db=%s, has_analysis=%s", 
                                conn_meta.get("database") if conn_meta else None,
                                bool(analysis_data))
    if not analysis_data or not conn_meta:
        return no_update, no_update, no_update, no_update

    db = conn_meta.get("database")
    if not db:
        return no_update, no_update, no_update, no_update

    # Set tree selection to the analyzed database
    new_sel = {"level": "database", "db": db, "collection": "", "index": ""}

    # Expand the analyzed database in the tree
    exp = expanded or {"databases": []}
    exp_dbs = list(exp.get("databases", []))
    if db not in exp_dbs:
        exp_dbs.append(db)
    new_expanded = {"databases": exp_dbs}

    # Mark this database as analyzed in conn_data
    updated_conn = dict(conn_data) if conn_data else {}
    analyzed = updated_conn.get("analyzed_dbs", [])
    if db not in analyzed:
        analyzed.append(db)
    updated_conn["analyzed_dbs"] = analyzed

    return new_sel, "overview", new_expanded, updated_conn


# ── Vertical nav click ───────────────────────────────────────────────────────
@callback(
    Output("active-nav", "data"),
    Input({"type": "vnav-btn", "index": dash.ALL}, "n_clicks"),
    State("active-nav", "data"),
    prevent_initial_call=True,
)
def cb_nav_click(clicks, current):
    if not ctx.triggered_id:
        return current
    return ctx.triggered_id["index"]


# ── Render active panel ──────────────────────────────────────────────────────
@callback(
    Output("results-area", "children"),
    Input("active-nav", "data"),
    Input("tree-selection", "data"),
    Input("analysis-store", "data"),
    State("conn-store", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=False,
)
def cb_render_panel(active, tree_sel, analysis_data, conn_meta, conn_data):
    # Guard: no connection yet (auth page)
    if not conn_data and not conn_meta:
        return no_update
    triggered = ctx.triggered_id if ctx.triggered_id else ""
    import logging as _log
    _log.getLogger("app").info("cb_render_panel: triggered=%s, active=%s, tree_sel=%s", triggered, active, tree_sel)

    # If triggered by analysis-store completing, force database-level view
    if triggered == "analysis-store" and conn_meta:
        db = conn_meta.get("database", "")
        meta = conn_meta
        conn_str = meta.get("connection_string", "")
        log_group = meta.get("log_group", "")
        region = meta.get("region", "us-east-1")
        if db and analysis_data and db in analysis_data:
            return html.Div(render_database_dashboard(analysis_data, db, conn_str, log_group, region, "overview", conn_meta.get("cluster_id", "")))

    sel_level = (tree_sel or {}).get("level", "database")
    sel_db = (tree_sel or {}).get("db", "")
    sel_coll = (tree_sel or {}).get("collection", "")
    sel_idx = (tree_sel or {}).get("index", "")

    # Use conn_meta if available (after analysis), else conn_data (before analysis)
    meta = conn_meta or conn_data or {}
    db = sel_db or meta.get("database", "")
    conn_str = meta.get("connection_string", "") or get_connection_string() or ""
    log_group = meta.get("log_group", "")
    region = meta.get("region", "us-east-1")
    cluster_id = meta.get("cluster_id")

    # ── Code Review panel ───────────────────────────────────────────────
    if active == "code_review":
        cluster_id = meta.get("cluster_id")
        return html.Div(render_code_review_panel(cluster_id, region))

    # ── Cluster-level tabs (from registry) ──────────────────────────────
    # Arg adapters: each cluster renderer has a unique signature.
    _CLUSTER_ARGS = {
        "snapshot":  lambda r: (cluster_id, region, conn_str),
        "wa":        lambda r: (cluster_id, region, conn_str),
        "wa2":       lambda r: (cluster_id, region, conn_str),
        "activity":  lambda r: (conn_str, cluster_id, region),
        "chat":      lambda r: (conn_str, sel_db or db, sel_coll, cluster_id, region),
        "cluster_slowquery": lambda r: (cluster_id, region, log_group),
        "rec_actions": lambda r: (cluster_id, region, conn_str),
    }
    cluster_tabs = get_tabs()
    if active in cluster_tabs:
        adapter = _CLUSTER_ARGS.get(active)
        renderer = cluster_tabs[active]["render"]
        return html.Div(renderer(*adapter(renderer)) if adapter else renderer())

    # ── Collection / Index detail ────────────────────────────────────────
    if active == "collection-detail" and sel_coll and analysis_data and sel_db:
        return html.Div(render_collection_detail(analysis_data, sel_db, sel_coll))
    if active == "index-detail" and sel_idx and analysis_data and sel_db:
        return html.Div(render_index_detail(analysis_data, sel_db, sel_coll, sel_idx))

    # ── Database-level tabs (from registry) ──────────────────────────────
    db_tabs = get_db_tabs()
    if active in db_tabs:
        # Auto-fetch from agent if not in analysis-store yet
        if db and (not analysis_data or db not in analysis_data):
            try:
                from agent_orchestrator import get_db_analysis_results
                agent_results = get_db_analysis_results()
                if agent_results and db in agent_results:
                    analysis_data = dict(analysis_data or {})
                    analysis_data[db] = agent_results[db]
            except Exception:
                pass
        if not analysis_data or not db:
            return html.Div(dbc.Alert("Select a database and run analysis first.", color="info"))
        return html.Div(render_database_dashboard(analysis_data, db, conn_str, log_group, region, active, cluster_id))

    # ── Tree selection fallback ──────────────────────────────────────────
    # Auto-fetch from agent for tree selection if not in analysis-store
    if sel_level in ("database", "collection", "index") and sel_db and (not analysis_data or sel_db not in analysis_data):
        try:
            from agent_orchestrator import get_db_analysis_results
            agent_results = get_db_analysis_results()
            if agent_results and sel_db in agent_results:
                analysis_data = dict(analysis_data or {})
                analysis_data[sel_db] = agent_results[sel_db]
        except Exception:
            pass
    if sel_level == "index" and sel_idx and analysis_data:
        return html.Div(render_index_detail(analysis_data, sel_db, sel_coll, sel_idx))
    if sel_level == "collection" and sel_coll and analysis_data:
        return html.Div(render_collection_detail(analysis_data, sel_db, sel_coll))
    if sel_level == "database" and analysis_data and db:
        return html.Div(render_database_dashboard(analysis_data, db, conn_str, log_group, region, "overview"))

    # Default
    return html.Div(dbc.Alert("Select a database or tool from the sidebar.", color="info"))






# ── Auto-populate databases from auth connection ─────────────────────────────
@callback(
    Output("db-dropdown", "options", allow_duplicate=True),
    Output("db-dropdown", "value"),
    Input("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_auto_populate_dbs(conn_data):
    if not conn_data or "databases" not in conn_data:
        return no_update, no_update
    dbs = conn_data["databases"]
    opts = [{"label": d, "value": d} for d in dbs]
    # Auto-select first database
    first = dbs[0] if dbs else None
    return opts, first


# ── Back button: return to auth screen ───────────────────────────────────────
# (will be added in Phase 2 with the tree sidebar)

# ── Back to connect screen ───────────────────────────────────────────────────
@callback(
    Output("app-view", "data", allow_duplicate=True),
    Input("back-to-connect-btn", "n_clicks"),
    prevent_initial_call=True,
)
def cb_back_to_connect(n):
    if not n:
        return no_update
    from ssh_tunnel import close_tunnel
    close_tunnel()
    # Stop agent and reset snapshot
    try:
        from agent_orchestrator import stop_agent
        stop_agent()
    except Exception:
        pass
    reset_snapshot()
    return "fleet"



# ── Lazy load: enable interval when connecting ───────────────────────────
@callback(
    Output("lazy-load-interval", "disabled"),
    Input("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_lazy_load_start(conn_data):
    """Enable lazy-load polling when a new connection is established."""
    if not conn_data:
        return True
    with _lazy_lock:
        has_pending = not _lazy_load["done"] or bool(_lazy_load["pending"])
    return not has_pending  # disabled=False means enabled


# ── Lazy load: poll background thread and merge into conn_data ────────────
@callback(
    Output("app-conn-data", "data", allow_duplicate=True),
    Output("lazy-load-interval", "disabled", allow_duplicate=True),
    Input("lazy-load-interval", "n_intervals"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_lazy_load_poll(n, conn_data):
    """Merge lazily-loaded database tree data into conn_data."""
    if not conn_data:
        return no_update, True

    with _lazy_lock:
        new_data = dict(_lazy_load["pending"])
        _lazy_load["pending"] = {}
        done = _lazy_load["done"]

    if not new_data:
        return no_update, done  # disabled=True when done

    updated = dict(conn_data)
    tree = dict(updated.get("tree_data", {}))
    tree.update(new_data)
    updated["tree_data"] = tree
    return updated, done


# ── Tree: render on data changes ─────────────────────────────────────────────
@callback(
    Output("sidebar-tree", "children"),
    Input("analysis-store", "data"),
    Input("app-conn-data", "data"),
    Input("tree-selection", "data"),
    Input("tree-expanded", "data"),
    Input("sidebar-refresh-btn", "n_clicks"),
    prevent_initial_call=False,
)
def cb_render_tree(analysis_data, conn_data, selection, expanded, _refresh):
    if not conn_data:
        return ""
    # On refresh click, merge latest agent results
    if ctx.triggered_id == "sidebar-refresh-btn":
        try:
            from agent_orchestrator import get_db_analysis_results
            agent_results = get_db_analysis_results()
            if agent_results:
                analysis_data = dict(analysis_data or {})
                for db_name, db_result in agent_results.items():
                    if db_name not in analysis_data:
                        analysis_data[db_name] = db_result
                analyzed = list(conn_data.get("analyzed_dbs", []))
                for db_name in agent_results:
                    if db_name not in analyzed:
                        analyzed.append(db_name)
                conn_data = dict(conn_data)
                conn_data["analyzed_dbs"] = analyzed
        except Exception:
            pass
    return build_cluster_tree(conn_data, analysis_data, selection, expanded)


# ── Tree: click handler ───────────────────────────────────────────────────
@callback(
    Output("tree-selection", "data", allow_duplicate=True),
    Output("tree-expanded", "data", allow_duplicate=True),
    Output("db-dropdown", "value", allow_duplicate=True),
    Output("active-nav", "data", allow_duplicate=True),
    Input({"type": "tree-click", "action": dash.ALL, "level": dash.ALL,
           "db": dash.ALL, "coll": dash.ALL, "idx": dash.ALL}, "n_clicks"),
    State("tree-selection", "data"),
    State("tree-expanded", "data"),
    prevent_initial_call=True,
)
def cb_tree_click(clicks, sel, expanded):
    if not ctx.triggered_id:
        return no_update, no_update, no_update, no_update
    # Only act on actual clicks, not initial renders of new nodes
    if not any(c for c in clicks if c):
        return no_update, no_update, no_update, no_update

    tid = ctx.triggered_id
    action = tid["action"]
    db = tid["db"]
    coll = tid["coll"]
    idx = tid["idx"]

    exp_dbs = list(expanded.get("databases", []))

    if action == "select":
        # Cluster root
        return {"level": "cluster", "db": "", "collection": "", "index": ""}, no_update, no_update, "snapshot"

    elif action == "toggle-db":
        if db in exp_dbs:
            exp_dbs.remove(db)
        else:
            exp_dbs.append(db)
        new_sel = {"level": "database", "db": db, "collection": "", "index": ""}
        return new_sel, {"databases": exp_dbs}, db, "overview"

    elif action == "show-more-dbs":
        from prism_cfg import get_config
        _tree_cfg = get_config().get("sidebar_tree", {})
        PAGE_SIZE = _tree_cfg.get("page_size", 10)
        MAX_PAGES_BEFORE_WARN = _tree_cfg.get("max_pages_before_warn", 4)
        cur_limit = expanded.get("db_limit", PAGE_SIZE)
        pages_shown = cur_limit // PAGE_SIZE
        if pages_shown >= MAX_PAGES_BEFORE_WARN:
            new_limit = 999999  # show all
        else:
            new_limit = cur_limit + PAGE_SIZE
        new_exp = dict(expanded)
        new_exp["db_limit"] = new_limit
        return no_update, new_exp, no_update, no_update

    elif action == "select-coll":
        new_sel = {"level": "collection", "db": db, "collection": coll, "index": ""}
        if db not in exp_dbs:
            exp_dbs.append(db)
        return new_sel, {"databases": exp_dbs}, db, "collection-detail"

    elif action == "select-idx":
        new_sel = {"level": "index", "db": db, "collection": coll, "index": idx}
        if db not in exp_dbs:
            exp_dbs.append(db)
        return new_sel, {"databases": exp_dbs}, db, "index-detail"

    return no_update, no_update, no_update, no_update


# ── Tree: tool click ─────────────────────────────────────────────────────
@callback(
    Output("active-nav", "data", allow_duplicate=True),
    Input({"type": "tree-tool", "tool": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def cb_tree_tool_click(clicks):
    if not ctx.triggered_id:
        return no_update
    return ctx.triggered_id["tool"]


# ── Tree: highlight active cluster tool node ──────────────────────────────
_CLUSTER_TOOLS = ["snapshot", "wa", "activity", "cluster_slowquery", "rec_actions", "chat"]

@callback(
    *[Output({"type": "tree-tool", "tool": t}, "className") for t in _CLUSTER_TOOLS],
    Input("active-nav", "data"),
    prevent_initial_call=False,
)
def cb_highlight_cluster_tool(active):
    return [
        "tree-node tree-node--active" if active == t else "tree-node"
        for t in _CLUSTER_TOOLS
    ]


# ── Database tab click (vertical nav inside database dashboard) ─────────────
@callback(
    Output("active-nav", "data", allow_duplicate=True),
    Input({"type": "db-tab", "tab": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def cb_db_tab_click(clicks):
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update
    return ctx.triggered_id["tab"]


# ── Index chip click (navigate to Indexes tab) ─────────────────────────────
@callback(
    Output("active-nav", "data", allow_duplicate=True),
    Input({"type": "idx-chip", "category": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def cb_idx_chip_click(clicks):
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update
    return "indexes"


# ═══ AGENT POLLING & STATUS ══════════════════════════════════════════════════

@callback(
    Output("agent-status-bar", "children"),
    Output("agent-store", "data"),
    Output("agent-poll", "disabled"),
    Output("agent-poll", "interval", allow_duplicate=True),
    Input("agent-poll", "n_intervals"),
    prevent_initial_call=True,
)
def cb_agent_poll(n):
    try:
        from agent_orchestrator import get_agent_state
        state = get_agent_state()
    except Exception:
        return no_update, no_update, True, no_update

    status = state.get("status", "idle")
    if status == "idle":
        return "", None, True, no_update

    # Build status bar
    modules = state.get("modules", {})
    done_count = sum(1 for m in modules.values() if m.get("status") == "done")
    total = len(modules)
    pct = state.get("pct", 0)
    insights_count = len(state.get("correlated_insights", []))

    if status == "complete":
        bar = html.Div([
            html.Div([
                html.Span("🤖 Agent Analysis  ", style={"fontWeight": "700", "fontSize": ".82rem"}),
                html.Span("✅ Complete", style={"color": "var(--accent-green)", "fontSize": ".8rem", "fontWeight": "600"}),
            ]),
            html.Small(f"{done_count} modules · {insights_count} insights",
                       style={"fontSize": ".72rem", "color": "var(--text-muted)"}),
            dbc.Button("📄 Download Report", id="agent-download-btn", color="warning",
                       size="sm", className="mt-1 w-100",
                       style={"fontSize": ".72rem", "fontWeight": "600"}),
        ], style={"padding": ".5rem", "borderRadius": "8px",
                  "border": "1px solid var(--border-default)", "background": "var(--bg-surface-alt)"})
        disable_poll = False  # Keep polling (slower) so tree updates with analyzed badges
    else:
        # Running state
        phase_icons = {"observing": "👁️", "reasoning": "🧠", "running": "▶"}
        icon = phase_icons.get(status, "⏳")
        current_mod = state.get("current_module", "")
        reasoning = state.get("current_reasoning", "")

        module_items = []
        for mname, mdata in modules.items():
            ms = mdata.get("status", "pending")
            if ms == "done":
                module_items.append(html.Div(f"✅ {mname}", style={"fontSize": ".7rem", "color": "var(--accent-green)"}))
            elif ms == "running":
                module_items.append(html.Div(f"▶ {mname}", style={"fontSize": ".7rem", "fontWeight": "700", "color": "var(--color-primary)"}))
            elif ms == "skipped":
                module_items.append(html.Div(f"⏭️ {mname}", style={"fontSize": ".7rem", "color": "var(--text-muted)"}))

        bar = html.Div([
            html.Div([
                html.Span("🤖 Agent Analysis", style={"fontWeight": "700", "fontSize": ".82rem"}),
            ]),
            html.Div(f"{icon} {status.title()}: {current_mod}",
                     style={"fontSize": ".76rem", "fontWeight": "600"}),
            html.Div(reasoning, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                                        "fontStyle": "italic", "marginBottom": ".3rem"}) if reasoning else None,
            html.Div([
                html.Div(style={"width": f"{pct}%", "height": "4px",
                                 "background": "var(--color-primary)", "borderRadius": "2px",
                                 "transition": "width .3s"}),
            ], style={"background": "var(--border-default)", "borderRadius": "2px",
                      "height": "4px", "marginBottom": ".3rem"}),
            html.Small(f"{pct}%  {done_count}/{total}",
                       style={"fontSize": ".68rem", "color": "var(--text-muted)"}),
            html.Div(module_items, style={"marginTop": ".3rem"}),
        ], style={"padding": ".5rem", "borderRadius": "8px",
                  "border": "1px solid var(--border-default)", "background": "var(--bg-surface-alt)"})
        disable_poll = False

    poll_ms = 30000 if status == "complete" else 6000
    return bar, state, disable_poll, poll_ms


# Enable agent polling when entering explore view
@callback(
    Output("agent-poll", "disabled", allow_duplicate=True),
    Input("app-view", "data"),
    prevent_initial_call=True,
)
def cb_enable_agent_poll(view):
    if view == "explore":
        return False
    return True


# Merge agent db_analysis into analysis-store + mark analyzed_dbs in conn-data
@callback(
    Output("analysis-store", "data", allow_duplicate=True),
    Output("app-conn-data", "data", allow_duplicate=True),
    Input("agent-store", "data"),
    State("analysis-store", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_merge_agent_analysis(agent_data, current_analysis, conn_data):
    if not agent_data:
        return no_update, no_update
    # Check if db_analysis has new data via version counter
    version = agent_data.get("_db_analysis_version", 0)
    if version == 0:
        return no_update, no_update
    try:
        from agent_orchestrator import get_db_analysis_results
        new_data = get_db_analysis_results()
    except Exception:
        return no_update, no_update
    if not new_data:
        return no_update, no_update
    merged = dict(current_analysis or {})
    analysis_changed = False
    newly_analyzed = []
    for db_name, db_result in new_data.items():
        # Add NEW databases only. Do NOT compare values to "refresh" existing
        # entries: the agent's in-memory results contain datetime/tuple values
        # that do not survive the analysis-store JSON round-trip, so a value
        # comparison is ALWAYS unequal and would rewrite the entire store on
        # every 4s agent poll — triggering full panel + tree re-renders and
        # making the whole app crawl. (Re-analyze freshness is handled by
        # clearing the store at re-analyze time, not here.)
        if db_name not in merged:
            merged[db_name] = db_result
            analysis_changed = True
            newly_analyzed.append(db_name)
    # Update analyzed_dbs in conn_data for checkmarks
    conn_changed = False
    updated_conn = no_update
    if conn_data:
        updated_conn = dict(conn_data)
        analyzed = list(updated_conn.get("analyzed_dbs", []))
        for db_name in new_data:
            if db_name not in analyzed:
                analyzed.append(db_name)
                conn_changed = True
        if conn_changed:
            updated_conn["analyzed_dbs"] = analyzed
    return (merged if analysis_changed else no_update,
            updated_conn if conn_changed else no_update)


@callback(
    Output("agent-report-download", "data"),
    Input("agent-download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def cb_download_report(n):
    if not n:
        return no_update
    try:
        from agent_orchestrator import get_agent_state
        state = get_agent_state()
        report = state.get("report", {})
        if report.get("ready") and report.get("markdown"):
            return dict(content=report["markdown"], filename="prism_report.md")
    except Exception:
        pass
    return no_update


def _detect_public_ip():
    """Return the EC2 public IPv4 via IMDSv2, or None if not on EC2/unavailable."""
    try:
        # Use requests (http/https only) rather than urllib.urlopen, which also
        # accepts file:// and other schemes (Bandit B310). The IMDS URLs are
        # fixed constants, so this is purely defensive/hardening.
        import requests
        token = requests.put(
            "http://169.254.169.254/latest/api/token",
            headers={"X-aws-ec2-metadata-token-ttl-seconds": "60"},
            timeout=2).text
        ip = requests.get(
            "http://169.254.169.254/latest/meta-data/public-ipv4",
            headers={"X-aws-ec2-metadata-token": token},
            timeout=2).text
        return ip.strip() or None
    except Exception:
        return None


if __name__ == "__main__":
    import os as _os
    _debug = _os.environ.get("PRISM_DEBUG", "0") == "1"
    # Bind 0.0.0.0 by default so the app is reachable on EC2 via its public IP
    # (the public IP itself is NAT'd and cannot be bound directly). Override with
    # PRISM_HOST=127.0.0.1 to restrict to localhost. Network exposure is
    # controlled by the EC2 security group (inbound 8501 restricted) and, for
    # team access, an ALB + Cognito front end — see SETUP.md / EC2 docs.
    _host = _os.environ.get("PRISM_HOST", "0.0.0.0")  # nosec B104 - intentional; access controlled by security group / ALB
    _port = int(_os.environ.get("PRISM_PORT", "8501"))
    _pub = _detect_public_ip()
    if _pub:
        print(f"Prism binding {_host}:{_port} — open http://{_pub}:{_port}")
        print(f"  \u21b3 Verify the EC2 security group allows inbound TCP {_port} from your IP.")
    else:
        print(f"Prism binding {_host}:{_port} — open http://localhost:{_port}")
    app.run(debug=_debug, host=_host, port=_port)
