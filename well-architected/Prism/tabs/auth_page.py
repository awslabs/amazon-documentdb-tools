"""Auth/Connection page — full-screen landing for cluster connection."""
import dash_bootstrap_components as dbc
from dash import html, dcc
from aws_discovery import get_aws_regions


def render_auth_page(conn_data=None):
    """Full-screen auth page. Accepts conn_data for fleet pre-selection."""
    fleet_region = (conn_data or {}).get("fleet_region", "us-east-1")
    fleet_clusters = (conn_data or {}).get("fleet_clusters") or []
    fleet_selected = (conn_data or {}).get("fleet_selected_cluster")

    pre_opts = []
    pre_value = None
    pre_status = ""
    if fleet_clusters and fleet_selected:
        pre_opts = [{"label": f"{c['cluster_id']} ({c['status']})", "value": i}
                    for i, c in enumerate(fleet_clusters)]
        for i, c in enumerate(fleet_clusters):
            if c["cluster_id"] == fleet_selected:
                pre_value = i
                break
        pre_status = dbc.Alert("Cluster pre-selected from fleet overview",
                               color="success", className="py-1 mb-0",
                               style={"fontSize": ".82rem"})
    return html.Div([
        # Header bar
        html.Div([
            dbc.Row([
                dbc.Col([
                    html.Span("\u2699\ufe0f", style={"fontSize": "1.4rem", "marginRight": ".5rem"}),
                    html.Strong("Amazon DocumentDB Agent",
                                style={"fontSize": "1.2rem", "letterSpacing": ".5px"}),
                ], className="d-flex align-items-center"),

            ], align="center"),
        ], className="auth-header"),

        # Main form area
        html.Div([
            # Title
            html.Div([
                html.H3("Connect to your cluster", className="auth-title"),
                html.P("Configure your connection to get started with analysis",
                       className="auth-subtitle"),
            ], className="text-center mb-4"),

            # Form card
            dbc.Card(dbc.CardBody([
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
                                value=fleet_region, clearable=False,
                            ),
                        ], md=4),
                        dbc.Col([
                            html.Label("Cluster", className="form-label"),
                            dbc.Row([
                                dbc.Col(
                                    dbc.Button("Discover", id="auth-discover-btn",
                                               color="secondary", size="sm", outline=True,
                                               className="w-100"),
                                    width=4,
                                ),
                                dbc.Col(
                                    dcc.Dropdown(id="auth-cluster-dropdown",
                                                 placeholder="Select cluster...",
                                                 style={"fontSize": ".88rem"}),
                                    width=8,
                                ),
                            ], className="g-2"),
                            html.Div(id="auth-discover-status", children=pre_status, className="mt-1"),
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

                    # SSH fields
                    html.Div(id="auth-ssh-fields", children=[
                        dbc.Row([
                            dbc.Col([
                                html.Label("Bastion Host IP", className="form-label"),
                                dbc.Input(id="auth-bastion", placeholder="10.0.1.50",
                                          size="sm"),
                            ], md=4),
                            dbc.Col([
                                html.Label("SSH User", className="form-label"),
                                dbc.Input(id="auth-ssh-user", value="ec2-user",
                                          size="sm"),
                            ], md=4),
                            dbc.Col([
                                html.Label("SSH Key Path (.pem)", className="form-label"),
                                dbc.Input(id="auth-ssh-key", placeholder="/path/to/key.pem",
                                          size="sm"),
                            ], md=4),
                        ], className="mb-2"),
                    ]),

                    # Direct fields
                    html.Div(id="auth-direct-fields", children=[
                        # When a cluster is selected from the fleet, its host is
                        # already known — the connection URI is built internally
                        # from host + username/password (Step 3). The manual URI
                        # field below is only shown when NO cluster is selected
                        # (advanced: connecting to a host not in the fleet).
                        html.Div(id="auth-manual-uri-wrap", children=[
                            html.Label("Connection String (manual host)",
                                       className="form-label"),
                            dbc.Input(id="auth-manual-conn",
                                      placeholder="mongodb://user:pass@host:27017/?...",
                                      size="sm"),
                            html.Small(
                                "Only needed for a host not in the fleet. "
                                "For a selected cluster, leave blank — the URI is "
                                "built from the host plus your username and password.",
                                className="text-muted",
                                style={"fontSize": ".72rem"}),
                        ]),
                        html.Div(id="auth-direct-hint", children=[
                            html.Small(
                                "Connecting to the selected cluster — just enter "
                                "your username and password below.",
                                className="text-muted",
                                style={"fontSize": ".72rem"}),
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
                            dbc.Input(id="auth-username", placeholder="docdb-user",
                                      size="sm"),
                        ], md=4),
                        dbc.Col([
                            html.Label("Password", className="form-label"),
                            dbc.Input(id="auth-password", type="password",
                                      placeholder="Password", size="sm"),
                        ], md=4),
                        dbc.Col([
                            html.Label("\u00a0", className="form-label"),  # spacer
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
                    dbc.Button("Connect & Load Databases", id="auth-connect-btn",
                               color="warning", className="w-100",
                               style={"fontWeight": "700", "fontSize": "1rem", "padding": ".6rem"}),
                    html.Div(id="auth-connect-status", className="mt-2"),
                ]),

            ]), className="auth-card"),

            # Feature cards (compact, below form)
            html.Div([
                html.H6("What you can analyze", className="text-center mt-4 mb-3",
                         style={"fontWeight": "600", "color": "var(--text-muted)"}),
                dbc.Row([
                    _feature_chip("\u26a1", "Live Activity"),
                    _feature_chip("\U0001f5c2\ufe0f", "Index Analysis"),
                    _feature_chip("\U0001f40c", "Slow Queries"),
                    _feature_chip("\U0001f5dc\ufe0f", "Compression"),
                    _feature_chip("\U0001f525", "Heat Maps"),
                    _feature_chip("\U0001f4cb", "Cluster Snapshot"),
                    _feature_chip("\U0001f3d7\ufe0f", "Well-Architected"),
                    _feature_chip("\U0001f4be", "Storage Optimizer"),
                ], className="justify-content-center g-2"),
            ], className="mb-4"),

        ], className="auth-container"),

        # Footer
        html.Div([
            html.Small("Amazon DocumentDB Agent v2.0  \u00b7  Built by Amazon DocumentDB SSA Team",
                       className="text-muted"),
        ], className="text-center", style={"padding": "1rem 0"}),

        # Stores
        dcc.Store(id="auth-clusters-store", data=fleet_clusters if fleet_clusters else None),
    ], className="auth-page")


def _feature_chip(icon, label):
    return dbc.Col(
        html.Div([
            html.Span(icon, style={"marginRight": ".3rem"}),
            html.Span(label, style={"fontSize": ".78rem", "fontWeight": "600"}),
        ], className="auth-feature-chip"),
        width="auto",
    )
