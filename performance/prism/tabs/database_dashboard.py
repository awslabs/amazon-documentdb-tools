"""Database Dashboard — vertical icon nav with all database-level analysis panels.

Tabs are discovered via the registry (tabs.registry.get_db_tabs).
Adding a new DB-level tab only requires calling register_db_tab() in the
new tab's module — no edits here.
"""
import dash_bootstrap_components as dbc
from dash import html

from tabs.executive_summary import render_executive_summary

# Import all DB-level tab modules so their register_db_tab() calls execute.
# Each module registers itself on import — we don't use the imported names.
import tabs.database_overview      # noqa: F401
import tabs.index_analyzer         # noqa: F401
import tabs.compression_analysis   # noqa: F401

from tabs.registry import get_db_tabs


def render_database_dashboard(analysis_data, db_name, conn_str="", log_group="", region="us-east-1",
                               active_tab="overview", cluster_id=""):
    """Render the full database-level dashboard with vertical icon nav."""
    if not analysis_data or db_name not in analysis_data:
        return dbc.Alert("No analysis data. Select a database and run analysis.", color="info")

    db_tabs = get_db_tabs()

    # Build vertical nav
    nav_items = []
    for tid, info in db_tabs.items():
        cls = "vnav-btn vnav-btn--active" if tid == active_tab else "vnav-btn"
        nav_items.append(
            html.Div(
                html.Div(info["icon"], className="vnav-icon"),
                className=cls,
                id={"type": "db-tab", "tab": tid},
                title=info["label"],
            )
        )

    # Build content for active tab
    tab_content = _render_tab(active_tab, db_tabs, analysis_data, db_name, conn_str, log_group, region, cluster_id)

    return html.Div([
        # Database header
        html.Div([
            html.Span(f"Analysis for ", style={"fontSize": ".92rem", "color": "var(--text-muted)"}),
            html.Span(db_name, style={"fontSize": ".95rem", "fontWeight": "700",
                       "color": "var(--text-body)", "fontFamily": "monospace"}),
        ], className="section-title", style={"marginBottom": ".75rem"}),

        # Summary (inline, no collapsible)
        render_executive_summary(analysis_data, db_name),

        # Vertical nav + content
        html.Div([
            html.Div(nav_items, className="vnav"),
            html.Div(tab_content, className="vnav-content", id="db-tab-content"),
        ], className="vnav-layout"),
    ])


# Signature map: each renderer has a different call signature.
# Keys that need extra args beyond (analysis_data, db_name).
_EXTRA_ARGS = {
    "slowquery":   lambda a, d, cs, lg, r, ci: (d, lg, r),
    "compression": lambda a, d, cs, lg, r, ci: (a, d, cs),
    "heatmap":     lambda a, d, cs, lg, r, ci: (a, cs, d, lg, r),
    "indexes":     lambda a, d, cs, lg, r, ci: (a, d, cs, r, ci),
}


def _render_tab(tab_id, db_tabs, analysis_data, db_name, conn_str, log_group, region, cluster_id=""):
    """Render a single database tab by calling its registered renderer."""
    info = db_tabs.get(tab_id) or db_tabs.get("overview")
    if not info:
        return dbc.Alert("No tabs registered.", color="warning")

    renderer = info["render"]
    tid = tab_id if tab_id in db_tabs else "overview"

    if tid in _EXTRA_ARGS:
        args = _EXTRA_ARGS[tid](analysis_data, db_name, conn_str, log_group, region, cluster_id)
        return renderer(*args)

    # Default: (analysis_data, db_name)
    return renderer(analysis_data, db_name)
