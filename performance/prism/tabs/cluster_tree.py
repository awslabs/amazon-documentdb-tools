"""Cluster Explorer tree — expandable hierarchy of databases, collections, indexes."""
import logging
from dash import html
from prism_cfg import get_config

logger = logging.getLogger(__name__)


def _fmt_size(b):
    """Format bytes to compact human-readable string."""
    if b >= 1024 ** 3:
        return f"{b / 1024 ** 3:.1f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024 ** 2:.0f} MB"
    if b >= 1024:
        return f"{b / 1024:.0f} KB"
    return f"{b} B"


def build_cluster_tree(conn_data, analysis_data=None, selection=None, expanded=None):
    """Build the cluster explorer tree.

    Data priority: analysis_data (full stats) > conn_data["tree_data"] (lightweight) > empty
    """
    if not conn_data:
        return html.Div("No connection", className="text-muted",
                         style={"fontSize": ".82rem", "padding": ".5rem"})

    selection = selection or {}
    expanded = expanded or {"databases": []}
    sel_level = selection.get("level", "cluster")
    sel_db = selection.get("db", "")
    sel_coll = selection.get("collection", "")
    sel_idx = selection.get("index", "")

    cluster_id = conn_data.get("cluster_id", "Cluster")
    all_databases = conn_data.get("databases", [])
    tree_data = conn_data.get("tree_data", {})  # lightweight data from connect
    analyzed_dbs = conn_data.get("analyzed_dbs", [])  # databases that have been analyzed
    db_sizes = conn_data.get("db_sizes", {})  # sizeOnDisk from list_databases()

    # ── Database pagination ──────────────────────────────────────────────
    _tree_cfg = get_config().get("sidebar_tree", {})
    PAGE_SIZE = _tree_cfg.get("page_size", 10)
    MAX_PAGES_BEFORE_WARN = _tree_cfg.get("max_pages_before_warn", 4)
    db_limit = expanded.get("db_limit", PAGE_SIZE)
    total_dbs = len(all_databases)
    databases = all_databases[:db_limit]
    remaining = total_dbs - len(databases)

    items = []

    # ── Cluster root
    root_cls = "tree-node tree-node--root"
    if sel_level == "cluster":
        root_cls += " tree-node--active"
    items.append(html.Div([
        html.Span("\U0001f4e6", className="tree-icon"),
        html.Span(cluster_id, className="tree-label", style={"fontWeight": "700"}),
    ], className=root_cls,
       id={"type": "tree-click", "action": "select", "level": "cluster",
           "db": "", "coll": "", "idx": ""}))

    # ── Databases
    for db_name in databases:
        is_selected = sel_level == "database" and sel_db == db_name

        # Get collection data: prefer analysis_data, fall back to tree_data
        # Get collection data: prefer analysis_data, fall back to tree_data
        has_full_analysis = False
        is_loading = False
        if analysis_data and db_name in analysis_data:
            db_colls = analysis_data[db_name]
            has_full_analysis = True
        elif db_name in tree_data:
            db_colls = tree_data[db_name]
        else:
            db_colls = {}
            is_loading = True  # not yet loaded by background thread

        collections = sorted(k for k, v in db_colls.items()
                             if isinstance(v, dict) and "error" not in v)
        n_colls = len(collections)

        db_cls = "tree-node tree-node--db"
        if is_selected:
            db_cls += " tree-node--active"

        # Status badge: check agent state directly for real-time updates
        is_analyzed = db_name in analyzed_dbs or has_full_analysis
        is_agent_analyzing = False
        try:
            from agent_orchestrator import get_agent_state, get_db_analysis_results
            _ag = get_agent_state()
            # Check if agent has results for this DB
            if not is_analyzed:
                _ag_results = get_db_analysis_results()
                if _ag_results and db_name in _ag_results:
                    is_analyzed = True
            # Check if agent is still active and this DB is in scope but not yet analyzed
            if not is_analyzed:
                agent_active = _ag.get("status") in ("running", "reasoning", "observing")
                if agent_active:
                    scope = _ag.get("analysis_scope", {}).get("in_scope", [])
                    if db_name in scope:
                        is_agent_analyzing = True
        except Exception:
            pass

        # Size label — prefer analysis_data total, fall back to sizeOnDisk from connect
        size_label = None
        if has_full_analysis:
            total_bytes = sum(
                cd.get("storageSize", cd.get("size", 0))
                for cd in db_colls.values()
                if isinstance(cd, dict) and "error" not in cd
            )
            if total_bytes > 0:
                size_label = _fmt_size(total_bytes)
        elif db_name in db_sizes and db_sizes[db_name] > 0:
            size_label = _fmt_size(db_sizes[db_name])

        if is_analyzed:
            status_badge = html.Span("\u2705", style={"fontSize": ".6rem", "marginLeft": ".3rem"})
        elif is_agent_analyzing:
            status_badge = html.Span("\U0001f504", style={"fontSize": ".6rem", "marginLeft": ".3rem",
                                                           "animation": "spin 1.5s linear infinite"})
        else:
            status_badge = None
        loading_badge = html.Span("\u23f3", style={"fontSize": ".6rem", "marginLeft": ".3rem"}) if is_loading and not is_analyzed else None
        items.append(html.Div([
            html.Span("\U0001f5c4\ufe0f", className="tree-icon"),
            html.Span(db_name, className="tree-label"),
            status_badge,
            loading_badge,
            html.Span(size_label, className="tree-count") if size_label else None,
        ], className=db_cls,
           id={"type": "tree-click", "action": "toggle-db", "level": "database",
               "db": db_name, "coll": "", "idx": ""}),
        )



    # ── "Show more databases" button ──────────────────────────────────────
    if remaining > 0:
        pages_shown = db_limit // PAGE_SIZE  # how many pages loaded so far
        if pages_shown >= MAX_PAGES_BEFORE_WARN:
            # After 4 pages (40 dbs), require explicit "show all" with warning
            btn_label = f"Show all {remaining} remaining databases"
            btn_children = [
                html.Span("⚠️", className="tree-icon"),
                html.Span(btn_label, className="tree-label",
                          style={"fontSize": ".75rem", "fontWeight": "600"}),
                html.Div("Large database count — gathering may take time",
                         style={"fontSize": ".68rem", "color": "var(--text-sidebar-muted)",
                                "paddingLeft": "1.6rem", "marginTop": ".1rem"}),
            ]
        else:
            next_batch = min(PAGE_SIZE, remaining)
            btn_label = f"Show {next_batch} more"
            btn_children = [
                html.Span("＋", className="tree-icon", style={"fontSize": ".75rem"}),
                html.Span(btn_label, className="tree-label",
                          style={"fontSize": ".75rem"}),
                html.Span(f"({remaining} remaining)", className="tree-count"),
            ]
        items.append(html.Div(
            btn_children,
            className="tree-node tree-node--more",
            id={"type": "tree-click", "action": "show-more-dbs", "level": "cluster",
                "db": "", "coll": "", "idx": ""},
        ))

    return html.Div(items, className="cluster-tree")
