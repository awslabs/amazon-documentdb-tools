"""Tab registry — tabs self-register so app.py and database_dashboard.py
never need editing when a new feature is added.

Usage (in any tab file, at module level):
    from tabs.registry import register_tab, register_db_tab

    # Cluster-level tool (sidebar)
    register_tab("snapshot", "📋", "Cluster Snapshot", "cluster", render_cluster_snapshot)

    # Database-level tab (inside database dashboard)
    register_db_tab("overview", "📊", "Overview", render_database_overview)
"""

# ── Cluster / tool-level tabs (sidebar nav) ──────────────────────────────────
_TABS = {}


def register_tab(nav_id, icon, label, group, renderer):
    """Register a cluster-level or tool-level tab.

    Args:
        nav_id:   unique string id used in active-nav store
        icon:     emoji shown in sidebar / vnav
        label:    tooltip / display name
        group:    "cluster" | "db" | "ai" (controls separator placement)
        renderer: callable that returns a Dash layout
    """
    _TABS[nav_id] = {
        "icon": icon,
        "label": label,
        "group": group,
        "render": renderer,
    }


def get_tabs():
    """Return all registered cluster/tool tabs as {nav_id: info}."""
    return dict(_TABS)


# ── Database-level tabs (inside database dashboard) ──────────────────────────
_DB_TABS = {}
_DB_TAB_ORDER = []


def register_db_tab(tab_id, icon, label, renderer):
    """Register a database-level analysis tab.

    Args:
        tab_id:   unique string id (e.g. "overview", "indexes")
        icon:     emoji for the vertical nav
        label:    display name / tooltip
        renderer: callable — signature varies per tab, see database_dashboard.py
    """
    _DB_TABS[tab_id] = {
        "icon": icon,
        "label": label,
        "render": renderer,
    }
    if tab_id not in _DB_TAB_ORDER:
        _DB_TAB_ORDER.append(tab_id)


def get_db_tabs():
    """Return registered DB tabs as {tab_id: info} preserving insertion order."""
    return {k: _DB_TABS[k] for k in _DB_TAB_ORDER if k in _DB_TABS}
