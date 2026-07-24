"""Collection Detail — collection stats + full index deep-dive in one page.
   Replaces both collection_detail and index_detail — clicking a collection
   or index in the tree both land here.
"""
import dash_bootstrap_components as dbc
from dash import html
from tabs.ui_helpers import code_block, info_tip

_TH = {"padding": ".35rem .6rem", "fontSize": ".68rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".5px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_TH_L = {**_TH, "textAlign": "left"}
_TH_R = {**_TH, "textAlign": "right"}
_TD   = {"padding": ".3rem .6rem", "fontSize": ".82rem", "color": "var(--text-body)",
         "borderBottom": "1px solid var(--border-default)", "verticalAlign": "middle"}
_TD_L = {**_TD, "textAlign": "left"}
_TD_R = {**_TD, "textAlign": "right"}
_TD_MONO = {**_TD_L, "fontFamily": "monospace", "fontSize": ".78rem", "fontWeight": "600"}
_TABLE = {"width": "100%", "borderCollapse": "collapse",
          "border": "1px solid var(--border-default)"}


def _fmt(b):
    if b >= 1024**3: return f"{b/(1024**3):.2f} GB"
    if b >= 1024**2: return f"{b/(1024**2):.1f} MB"
    if b >= 1024:    return f"{b/1024:.0f} KB"
    return f"{b} B"


def _ks(fields):
    return "||".join(f"{f}||{d}" for f, d in fields.items())


def _kpi(label, value, color=None):
    return html.Div([
        html.Div(str(value), style={"fontSize": ".88rem", "fontWeight": "700",
                                    "lineHeight": "1",
                                    **({"color": color} if color else {})}),
        html.Div(label, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                                "textTransform": "uppercase", "letterSpacing": ".3px"}),
    ], style={"textAlign": "center", "flex": "1", "minWidth": "0"})


def render_collection_detail(analysis_data, db_name, coll_name):
    if not analysis_data or db_name not in analysis_data:
        return dbc.Alert("No analysis data. Run analysis first.", color="info")

    db = analysis_data[db_name]
    cd = db.get(coll_name)
    if not cd or not isinstance(cd, dict) or "error" in cd:
        return dbc.Alert(f"No data for collection '{coll_name}'.", color="info")

    # ── Collection stats
    doc_count    = cd.get("count", 0)
    on_disk      = cd.get("storageSize", 0)
    uncompressed = cd.get("size", 0)
    avg_doc      = cd.get("avgObjSize", 0)
    ratio        = (uncompressed / on_disk) if on_disk > 0 else 1.0
    indexes      = cd.get("indexes", [])
    bloat_pct    = cd.get("unusedStorageSize", {}).get("unusedPercent", 0.0)
    comp_enabled = cd.get("compression", {}).get("enabled", False)

    # ── KPI bar
    kpi_bar = html.Div([
        _kpi("Documents",    f"{doc_count:,}"),
        _kpi("On-Disk",      _fmt(on_disk)),
        _kpi("Uncompressed", _fmt(uncompressed)),
        _kpi("Ratio",        f"{ratio:.1f}x",
             "var(--accent-green)" if ratio > 1.5 else None),
        _kpi("Avg Doc",      _fmt(avg_doc)),
        _kpi("Indexes",      len(indexes)),
        _kpi("Bloat",        f"{bloat_pct:.0f}%",
             "var(--accent-red)" if bloat_pct > 20 else None),
        _kpi("Compression",  "Enabled" if comp_enabled else "Disabled",
             "var(--accent-green)" if comp_enabled else "var(--accent-red)"),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # ── Build index rows with full detail
    drop_cmds    = []
    reindex_cmds = []
    rows = []

    for idx in indexes:
        name     = idx["name"]
        fields   = idx.get("fields", {})
        is_sys   = name in ("_id", "_id_")
        u        = idx.get("usage", {})
        c        = idx.get("cardinality", {})
        b        = idx.get("bloat", {})

        ops         = u.get("ops_count", 0)
        since       = u.get("since_date") or u.get("since") or "—"
        is_unused   = u.get("potential_unused", False)
        card_pct    = c.get("percentage", 0)
        is_low_card = c.get("is_low", False)
        distinct    = c.get("distinct_values", 0)
        sampled     = c.get("total_sampled", 0)
        bloat_i_pct = b.get("unusedPercent", 0.0)
        bloat_i_b   = b.get("unusedBytes", 0)

        # Redundancy
        tks = _ks(fields)
        redundant_with = [i["name"] for i in indexes
                          if i["name"] != name and i["name"] != "_id_"
                          and _ks(i.get("fields", {})).startswith(tks + "||")]
        is_redundant = bool(redundant_with)

        issues = sum([is_unused, is_low_card, is_redundant, bloat_i_pct > 20])

        if is_sys:
            usage_text, usage_color = "System", "var(--text-muted)"
            health, h_color = "—", "var(--text-muted)"
        elif is_unused:
            usage_text, usage_color = "Unused", "var(--accent-red)"
            health, h_color = "Drop", "var(--accent-red)"
            drop_cmds.append(f'db.{coll_name}.dropIndex("{name}")')
        elif ops < 100:
            usage_text, usage_color = "Low", "var(--color-warning)"
            health, h_color = "Review" if issues else "Healthy", \
                              "var(--color-warning)" if issues else "var(--accent-green)"
        else:
            usage_text, usage_color = "Active", "var(--accent-green)"
            health, h_color = "Action" if issues >= 2 else \
                              "Review" if issues == 1 else "Healthy", \
                              "var(--accent-red)" if issues >= 2 else \
                              "var(--color-warning)" if issues == 1 else "var(--accent-green)"

        if is_redundant and not is_unused:
            drop_cmds.append(f'// Redundant: db.{coll_name}.dropIndex("{name}")')
        if bloat_i_pct > 20:
            reindex_cmds.append(
                f'db.runCommand({{ reIndex: "{coll_name}", index: "{name}" }})')

        row_style = {"background": "rgba(217,21,21,.03)"} if issues >= 2 else {}

        rows.append(html.Tr([
            html.Td(name, style=_TD_MONO),
            html.Td(", ".join(fields.keys()),
                    style={**_TD_L, "fontSize": ".76rem", "color": "var(--text-muted)"}),
            html.Td(f"{ops:,}" if not is_sys else "—", style=_TD_R),
            html.Td(usage_text,
                    style={**_TD_L, "color": usage_color, "fontWeight": "600"}),
            html.Td(f"{card_pct:.2f}%" if card_pct and not is_sys else "—",
                    style={**_TD_R,
                           **({"color": "var(--color-warning)", "fontWeight": "600"}
                              if is_low_card else {})}),
            html.Td(f"{distinct:,}" if distinct and not is_sys else "—", style=_TD_R),
            html.Td(f"{sampled:,}" if sampled and not is_sys else "—", style=_TD_R),
            html.Td(", ".join(redundant_with) if redundant_with else "—",
                    style={**_TD_L, "fontSize": ".76rem",
                           "color": "var(--color-warning)" if redundant_with
                           else "var(--text-muted)"}),
            html.Td(f"{bloat_i_pct:.0f}%" if bloat_i_pct > 0 else "—",
                    style={**_TD_R,
                           **({"color": "var(--accent-red)", "fontWeight": "600"}
                              if bloat_i_pct > 20 else {})}),
            html.Td(health, style={**_TD_L, "color": h_color, "fontWeight": "600"}),
        ], style=row_style))

    idx_table = html.Table([
        html.Thead(html.Tr([
            html.Th("Index",      style=_TH_L),
            html.Th("Fields",     style=_TH_L),
            html.Th("Ops",        style=_TH_R),
            html.Th("Usage",      style=_TH_L),
            html.Th("Cardinality",style=_TH_R),
            html.Th("Distinct",   style=_TH_R),
            html.Th("Sampled",    style=_TH_R),
            html.Th("Redundant",  style=_TH_L),
            html.Th("Bloat",      style=_TH_R),
            html.Th("Health",     style=_TH_L),
        ])),
        html.Tbody(rows),
    ], style=_TABLE)

    # ── Assemble page
    children = [
        html.Div([
            html.Span(f"{db_name} / ", style={"color": "var(--text-muted)",
                                               "fontSize": ".78rem"}),
            html.Span(coll_name, style={"fontWeight": "700", "fontSize": ".78rem"}),
        ], style={"marginBottom": ".25rem"}),
        html.Div("Collection Overview", className="section-title"),
        kpi_bar,
        html.Div("Indexes", className="section-title"),
        idx_table if rows else dbc.Alert("No indexes found.", color="info"),
    ]

    if drop_cmds:
        children += [
            dbc.Alert(f"🗑️  {len([c for c in drop_cmds if not c.startswith('//')])}"
                      f" index(es) recommended for removal",
                      color="warning", className="mt-3"),
            code_block("\n".join(drop_cmds)),
        ]
    if reindex_cmds:
        children += [
            dbc.Alert(f"🔧  {len(reindex_cmds)} index(es) with high bloat — rebuild recommended",
                      color="warning", className="mt-2"),
            code_block("\n".join(reindex_cmds)),
        ]

    return html.Div(children)
