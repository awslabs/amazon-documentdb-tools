"""Index Detail — single index deep-dive, consistent with database_overview layout."""
import dash_bootstrap_components as dbc
from dash import html
from tabs.ui_helpers import code_block, info_tip

_TH = {"padding": ".4rem .6rem", "fontSize": ".68rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".5px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_TD = {"padding": ".35rem .6rem", "fontSize": ".82rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "verticalAlign": "middle"}
_TABLE = {"width": "100%", "borderCollapse": "collapse",
          "border": "1px solid var(--border-default)"}


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


def render_index_detail(analysis_data, db_name, coll_name, idx_name):
    if not analysis_data or db_name not in analysis_data:
        return dbc.Alert("No analysis data. Run analysis first.", color="info")

    db = analysis_data[db_name]
    cd = db.get(coll_name)
    if not cd or not isinstance(cd, dict) or "error" in cd:
        return dbc.Alert(f"No data for '{coll_name}'.", color="info")

    indexes = cd.get("indexes", [])
    idx = next((i for i in indexes if i["name"] == idx_name), None)
    if not idx:
        return dbc.Alert(f"Index '{idx_name}' not found.", color="info")

    fields      = idx.get("fields", {})
    fields_str  = ", ".join(f"{f}: {d}" for f, d in fields.items())
    u           = idx.get("usage", {})
    c           = idx.get("cardinality", {})
    b           = idx.get("bloat", {})
    ops         = u.get("ops_count", 0)
    since       = u.get("since_date") or u.get("since") or "—"
    is_unused   = u.get("potential_unused", False)
    card_pct    = c.get("percentage", 0)
    is_low_card = c.get("is_low", False)
    distinct    = c.get("distinct_values", 0)
    sampled     = c.get("total_sampled", 0)
    bloat_pct   = b.get("unusedPercent", 0.0)
    bloat_bytes = b.get("unusedBytes", 0)

    tks = _ks(fields)
    redundant_with = [i["name"] for i in indexes
                      if i["name"] != idx_name and i["name"] != "_id_"
                      and _ks(i["fields"]).startswith(tks + "||")]
    is_redundant = bool(redundant_with)

    issues = sum([is_unused, is_low_card, is_redundant, bloat_pct > 20])
    if issues == 0:
        health, health_color = "Healthy", "var(--accent-green)"
    elif issues == 1:
        health, health_color = "Review", "var(--color-warning)"
    else:
        health, health_color = "Action needed", "var(--accent-red)"

    if is_unused:
        usage_label, usage_color = "Unused", "var(--accent-red)"
    elif ops < 100:
        usage_label, usage_color = "Low usage", "var(--color-warning)"
    else:
        usage_label, usage_color = "Active", "var(--accent-green)"

    # ── KPI bar (same style as database_overview)
    kpi_bar = html.Div([
        _kpi("Operations",   f"{ops:,}"),
        _kpi("Usage",        usage_label, usage_color),
        _kpi("Cardinality",  f"{card_pct:.2f}%",
             "var(--accent-red)" if is_low_card else None),
        _kpi("Distinct",     f"{distinct:,}"),
        _kpi("Sampled",      f"{sampled:,}"),
        _kpi("Bloat",        f"{bloat_pct:.1f}%",
             "var(--accent-red)" if bloat_pct > 20 else None),
        _kpi("Health",       health, health_color),
        _kpi("Fields",       len(fields)),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # ── Detail table — all properties in one table
    detail_rows = [
        ("Index name",        idx_name),
        ("Collection",        f"{db_name}.{coll_name}"),
        ("Fields",            fields_str),
        ("Operations",        f"{ops:,}"),
        ("Usage status",      usage_label),
        ("Ops counted since", since),
        ("Cardinality",       f"{card_pct:.4f}%"),
        ("Distinct values",   f"{distinct:,}"),
        ("Documents sampled", f"{sampled:,}"),
        ("Low cardinality",   "Yes ⚠️" if is_low_card else "No"),
        ("Unused space",      f"{bloat_bytes/1024:.1f} KB ({bloat_pct:.1f}%)"),
        ("Bloat status",      "High ⚠️" if bloat_pct > 20 else "Healthy"),
        ("Redundant",         f"Yes — covered by: {', '.join(redundant_with)}" if is_redundant else "No"),
        ("Health",            health),
    ]

    table = html.Table([
        html.Thead(html.Tr([
            html.Th("Property", style={**_TH, "textAlign": "left"}),
            html.Th("Value",    style={**_TH, "textAlign": "left"}),
        ])),
        html.Tbody([
            html.Tr([
                html.Td(label, style={**_TD, "fontWeight": "600",
                                      "color": "var(--text-heading)", "width": "220px"}),
                html.Td(value, style=_TD),
            ]) for label, value in detail_rows
        ]),
    ], style=_TABLE)

    children = [
        html.Div([
            html.Span(f"{db_name}.{coll_name} / ",
                      style={"color": "var(--text-muted)", "fontSize": ".78rem"}),
            html.Span(idx_name, style={"fontWeight": "700", "fontSize": ".78rem"}),
        ], style={"marginBottom": ".25rem"}),
        html.Div("Index Detail", className="section-title"),
        kpi_bar,
        html.Div("Properties", className="section-title"),
        table,
    ]

    # ── Actions
    if is_redundant:
        children.append(dbc.Alert([
            html.Strong(f"Redundant — covered by: {', '.join(redundant_with)}"),
            html.Br(),
            html.Small("This index is a prefix of a compound index. Consider dropping it."),
        ], color="warning", className="mt-2"))

    if is_unused:
        children += [
            dbc.Alert(html.Strong("🗑️  Drop this unused index"), color="danger", className="mt-2"),
            code_block(f'db.{coll_name}.dropIndex("{idx_name}")'),
        ]
    if bloat_pct > 20:
        children += [
            dbc.Alert(html.Strong("🔧  Rebuild this index to reclaim space"), color="warning", className="mt-2"),
            code_block(f'db.runCommand({{ reIndex: "{coll_name}", index: "{idx_name}" }})'),
        ]
    if is_low_card and not is_unused:
        children.append(info_tip(
            f"Low cardinality ({card_pct:.2f}%) — consider combining with other fields "
            "in a compound index for better selectivity."
        ))
    if not any([is_unused, bloat_pct > 20, is_low_card, is_redundant]):
        children.append(dbc.Alert("✅  This index is healthy — no action needed.",
                                   color="success", className="mt-2"))

    return html.Div(children)
