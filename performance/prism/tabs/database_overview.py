"""Database Overview — sleek collection-level summary with inline health indicators."""
import dash_bootstrap_components as dbc
from dash import html

# ── Table styles ─────────────────────────────────────────────────────────────
_TH = {"padding": ".4rem .6rem", "fontSize": ".68rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".5px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap", "textAlign": "right"}
_TH_L = {**_TH, "textAlign": "left"}
_TD = {"padding": ".35rem .6rem", "fontSize": ".82rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "textAlign": "right",
       "verticalAlign": "middle", "wordBreak": "break-word"}
_TD_L = {**_TD, "textAlign": "left"}
_TD_NAME = {**_TD_L, "fontWeight": "600", "fontFamily": "monospace", "fontSize": ".8rem",
            "maxWidth": "420px", "overflow": "hidden", "textOverflow": "ellipsis",
            "whiteSpace": "nowrap"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)",
          "tableLayout": "fixed"}

_MAX_NAME_LEN = 64


def _trunc_name(name):
    """Truncate name to _MAX_NAME_LEN chars; full name shown on hover."""
    if len(name) <= _MAX_NAME_LEN:
        return name
    return html.Span(name[:_MAX_NAME_LEN] + "…", title=name)


def _check_redundancy(target, all_indexes):
    target_ks = _ks(target['fields'])
    return [o['name'] for o in all_indexes
            if o['name'] != target['name'] and o['name'] not in ('_id', '_id_')
            and _ks(o['fields']).startswith(target_ks + "||")]


def _ks(fields):
    return "||".join(f"{f}||{d}" for f, d in fields.items())


def _fmt(b):
    if b >= 1024**3:
        return f"{b / (1024**3):.2f} GB"
    if b >= 1024**2:
        return f"{b / (1024**2):.1f} MB"
    if b >= 1024:
        return f"{b / 1024:.0f} KB"
    return f"{b} B"


def _fmt_count(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(n)


def _health_dot(issues):
    """Tiny colored indicator based on issue count."""
    if issues == 0:
        return ""
    color = "var(--accent-red)" if issues >= 3 else "var(--color-warning)"
    return html.Span(str(issues), style={"color": color, "fontWeight": "700",
                      "fontSize": ".78rem"})


def render_database_overview(analysis_data, database_name):
    if database_name not in analysis_data:
        return dbc.Alert("No data available.", color="info")

    db = analysis_data[database_name]

    # Aggregate metrics
    n_coll = 0
    n_docs = 0
    n_idx = 0
    n_unused = 0
    n_redundant = 0
    n_low_card = 0
    n_bloated = 0
    total_disk = 0
    total_uncomp = 0
    n_no_comp = 0

    for cd in db.values():
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        n_coll += 1
        n_docs += cd.get('count', 0)
        total_disk += cd.get('storageSize', 0)
        total_uncomp += cd.get('size', 0)
        if not cd.get('compression', {}).get('enabled', False):
            n_no_comp += 1
        ia = cd.get('index_analysis', {})
        n_idx += ia.get('total_indexes', 0)
        n_unused += len(ia.get('unused_indexes', []))
        n_low_card += len(ia.get('low_cardinality_indexes', []))
        idxs = cd.get('indexes', [])
        n_redundant += sum(1 for i in idxs if i['name'] not in ('_id', '_id_') and _check_redundancy(i, idxs))
        n_bloated += sum(1 for i in idxs if i['name'] not in ('_id', '_id_')
                         and i.get('bloat', {}).get('unusedPercent', 0) > 20)

    ratio = (total_uncomp / total_disk) if total_disk > 0 else 1.0

    # ── KPI bar: single row, compact ─────────────────────────────────────
    def _kpi(label, value, color=None):
        v_style = {"fontSize": ".88rem", "fontWeight": "700", "lineHeight": "1"}
        if color:
            v_style["color"] = color
        return html.Div([
            html.Div(str(value), style=v_style),
            html.Div(label, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                      "textTransform": "uppercase", "letterSpacing": ".3px"}),
        ], style={"textAlign": "center", "flex": "1", "minWidth": "0"})

    issues_total = n_unused + n_redundant + n_low_card + n_bloated
    kpi_bar = html.Div([
        _kpi("Collections", n_coll),
        _kpi("Documents", _fmt_count(n_docs)),
        _kpi("On-Disk", _fmt(total_disk)),
        _kpi("Uncompressed", _fmt(total_uncomp)),
        _kpi("Compression", f"{ratio:.1f}x", "var(--accent-green)" if ratio > 1.5 else None),
        _kpi("Indexes", n_idx),
        _kpi("Issues", issues_total,
             "var(--accent-red)" if issues_total > 5 else
             "var(--color-warning)" if issues_total > 0 else "var(--accent-green)"),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)", "borderRadius": "8px",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # ── Issue chips (only if issues exist) ───────────────────────────────
    chips = []
    if n_unused:
        chips.append(html.Div(f"{n_unused} unused indexes",
                     id={"type": "idx-chip", "category": "unused"},
                     className="idx-chip idx-chip--clickable",
                     style={"fontSize": ".72rem", "fontWeight": "600", "color": "var(--accent-red)",
                            "padding": ".15rem .5rem", "borderRadius": "4px",
                            "border": "1px solid var(--accent-red)", "marginRight": ".4rem",
                            "cursor": "pointer", "display": "inline-block"}))
    if n_redundant:
        chips.append(html.Div(f"{n_redundant} redundant indexes",
                     id={"type": "idx-chip", "category": "redundant"},
                     className="idx-chip idx-chip--clickable",
                     style={"fontSize": ".72rem", "fontWeight": "600", "color": "var(--color-warning)",
                            "padding": ".15rem .5rem", "borderRadius": "4px",
                            "border": "1px solid var(--color-warning)", "marginRight": ".4rem",
                            "cursor": "pointer", "display": "inline-block"}))
    if n_low_card:
        chips.append(html.Div(f"{n_low_card} low cardinality indexes",
                     id={"type": "idx-chip", "category": "low_cardinality"},
                     className="idx-chip idx-chip--clickable",
                     style={"fontSize": ".72rem", "fontWeight": "600", "color": "var(--accent-blue)",
                            "padding": ".15rem .5rem", "borderRadius": "4px",
                            "border": "1px solid var(--accent-blue)", "marginRight": ".4rem",
                            "cursor": "pointer", "display": "inline-block"}))
    if n_bloated:
        chips.append(html.Div(f"{n_bloated} bloated indexes",
                     id={"type": "idx-chip", "category": "bloated"},
                     className="idx-chip idx-chip--clickable",
                     style={"fontSize": ".72rem", "fontWeight": "600", "color": "var(--color-warning)",
                            "padding": ".15rem .5rem", "borderRadius": "4px",
                            "border": "1px solid var(--color-warning)", "marginRight": ".4rem",
                            "cursor": "pointer", "display": "inline-block"}))
    if n_no_comp:
        chips.append(html.Span(f"{n_no_comp} no compression",
                     style={"fontSize": ".72rem", "fontWeight": "600", "color": "var(--text-muted)",
                            "padding": ".15rem .5rem", "borderRadius": "4px",
                            "border": "1px solid var(--border-default)", "marginRight": ".4rem"}))

    chip_row = html.Div(chips, style={"marginBottom": ".6rem"}) if chips else None

    # ── Collections table ────────────────────────────────────────────────
    header = html.Tr([
        html.Th("Collection", style=_TH_L),
        html.Th("Docs", style=_TH),
        html.Th("On-Disk", style=_TH),
        html.Th("Uncomp.", style=_TH),
        html.Th("Ratio", style=_TH),
        html.Th("Avg Doc", style=_TH),
        html.Th("Idx", style=_TH),
        html.Th("Unused", style=_TH),
        html.Th("Redund.", style=_TH),
        html.Th("Low Card.", style=_TH),
        html.Th("Coll Bloat", style=_TH),
        html.Th("Idx Bloat", style=_TH),
    ])

    rows = []
    sorted_colls = sorted(db.items(),
                          key=lambda x: x[1].get('storageSize', 0) if isinstance(x[1], dict) else 0,
                          reverse=True)

    for name, cd in sorted_colls:
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        ia = cd.get('index_analysis', {})
        ti = ia.get('total_indexes', 0)
        idxs = cd.get('indexes', [])
        unused = len(ia.get('unused_indexes', []))
        redundant = sum(1 for i in idxs if i['name'] not in ('_id', '_id_') and _check_redundancy(i, idxs))
        low_card = len(ia.get('low_cardinality_indexes', []))
        on_disk = cd.get('storageSize', 0)
        uncomp = cd.get('size', 0)
        r = (uncomp / on_disk) if on_disk > 0 else 1.0
        coll_bloat_pct = cd.get('unusedStorageSize', {}).get('unusedPercent', 0)

        # Index bloat — max bloat % across non-system indexes
        non_sys = [i for i in idxs if i['name'] not in ('_id', '_id_')]
        idx_bloat_pct = max((i.get('bloat', {}).get('unusedPercent', 0) for i in non_sys), default=0)

        # Row highlight for collections with issues
        row_style = {}
        if unused + redundant + low_card >= 3:
            row_style = {"background": "rgba(217, 21, 21, 0.03)"}

        coll_bloat_style = {**_TD}
        if coll_bloat_pct > 30:
            coll_bloat_style["color"] = "var(--accent-red)"
            coll_bloat_style["fontWeight"] = "700"
        elif coll_bloat_pct > 15:
            coll_bloat_style["color"] = "var(--color-warning)"

        idx_bloat_style = {**_TD}
        if idx_bloat_pct > 30:
            idx_bloat_style["color"] = "var(--accent-red)"
            idx_bloat_style["fontWeight"] = "700"
        elif idx_bloat_pct > 15:
            idx_bloat_style["color"] = "var(--color-warning)"

        idx_style = {**_TD}
        if ti > 10:
            idx_style["color"] = "var(--color-warning)"
            idx_style["fontWeight"] = "600"

        rows.append(html.Tr([
            html.Td(_trunc_name(name), style=_TD_NAME, title=name),
            html.Td(_fmt_count(cd.get('count', 0)), style=_TD),
            html.Td(_fmt(on_disk), style=_TD),
            html.Td(_fmt(uncomp), style=_TD),
            html.Td(f"{r:.1f}x", style=_TD),
            html.Td(_fmt(round(cd.get('avgObjSize', 0))), style=_TD),
            html.Td(str(ti), style=idx_style),
            html.Td(_health_dot(unused) or "\u2014", style=_TD),
            html.Td(_health_dot(redundant) or "\u2014", style=_TD),
            html.Td(_health_dot(low_card) or "\u2014", style=_TD),
            html.Td(f"{coll_bloat_pct:.0f}%" if coll_bloat_pct > 0 else "\u2014", style=coll_bloat_style),
            html.Td(f"{idx_bloat_pct:.0f}%" if idx_bloat_pct > 0 else "\u2014", style=idx_bloat_style),
        ], style=row_style))

    table = html.Table([html.Thead(header), html.Tbody(rows)], style=_TABLE)

    children = [
        html.Div("Collections Overview", className="section-title"),
        kpi_bar,
    ]
    if chip_row:
        children.append(chip_row)
    children.append(table)

    return html.Div(children)


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_db_tab
register_db_tab("overview", "\U0001f4ca", "Overview", render_database_overview)
