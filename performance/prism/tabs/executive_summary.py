"""Executive Summary Dashboard — health score, KPIs, top actions."""
import dash_bootstrap_components as dbc
from dash import html


def _ks(fields):
    return "||".join(f"{f}||{d}" for f, d in fields.items())


def _check_redundancy(target, all_indexes):
    tks = _ks(target['fields'])
    return [o['name'] for o in all_indexes
            if o['name'] != target['name'] and o['name'] not in ('_id', '_id_')
            and _ks(o['fields']).startswith(tks + "||")]


def _compute_metrics(analysis_data, database_name):
    """Extract all key metrics from analysis_data for the summary."""
    db = analysis_data.get(database_name, {})

    n_coll = 0
    n_docs = 0
    n_idx = 0
    n_unused = 0
    n_redundant = 0
    n_low_card = 0
    n_bloated = 0
    total_size = 0
    total_storage = 0
    total_unused_bytes = 0
    n_no_compression = 0

    for coll, cd in db.items():
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        n_coll += 1
        n_docs += cd.get('count', 0)
        total_size += cd.get('storageSize', cd.get('size', 0))
        total_storage += cd.get('storageSize', 0)

        u = cd.get('unusedStorageSize', {})
        total_unused_bytes += u.get('unusedBytes', 0)
        if u.get('unusedPercent', 0) > 20:
            n_bloated += 1

        if not cd.get('compression', {}).get('enabled', False):
            n_no_compression += 1

        ia = cd.get('index_analysis', {})
        n_idx += ia.get('total_indexes', 0)
        n_unused += len(ia.get('unused_indexes', []))
        n_low_card += len(ia.get('low_cardinality_indexes', []))

        idxs = cd.get('indexes', [])
        for idx in idxs:
            if idx['name'] not in ('_id', '_id_') and _check_redundancy(idx, idxs):
                n_redundant += 1

    avg_bloat = 0
    if total_storage > 0:
        avg_bloat = ((total_storage - total_size) / total_storage) * 100

    return {
        'n_coll': n_coll, 'n_docs': n_docs, 'n_idx': n_idx,
        'n_unused': n_unused, 'n_redundant': n_redundant, 'n_low_card': n_low_card,
        'n_bloated': n_bloated,
        'total_size': total_size, 'total_storage': total_storage,
        'total_unused_bytes': total_unused_bytes, 'avg_bloat': max(0, avg_bloat),
        'n_no_compression': n_no_compression,
        'total_issues': n_unused + n_redundant + n_low_card + n_no_compression + n_bloated,
    }


def _health_score(m):
    """Compute 0-100 health score. Start at 100, deduct for issues."""
    score = 100
    # Unused indexes: -4 each, max -20
    score -= min(20, m['n_unused'] * 4)
    # Redundant indexes: -3 each, max -15
    score -= min(15, m['n_redundant'] * 3)
    # Low cardinality: -2 each, max -10
    score -= min(10, m['n_low_card'] * 2)
    # Bloat penalty: up to -25 based on avg bloat %
    score -= min(25, m['avg_bloat'] * 0.5)
    # No compression: -2 each, max -15
    score -= min(15, m['n_no_compression'] * 2)
    # Unused storage > 20% of total: extra -15
    if m['total_storage'] > 0 and (m['total_unused_bytes'] / m['total_storage']) > 0.2:
        score -= 15
    return max(0, min(100, round(score)))


def _score_color(score):
    if score >= 80:
        return '#037f0c'  # green
    if score >= 60:
        return '#8d6605'  # amber
    return '#d91515'      # red


def _score_label(score):
    if score >= 80:
        return 'Healthy'
    if score >= 60:
        return 'Needs Attention'
    return 'Critical'


def _top_actions(m):
    """Generate prioritised recommended actions. Sorted: danger > warning > info."""
    actions = []
    if m['n_unused'] > 0:
        actions.append((1, f"Drop {m['n_unused']} unused index(es)",
                        'Reduces storage overhead and improves write throughput', 'danger'))
    if m['avg_bloat'] > 15:
        actions.append((2, f"Compact collections \u2014 {m['avg_bloat']:.0f}% avg bloat",
                        'Run compact command on high-bloat collections', 'danger'))
    if m['n_redundant'] > 0:
        actions.append((3, f"Review {m['n_redundant']} redundant index(es)",
                        'Covered by compound indexes \u2014 safe to consolidate', 'warning'))
    if m['n_no_compression'] > 0:
        actions.append((4, f"Enable compression on {m['n_no_compression']} collection(s)",
                        'Reduce storage costs with LZ4 or ZSTD compression', 'warning'))
    if m['n_low_card'] > 0:
        actions.append((5, f"Investigate {m['n_low_card']} low cardinality index(es)",
                        'May not be effective for query selectivity', 'info'))
    actions.sort(key=lambda x: x[0])
    return [(a[1], a[2], a[3]) for a in actions[:4]]




def render_executive_summary(analysis_data, database_name):
    """Compact database analysis summary — score, KPIs, category bars, actions."""
    if database_name not in analysis_data:
        return html.Div()

    m = _compute_metrics(analysis_data, database_name)
    score = _health_score(m)
    actions = _top_actions(m)
    score_color = _score_color(score)
    size_mb = m['total_size'] / (1024 ** 2)

    # Category scores
    idx_score = max(0, 100 - min(40, m['n_unused'] * 8) - min(30, m['n_redundant'] * 6))
    bloat_score = max(0, round(100 - min(100, m['avg_bloat'] * 2)))
    compress_score = max(0, round(100 - (m['n_no_compression'] / max(m['n_coll'], 1)) * 100))
    card_score = max(0, 100 - min(50, m['n_low_card'] * 10))

    # ── Row 1: Score + KPIs + Category bars ──────────────────────────────
    # Left: score (top-aligned)
    score_block = html.Div([
        html.Span(f"{score}/100", style={"fontSize": ".9rem", "fontWeight": "700",
                   "color": score_color}),
        html.Span(f" {_score_label(score)}", style={"fontSize": ".75rem", "fontWeight": "500",
                   "color": "var(--text-muted)", "marginLeft": ".3rem"}),
    ], style={"marginBottom": ".5rem"})

    # KPI row (centered)
    kpi_row = html.Div([
        _kpi_item("Collections", m['n_coll']),
        _kpi_item("Documents", f"{m['n_docs']:,}"),
        _kpi_item("Indexes", m['n_idx']),
        _kpi_item("Size", f"{size_mb:.1f} MB"),
        _kpi_item("Issues", m['total_issues'],
                   "var(--accent-red)" if m['total_issues'] > 0 else None),
    ], style={"display": "flex", "gap": "1.2rem", "flexWrap": "wrap",
              "justifyContent": "center"})

    # Center: category bars
    bars = [
        ("Indexes", idx_score),
        ("Bloat", bloat_score),
        ("Compression", compress_score),
        ("Cardinality", card_score),
    ]
    bar_items = []
    for label, sc in bars:
        c = _score_color(sc)
        bar_items.append(html.Div([
            html.Div([
                html.Span(label, style={"fontSize": ".72rem", "color": "var(--text-muted)"}),
                html.Span(f"{sc}%", style={"fontSize": ".72rem", "fontWeight": "700", "color": c}),
            ], style={"display": "flex", "justifyContent": "space-between", "marginBottom": ".1rem"}),
            html.Div(
                html.Div(style={"width": f"{sc}%", "height": "3px", "background": c,
                                 "borderRadius": "2px", "transition": "width .3s"}),
                style={"background": "var(--border-default)", "borderRadius": "2px",
                        "height": "3px", "marginBottom": ".3rem"}),
        ]))
    category_bars = html.Div(bar_items, style={"flex": "1", "minWidth": "0", "padding": "0 .6rem"})

    # Right: recommended actions
    if actions:
        action_items = []
        for title, desc, color in actions:
            dot_color = {"danger": "var(--accent-red)", "warning": "var(--color-warning)",
                         "info": "var(--accent-blue)"}.get(color, "var(--text-muted)")
            action_items.append(html.Div([
                html.Div([
                    html.Span("\u25cf", style={"color": dot_color, "marginRight": ".3rem",
                               "fontSize": ".45rem", "verticalAlign": "middle"}),
                    html.Span(title, style={"fontSize": ".75rem", "fontWeight": "600"}),
                ]),
                html.Div(desc, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                         "paddingLeft": ".75rem"}),
            ], style={"marginBottom": ".3rem"}))
        right = html.Div([
            html.Div("Recommended Actions", style={"fontSize": ".65rem", "fontWeight": "700",
                      "textTransform": "uppercase", "letterSpacing": ".4px",
                      "color": "var(--text-muted)", "marginBottom": ".3rem"}),
            *action_items,
        ], style={"flex": "1", "minWidth": "0"})
    else:
        right = html.Div(
            html.Span("No issues found", style={"fontSize": ".78rem",
                       "color": "var(--accent-green)", "fontWeight": "600"}),
            style={"flex": "1", "display": "flex", "alignItems": "center"})

    return html.Div([
        # Top row: score (left-aligned)
        score_block,
        # Bottom row: KPIs (centered) + category bars + actions
        html.Div([kpi_row, category_bars, right],
                 style={"display": "flex", "gap": "1.5rem", "alignItems": "center",
                        "justifyContent": "center"}),
    ], style={"padding": ".8rem 1.2rem", "borderRadius": "8px",
              "border": "1px solid var(--border-default)",
              "background": "var(--bg-surface-alt)", "marginBottom": ".6rem"},
       className="exec-summary")


def _kpi_item(label, value, color=None):
    """Single inline KPI."""
    v_style = {"fontSize": ".8rem", "fontWeight": "600", "lineHeight": "1"}
    if color:
        v_style["color"] = color
    return html.Div([
        html.Div(str(value), style=v_style),
        html.Div(label, style={"fontSize": ".6rem", "color": "var(--text-muted)",
                  "letterSpacing": ".2px"}),
    ], style={"textAlign": "center"})
