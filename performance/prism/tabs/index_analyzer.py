"""Unified Index Analyzer — cardinality, usage, redundancy, bloat in one sleek view."""
import dash_bootstrap_components as dbc
from dash import html
from tabs.ui_helpers import code_block, info_tip

# ── Table styles ─────────────────────────────────────────────────────────────
_TH = {"padding": ".3rem .4rem", "fontSize": ".65rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".4px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap", "textAlign": "right"}
_TH_L = {**_TH, "textAlign": "left"}
_TD = {"padding": ".25rem .4rem", "fontSize": ".78rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "textAlign": "right",
       "verticalAlign": "middle", "wordBreak": "break-word"}
_TD_L = {**_TD, "textAlign": "left"}
_TD_NAME = {**_TD_L, "fontWeight": "600", "fontFamily": "monospace", "fontSize": ".75rem",
            "maxWidth": "300px", "overflow": "hidden", "textOverflow": "ellipsis",
            "whiteSpace": "nowrap"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)",
          "tableLayout": "fixed"}

_MAX_NAME_LEN = 64


def _trunc_name(name):
    """Truncate name to _MAX_NAME_LEN chars; full name shown on hover."""
    if len(name) <= _MAX_NAME_LEN:
        return name
    from dash import html as _html
    return _html.Span(name[:_MAX_NAME_LEN] + "…", title=name)


def _ks(fields):
    return "||".join(f"{f}||{d}" for f, d in fields.items())


def _check_redundancy(target, all_indexes):
    tks = _ks(target['fields'])
    return [o['name'] for o in all_indexes
            if o['name'] != target['name'] and o['name'] not in ('_id', '_id_')
            and _ks(o['fields']).startswith(tks + "||")]


def render_index_analyzer(analysis_data, database_name, conn_str="", region="us-east-1", cluster_id=""):
    if database_name not in analysis_data:
        return dbc.Alert("No data available.", color="info")

    db = analysis_data[database_name]

    # ── Lazy cluster-wide usage resolution ───────────────────────────────
    # Primary $indexStats only counts ops served by the writer. An index used by
    # reads routed to a secondary can look unused on the primary. So for indexes
    # that show ZERO ops on the primary, query the readers and treat the index as
    # unused only if it is unused cluster-wide (max ops across instances == 0).
    candidates = {}
    for coll, cd in db.items():
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        for idx in cd.get('indexes', []):
            name = idx.get('name', '')
            if name in ('_id', '_id_'):
                continue
            if idx.get('usage', {}).get('ops_count', 0) == 0:
                candidates.setdefault(coll, set()).add(name)

    reader_ops = {}
    coverage_partial = False
    coverage_checked = 0
    coverage_total = 0
    coverage_unreachable = []
    if candidates and cluster_id and conn_str:
        try:
            import index_usage_cluster
            res = index_usage_cluster.get_reader_ops(
                cluster_id, region, conn_str, database_name, candidates)
            reader_ops = res.get("ops", {})
            coverage_total = res.get("instances_total", 0)
            coverage_checked = res.get("instances_checked", 0)
            coverage_unreachable = res.get("unreachable", [])
            coverage_partial = res.get("partial", False)
        except Exception:
            import logging
            logging.getLogger(__name__).debug("cluster-wide index usage failed", exc_info=True)

    def _cluster_ops(coll, idx_name, primary_ops):
        """Max ops across primary + readers for this index."""
        r = reader_ops.get(coll, {}).get(idx_name, 0)
        return max(primary_ops, r)

    # Coverage note shown near Drop Commands when readers could not all be checked.
    if candidates and cluster_id and conn_str and coverage_total > 0 and coverage_partial:
        _coverage_note = html.Div(
            f"Partial coverage: checked {coverage_checked} of {coverage_total} reader "
            f"instance(s) for cross-instance usage"
            + (f" (unreachable: {', '.join(coverage_unreachable)})." if coverage_unreachable else ".")
            + " Unused flags are based on the instances that responded.",
            style={"fontSize": ".7rem", "color": "var(--color-warning)", "fontStyle": "italic",
                   "marginBottom": ".4rem"})
    elif candidates and cluster_id and conn_str and coverage_total > 0:
        _coverage_note = html.Div(
            f"Cluster-wide check: usage confirmed across writer + {coverage_checked} reader "
            f"instance(s).",
            style={"fontSize": ".7rem", "color": "var(--text-muted)", "fontStyle": "italic",
                   "marginBottom": ".4rem"})
    else:
        _coverage_note = html.Div()

    # Collect all index data
    table_rows = []
    n_total = 0
    n_unused = 0
    n_redundant = 0
    n_low_card = 0
    n_bloated = 0
    drop_cmds = []
    reindex_cmds = []

    prev_coll = None
    for coll, cd in sorted(db.items()):
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        idxs = cd.get('indexes', [])
        # Add collection separator row
        if prev_coll is not None and coll != prev_coll:
            table_rows.append(html.Tr(
                html.Td(colSpan=10, style={"padding": "0", "height": "3px",
                         "background": "var(--border-default)", "border": "none"}),
            ))
        first_in_group = True
        prev_coll = coll
        for idx in idxs:
            is_system = idx['name'] in ('_id', '_id_')
            fields = ', '.join(idx['fields'].keys())
            u = idx.get('usage', {})
            c = idx.get('cardinality', {})
            b = idx.get('bloat', {})

            if is_system:
                table_rows.append(_build_row(coll if first_in_group else "", idx['name'], fields, system=True))
                first_in_group = False
                continue

            n_total += 1
            primary_ops = u.get('ops_count', 0)
            ops = _cluster_ops(coll, idx['name'], primary_ops)  # cluster-wide max
            since = u.get('since_date') or "\u2014"
            # Cluster-wide unused: zero ops on every reachable instance.
            is_unused = (ops == 0)
            card_pct = c.get('percentage', 0)
            is_low_card = c.get('is_low', False)
            redundant_with = _check_redundancy(idx, idxs)
            is_redundant = len(redundant_with) > 0
            bloat_pct = b.get('unusedPercent', 0.0)
            is_bloated = bloat_pct > 20

            if is_unused:
                n_unused += 1
            if is_low_card:
                n_low_card += 1
            if is_redundant:
                n_redundant += 1
            if is_bloated:
                n_bloated += 1

            issues = sum([is_unused, is_low_card, is_redundant, is_bloated])

            table_rows.append(_build_row(
                coll if first_in_group else "", idx['name'], fields,
                ops=ops, is_unused=is_unused,
                card_pct=card_pct, is_low_card=is_low_card,
                redundant_with=redundant_with,
                bloat_pct=bloat_pct, is_bloated=is_bloated,
                issues=issues, since=since,
            ))
            first_in_group = False

            if is_unused:
                drop_cmds.append(f'db.{coll}.dropIndex("{idx["name"]}")')
            if is_redundant and not is_unused:
                drop_cmds.append(f'// Redundant: db.{coll}.dropIndex("{idx["name"]}")')
            if is_bloated:
                reindex_cmds.append(f'db.runCommand({{ reIndex: "{coll}", index: "{idx["name"]}" }})')

    issues_total = n_unused + n_redundant + n_low_card + n_bloated

    # ── Collect indexes requiring action ─────────────────────────────────
    action_indexes = []
    prev_coll2 = None
    for coll, cd in sorted(db.items()):
        if not isinstance(cd, dict) or 'error' in cd:
            continue
        idxs = cd.get('indexes', [])
        for idx in idxs:
            if idx['name'] in ('_id', '_id_'):
                continue
            u = idx.get('usage', {})
            c = idx.get('cardinality', {})
            b = idx.get('bloat', {})
            # Cluster-wide unused (same logic as the main table).
            is_unused = (_cluster_ops(coll, idx['name'], u.get('ops_count', 0)) == 0)
            is_low_card = c.get('is_low', False)
            redundant_with = _check_redundancy(idx, idxs)
            is_redundant = len(redundant_with) > 0
            bloat_pct = b.get('unusedPercent', 0.0)
            is_bloated = bloat_pct > 20

            if is_unused or is_low_card or is_redundant or is_bloated:
                reasons = []
                if is_unused:
                    reasons.append(("Unused", "var(--accent-red)"))
                if is_redundant:
                    reasons.append(("Redundant", "var(--color-warning)"))
                if is_low_card:
                    reasons.append((f"Low card. ({c.get('percentage', 0):.1f}%)", "var(--color-warning)"))
                if is_bloated:
                    reasons.append((f"Bloated ({bloat_pct:.0f}%)", "var(--color-warning)"))
                action_indexes.append({
                    "collection": coll,
                    "name": idx['name'],
                    "fields": ', '.join(idx['fields'].keys()),
                    "reasons": reasons,
                })

    # ── KPI bar ──────────────────────────────────────────────────────────
    def _kpi(label, value, color=None):
        v_style = {"fontSize": ".88rem", "fontWeight": "700", "lineHeight": "1"}
        if color:
            v_style["color"] = color
        return html.Div([
            html.Div(str(value), style=v_style),
            html.Div(label, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                      "textTransform": "uppercase", "letterSpacing": ".3px"}),
        ], style={"textAlign": "center", "flex": "1", "minWidth": "0"})

    kpi_bar = html.Div([
        _kpi("Total", n_total),
        _kpi("Unused", n_unused, "var(--accent-red)" if n_unused else None),
        _kpi("Redundant", n_redundant, "var(--accent-red)" if n_redundant else None),
        _kpi("Low Card.", n_low_card, "var(--color-warning)" if n_low_card else None),
        _kpi("Bloated", n_bloated, "var(--color-warning)" if n_bloated else None),
        _kpi("Issues", issues_total,
             "var(--accent-red)" if issues_total > 5 else
             "var(--color-warning)" if issues_total > 0 else "var(--accent-green)"),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)", "borderRadius": "8px",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # ── Table ────────────────────────────────────────────────────────────
    header = html.Tr([
        html.Th("Collection", style={**_TH_L, "width": "12%"}),
        html.Th("Index", style={**_TH_L, "width": "13%"}),
        html.Th("Fields", style={**_TH_L, "width": "15%"}),
        html.Th("Ops", style={**_TH, "width": "6%"}),
        html.Th("Ops since", style={**_TH, "width": "9%"}),
        html.Th("Usage", style={**_TH, "width": "8%"}),
        html.Th("Card.", style={**_TH, "width": "7%"}),
        html.Th("Redundant", style={**_TH_L, "width": "13%"}),
        html.Th("Bloat", style={**_TH, "width": "6%"}),
        html.Th("Health", style={**_TH, "width": "8%"}),
    ])

    table = html.Div(
        html.Table([html.Thead(header), html.Tbody(table_rows)], style=_TABLE),
        style={"overflowX": "auto", "marginBottom": ".5rem"})

    # ── Assemble ───────────────────────────────────────────────────────
    children = [
        html.Div("Index Health", className="section-title"),
        kpi_bar,
    ]

    # ── Build "Needs Attention" table (issues only, sorted by issue type) ──
    # Sort order: Unused > Redundant > Low Cardinality > Bloated
    issue_priority = {"Unused": 0, "Redundant": 1, "Low card.": 2, "Bloated": 3}
    def _sort_key(ai):
        first_reason = ai["reasons"][0][0] if ai["reasons"] else ""
        for key, pri in issue_priority.items():
            if key.lower() in first_reason.lower():
                return pri
        return 99

    attention_rows = []
    prev_coll_attn = None
    prev_issue_type = None
    for ai in sorted(action_indexes, key=_sort_key):
        # Determine issue type for grouping
        worst_reason, worst_color = ai["reasons"][0]
        current_type = worst_reason.split("(")[0].strip()

        # Add separator between issue types
        if prev_issue_type is not None and current_type != prev_issue_type:
            prev_coll_attn = None  # Reset collection dedup on type change

        show_coll = ai["collection"] if ai["collection"] != prev_coll_attn else ""
        prev_coll_attn = ai["collection"]
        prev_issue_type = current_type

        # Show ALL issue badges (not just the first one); allow them to wrap
        # so multiple reasons (e.g. Low card. + Bloated) are fully visible.
        reason_badges = []
        for reason_text, reason_color in ai["reasons"]:
            reason_badges.append(html.Span(reason_text, style={
                "fontSize": ".68rem", "fontWeight": "600", "color": reason_color,
                "padding": ".1rem .4rem", "borderRadius": "3px",
                "border": f"1px solid {reason_color}", "marginRight": ".3rem",
                "marginBottom": ".2rem", "display": "inline-block",
                "whiteSpace": "nowrap"}))
        status_cell = html.Td(reason_badges, style={
            **_TD_L, "whiteSpace": "normal", "overflow": "visible",
            "textOverflow": "clip"})
        attention_rows.append(html.Tr([
            html.Td(_trunc_name(show_coll), style={**_TD_L, "fontWeight": "600", "fontSize": ".8rem"}, title=show_coll),
            html.Td(_trunc_name(ai["name"]), style={**_TD_L, "fontFamily": "monospace", "fontSize": ".78rem"}, title=ai["name"]),
            html.Td(ai["fields"], style={**_TD_L, "fontSize": ".76rem", "color": "var(--text-muted)"}),
            status_cell,
        ]))

    attention_table = html.Table([
        html.Thead(html.Tr([
            html.Th("Collection", style={**_TH_L, "width": "20%"}),
            html.Th("Index", style={**_TH_L, "width": "22%"}),
            html.Th("Fields", style={**_TH_L, "width": "28%"}),
            html.Th("Issue", style={**_TH_L, "width": "30%"}),
        ])),
        html.Tbody(attention_rows),
    ], style=_TABLE) if attention_rows else html.Div(
        "✅ No issues — all indexes are healthy.",
        style={"fontSize": ".82rem", "color": "var(--accent-green)",
               "fontWeight": "600", "padding": ".6rem", "textAlign": "center"})

    # ── Toggle bar ───────────────────────────────────────────────────────
    toggle_bar = html.Div([
        html.Span(f"⚠️ Needs Attention ({len(action_indexes)})",
                  id="idx-toggle-attention",
                  style={"fontSize": ".78rem", "fontWeight": "700", "padding": ".3rem .8rem",
                         "borderRadius": "4px", "cursor": "pointer",
                         "background": "var(--color-warning)", "color": "#fff"}),
        html.Span(f"All Indexes ({n_total})",
                  id="idx-toggle-all",
                  style={"fontSize": ".78rem", "fontWeight": "700", "padding": ".3rem .8rem",
                         "borderRadius": "4px", "cursor": "pointer",
                         "background": "var(--bg-surface-alt)", "color": "var(--text-muted)",
                         "border": "1px solid var(--border-default)"}),
    ], style={"display": "flex", "gap": ".4rem", "marginBottom": ".5rem"})

    children.append(toggle_bar)

    # ── Both views (JS toggles visibility) ───────────────────────────────
    children.append(html.Div(attention_table, id="idx-view-attention",
                             style={"display": "block"}))
    children.append(html.Div(table, id="idx-view-all",
                             style={"display": "none"}))

    if drop_cmds:
        children.extend([
            html.Div("Drop Commands", style={"fontSize": ".75rem", "fontWeight": "700",
                      "textTransform": "uppercase", "letterSpacing": ".5px",
                      "color": "var(--text-muted)", "marginBottom": ".3rem", "marginTop": ".75rem"}),
            html.Div([
                html.Span("\u26a0 ", style={"color": "var(--color-warning)"}),
                html.Span("Due diligence before dropping. ", style={"fontWeight": "700"}),
                html.Span("These indexes show zero operations, but confirm against your "
                          "application's real query patterns before acting. The counter resets "
                          "on instance restart, so check the \u201cOps since\u201d date \u2014 a recent "
                          "restart can make an active index look unused. Validate with app owners "
                          "and test in non-production first."),
            ], style={"fontSize": ".72rem", "color": "var(--text-muted)",
                      "background": "var(--bg-surface-alt)", "border": "1px solid var(--border-default)",
                      "borderLeft": "3px solid var(--color-warning)", "borderRadius": "4px",
                      "padding": ".4rem .6rem", "marginBottom": ".4rem"}),
            _coverage_note,
            code_block('\n'.join(drop_cmds)),
        ])

    if reindex_cmds:
        children.extend([
            html.Div("Rebuild Commands", style={"fontSize": ".75rem", "fontWeight": "700",
                      "textTransform": "uppercase", "letterSpacing": ".5px",
                      "color": "var(--text-muted)", "marginBottom": ".3rem", "marginTop": ".75rem"}),
            code_block('\n'.join(dict.fromkeys(reindex_cmds))),
        ])

    children.append(info_tip(
        "Always test index changes in staging first. "
        "Monitor query performance after dropping indexes. "
        "Schedule reIndex during maintenance windows."
    ))

    return html.Div(children)


def _build_row(coll, name, fields, system=False, ops=0, is_unused=False,
               card_pct=0, is_low_card=False, redundant_with=None,
               bloat_pct=0, is_bloated=False, issues=0, since="\u2014"):
    """Build a single table row."""
    if system:
        muted = {"color": "var(--text-muted)", "fontStyle": "italic"}
        return html.Tr([
            html.Td(_trunc_name(coll), style=_TD_NAME, title=coll),
            html.Td(_trunc_name(name), style={**_TD_L, **muted, "fontSize": ".78rem"}, title=name),
            html.Td(fields, style={**_TD_L, **muted}),
            html.Td("\u2014", style=_TD),          # Ops
            html.Td("\u2014", style=_TD),          # Ops since
            html.Td("system", style={**_TD, **muted}),  # Usage
            html.Td("\u2014", style=_TD),          # Card.
            html.Td("", style=_TD_L),              # Redundant
            html.Td("\u2014", style=_TD),          # Bloat
            html.Td("\u2014", style=_TD),          # Health
        ])

    # Usage
    if is_unused:
        usage_text = "Unused"
        usage_color = "var(--accent-red)"
    elif ops == 0:
        usage_text = "No ops"
        usage_color = "var(--color-warning)"
    elif ops < 100:
        usage_text = "Low"
        usage_color = "var(--color-warning)"
    else:
        usage_text = "Active"
        usage_color = "var(--accent-green)"

    # Health
    if issues == 0:
        health = "Healthy"
        h_color = "var(--accent-green)"
    elif issues == 1:
        health = "Review"
        h_color = "var(--color-warning)"
    else:
        health = "Action"
        h_color = "var(--accent-red)"

    # Row tint for problematic indexes
    row_style = {}
    if issues >= 2:
        row_style = {"background": "rgba(217, 21, 21, 0.03)"}

    card_style = {**_TD}
    if is_low_card:
        card_style["color"] = "var(--color-warning)"
        card_style["fontWeight"] = "600"

    bloat_style = {**_TD}
    if is_bloated:
        bloat_style["color"] = "var(--accent-red)"
        bloat_style["fontWeight"] = "600"

    return html.Tr([
        html.Td(_trunc_name(coll), style=_TD_NAME, title=coll),
        html.Td(_trunc_name(name), style={**_TD_L, "fontSize": ".78rem", "fontFamily": "monospace"}, title=name),
        html.Td(fields, style={**_TD_L, "fontSize": ".76rem", "color": "var(--text-muted)"}),
        html.Td(f"{ops:,}", style=_TD),
        html.Td(since or "\u2014", style={**_TD, "fontSize": ".74rem",
                "color": "var(--text-muted)"}),
        html.Td(usage_text, style={**_TD, "color": usage_color, "fontWeight": "600"}),
        html.Td(f"{card_pct:.1f}%" if card_pct > 0 else "\u2014", style=card_style),
        html.Td(", ".join(redundant_with) if redundant_with else "\u2014",
                style={**_TD_L, "fontSize": ".76rem",
                       "color": "var(--color-warning)" if redundant_with else "var(--text-muted)"}),
        html.Td(f"{bloat_pct:.0f}%" if bloat_pct > 0 else "\u2014", style=bloat_style),
        html.Td(health, style={**_TD, "color": h_color, "fontWeight": "600"}),
    ], style=row_style)


def _rec_item(title, desc, color):
    """Single recommendation item."""
    return html.Div([
        html.Div([
            html.Span("\u25cf", style={"color": color, "marginRight": ".35rem",
                       "fontSize": ".5rem", "verticalAlign": "middle"}),
            html.Span(title, style={"fontSize": ".82rem", "fontWeight": "600"}),
        ]),
        html.Div(desc, style={"fontSize": ".72rem", "color": "var(--text-muted)",
                 "paddingLeft": ".85rem"}),
    ], style={"marginBottom": ".35rem"})


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_db_tab
register_db_tab("indexes", "\U0001f5c2\ufe0f", "Indexes", render_index_analyzer)
