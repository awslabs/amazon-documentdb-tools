"""Cluster-level Slow Query Analysis — all databases from CloudWatch profiler logs."""
import logging
import threading
import json
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ALL, ctx
from query_analyzer import get_query_patterns
import slow_query_recommender as sqr

logger = logging.getLogger(__name__)

# Background state
_csq = {"patterns": [], "done": False, "running": False, "error": None,
        "region": "us-east-1", "cluster_id": ""}
_csq_lock = threading.Lock()


def reset_cache():
    """Clear cached slow query results (called on cluster reconnect)."""
    with _csq_lock:
        _csq.update(patterns=[], done=False, running=False, error=None)
    # Clear AI recommendation state so no stale recs from a prior cluster show (Req 12.2, 13.6)
    try:
        sqr.reset()
    except Exception:
        logger.debug("sqr.reset() failed during reset_cache", exc_info=True)

# Table styles
_TH = {"padding": ".35rem .5rem", "fontSize": ".65rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".4px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap", "textAlign": "right"}
_TH_L = {**_TH, "textAlign": "left"}
_TD = {"padding": ".3rem .5rem", "fontSize": ".8rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "textAlign": "right",
       "verticalAlign": "top"}
_TD_L = {**_TD, "textAlign": "left"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)"}


def _run_cluster_slow_query(log_group, region, days, cluster_id=""):
    """Fetch slow queries across all databases."""
    with _csq_lock:
        _csq.update(patterns=[], done=False, running=True, error=None,
                    region=region or "us-east-1", cluster_id=cluster_id or "")
    # New analysis run — clear prior AI recommendation state so Re-analyze
    # produces fresh recommendations (Req 12.1, 12.2).
    try:
        sqr.reset()
    except Exception:
        logger.debug("sqr.reset() failed at run start", exc_info=True)
    try:
        hours = days * 24
        patterns = get_query_patterns("*", "*", hours=hours,
                                      log_group_name=log_group, aws_region=region)
        with _csq_lock:
            _csq.update(patterns=patterns or [], done=True, running=False)
    except Exception as e:
        logger.error("Cluster slow query failed: %s", e)
        with _csq_lock:
            _csq.update(done=True, running=False, error=str(e))


def render_cluster_slow_queries(cluster_id, region, log_group):
    """Render the cluster-level slow query tab."""
    if not log_group:
        # Auto-derive
        if cluster_id:
            log_group = f"/aws/docdb/{cluster_id}/profiler"
        else:
            return html.Div([
                html.Div("Slow Query Analysis", className="section-title"),
                dbc.Alert("No log group available. Enable profiler logging on the cluster.", color="warning"),
            ])

    # Check for cached results — first from own tab runs, then from agent
    with _csq_lock:
        cached_done = _csq["done"]
        cached_patterns = list(_csq["patterns"])

    # If tab hasn't run its own analysis, check if the agent has results
    if not cached_done:
        try:
            from agent_orchestrator import _agent_state, _lock
            with _lock:
                sq_mod = _agent_state["modules"]["slow_query"]
                if sq_mod.get("status") == "done" and sq_mod.get("result"):
                    r = sq_mod["result"]
                    cached_patterns = r if isinstance(r, list) else []
                    cached_done = True
        except Exception:
            pass
        # Mirror agent-sourced patterns into _csq so the poll callback (which
        # only refreshes when _csq["done"] is True) surfaces recommendations as
        # they complete on THIS visit — without this, recs only appeared after
        # navigating away and back (which re-runs render()).
        if cached_done and cached_patterns:
            with _csq_lock:
                _csq["patterns"] = list(cached_patterns)
                _csq["done"] = True
                _csq["error"] = None

    if cached_done and cached_patterns:
        # Make region/cluster available to the engine scheduling done inside _render_patterns.
        with _csq_lock:
            _csq["region"] = region or _csq.get("region", "us-east-1")
            _csq["cluster_id"] = cluster_id or _csq.get("cluster_id", "")
        initial = _render_patterns(cached_patterns, cluster_id)
        btn_label = "\U0001f504  Re-analyze"
        poll_disabled = False  # let recommendations surface as they complete
    elif cached_done and not cached_patterns:
        initial = dbc.Alert("No slow queries found in the profiler logs.", color="success",
                            style={"fontSize": ".85rem"})
        btn_label = "\U0001f504  Re-analyze"
        poll_disabled = True
    else:
        initial = html.Div("Click Analyze to scan CloudWatch profiler logs for slow queries across all databases.",
                           className="text-muted", style={"fontSize": ".85rem", "padding": ".5rem 0"})
        btn_label = "\U0001f50d  Analyze Slow Queries"
        poll_disabled = True

    # Build initial database filter options from cached patterns
    initial_db_options = []
    if cached_patterns:
        dbs = set()
        for p in cached_patterns:
            ns = p.get("ns", "")
            if "." in ns:
                dbs.add(ns.split(".", 1)[0])
        initial_db_options = [{"label": db, "value": db} for db in sorted(dbs)]

    return html.Div([
        html.Div([
            html.Div([
                html.Span("Slow Query Analysis", className="section-title",
                          style={"marginBottom": "0", "borderBottom": "none", "paddingBottom": "0"}),
                html.Span("  for  ", style={"color": "var(--text-muted)", "fontSize": ".85rem",
                           "margin": "0 .2rem"}),
                html.Span(cluster_id or "cluster", style={"fontSize": ".88rem", "fontWeight": "700",
                           "color": "var(--text-body)", "fontFamily": "monospace"}),
            ], style={"display": "flex", "alignItems": "baseline", "flexWrap": "wrap"}),
            html.Div([
                html.Div([
                    html.Span("Days: ", style={"fontSize": ".78rem", "color": "var(--text-muted)"}),
                    dbc.Input(id="csq-days", type="number", value=7, min=1, max=30,
                              size="sm", style={"width": "60px", "fontSize": ".78rem",
                                                 "display": "inline-block"}),
                ], style={"display": "flex", "alignItems": "center", "gap": ".3rem"}),
                dbc.Button(btn_label, id="csq-run-btn", color="warning",
                           size="sm", style={"fontWeight": "600", "whiteSpace": "nowrap"}),
            ], style={"display": "flex", "gap": ".5rem", "alignItems": "center"}),
        ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
                  "marginBottom": ".75rem", "paddingBottom": ".4rem",
                  "borderBottom": "2px solid var(--color-primary)"}),
        # Database filter dropdown
        html.Div([
            html.Span("Filter by database: ", style={"fontSize": ".78rem", "color": "var(--text-muted)"}),
            dcc.Dropdown(id="csq-db-filter", placeholder="All databases",
                         options=initial_db_options,
                         clearable=True, style={"width": "200px", "fontSize": ".78rem"}),
        ], style={"display": "flex", "alignItems": "center", "gap": ".3rem", "marginBottom": ".5rem"}),
        dcc.Store(id="csq-meta", data={"cluster_id": cluster_id, "region": region, "log_group": log_group}),
        html.Div(id="csq-loading"),
        html.Div(id="csq-results", children=initial),
        dcc.Interval(id="csq-poll", interval=1500, disabled=poll_disabled),
    ])


def _render_rec_cell(pattern_key, meta_region=None):
    """Render the AI recommendation cell for one pattern based on engine state.

    States (Req 2, 8, 9, 13):
      - done        -> action badge + expandable markdown (+ safety flag if unsafe)
      - unavailable -> "AI recommendations unavailable"
      - failed      -> error indication
      - otherwise   -> "Analysing for optimisations..." placeholder
    """
    state = sqr.get_state([pattern_key]).get(pattern_key)
    status = (state or {}).get("status")

    if status == "done" and (state or {}).get("recommendation"):
        rec = state["recommendation"]
        action = rec.get("action", "Other")
        # Action badge color
        badge_color = {"Add index": "var(--accent-blue, #0972d3)",
                       "Rewrite query": "var(--color-warning, #906806)",
                       "Scale compute": "var(--color-primary, #5f3dc4)"}.get(action, "var(--text-muted)")
        header_children = [
            html.Span(action, style={"fontSize": ".7rem", "fontWeight": "700",
                                     "color": "#fff", "background": badge_color,
                                     "padding": ".05rem .4rem", "borderRadius": "4px"}),
        ]
        if rec.get("unsafe"):
            header_children.append(
                html.Span("\u26a0 Suggestion not validated as safe",
                          style={"fontSize": ".68rem", "color": "var(--accent-red, #d91515)",
                                 "marginLeft": ".4rem", "fontWeight": "600"}))
        return html.Details([
            html.Summary(header_children,
                         style={"cursor": "pointer", "listStyle": "none", "display": "flex",
                                "alignItems": "center", "flexWrap": "wrap", "gap": ".2rem"}),
            html.Div(dcc.Markdown(rec.get("markdown", ""), className="csq-rec-md"),
                     style={"fontSize": ".72rem", "marginTop": ".3rem",
                            "padding": ".3rem .4rem", "background": "var(--bg-surface-alt)",
                            "borderRadius": "4px"}),
        ])

    if status == "unavailable":
        return html.Span(sqr.UNAVAILABLE_TEXT,
                         style={"fontSize": ".72rem", "color": "var(--text-muted)",
                                "fontStyle": "italic"})

    if status == "failed":
        return html.Span("\u274c Could not produce recommendation",
                         style={"fontSize": ".72rem", "color": "var(--accent-red, #d91515)"})

    # placeholder / generating / not-yet-scheduled (Req 2.1)
    return html.Span(sqr.PLACEHOLDER_TEXT,
                     style={"fontSize": ".72rem", "color": "var(--text-muted)",
                            "fontStyle": "italic"})


def _render_patterns(patterns, cluster_id=""):
    """Render slow query patterns grouped by database."""
    if not patterns:
        return dbc.Alert("No slow queries found.", color="success", style={"fontSize": ".85rem"})

    # Group by database
    by_db = {}
    for p in patterns:
        ns = p.get("ns", "")
        db_name = ns.split(".", 1)[0] if "." in ns else "unknown"
        if db_name not in by_db:
            by_db[db_name] = []
        by_db[db_name].append(p)

    # Summary KPIs
    total_patterns = len(patterns)
    total_dbs = len(by_db)
    worst_avg = max((p.get("avg_time", 0) for p in patterns), default=0)
    worst_max = max((p.get("max_time", 0) for p in patterns), default=0)
    total_occurrences = sum(p.get("count", 0) for p in patterns)

    def _kpi(label, value, color=None):
        v_style = {"fontSize": ".88rem", "fontWeight": "700", "lineHeight": "1"}
        if color:
            v_style["color"] = color
        return html.Div([
            html.Div(str(value), style=v_style),
            html.Div(label, style={"fontSize": ".68rem", "color": "var(--text-muted)",
                      "textTransform": "uppercase", "letterSpacing": ".3px"}),
        ], style={"textAlign": "center", "flex": "1"})

    kpi_bar = html.Div([
        _kpi("Patterns", total_patterns),
        _kpi("Databases", total_dbs),
        _kpi("Occurrences", f"{total_occurrences:,}"),
        _kpi("Worst Avg", f"{worst_avg:.0f}ms",
             "var(--accent-red)" if worst_avg > 1000 else "var(--color-warning)" if worst_avg > 200 else None),
        _kpi("Worst Max", f"{worst_max:.0f}ms",
             "var(--accent-red)" if worst_max > 5000 else None),
    ], style={"display": "flex", "gap": ".5rem", "padding": ".5rem .6rem",
              "border": "1px solid var(--border-default)", "borderRadius": "8px",
              "background": "var(--bg-surface-alt)", "marginBottom": ".75rem"})

    # Per-database sections
    children = [kpi_bar]

    # Sort databases by total impact (count * avg_time)
    sorted_dbs = sorted(by_db.items(),
                        key=lambda x: sum(p["count"] * p.get("avg_time", 0) for p in x[1]),
                        reverse=True)

    for db_name, db_patterns in sorted_dbs:
        db_patterns.sort(key=lambda x: x["count"] * x.get("avg_time", 0), reverse=True)

        # Database header
        db_count = sum(p["count"] for p in db_patterns)
        db_worst = max((p.get("avg_time", 0) for p in db_patterns), default=0)

        header = html.Div([
            html.Span(db_name, style={"fontWeight": "700", "fontSize": ".88rem",
                       "fontFamily": "monospace"}),
            html.Span(f"  {len(db_patterns)} patterns \u00b7 {db_count} occurrences \u00b7 worst avg {db_worst:.0f}ms",
                       style={"fontSize": ".75rem", "color": "var(--text-muted)", "marginLeft": ".5rem"}),
        ], style={"padding": ".4rem .5rem", "background": "var(--bg-surface-alt)",
                  "borderRadius": "6px 6px 0 0", "border": "1px solid var(--border-default)",
                  "borderBottom": "none", "marginTop": ".5rem"})

        # Table
        th_row = html.Tr([
            html.Th("Operation", style=_TH_L),
            html.Th("Collection", style=_TH_L),
            html.Th("Count", style=_TH),
            html.Th("Avg (ms)", style=_TH),
            html.Th("Max (ms)", style=_TH),
            html.Th("Pattern", style=_TH_L),
            html.Th("AI Recommendation", style=_TH_L),
        ])

        displayed = db_patterns[:15]
        # Schedule background AI recommendations for the displayed patterns (Req 3.1).
        # Idempotent via the engine's in-flight dedup, so calling every render is safe.
        try:
            sqr.schedule(displayed, cluster_id, _csq.get("region", "us-east-1"))
        except Exception:
            logger.debug("sqr.schedule failed", exc_info=True)

        rows = []
        for p in displayed:
            ns = p.get("ns", "")
            coll = ns.split(".", 1)[1] if "." in ns else ns
            avg = p.get("avg_time", 0)
            mx = p.get("max_time", 0)

            avg_style = {**_TD}
            if avg > 1000:
                avg_style["color"] = "var(--accent-red)"
                avg_style["fontWeight"] = "700"
            elif avg > 200:
                avg_style["color"] = "var(--color-warning)"

            # Expandable query display
            example = p.get("example_query", {})
            pattern_short = json.dumps(example, default=str)[:80]
            pattern_full = json.dumps(example, indent=2, default=str)

            query_cell = html.Td(
                html.Details([
                    html.Summary(pattern_short, style={"fontSize": ".72rem", "color": "var(--text-muted)",
                                  "cursor": "pointer", "whiteSpace": "nowrap", "overflow": "hidden",
                                  "textOverflow": "ellipsis", "maxWidth": "300px"}),
                    html.Pre(pattern_full, style={"fontSize": ".7rem", "margin": ".3rem 0 0 0",
                              "padding": ".4rem", "background": "var(--bg-surface-alt)",
                              "borderRadius": "4px", "whiteSpace": "pre-wrap",
                              "wordBreak": "break-all", "maxHeight": "200px", "overflowY": "auto"}),
                ]),
                style=_TD_L,
            )

            rec_cell = html.Td(_render_rec_cell(p.get("pattern_key", "")),
                               style={**_TD_L, "maxWidth": "320px"})

            rows.append(html.Tr([
                html.Td(p.get("operation", "\u2014"), style={**_TD_L, "fontWeight": "600"}),
                html.Td(coll or "\u2014", style={**_TD_L, "fontFamily": "monospace", "fontSize": ".76rem"}),
                html.Td(str(p.get("count", 0)), style={**_TD, "fontWeight": "600"}),
                html.Td(f"{avg:.0f}", style=avg_style),
                html.Td(f"{mx:.0f}", style=_TD),
                query_cell,
                rec_cell,
            ]))

        table = html.Table([html.Thead(th_row), html.Tbody(rows)], style=_TABLE)
        children.extend([header, table])

        # Non-displayed patterns (beyond top 15): offer on-demand recommendation (Req 4.1)
        extra = db_patterns[15:]
        if extra:
            extra_rows = []
            for p in extra:
                pk = p.get("pattern_key", "")
                ns = p.get("ns", "")
                coll = ns.split(".", 1)[1] if "." in ns else ns
                state = sqr.get_state([pk]).get(pk)
                if state and state.get("status") in ("done", "unavailable", "failed", "generating", "placeholder"):
                    control = _render_rec_cell(pk)
                else:
                    control = dbc.Button("Get recommendation",
                                         id={"type": "csq-rec-req", "pk": pk},
                                         color="link", size="sm",
                                         style={"fontSize": ".72rem", "padding": "0",
                                                "fontWeight": "600"})
                extra_rows.append(html.Tr([
                    html.Td(p.get("operation", "\u2014"), style={**_TD_L, "fontWeight": "600"}),
                    html.Td(coll or "\u2014", style={**_TD_L, "fontFamily": "monospace", "fontSize": ".76rem"}),
                    html.Td(str(p.get("count", 0)), style={**_TD, "fontWeight": "600"}),
                    html.Td(f"{p.get('avg_time', 0):.0f}", style=_TD),
                    html.Td(f"{p.get('max_time', 0):.0f}", style=_TD),
                    html.Td("\u2014", style=_TD_L),
                    html.Td(control, style={**_TD_L, "maxWidth": "320px"}),
                ]))
            extra_section = html.Details([
                html.Summary(f"+ {len(extra)} more patterns (on-demand recommendations)",
                             style={"fontSize": ".72rem", "color": "var(--text-muted)",
                                    "cursor": "pointer", "padding": ".3rem .5rem"}),
                html.Table([html.Thead(th_row), html.Tbody(extra_rows)], style=_TABLE),
            ])
            children.append(extra_section)

    return html.Div(children)


# ── Callbacks ────────────────────────────────────────────────────────────────

def _recs_pending(patterns):
    """True if any displayed pattern (top 15 per db) is still awaiting a terminal
    recommendation state (Req 2.3, 13.4)."""
    by_db = {}
    for p in patterns:
        ns = p.get("ns", "")
        db_name = ns.split(".", 1)[0] if "." in ns else "unknown"
        by_db.setdefault(db_name, []).append(p)
    displayed_keys = []
    for db_patterns in by_db.values():
        db_patterns.sort(key=lambda x: x["count"] * x.get("avg_time", 0), reverse=True)
        displayed_keys.extend(p.get("pattern_key", "") for p in db_patterns[:15])
    states = sqr.get_state(displayed_keys)
    for pk in displayed_keys:
        status = states.get(pk, {}).get("status")
        if status not in ("done", "failed", "unavailable"):
            return True
    return False

@callback(
    Output("csq-loading", "children", allow_duplicate=True),
    Output("csq-results", "children", allow_duplicate=True),
    Output("csq-poll", "disabled", allow_duplicate=True),
    Input("csq-run-btn", "n_clicks"),
    State("csq-meta", "data"),
    State("csq-days", "value"),
    prevent_initial_call=True,
)
def cb_csq_start(n, meta, days):
    if not meta or not n:
        return no_update, no_update, True
    days = int(days or 7)
    threading.Thread(target=_run_cluster_slow_query,
                     args=(meta["log_group"], meta["region"], days, meta.get("cluster_id", "")),
                     daemon=True).start()
    loading = html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span("Scanning CloudWatch profiler logs\u2026",
                  style={"fontSize": ".85rem", "color": "#5f6b7a"}),
    ], className="d-flex align-items-center mb-2")
    return loading, "", False


@callback(
    Output("csq-loading", "children"),
    Output("csq-results", "children"),
    Output("csq-poll", "disabled"),
    Input("csq-poll", "n_intervals"),
    State("csq-meta", "data"),
    prevent_initial_call=True,
)
def cb_csq_poll(n, meta):
    with _csq_lock:
        done = _csq["done"]
        error = _csq["error"]
        patterns = list(_csq["patterns"])
        # Keep region/cluster current for scheduling done from the render path.
        if meta:
            _csq["region"] = meta.get("region") or _csq.get("region", "us-east-1")
            _csq["cluster_id"] = meta.get("cluster_id") or _csq.get("cluster_id", "")

    if not done:
        logger.info("cb_csq_poll[n=%s]: done=False — returning no_update (table not refreshed)", n)
        return no_update, no_update, False

    if error:
        return html.Span(f"\u274c {error[:80]}", style={"color": "#d91515", "fontSize": ".85rem"}), "", True

    cluster_id = (meta or {}).get("cluster_id", "")
    pending = _recs_pending(patterns)
    logger.info("cb_csq_poll[n=%s]: done=True patterns=%d recs_pending=%s — rendering table",
                n, len(patterns), pending)
    done_msg = html.Span(f"\u2705 Found {len(patterns)} slow query patterns",
                         style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})

    rendered = _render_patterns(patterns, cluster_id)
    # Keep polling while any displayed pattern's recommendation is still pending,
    # so results land as they complete; disable once all are terminal (Req 2.3).
    return done_msg, rendered, (not pending)


@callback(
    Output("csq-results", "children", allow_duplicate=True),
    Input("csq-db-filter", "value"),
    State("csq-meta", "data"),
    prevent_initial_call=True,
)
def cb_csq_filter_db(selected_db, meta):
    """Filter displayed patterns by database."""
    with _csq_lock:
        done = _csq["done"]
        patterns = list(_csq["patterns"])

    # If tab hasn't run its own analysis, check agent cache
    if not done or not patterns:
        try:
            from agent_orchestrator import _agent_state, _lock
            with _lock:
                sq_mod = _agent_state["modules"]["slow_query"]
                if sq_mod.get("status") == "done" and sq_mod.get("result"):
                    r = sq_mod["result"]
                    patterns = r if isinstance(r, list) else []
                    done = True
        except Exception:
            pass

    if not done or not patterns:
        return no_update

    cluster_id = (meta or {}).get("cluster_id", "")

    if selected_db:
        filtered = [p for p in patterns if p.get("ns", "").startswith(f"{selected_db}.")]
    else:
        filtered = patterns

    return _render_patterns(filtered, cluster_id)


@callback(
    Output("csq-poll", "disabled", allow_duplicate=True),
    Input({"type": "csq-rec-req", "pk": ALL}, "n_clicks"),
    State("csq-meta", "data"),
    prevent_initial_call=True,
)
def cb_csq_request_rec(clicks, meta):
    """On-demand recommendation request for a non-displayed pattern (Req 4.2, 4.3)."""
    if not ctx.triggered_id or not any(c for c in (clicks or []) if c):
        return no_update
    pattern_key = ctx.triggered_id.get("pk")
    if not pattern_key:
        return no_update

    # Find the full pattern dict from cached patterns.
    with _csq_lock:
        patterns = list(_csq["patterns"])
        region = _csq.get("region", "us-east-1")
        cluster_id = _csq.get("cluster_id", "") or (meta or {}).get("cluster_id", "")
    if not patterns:
        try:
            from agent_orchestrator import _agent_state, _lock
            with _lock:
                sq_mod = _agent_state["modules"]["slow_query"]
                if sq_mod.get("status") == "done" and isinstance(sq_mod.get("result"), list):
                    patterns = sq_mod["result"]
        except Exception:
            pass

    target = next((p for p in patterns if p.get("pattern_key") == pattern_key), None)
    if target is None:
        return no_update

    try:
        sqr.request_one(target, cluster_id, region)
    except Exception:
        logger.debug("sqr.request_one failed", exc_info=True)
    # Re-enable polling so the result surfaces on the next cycle (Req 4.3).
    return False


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_tab
register_tab("cluster_slowquery", "", "Slow Queries", "cluster", render_cluster_slow_queries)
