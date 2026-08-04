"""WA Review v2 — orchestrator, layout, callbacks."""
import os
import threading
import logging
import boto3
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update

from tabs.well_architected import _run_wa_checks, _wa, _wa_lock
from tabs.wa_v2 import cost
from tabs.wa_v2.base import (
    pillar_section, score_bar, ai_section,
    WA_QUESTIONS, PILLAR_COLORS,
)

logger = logging.getLogger(__name__)

# ── Shared async-checks state (COST2/PERF1d COLLSCAN detection only) ─────────

_v2_state = {"results": [], "done": False, "running": False}
_v2_lock  = threading.Lock()


def _run_all_extra(cluster_id, region, conn_str=None):
    """Run remaining async checks (COST2/PERF1d — 30s CW Logs polls).

    Called in a background thread. Only cost.run_checks remains; all other
    checks migrated to wa_checks/ plugin registry (Phase 3B/3C).
    """
    cluster, instances = None, None
    try:
        docdb = boto3.client("docdb", region_name=region)
        cluster = docdb.describe_db_clusters(
            DBClusterIdentifier=cluster_id)["DBClusters"][0]
        instances = docdb.describe_db_instances(
            Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}]
        )["DBInstances"]
    except Exception as e:
        logger.warning("Pre-fetch cluster/instances failed: %s", e)

    try:
        results = cost.run_checks(cluster_id, region, conn_str,
                                  cluster=cluster, instances=instances)
    except Exception as e:
        logger.warning("COLLSCAN checks failed: %s", e)
        results = []

    with _v2_lock:
        _v2_state["results"] = results
        _v2_state["done"] = True
        _v2_state["running"] = False


def _ensure_extra_started(cluster_id, region, conn_str=None):
    """Start the v2 extra-checks thread if it has not produced results yet.

    Idempotent and self-triggering: any render/poll path can call this without
    depending on the "Run Review" button or the agent's gated trigger. Runs
    the async COLLSCAN checks (COST2/PERF1d) which take ~30s and can't run
    in the synchronous plugin pipeline.
    """
    with _v2_lock:
        if _v2_state["running"]:
            return False
        if _v2_state["results"]:
            return False  # already have results
        _v2_state["running"] = True
        _v2_state["done"] = False
    threading.Thread(
        target=_run_all_extra,
        args=(cluster_id, region, conn_str),
        daemon=True,
    ).start()
    return True


def _all_results_merged(cluster_id=""):
    """Return plugin results + async COLLSCAN results.

    Falls back to disk (agent_memory) when both in-memory states are empty.
    No dedup needed — COST2/PERF1d are not in the plugin registry.
    """
    with _wa_lock:
        v1 = list(_wa["results"])
    with _v2_lock:
        v2 = list(_v2_state["results"])

    # If both in-memory are empty (e.g. after app restart), load from disk
    if not v1 and not v2 and cluster_id:
        try:
            from agent_memory import load_wa_results
            saved = load_wa_results(cluster_id)
            if saved and saved.get("checks"):
                return saved["checks"]
        except Exception:
            pass

    # If v1 in memory but COLLSCAN thread not yet run, supplement from disk
    if v1 and not v2 and cluster_id:
        try:
            from agent_memory import load_wa_results
            saved = load_wa_results(cluster_id)
            if saved and saved.get("checks"):
                v1_ids = {(c.get("id"), c.get("pillar")) for c in v1}
                extra_from_disk = [c for c in saved["checks"]
                                   if (c.get("id"), c.get("pillar")) not in v1_ids]
                if extra_from_disk:
                    return v1 + extra_from_disk
        except Exception:
            pass

    return v1 + v2


# ── Drift / trend chart ───────────────────────────────────────────────────────

def _build_trend(all_results, cluster_id):
    """Vertical grouped bar chart — current vs previous, drift shown on bars."""
    try:
        import plotly.graph_objects as go
        from agent_memory import load_wa_results_previous

        PILLARS = ["Reliability", "Security", "Operational Excellence",
                   "Performance Efficiency", "Cost Optimization", "Sustainability"]
        # Two-line labels for longer names
        LABELS = ["Reliability", "Security", "Operational<br>Excellence",
                  "Performance<br>Efficiency", "Cost<br>Optimization", "Sustainability"]

        def _stats(results):
            s = {p: {"pass": 0, "warn": 0, "fail": 0} for p in PILLARS}
            for r in results:
                p = r.get("pillar", "")
                if p in s:
                    st = r.get("status", "")
                    if st in s[p]:
                        s[p][st] += 1
            return s

        curr = _stats(all_results)

        # Load previous run
        prev_data = load_wa_results_previous(cluster_id)
        prev_results = prev_data.get("checks", []) if prev_data else []
        has_previous = bool(prev_results)
        prev = _stats(prev_results) if has_previous else None

        fig = go.Figure()

        if has_previous:
            # Previous run (muted/transparent bars)
            p_pass = [prev[p]["pass"] for p in PILLARS]
            p_warn = [prev[p]["warn"] for p in PILLARS]
            p_fail = [prev[p]["fail"] for p in PILLARS]

            fig.add_trace(go.Bar(
                name="Prev Pass", x=LABELS, y=p_pass, marker_color="rgba(29,129,2,0.25)",
                text=[str(v) if v > 1 else "" for v in p_pass],
                textposition="inside", textfont=dict(size=9, color="#1D8102"),
                offsetgroup=0, showlegend=False,
            ))
            fig.add_trace(go.Bar(
                name="Prev Warn", x=LABELS, y=p_warn, marker_color="rgba(242,201,76,0.25)",
                text=[str(v) if v > 1 else "" for v in p_warn],
                textposition="inside", textfont=dict(size=9, color="#906806"),
                offsetgroup=0, base=p_pass, showlegend=False,
            ))
            fig.add_trace(go.Bar(
                name="Prev Fail", x=LABELS, y=p_fail, marker_color="rgba(209,50,18,0.25)",
                text=[str(v) if v > 1 else "" for v in p_fail],
                textposition="inside", textfont=dict(size=9, color="#D13212"),
                offsetgroup=0, base=[p + w for p, w in zip(p_pass, p_warn)], showlegend=False,
            ))

        # Current run (solid bars)
        c_pass = [curr[p]["pass"] for p in PILLARS]
        c_warn = [curr[p]["warn"] for p in PILLARS]
        c_fail = [curr[p]["fail"] for p in PILLARS]

        fig.add_trace(go.Bar(
            name="Pass", x=LABELS, y=c_pass, marker_color="#1D8102",
            text=[str(v) if v > 1 else "" for v in c_pass],
            textposition="inside", textfont=dict(size=10, color="white"),
            offsetgroup=1,
        ))
        fig.add_trace(go.Bar(
            name="Warn", x=LABELS, y=c_warn, marker_color="#F2C94C",
            text=[str(v) if v > 1 else "" for v in c_warn],
            textposition="inside", textfont=dict(size=10, color="#5e4b00"),
            offsetgroup=1, base=c_pass,
        ))
        fig.add_trace(go.Bar(
            name="Fail", x=LABELS, y=c_fail, marker_color="#D13212",
            text=[str(v) if v > 1 else "" for v in c_fail],
            textposition="inside", textfont=dict(size=10, color="white"),
            offsetgroup=1, base=[p + w for p, w in zip(c_pass, c_warn)],
        ))

        title = "Well-Architected Pillars Analysis over time (Drift)"
        if has_previous:
            prev_ts = (prev_data.get("ts", "") or "")[:10]
            if prev_ts:
                title = f"Well-Architected Pillars Analysis over time (Drift) — Previous: {prev_ts}"

        fig.update_layout(
            barmode="group" if has_previous else "stack",
            height=220,
            template="plotly_white",
            font_family="sans-serif",
            margin=dict(t=8, b=60, l=28, r=12),
            xaxis=dict(tickfont=dict(size=10, family="sans-serif"),
                       tickangle=0, type="category",
                       fixedrange=True),
            yaxis=dict(title="", showticklabels=False, showgrid=False,
                       zeroline=False, fixedrange=True),
            showlegend=False,
            plot_bgcolor="rgba(0,0,0,0)",
            paper_bgcolor="rgba(0,0,0,0)",
            bargap=0.25,
            bargroupgap=0.1,
        )

        # Color legend as HTML below chart
        legend_items = [
            html.Span("● Pass", style={"color": "#1D8102", "fontSize": ".72rem", "fontWeight": "600", "marginRight": ".6rem"}),
            html.Span("● Warn", style={"color": "#F2C94C", "fontSize": ".72rem", "fontWeight": "600", "marginRight": ".6rem"}),
            html.Span("● Fail", style={"color": "#D13212", "fontSize": ".72rem", "fontWeight": "600", "marginRight": "1.2rem"}),
        ]
        if has_previous:
            legend_items.extend([
                html.Span("○ Prev Pass", style={"color": "#1D8102", "fontSize": ".72rem", "fontWeight": "500", "marginRight": ".6rem", "opacity": "0.5"}),
                html.Span("○ Prev Warn", style={"color": "#c4a000", "fontSize": ".72rem", "fontWeight": "500", "marginRight": ".6rem", "opacity": "0.5"}),
                html.Span("○ Prev Fail", style={"color": "#D13212", "fontSize": ".72rem", "fontWeight": "500", "opacity": "0.5"}),
            ])

        els = [
            html.Div(title, style={"fontSize": ".78rem", "fontWeight": "700",
                                    "color": "var(--text-muted)", "marginTop": ".5rem"}),
            dcc.Graph(figure=fig, config={"displayModeBar": False}),
            html.Div(legend_items, style={"display": "flex", "justifyContent": "center",
                                           "flexWrap": "wrap", "marginTop": "-.2rem"}),
        ]

        return html.Div(els, style={"border": "1px solid var(--border-default)",
                                     "borderRadius": "8px", "padding": ".4rem .5rem",
                                     "marginTop": ".5rem"})

    except Exception as e:
        logger.debug("Trend chart failed: %s", e)
        return None


# ── Results builder ───────────────────────────────────────────────────────────

def _build_ui(results, ai_md, active_filters, cluster_id=""):
    if not results:
        return html.Div("No results.", className="text-muted")

    with _v2_lock:
        extra = list(_v2_state["results"])
    all_results = results + extra

    def _panel(pillar):
        checks = [r for r in all_results if r["pillar"] == pillar]
        return html.Div(pillar_section(pillar, checks, active_filters) or html.Div(),
                        style={"flex": "1", "minWidth": "0"})

    def _row(p1, p2):
        return html.Div([_panel(p1), _panel(p2)],
                        style={"display": "flex", "gap": "1rem",
                               "alignItems": "flex-start", "marginBottom": ".5rem"})

    children = [
        _row("Reliability", "Security"),
        _row("Operational Excellence", "Cost Optimization"),
        _row("Performance Efficiency", "Sustainability"),
    ]

    ai = ai_section(ai_md)
    if ai:
        children.append(ai)

    trend = _build_trend(all_results, cluster_id) if cluster_id else None
    if trend:
        children.append(trend)

    return html.Div(children)


# ── Layout ────────────────────────────────────────────────────────────────────

def render_well_architected_v2(cluster_id, region, conn_str=None):
    if not cluster_id:
        return html.Div([
            html.Span("Well-Architected Review", className="section-title"),
            dbc.Alert("Select a cluster to run the review.", color="warning"),
        ])

    with _wa_lock:
        cached_done    = _wa["done"]
        cached_results = list(_wa["results"]) if _wa["results"] else []
        cached_ai      = _wa.get("ai_md")
        wa_running     = _wa.get("running", False)

    has_results = cached_done and cached_results
    btn_label   = "🔄  Re-run Review" if has_results else "🔍  Run Review"

    # Compute counts for filter bar
    if cached_results:
        with _v2_lock:
            _extra = list(_v2_state["results"])
        _all = cached_results + _extra
        _n_pass = sum(1 for r in _all if r["status"] == "pass")
        _n_warn = sum(1 for r in _all if r["status"] == "warn")
        _n_fail = sum(1 for r in _all if r["status"] == "fail")
        _n_info = sum(1 for r in _all if r["status"] == "info")
        _actionable = _n_pass + _n_warn + _n_fail
        _score = int(_n_pass / _actionable * 100) if _actionable else 0
    else:
        _n_pass = _n_warn = _n_fail = _n_info = _score = 0

    # Self-trigger async COLLSCAN checks (COST2/PERF1d) if we have plugin results
    # but the async thread hasn't run yet (e.g. tab opened from agent/cached results
    # without clicking "Run Review").
    extras_starting = False
    with _v2_lock:
        _extras_present = bool(_v2_state["results"])
        _extras_running = _v2_state["running"]
    if has_results and not _extras_present and not _extras_running:
        extras_starting = _ensure_extra_started(cluster_id, region, conn_str or None)

    # Enable poll if WA is currently running (e.g. triggered by the agent)
    # OR if not done yet but agent is active (checks may start any moment)
    # OR if we just kicked off / are still running the v2 extras.
    agent_active = False
    try:
        from agent_orchestrator import is_agent_running
        agent_active = is_agent_running()
    except Exception:
        pass
    with _v2_lock:
        _extras_pending = _v2_state["running"] or not _v2_state["done"]
    poll_disabled = not (wa_running or (agent_active and not cached_done)
                         or extras_starting or _extras_pending)

    return html.Div([
        # Header
        html.Div([
            html.Div([
                html.Img(src="/assets/Well-Architected-Tool.svg",
                         style={"width": "24px", "height": "24px",
                                "marginRight": ".5rem", "borderRadius": "4px",
                                "flexShrink": "0"}),
                html.Span("Well-Architected Review", className="section-title",
                          style={"marginBottom": "0", "borderBottom": "none",
                                 "paddingBottom": "0"}),
                html.Span(f"  {cluster_id}",
                          style={"fontSize": ".88rem", "fontWeight": "700",
                                 "color": "var(--text-body)", "fontFamily": "monospace",
                                 "marginLeft": ".4rem"}),
                html.Span(f"  ({region})",
                          style={"fontSize": ".78rem", "color": "var(--text-muted)"}),
            ], style={"display": "flex", "alignItems": "baseline",
                      "flexWrap": "wrap", "gap": ".1rem"}),
            html.Div([
                dbc.Button("📄 PDF", id="wa2-pdf-btn", color="secondary",
                           size="sm", outline=True, disabled=not has_results,
                           style={"fontWeight": "600", "whiteSpace": "nowrap",
                                  "marginRight": ".3rem"}),
                dbc.Button("📄 PDF + AI", id="wa2-pdf-ai-btn", color="warning",
                           size="sm", disabled=not (has_results and cached_ai),
                           style={"fontWeight": "600", "whiteSpace": "nowrap",
                                  "marginRight": ".4rem"}),
                dbc.Button(btn_label, id="wa2-run-btn", color="warning",
                           size="sm", style={"fontWeight": "600", "whiteSpace": "nowrap"}),
            ], style={"display": "flex", "alignItems": "center"}),
        ], style={"display": "flex", "justifyContent": "space-between",
                  "alignItems": "center", "marginBottom": ".75rem",
                  "paddingBottom": ".4rem",
                  "borderBottom": "2px solid var(--color-primary)"}),

        dcc.Store(id="wa2-meta", data={"cluster_id": cluster_id, "region": region,
                                       "conn_str": conn_str or ""}),
        dcc.Interval(id="wa2-poll", interval=800, disabled=poll_disabled),
        dcc.Download(id="wa2-pdf-download"),
        dcc.Download(id="wa2-pdf-ai-download"),
        html.Div(id="wa2-loading"),

        # Filter bar with health score and color-coded counts
        html.Div([
            html.Span([
                html.Span("Health Score: ", style={"color": "var(--text-muted)"}),
                html.Span(f"{_score}%", style={
                    "color": "#1D8102" if _score >= 80 else "#906806" if _score >= 60 else "#E07020",
                }),
            ], style={
                "fontSize": ".82rem", "fontWeight": "700",
                "marginRight": "1rem", "alignSelf": "center",
            }) if cached_results else None,
            dbc.Checklist(
                id="wa2-filter-checklist",
                options=[
                    {"label": html.Span(f"✓ Pass ({_n_pass})", style={"color": "#1D8102"}), "value": "pass"},
                    {"label": html.Span(f"! Warn ({_n_warn})", style={"color": "#906806"}), "value": "warn"},
                    {"label": html.Span(f"✗ Fail ({_n_fail})", style={"color": "#D13212"}), "value": "fail"},
                    {"label": html.Span(f"ℹ Info ({_n_info})", style={"color": "#5e6b7a"}), "value": "info"},
                ],
                value=[],
                inline=True,
                inputStyle={"marginRight": ".3rem"},
                labelStyle={"marginRight": "1rem", "fontSize": ".82rem",
                            "fontWeight": "600", "cursor": "pointer"},
            ),
            html.Div([
                dbc.Button("Expand All", id="wa2-expand-all", color="link", size="sm",
                           style={"fontSize": ".75rem", "padding": ".1rem .4rem", "fontWeight": "600"}),
                dbc.Button("Collapse All", id="wa2-collapse-all", color="link", size="sm",
                           style={"fontSize": ".75rem", "padding": ".1rem .4rem", "fontWeight": "600"}),
            ], style={"marginLeft": "auto", "display": "flex", "gap": ".2rem", "alignItems": "center"}),
        ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
                  "marginBottom": ".4rem", "padding": ".3rem .5rem",
                  "background": "var(--bg-surface-alt)",
                  "border": "1px solid var(--border-default)",
                  "borderRadius": "8px"}),

        html.Div(id="wa2-results",
                 children=_build_ui(cached_results, cached_ai, [], cluster_id)
                 if has_results else
                 html.Div("Click Run Review to assess your cluster.",
                          className="text-muted",
                          style={"fontSize": ".88rem", "padding": "1rem 0"})),
    ])


# ── Callbacks ─────────────────────────────────────────────────────────────────

@callback(
    Output("wa2-loading", "children", allow_duplicate=True),
    Output("wa2-results", "children", allow_duplicate=True),
    Output("wa2-poll", "disabled", allow_duplicate=True),
    Input("wa2-run-btn", "n_clicks"),
    State("wa2-meta", "data"),
    State("analysis-store", "data"),
    prevent_initial_call=True,
)
def cb_wa2_start(n, meta, analysis_data):
    if not meta:
        return no_update, no_update, True
    with _v2_lock:
        _v2_state["done"] = False
        _v2_state["running"] = True
    threading.Thread(
        target=_run_wa_checks,
        args=(meta["cluster_id"], meta["region"], analysis_data),
        daemon=True,
    ).start()
    threading.Thread(
        target=_run_all_extra,
        args=(meta["cluster_id"], meta["region"], meta.get("conn_str") or None),
        daemon=True,
    ).start()
    loading = html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span("Running Well-Architected checks…",
                  style={"fontSize": ".88rem", "color": "var(--text-muted)"}),
    ], className="d-flex align-items-center mb-2")
    return loading, no_update, False


@callback(
    Output("wa2-loading", "children"),
    Output("wa2-results", "children"),
    Output("wa2-poll", "disabled"),
    Output("wa2-pdf-btn", "disabled"),
    Output("wa2-pdf-ai-btn", "disabled"),
    Input("wa2-poll", "n_intervals"),
    State("wa2-filter-checklist", "value"),
    State("wa2-meta", "data"),
    prevent_initial_call=True,
)
def cb_wa2_poll(n, active_filters, meta):
    with _wa_lock:
        done    = _wa["done"]
        error   = _wa["error"]
        results = list(_wa["results"])
        ai_md   = _wa.get("ai_md")
    with _v2_lock:
        extra_done = _v2_state["done"]
        extra      = list(_v2_state["results"])

    # Render as soon as v1 is done — don't block on v2 extra checks
    if not done:
        return no_update, no_update, False, True, True
    if error:
        return (html.Span(f"Error: {error[:80]}",
                          style={"color": "#D13212", "fontSize": ".85rem"}),
                no_update, True, True, True)

    all_results = results + extra

    # Keep polling if v2 extras are still running (will re-render with merged results)
    still_polling = not extra_done

    # Persist merged results for drift detection
    cluster_id = (meta or {}).get("cluster_id", "")
    if cluster_id and all_results:
        try:
            from agent_memory import save_wa_results
            save_wa_results(cluster_id, all_results)
        except Exception as e:
            logger.debug("save_wa_results failed: %s", e)

    # Use suppressed counts to match UI
    from tabs.wa_v2.base import actionable_counts as _ac
    n_pass, n_warn, n_fail, n_info, actionable, _ = _ac(all_results)
    count_str = f"{actionable} checks"
    if n_info:
        count_str += f" · {n_info} info"

    ui = _build_ui(results, ai_md, active_filters or [], cluster_id)

    # Show results immediately; keep polling if v2 extras still running
    if still_polling:
        loading_msg = html.Span(
            f"✅ {actionable} checks loaded · extra pillar checks running…",
            style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})
        return loading_msg, ui, False, False, (ai_md is None)

    done_msg = html.Span(
        f"✅ {count_str} complete",
        style={"color": "#037f0c", "fontSize": ".85rem", "fontWeight": "600"})
    return done_msg, ui, True, False, (ai_md is None)


@callback(
    Output("wa2-results", "children", allow_duplicate=True),
    Input("wa2-filter-checklist", "value"),
    State("wa2-meta", "data"),
    prevent_initial_call=True,
)
def cb_wa2_filter(active_filters, meta):
    with _wa_lock:
        results = list(_wa["results"])
        ai_md   = _wa.get("ai_md")
    if not results:
        return no_update
    cluster_id = (meta or {}).get("cluster_id", "")
    return _build_ui(results, ai_md, active_filters or [], cluster_id)


@callback(
    Output("wa2-pdf-download", "data"),
    Input("wa2-pdf-btn", "n_clicks"),
    State("wa2-meta", "data"),
    prevent_initial_call=True,
)
def cb_wa2_pdf(n, meta):
    if not meta:
        return no_update
    all_results = _all_results_merged(meta.get("cluster_id", ""))
    if not all_results:
        return no_update
    try:
        from wa_pdf import generate_wa_pdf
        pdf_bytes = generate_wa_pdf(
            all_results, None,
            meta.get("cluster_id", "cluster"),
            meta.get("region", "")
        )
        fname = f"wa_review_{meta.get('cluster_id', 'cluster')}.pdf"
        return dcc.send_bytes(pdf_bytes, fname)
    except Exception as e:
        logger.error("PDF generation failed: %s", e, exc_info=True)
        return no_update


@callback(
    Output("wa2-pdf-ai-download", "data"),
    Input("wa2-pdf-ai-btn", "n_clicks"),
    State("wa2-meta", "data"),
    prevent_initial_call=True,
)
def cb_wa2_pdf_ai(n, meta):
    if not meta:
        return no_update
    all_results = _all_results_merged(meta.get("cluster_id", ""))
    with _wa_lock:
        ai_md = _wa.get("ai_md")
    if not all_results:
        return no_update
    try:
        from wa_pdf import generate_wa_pdf
        pdf_bytes = generate_wa_pdf(
            all_results, ai_md,
            meta.get("cluster_id", "cluster"),
            meta.get("region", "")
        )
        fname = f"wa_review_{meta.get('cluster_id', 'cluster')}_with_ai.pdf"
        return dcc.send_bytes(pdf_bytes, fname)
    except Exception as e:
        logger.error("PDF+AI generation failed: %s", e, exc_info=True)
        return no_update


# ── Registration — replaces v1 as the primary WA tab ─────────────────────────
from tabs.registry import register_tab
register_tab("wa", "", "Well-Architected", "cluster", render_well_architected_v2)
register_tab("wa2", "", "WA Review", "cluster", render_well_architected_v2)
