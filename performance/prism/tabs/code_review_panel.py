"""Code Review panel — UI for running DocumentDB client code best practices review."""
import os
import json
import glob
import dash
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ALL, ctx


_CHECKLIST_PATH = os.path.join(
    os.path.dirname(__file__), "..", "documentdb-code-review", "context", "checklist.md"
)

# Module-level state to persist results across navigation
import threading
_cr_state = {"results": None}
_cr_lock = threading.Lock()

# Cache directory for previous runs
_CR_CACHE_DIR = os.path.join(os.path.dirname(__file__), "..", ".prism_cache", "code_reviews")


def _load_previous_runs():
    """Load list of previous code review runs from cache."""
    if not os.path.isdir(_CR_CACHE_DIR):
        return []
    runs = []
    for f in sorted(glob.glob(os.path.join(_CR_CACHE_DIR, "*.json")), reverse=True):
        try:
            with open(f) as fh:
                data = json.load(fh)
            runs.append({
                "label": f"{os.path.basename(data.get('target_dir', '?'))} — {data.get('compliance_pct', 0)}% ({data.get('_timestamp', '')})",
                "value": f,
            })
        except Exception:
            continue
    return runs


def _save_run(results, target_dir):
    """Save a code review run to cache. Keeps last 5 runs per project."""
    os.makedirs(_CR_CACHE_DIR, exist_ok=True)
    from datetime import datetime
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    project = os.path.basename(target_dir.rstrip("/\\"))
    results["target_dir"] = target_dir
    results["_timestamp"] = ts

    # Keep only last 5 runs for this project (delete oldest)
    existing = sorted(glob.glob(os.path.join(_CR_CACHE_DIR, f"{project}_*.json")))
    while len(existing) >= 5:
        try:
            os.remove(existing.pop(0))
        except Exception:
            pass

    path = os.path.join(_CR_CACHE_DIR, f"{project}_{ts}.json")
    with open(path, "w") as fh:
        json.dump(results, fh, default=str)
    return path


def _get_initial_results():
    """Return cached results for initial render (persists across navigation)."""
    with _cr_lock:
        results = _cr_state.get("results")
    if results:
        return _render_results(results, [])
    return None


def render_code_review_panel(cluster_id=None, region=None):
    """Render the code review panel."""
    previous_runs = _load_previous_runs()

    # Build dropdown menu items for previous runs
    history_items = []
    for run in previous_runs[:10]:
        history_items.append(
            dbc.DropdownMenuItem(
                run["label"],
                id={"type": "cr-history-item", "path": run["value"]},
                style={"fontSize": ".78rem"},
            )
        )
    if not history_items:
        history_items = [dbc.DropdownMenuItem("No previous runs", disabled=True,
                                              style={"fontSize": ".78rem", "color": "var(--text-muted)"})]

    return html.Div([
        # Header
        html.Div([
            html.Span("Application Code Review", className="section-title",
                      style={"marginBottom": "0", "borderBottom": "none", "paddingBottom": "0"}),
        ], style={"marginBottom": ".6rem", "paddingBottom": ".4rem",
                  "borderBottom": "2px solid var(--color-primary)"}),

        # Help text
        html.Div("Enter a path to scan or select a previous run from the dropdown.",
                 style={"fontSize": ".75rem", "color": "var(--text-muted)",
                        "marginBottom": ".4rem"}),

        # Unified input: [Input] [▾] [Go button]
        html.Div([
            dbc.Input(id="code-review-target-dir",
                      placeholder="/path/to/application source code",
                      size="sm", style={"flex": "1", "fontSize": ".82rem"}),
            dbc.DropdownMenu(
                history_items,
                label="", color="secondary", size="sm", align_end=True,
                toggle_style={"fontSize": ".75rem", "padding": ".3rem .5rem",
                              "marginLeft": "-1px", "borderRadius": "0 4px 4px 0"},
                toggle_class_name="dropdown-toggle-split",
            ),
            dbc.Button("Scan", id="code-review-run-btn", color="warning",
                       size="sm", style={"fontWeight": "600", "whiteSpace": "nowrap",
                                         "marginLeft": ".75rem", "minWidth": "5.5rem"}),
        ], style={"display": "flex", "alignItems": "center", "marginBottom": ".6rem"}),

        # Hidden stores
        dbc.Input(id="code-review-output-dir", style={"display": "none"}),
        dcc.Store(id="cr-results-store", data=None),
        dcc.Interval(id="cr-poll", interval=800, disabled=True),
        # Hidden for callback compatibility
        dcc.Dropdown(id="cr-previous-runs", style={"display": "none"}),

        # Progress area
        html.Div(id="cr-progress"),

        # Results — show cached if available
        html.Div(id="code-review-results", children=_get_initial_results()),

        # AI Suggestions (populated after scan)
        html.Div(id="cr-ai-suggestions"),

        # Download
        dcc.Download(id="code-review-download"),
    ])


# ── AI Suggestions ───────────────────────────────────────────────────────────

def _load_advisor_context():
    """Load SKILL.md and best-practices reference for AI grounding."""
    skill_path = os.path.join(os.path.dirname(__file__), "..", "documentdb-advisor", "SKILL.md")
    ref_path = os.path.join(os.path.dirname(__file__), "..", "documentdb-advisor", "references", "best-practices.md")

    context_parts = []
    for path in [skill_path, ref_path]:
        try:
            with open(path, "r", encoding="utf-8") as f:
                text = f.read()
                # Strip YAML frontmatter
                if text.startswith("---"):
                    end = text.find("---", 3)
                    if end > 0:
                        text = text[end + 3:].strip()
                context_parts.append(text)
        except Exception:
            pass
    return "\n\n".join(context_parts)


def _generate_ai_suggestions(results, region="us-east-1"):
    """Generate DocumentDB-specific AI suggestions based on scan findings."""
    import boto3
    from bedrock_advisor import MODEL_ID, FALLBACK_MODEL_ID

    findings = results.get("findings", {})

    # Collect non-passing findings grouped by category (skip N/A)
    categories = [
        ("Connection Configuration", ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7"]),
        ("Connection Pooling", ["2.1", "2.2", "2.3", "2.4", "2.5"]),
        ("Timeout Settings", ["3.1", "3.2", "3.3", "3.4"]),
        ("Failover & HA", ["4.1", "4.2", "4.3"]),
        ("Exception Handling", ["5.1", "5.2", "5.3", "5.4", "5.5", "5.6"]),
        ("Idempotency", ["6.1", "6.2", "6.3", "6.4", "6.5"]),
        ("Cursor Management", ["7.1", "7.2", "7.3", "7.4"]),
        ("Lambda Integration", ["8.1", "8.2", "8.3", "8.4", "8.5", "8.6"]),
        ("Security", ["9.1", "9.2", "9.3", "9.4", "9.5"]),
        ("Monitoring", ["10.1"]),
        ("Query & Cost", ["11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8"]),
    ]

    issues_by_category = []
    for cat_name, items in categories:
        cat_issues = []
        for item_id in items:
            f = findings.get(item_id, {})
            status = f.get("status", "")
            finding = f.get("finding", "")
            if status in ("❌", "⚠️") and "N/A" not in finding:
                cat_issues.append(f"  - {item_id} [{status}] {finding}")
        if cat_issues:
            issues_by_category.append(f"Category: {cat_name}\n" + "\n".join(cat_issues))

    if not issues_by_category:
        return None

    # App context from results
    languages = results.get("languages", [])
    has_lambda = results.get("has_lambda", False)
    files_with_mongo = results.get("files_with_mongo", 0)
    source_files = results.get("source_files", 0)

    # Load reference context
    advisor_context = _load_advisor_context()

    system_prompt = f"""{advisor_context}

You are reviewing application source code that connects to Amazon DocumentDB.
Your role is to provide specific, actionable guidance for the issues found during the code scan.
Base your recommendations on the DocumentDB best practices reference above.
Do NOT provide generic database advice — every recommendation must be grounded in Amazon DocumentDB behavior and APIs."""

    user_msg = f"""I ran a DocumentDB client code best practices review on an application.

Application context:
- Languages detected: {', '.join(languages) if languages else 'unknown'}
- Lambda handlers: {'detected' if has_lambda else 'not detected'}
- Files with MongoDB/DocumentDB usage: {files_with_mongo}
- Source files scanned: {source_files}

Here are the checks that failed or need attention:

{chr(10).join(issues_by_category)}

For each failing or warning check, provide a recommendation in this exact format:

### Category Name
**[check_id]** — One sentence explaining why this matters for DocumentDB. Specific fix: `code or config change`. DocumentDB consideration: any gotcha.

Rules:
- Every recommendation MUST start with the check ID in bold (e.g., **1.1**, **2.3**)
- Group recommendations under their category heading (### Category Name)
- Be concise — max 2-3 sentences per check
- Code fixes must be for the detected language(s): {', '.join(languages) if languages else 'general'}
- Do NOT provide generic advice — every point must be DocumentDB-specific"""

    try:
        client = boto3.client("bedrock-runtime", region_name=region)
        for model_id in [MODEL_ID, FALLBACK_MODEL_ID]:
            try:
                resp = client.invoke_model(
                    modelId=model_id,
                    body=json.dumps({
                        "messages": [{"role": "user", "content": user_msg}],
                        "max_tokens": 2000,
                        "temperature": 0.2,
                        "system": system_prompt,
                        "anthropic_version": "bedrock-2023-05-31",
                    }),
                )
                result = json.loads(resp["body"].read())
                return result["content"][0]["text"].strip()
            except Exception as e:
                logging.getLogger(__name__).warning("AI suggestions model %s failed: %s", model_id, e)
                continue
    except Exception as e:
        logging.getLogger(__name__).error("AI suggestions failed: %s", e)

    return None



# ── Callbacks ────────────────────────────────────────────────────────────────

@callback(
    Output("cr-progress", "children"),
    Output("cr-poll", "disabled"),
    Output("code-review-run-btn", "disabled"),
    Input("code-review-run-btn", "n_clicks"),
    State("code-review-target-dir", "value"),
    prevent_initial_call=True,
)
def cb_start_review(n, target_dir):
    """Start the review or load from cache."""
    if not n:
        return no_update, no_update, no_update

    if not target_dir:
        return html.Span("⚠ Provide a source directory",
                         style={"fontSize": ".82rem", "color": "#D13212"}), True, False

    if not os.path.isdir(target_dir):
        return html.Span(f"⚠ Directory not found: {target_dir}",
                         style={"fontSize": ".82rem", "color": "#D13212"}), True, False

    # Security: validate path against denylist + allowlist
    from code_review_engine import validate_target_dir
    rejection = validate_target_dir(target_dir)
    if rejection:
        return html.Span(f"⚠ {rejection}",
                         style={"fontSize": ".82rem", "color": "#D13212"}), True, False

    from code_review_engine import start_code_review, get_review_state
    state = get_review_state()
    if state["running"]:
        return no_update, False, True

    started = start_code_review(target_dir, target_dir)
    if not started:
        return html.Span("⚠ Could not start review",
                         style={"fontSize": ".82rem", "color": "#D13212"}), True, False

    progress = html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span("Starting review...", style={"fontSize": ".82rem", "color": "var(--text-muted)"}),
    ], style={"display": "flex", "alignItems": "center", "padding": ".4rem 0"})
    return progress, False, True


@callback(
    Output("cr-progress", "children", allow_duplicate=True),
    Output("code-review-results", "children"),
    Output("cr-results-store", "data"),
    Output("cr-poll", "disabled", allow_duplicate=True),
    Output("code-review-run-btn", "disabled", allow_duplicate=True),
    Input("cr-poll", "n_intervals"),
    State("code-review-target-dir", "value"),
    prevent_initial_call=True,
)
def cb_poll_review(n, target_dir):
    """Poll review progress and AI generation."""
    from code_review_engine import get_review_state
    state = get_review_state()

    if not state["done"]:
        # Still running — show progress
        progress = html.Div([
            dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
            html.Span(state.get("progress", "Working..."),
                      style={"fontSize": ".82rem", "color": "var(--text-muted)"}),
        ], style={"display": "flex", "alignItems": "center", "padding": ".4rem 0"})
        return progress, no_update, no_update, False, True

    # Done
    if state["error"]:
        return (html.Span(f"✗ {state['error'][:80]}",
                          style={"fontSize": ".82rem", "color": "#D13212"}),
                no_update, no_update, True, False)

    if state["results"]:
        results = state["results"]

        # Check if we already saved and started AI
        with _cr_lock:
            prev = _cr_state.get("results")
            already_started = prev is not None and prev.get("report_path") == results.get("report_path")

        if not already_started:
            # First time seeing results — save and start AI
            if target_dir:
                _save_run(results, target_dir)
            with _cr_lock:
                _cr_state["results"] = results

            # Start AI in background
            import logging as _logging
            _ai_logger = _logging.getLogger(__name__)
            def _gen_ai():
                try:
                    _ai_logger.info("Starting AI suggestions generation...")
                    ai_md = _generate_ai_suggestions(results)
                    if ai_md:
                        results["_ai_suggestions"] = ai_md
                        with _cr_lock:
                            _cr_state["results"] = results
                        if target_dir:
                            _save_run(results, target_dir)
                        _ai_logger.info("AI suggestions generated successfully")
                    else:
                        _ai_logger.info("AI suggestions: no issues to advise on")
                        results["_ai_suggestions"] = ""  # Mark as done (empty)
                        with _cr_lock:
                            _cr_state["results"] = results
                except Exception as e:
                    _ai_logger.error("AI suggestions failed: %s", e, exc_info=True)
                    results["_ai_suggestions"] = ""  # Mark as done (failed)
                    with _cr_lock:
                        _cr_state["results"] = results
            threading.Thread(target=_gen_ai, daemon=True).start()

        # Check if AI is ready yet
        with _cr_lock:
            current = _cr_state.get("results", {})
        ai_ready = "_ai_suggestions" in current

        if ai_ready:
            # Render with AI
            return "", _render_results(current, []), current, True, False
        else:
            # Render without AI, keep polling
            return "", _render_results(results, []), results, False, True

    return "", no_update, no_update, True, False


@callback(
    Output("cr-progress", "children", allow_duplicate=True),
    Output("code-review-results", "children", allow_duplicate=True),
    Output("cr-results-store", "data", allow_duplicate=True),
    Output("code-review-target-dir", "value"),
    Output("code-review-run-btn", "children", allow_duplicate=True),
    Input({"type": "cr-history-item", "path": dash.ALL}, "n_clicks"),
    prevent_initial_call=True,
)
def cb_load_from_history(clicks):
    """Load a previous run when a history menu item is clicked."""
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update, no_update, no_update, no_update, no_update
    selected_file = ctx.triggered_id["path"]
    try:
        with open(selected_file) as fh:
            results = json.load(fh)
        target = results.get("target_dir", "")
        # Persist in module state
        with _cr_lock:
            _cr_state["results"] = results
        return "", _render_results(results, []), results, target, "📂 Loaded"
    except Exception as e:
        return (html.Span(f"✗ Failed to load: {e}",
                          style={"fontSize": ".82rem", "color": "#D13212"}),
                no_update, no_update, no_update, no_update)


@callback(
    Output("code-review-results", "children", allow_duplicate=True),
    Input("cr-filter-checklist", "value"),
    State("cr-results-store", "data"),
    prevent_initial_call=True,
)
def cb_cr_filter(active_filters, results):
    """Re-render results when filter changes."""
    if not results:
        return no_update
    return _render_results(results, active_filters or [])


@callback(
    Output("code-review-download", "data"),
    Input("code-review-download-btn", "n_clicks"),
    prevent_initial_call=True,
)
def cb_download_review(n):
    """Download the review report as a Markdown (.md) file."""
    if not n:
        return no_update

    from code_review_engine import get_review_state
    state = get_review_state()
    if not state["results"] or not state["results"].get("report_path"):
        return no_update

    report_path = state["results"]["report_path"]
    if not os.path.exists(report_path):
        return no_update

    return dcc.send_file(report_path)



# ── Results Rendering ────────────────────────────────────────────────────────

def _render_results(results, active_filters=None):
    """Render code review results — WA-style with side-by-side panels."""
    if active_filters is None:
        active_filters = []
    findings = results["findings"]

    score_color = "#1D8102" if results["compliance_pct"] >= 70 else (
        "#906806" if results["compliance_pct"] >= 40 else "#E07020")

    # Score + filter bar
    score_bar = html.Div([
        html.Span([
            html.Span("Compliance: ", style={"color": "var(--text-muted)"}),
            html.Span(f"{results['compliance_pct']}%", style={"color": score_color}),
        ], style={"fontSize": ".82rem", "fontWeight": "700", "marginRight": "1rem"}),
        dbc.Checklist(
            id="cr-filter-checklist",
            options=[
                {"label": html.Span(f"✓ Pass ({results['compliant']})", style={"color": "#1D8102"}), "value": "✅"},
                {"label": html.Span(f"! Warn ({results['warning']})", style={"color": "#906806"}), "value": "⚠️"},
                {"label": html.Span(f"✗ Fail ({results['non_compliant']})", style={"color": "#D13212"}), "value": "❌"},
                {"label": html.Span(f"☐ N/A ({results['na']})", style={"color": "#8d99a8"}), "value": "☐ N/A"},
            ],
            value=active_filters,
            inline=True,
            inputStyle={"marginRight": ".3rem"},
            labelStyle={"marginRight": ".75rem", "fontSize": ".78rem",
                        "fontWeight": "600", "cursor": "pointer"},
        ),
        html.Div([
            dbc.Button("Expand All", id="cr-expand-all", color="link", size="sm",
                       style={"fontSize": ".72rem", "padding": ".1rem .3rem", "fontWeight": "600"}),
            dbc.Button("Collapse All", id="cr-collapse-all", color="link", size="sm",
                       style={"fontSize": ".72rem", "padding": ".1rem .3rem", "fontWeight": "600"}),
        ], style={"marginLeft": "auto", "display": "flex", "gap": ".2rem"}),
    ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap",
              "padding": ".3rem .5rem", "marginBottom": ".5rem",
              "background": "var(--bg-surface-alt)",
              "border": "1px solid var(--border-default)", "borderRadius": "8px"})

    # Categories
    categories = [
        ("1. Connection Configuration", ["1.1", "1.2", "1.3", "1.4", "1.5", "1.6", "1.7"]),
        ("2. Connection Pooling", ["2.1", "2.2", "2.3", "2.4", "2.5"]),
        ("3. Timeout Settings", ["3.1", "3.2", "3.3", "3.4"]),
        ("4. Failover & HA", ["4.1", "4.2", "4.3"]),
        ("5. Exception Handling", ["5.1", "5.2", "5.3", "5.4", "5.5", "5.6"]),
        ("6. Idempotency", ["6.1", "6.2", "6.3", "6.4", "6.5"]),
        ("7. Cursor Management", ["7.1", "7.2", "7.3", "7.4"]),
        ("8. Lambda Integration", ["8.1", "8.2", "8.3", "8.4", "8.5", "8.6"]),
        ("9. Security", ["9.1", "9.2", "9.3", "9.4", "9.5"]),
        ("10. Monitoring", ["10.1"]),
        ("11. Query & Cost", ["11.1", "11.2", "11.3", "11.4", "11.5", "11.6", "11.7", "11.8"]),
    ]

    def _build_category(cat_name, items):
        visible_items = items
        if active_filters:
            visible_items = [i for i in items
                            if findings.get(i, {}).get("status", "☐ N/A") in active_filters]
        if not visible_items:
            return None

        statuses = [findings.get(i, {}).get("status", "☐ N/A") for i in visible_items]
        n_pass = statuses.count("✅")
        n_warn = statuses.count("⚠️")
        n_fail = statuses.count("❌")

        badges = []
        if n_fail:
            badges.append(html.Span(f"✗{n_fail}", style={"fontSize": ".68rem", "color": "#D13212", "fontWeight": "700", "marginLeft": ".4rem"}))
        if n_warn:
            badges.append(html.Span(f"!{n_warn}", style={"fontSize": ".68rem", "color": "#906806", "fontWeight": "700", "marginLeft": ".4rem"}))
        if n_pass:
            badges.append(html.Span(f"✓{n_pass}", style={"fontSize": ".68rem", "color": "#1D8102", "fontWeight": "700", "marginLeft": ".4rem"}))

        hl_col = "#D13212" if n_fail else "#906806" if n_warn else "var(--text-muted)"
        hl_bg = "rgba(209,50,18,.04)" if n_fail else "rgba(144,104,6,.04)" if n_warn else "var(--bg-surface-alt)"

        check_items = []
        for item_id in visible_items:
            f = findings.get(item_id, {"status": "☐ N/A", "finding": "Not evaluated"})
            check_items.append(html.Div([
                html.Span(f["status"], style={"fontSize": ".75rem", "width": "1.4rem",
                                              "display": "inline-block", "textAlign": "center",
                                              "flexShrink": "0"}),
                html.Span(item_id, style={"fontSize": ".68rem", "fontWeight": "700",
                                          "color": "var(--text-muted)", "fontFamily": "monospace",
                                          "marginRight": ".3rem", "marginLeft": ".1rem",
                                          "flexShrink": "0"}),
                html.Span(f.get("finding", ""), style={"fontSize": ".78rem", "color": "var(--text-body)"}),
            ], style={"padding": ".25rem .5rem", "borderBottom": "1px solid var(--border-default)",
                      "display": "flex", "alignItems": "baseline"}))

        return html.Details([
            html.Summary([
                html.Span(cat_name, style={"fontSize": ".72rem", "fontWeight": "700",
                                            "color": hl_col, "letterSpacing": ".3px"}),
                *badges,
            ], style={"padding": ".25rem .5rem", "background": hl_bg,
                      "borderBottom": "1px solid var(--border-default)",
                      "borderLeft": f"3px solid {hl_col}",
                      "cursor": "pointer", "display": "flex", "alignItems": "center",
                      "listStyle": "none"}),
            html.Div(check_items),
        ], open=bool(n_fail), className="cr-subsection")

    # Two-column layout — 5 left, 6 right
    mid = 5
    left_cats = [c for c in [_build_category(n, i) for n, i in categories[:mid]] if c is not None]
    right_cats = [c for c in [_build_category(n, i) for n, i in categories[mid:]] if c is not None]

    panels = html.Div([
        html.Div(left_cats, style={"flex": "1", "minWidth": "0",
                                    "border": "1px solid var(--border-default)",
                                    "borderRadius": "6px", "overflow": "hidden"}),
        html.Div(right_cats, style={"flex": "1", "minWidth": "0",
                                     "border": "1px solid var(--border-default)",
                                     "borderRadius": "6px", "overflow": "hidden"}),
    ], style={"display": "flex", "gap": ".75rem", "marginBottom": ".5rem"}, id="cr-results-panels")

    # Report link
    report_path = results.get("report_path", "")
    report_link = html.Div([
        html.Code(os.path.basename(report_path), style={"fontSize": ".72rem", "color": "var(--text-muted)"}),
        dbc.Button("📥 Markdown", id="code-review-download-btn", color="secondary",
                   outline=True, size="sm",
                   style={"fontWeight": "600", "fontSize": ".72rem", "marginLeft": ".5rem"}),
    ], style={"display": "flex", "alignItems": "center", "padding": ".2rem 0"}) if report_path else html.Div()

    # AI Suggestions section
    ai_md = results.get("_ai_suggestions")
    ai_section = html.Div()
    if ai_md:
        ai_section = html.Details([
            html.Summary([
                html.Span("AI Recommendations", style={"fontSize": ".78rem", "fontWeight": "700",
                                                           "color": "var(--color-primary)"}),
            ], style={"padding": ".3rem .6rem", "background": "var(--bg-surface-alt)",
                      "borderBottom": "1px solid var(--border-default)",
                      "borderLeft": "3px solid var(--color-primary)",
                      "cursor": "pointer", "display": "flex", "alignItems": "center",
                      "listStyle": "none"}),
            html.Div(
                dcc.Markdown(ai_md, className="cr-ai-md"),
                style={"padding": ".4rem .6rem"},
            ),
        ], open=True, className="cr-subsection",
           style={"marginTop": ".5rem", "border": "1px solid var(--border-default)",
                  "borderRadius": "6px", "overflow": "hidden"})

    return html.Div([score_bar, panels, ai_section, report_link])
