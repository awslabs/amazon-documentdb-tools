"""AI Advisor — agentic chat powered by Bedrock + MCP tools."""
import logging
import threading
import json
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update, ctx, ALL
from tabs.ui_helpers import section_title

logger = logging.getLogger(__name__)

_chat = {"messages": [], "processing": False}
_chat_lock = threading.Lock()


def _build_context_summary(conn_data, analysis_data, db_name, coll_name=""):
    """Build deterministic context. Sends full data, trims only if over token limit."""
    TOKEN_LIMIT_CHARS = 500000  # ~125K tokens, well under 200K model limit
    parts = []

    # Cluster info
    if conn_data:
        parts.append(f"CLUSTER: {conn_data.get('cluster_id', '?')} (region: {conn_data.get('region', '?')})")

    # Tree data — all databases with collections and indexes
    tree_data = conn_data.get("tree_data", {}) if conn_data else {}
    analyzed_dbs = conn_data.get("analyzed_dbs", []) if conn_data else []

    parts.append("")
    parts.append("ALL DATABASES:")
    for tdb in sorted(tree_data.keys()):
        colls = tree_data[tdb]
        if not isinstance(colls, dict):
            continue
        total_docs = sum(c.get("count", 0) for c in colls.values() if isinstance(c, dict))
        n_colls = len(colls)
        coll_names = sorted(colls.keys())
        status = "ANALYZED" if tdb in analyzed_dbs else "basic"
        parts.append(f"  {tdb} [{status}]: {n_colls} collections, {total_docs:,} docs — {', '.join(coll_names)}")

    # Full analysis data for ALL analyzed databases
    if analysis_data:
        parts.append("")
        parts.append("DETAILED ANALYSIS:")
        for adb in sorted(analysis_data.keys()):
            colls = analysis_data[adb]
            if not isinstance(colls, dict):
                continue
            parts.append(f"  Database: {adb}")
            for cname in sorted(colls.keys()):
                cdata = colls[cname]
                if not isinstance(cdata, dict) or "error" in cdata:
                    continue
                count = cdata.get("count", 0)
                storage = cdata.get("storageSize", 0)
                size = cdata.get("size", 0)
                avg_obj = cdata.get("avgObjSize", 0)
                comp = "enabled" if cdata.get("compression", {}).get("enabled") else "disabled"
                bloat = cdata.get("unusedStorageSize", {}).get("unusedPercent", 0)
                ia = cdata.get("index_analysis", {})
                n_idx = ia.get("total_indexes", 0)
                unused = [i["name"] for i in ia.get("unused_indexes", [])]
                low_card = [i["name"] for i in ia.get("low_cardinality_indexes", [])]

                parts.append(f"    {cname}: {count:,} docs, storage={storage}, uncompressed={size}, "
                             f"avgDoc={avg_obj}, compression={comp}, bloat={bloat:.1f}%, "
                             f"indexes={n_idx}, unused={unused}, lowCard={low_card}")

                # Per-index details
                for idx in cdata.get("indexes", []):
                    if idx.get("name") == "_id_":
                        continue
                    u = idx.get("usage", {})
                    c = idx.get("cardinality", {})
                    b = idx.get("bloat", {})
                    parts.append(f"      idx '{idx['name']}': fields={list(idx.get('fields', {}).keys())}, "
                                 f"ops={u.get('ops_count', 0)}, unused={u.get('potential_unused', False)}, "
                                 f"cardinality={c.get('percentage', 0):.2f}%, bloat={b.get('unusedPercent', 0):.1f}%")

    result = "\n".join(parts)

    # Only trim if over the limit
    if len(result) > TOKEN_LIMIT_CHARS:
        # Rebuild with summaries instead of full detail
        parts_trimmed = []
        parts_trimmed.append(parts[0] if parts else "")  # cluster info
        parts_trimmed.append("")
        parts_trimmed.append("ALL DATABASES:")
        for tdb in sorted(tree_data.keys()):
            colls = tree_data[tdb]
            if not isinstance(colls, dict):
                continue
            total_docs = sum(c.get("count", 0) for c in colls.values() if isinstance(c, dict))
            coll_names = sorted(colls.keys())
            status = "ANALYZED" if tdb in analyzed_dbs else "basic"
            parts_trimmed.append(f"  {tdb} [{status}]: {len(colls)} collections, {total_docs:,} docs — {', '.join(coll_names[:10])}")

        # Current database gets full detail
        if analysis_data and db_name and db_name in analysis_data:
            parts_trimmed.append("")
            parts_trimmed.append(f"DETAILED ANALYSIS FOR '{db_name}':")
            colls = analysis_data[db_name]
            for cname in sorted(colls.keys()):
                cdata = colls[cname]
                if not isinstance(cdata, dict) or "error" in cdata:
                    continue
                count = cdata.get("count", 0)
                storage = cdata.get("storageSize", 0)
                ia = cdata.get("index_analysis", {})
                unused = [i["name"] for i in ia.get("unused_indexes", [])]
                low_card = [i["name"] for i in ia.get("low_cardinality_indexes", [])]
                parts_trimmed.append(f"    {cname}: {count:,} docs, storage={storage}, "
                                     f"indexes={ia.get('total_indexes', 0)}, unused={unused}, lowCard={low_card}")
                if cname == coll_name:
                    for idx in cdata.get("indexes", []):
                        if idx.get("name") == "_id_":
                            continue
                        u = idx.get("usage", {})
                        parts_trimmed.append(f"      idx '{idx['name']}': ops={u.get('ops_count', 0)}, unused={u.get('potential_unused', False)}")

        # Other analyzed dbs — summary only
        if analysis_data:
            other = [k for k in analysis_data if k != db_name and isinstance(analysis_data[k], dict)]
            if other:
                parts_trimmed.append("")
                parts_trimmed.append("OTHER ANALYZED (summary):")
                for odb in other:
                    oc = analysis_data[odb]
                    parts_trimmed.append(f"  {odb}: {len(oc)} collections, "
                                         f"{sum(c.get('count', 0) for c in oc.values() if isinstance(c, dict)):,} docs")

        parts_trimmed.append("")
        parts_trimmed.append("(Context was trimmed to fit token limit. Use MCP tools for details on other databases.)")
        result = "\n".join(parts_trimmed)

    return result if result.strip() else "No data available. Use MCP tools to query the cluster."

def _run_advisor_bg(question, conn_str, db_context, region, analysis_data, conn_data):
    """Run the two-step advisor via chat_advisor."""
    from chat_advisor import send_message as _ca_send
    conn_meta = {
        "connection_string": conn_str,
        "cluster_id": db_context.get("cluster_id", ""),
        "region": region,
        "log_group": (conn_data or {}).get("log_group", ""),
        "databases": (conn_data or {}).get("databases", []),
    }
    db_name = db_context.get("database", "")
    _ca_send(question, analysis_data, db_name, conn_meta=conn_meta, region=region)


def _append_ai(content):
    with _chat_lock:
        _chat["messages"].append({"role": "ai", "content": content})
        _chat["processing"] = False


# ── Suggested questions ──────────────────────────────────────────────────────
SUGGESTIONS = [
    "Which is the largest database?",
    "Show me all collections in this database",
    "Are there any unused indexes?",
    "What's the schema of this collection?",
    "How many documents are in each collection?",
    "Explain the query plan for a find on this collection",
]


def render_chat_panel(conn_str="", db_name="", coll_name="", cluster_id="", region="us-east-1"):
    return html.Div([
        section_title("\U0001f916  AI Advisor"),

        # Context
        html.Div([
            html.Small([
                html.Span("\U0001f4e1 Connected", style={"color": "var(--color-primary)", "fontWeight": "600"}),
                html.Span(f"  \u00b7  {db_name or 'No database selected'}", style={"color": "var(--text-muted)"}),
                html.Span(f"  \u00b7  {coll_name}" if coll_name else "", style={"color": "var(--text-muted)"}),
            ], style={"fontSize": ".78rem"}),
        ], className="mb-2"),

        dcc.Store(id="advisor-context", data={
            "conn_str": conn_str, "db_name": db_name,
            "coll_name": coll_name, "cluster_id": cluster_id, "region": region,
        }),

        # Suggested questions
        html.Div([
            html.Small("Try asking:", className="d-block mb-1",
                       style={"fontWeight": "600", "color": "var(--text-muted)", "fontSize": ".72rem"}),
            html.Div([
                html.Div(q, className="advisor-suggest-chip",
                         id={"type": "advisor-suggest", "q": q})
                for q in SUGGESTIONS
            ], className="d-flex flex-wrap gap-1"),
        ], className="mb-3"),

        # Messages — restore from session if any exist
        html.Div(id="advisor-messages", className="advisor-messages mb-3",
                 children=_render_messages(_chat["messages"]),
                 style={"minHeight": "250px", "maxHeight": "500px", "overflowY": "auto",
                        "padding": ".75rem", "border": "1px solid var(--border-default)",
                        "borderRadius": "10px", "background": "var(--bg-surface-alt)"}),

        # Input
        html.Div([
            dbc.Input(id="advisor-input", placeholder="Ask about your database...",
                      size="sm", className="me-2", style={"flex": "1"},
                      n_submit=0),
            dbc.Button("\u27a4", id="advisor-send-btn", color="warning", size="sm",
                       style={"fontWeight": "700", "width": "38px"}),
        ], className="d-flex"),

        dcc.Interval(id="advisor-poll", interval=500, disabled=not _chat.get("processing", False)),
    ])


def _render_messages(messages):
    if not messages:
        return html.Div([
            html.Div("\U0001f916  Hi! I'm your DocumentDB advisor.", className="mb-2",
                     style={"fontSize": ".9rem", "fontWeight": "600"}),
            html.Div("I can query your cluster live, analyze schemas, explain queries, "
                     "and provide optimization recommendations. Ask me anything!",
                     className="text-muted", style={"fontSize": ".85rem"}),
        ], style={"padding": ".5rem"})

    items = []
    for msg in messages:
        if msg["role"] == "user":
            items.append(html.Div(
                html.Div(msg["content"], className="advisor-bubble advisor-bubble--user"),
                className="d-flex justify-content-end mb-2",
            ))
        else:
            items.append(html.Div(
                html.Div(
                    dcc.Markdown(msg["content"], className="advisor-md"),
                    className="advisor-bubble advisor-bubble--ai",
                ),
                className="d-flex justify-content-start mb-2",
            ))
    return html.Div(items)


def _start_advisor(question, context, analysis_data, conn_data=None):
    """Common function to start the advisor thread."""
    conn_str = context.get("conn_str", "")
    region = context.get("region", "us-east-1")
    db_context = {
        "database": context.get("db_name"),
        "collection": context.get("coll_name"),
        "cluster_id": context.get("cluster_id"),
    }

    with _chat_lock:
        _chat["messages"].append({"role": "user", "content": question})
        _chat["processing"] = True
    # Also clear chat_advisor cache on first message
    from chat_advisor import clear_chat as _ca_clear
    if len(_chat["messages"]) == 1:
        _ca_clear()

    threading.Thread(target=_run_advisor_bg,
                     args=(question, conn_str, db_context, region, analysis_data, conn_data),
                     daemon=True).start()


# ── Send button click ────────────────────────────────────────────────────────
@callback(
    Output("advisor-messages", "children", allow_duplicate=True),
    Output("advisor-input", "value"),
    Output("advisor-poll", "disabled", allow_duplicate=True),
    Input("advisor-send-btn", "n_clicks"),
    Input("advisor-input", "n_submit"),
    State("advisor-input", "value"),
    State("advisor-context", "data"),
    State("analysis-store", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_advisor_send(n_click, n_submit, question, context, analysis_data, conn_data):
    if not question or not question.strip():
        return no_update, no_update, True
    _start_advisor(question.strip(), context, analysis_data, conn_data)
    return _render_messages(_chat["messages"]), "", False


# ── Suggested question click ─────────────────────────────────────────────────
@callback(
    Output("advisor-messages", "children", allow_duplicate=True),
    Output("advisor-poll", "disabled", allow_duplicate=True),
    Input({"type": "advisor-suggest", "q": ALL}, "n_clicks"),
    State("advisor-context", "data"),
    State("analysis-store", "data"),
    State("app-conn-data", "data"),
    prevent_initial_call=True,
)
def cb_advisor_suggest(clicks, context, analysis_data, conn_data):
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update, True
    question = ctx.triggered_id["q"]
    _start_advisor(question, context, analysis_data, conn_data)
    return _render_messages(_chat["messages"]), False


# ── Poll for responses ───────────────────────────────────────────────────────
@callback(
    Output("advisor-messages", "children"),
    Output("advisor-poll", "disabled"),
    Input("advisor-poll", "n_intervals"),
    prevent_initial_call=True,
)
def cb_advisor_poll(n):
    # Read from chat_advisor state
    from chat_advisor import get_pending_state, get_chat_messages
    pending, response, error = get_pending_state()
    ca_messages = get_chat_messages()

    # Sync chat_advisor messages into local _chat for rendering
    if ca_messages:
        with _chat_lock:
            local_count = len(_chat["messages"])
            for msg in ca_messages[local_count:]:
                _chat["messages"].append(msg)
            messages = list(_chat["messages"])
            processing = pending
            if not pending:
                _chat["processing"] = False
    else:
        with _chat_lock:
            processing = _chat["processing"]
            messages = list(_chat["messages"])

    rendered = _render_messages(messages)

    if processing:
        typing = html.Div(
            html.Div([
                dbc.Spinner(size="sm", color="secondary", spinner_class_name="me-2"),
                html.Span("Querying cluster & thinking...",
                          style={"fontSize": ".82rem", "color": "var(--text-muted)"}),
            ], className="advisor-bubble advisor-bubble--ai"),
            className="d-flex justify-content-start mb-2",
        )
        return html.Div([rendered, typing]), False

    return rendered, True


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_tab
register_tab("chat", "", "AI Advisor", "ai", render_chat_panel)
