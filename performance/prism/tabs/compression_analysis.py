"""Compression Analysis tab — async background processing with live updates."""
import logging
import threading
import dash_bootstrap_components as dbc
from dash import html, dcc, callback, Input, Output, State, no_update
from tabs.ui_helpers import section_title, data_table, info_tip
from compression_analyzer import analyze_collection_compression

logger = logging.getLogger(__name__)

_comp = {"rows": [], "skipped": 0, "total": 0, "done_count": 0, "running": False, "done": False}
_comp_lock = threading.Lock()


def _run_compression_bg(analysis_data, database_name, connection_string):
    collections = []
    for coll, cd in analysis_data.get(database_name, {}).items():
        if isinstance(cd, dict) and 'error' not in cd:
            collections.append((coll, cd))

    with _comp_lock:
        _comp.update(rows=[], skipped=0, total=len(collections), done_count=0, running=True, done=False)

    for coll, cd in collections:
        try:
            r = analyze_collection_compression(connection_string, database_name, coll)
        except Exception as e:
            logger.warning("Compression failed for %s.%s: %s", database_name, coll, e)
            with _comp_lock:
                _comp["skipped"] += 1
                _comp["done_count"] += 1
            continue

        if 'error' in r:
            logger.warning("Compression error for %s.%s: %s", database_name, coll, r['error'])
            with _comp_lock:
                _comp["skipped"] += 1
                _comp["done_count"] += 1
            continue

        enabled = cd.get('compression', {}).get('enabled', False)
        rec = r['recommendation']
        row = {
            'Collection': coll,
            'Status': "✅ Enabled" if enabled else "❌ Disabled",
            'Samples': r['sample_size'],
            'Original (KB)': f"{r['original_size_bytes']/1024:.1f}",
            'LZ4 Ratio': r['algorithms']['lz4_fast']['compression_ratio'],
            'LZ4 Speed (MB/s)': r['algorithms']['lz4_fast']['speed_mbps'],
            'ZSTD Ratio': r['algorithms']['zstd_dict']['compression_ratio'],
            'ZSTD Speed (MB/s)': r['algorithms']['zstd_dict']['speed_mbps'],
            'Recommendation': rec[:55] + "…" if len(rec) > 55 else rec,
        }
        with _comp_lock:
            _comp["rows"].append(row)
            _comp["done_count"] += 1

    with _comp_lock:
        _comp["running"] = False
        _comp["done"] = True


def render_compression_analysis(analysis_data, database_name, connection_string):
    if database_name not in analysis_data:
        return dbc.Alert("No data available.", color="info")
    if not connection_string:
        return html.Div([section_title("Compression Analysis"),
                         dbc.Alert("Connection string not available.", color="warning")])

    n = sum(1 for cd in analysis_data[database_name].values()
            if isinstance(cd, dict) and 'error' not in cd)

    threading.Thread(target=_run_compression_bg,
                     args=(analysis_data, database_name, connection_string), daemon=True).start()

    # Enabled collections (instant from collStats)
    enabled_rows = []
    for coll, cd in analysis_data[database_name].items():
        if isinstance(cd, dict) and 'error' not in cd:
            ci = cd.get('compression', {})
            if ci.get('enabled'):
                enabled_rows.append({'Collection': coll, 'Status': '✅ Enabled',
                                     'Threshold': ci.get('threshold', 'N/A')})

    enabled_section = []
    if enabled_rows:
        enabled_section = [
            html.Div(section_title("Compression-Enabled Collections"), className="mt-4"),
            data_table(enabled_rows),
        ]
    else:
        enabled_section = [
            dbc.Alert("ℹ️  No collections currently have compression enabled",
                      color="info", className="mt-3"),
        ]

    return html.Div([
        section_title("Compression Analysis"),
        *enabled_section,
        html.Hr(className="my-3"),
        html.Div(id="comp-loading-area", children=[
            html.Div([
                dbc.Spinner(size="sm", color="primary", spinner_class_name="me-2"),
                html.Span(f"Running compression tests on {n} collection(s)…",
                          style={"fontSize": ".88rem", "color": "#5f6b7a"}),
            ], className="d-flex align-items-center mb-2"),
            dbc.Progress(value=0, color="info",
                         style={"height": "4px", "borderRadius": "2px"}, className="mb-3"),
        ]),
        html.Div(id="comp-results-area"),
        dcc.Interval(id="comp-poll-interval", interval=600, disabled=False),
    ])


@callback(
    Output("comp-results-area", "children"),
    Output("comp-loading-area", "children"),
    Output("comp-poll-interval", "disabled"),
    Input("comp-poll-interval", "n_intervals"),
    prevent_initial_call=True,
)
def cb_comp_poll(n):
    with _comp_lock:
        rows = list(_comp["rows"])
        skipped = _comp["skipped"]
        total = _comp["total"]
        done_count = _comp["done_count"]
        done = _comp["done"]

    pct = int((done_count / max(total, 1)) * 100)

    results_children = []
    if rows:
        results_children.append(data_table(rows))

    if not done:
        loading = html.Div([
            html.Div([
                dbc.Spinner(size="sm", color="primary", spinner_class_name="me-2"),
                html.Span(f"Analyzing… {done_count}/{total} collections ({pct}%)",
                          style={"fontSize": ".88rem", "color": "#5f6b7a"}),
            ], className="d-flex align-items-center mb-2"),
            dbc.Progress(value=pct, color="info",
                         style={"height": "4px", "borderRadius": "2px"}, className="mb-3"),
        ])
        return results_children or no_update, loading, False

    # Done
    final = []
    if skipped:
        final.append(dbc.Alert(
            f"ℹ️  {skipped} collection(s) skipped — see server logs for details.",
            color="info", className="mb-2", style={"fontSize": ".85rem"}))
    if rows:
        final.append(data_table(rows))
        final.append(info_tip("Lower ratio = better compression. Ratio ≥ 1.0 means compression is not beneficial."))
    else:
        final.append(dbc.Alert("No compression data could be collected.", color="info"))

    done_msg = html.Div([
        html.Span("✅", style={"marginRight": ".4rem"}),
        html.Span(f"Compression analysis complete — {len(rows)} of {total} collection(s) tested",
                  style={"fontSize": ".85rem", "color": "#037f0c", "fontWeight": "600"}),
    ], className="mb-2")

    return final, done_msg, True


# ── Self-registration ────────────────────────────────────────────────────────
from tabs.registry import register_db_tab
register_db_tab("compression", "\U0001f5dc\ufe0f", "Compression", render_compression_analysis)
