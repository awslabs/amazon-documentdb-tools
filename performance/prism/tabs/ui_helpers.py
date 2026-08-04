"""Shared UI components for consistent styling across all tabs."""
import dash_bootstrap_components as dbc
from dash import html, dash_table


def metric_card(value, label, accent=""):
    cls = "metric-card"
    if accent:
        cls += f" accent-{accent}"
    return dbc.Card(dbc.CardBody([
        html.H4(value, className="mb-0"),
        html.P(label, className="mb-0"),
    ]), className=cls)


def section_title(text):
    return html.Div(text, className="section-title")


def data_table(data, page_size=20):
    if not data:
        return html.P("No data available.", className="text-muted")
    columns = [{"name": c, "id": c} for c in data[0].keys()]
    # Tooltip: show full cell value on hover for any cell with truncated text
    tooltip_data = [
        {col: {"value": str(row.get(col, "")), "type": "text"}
         for col in data[0].keys()}
        for row in data
    ]
    return dash_table.DataTable(
        data=data,
        columns=columns,
        style_table={"overflowX": "auto", "tableLayout": "fixed"},
        style_cell={"textAlign": "left", "padding": "8px 12px", "fontSize": ".82rem",
                     "fontFamily": '"Amazon Ember", "Noto Sans", Arial, sans-serif',
                     "backgroundColor": "#ffffff", "color": "#545b64",
                     "borderBottom": "1px solid #eaeded", "border": "none",
                     "overflow": "hidden", "textOverflow": "ellipsis",
                     "maxWidth": "300px", "whiteSpace": "nowrap"},
        style_cell_conditional=[
            {"if": {"column_id": "Collection"},
             "maxWidth": "420px", "fontWeight": "600", "fontFamily": "monospace"},
            {"if": {"column_id": "Recommendation"},
             "maxWidth": "350px", "whiteSpace": "normal", "wordBreak": "break-word"},
        ],
        style_header={"backgroundColor": "#fafafa", "color": "#16191f",
                       "fontWeight": "700", "fontSize": ".75rem", "textTransform": "uppercase",
                       "letterSpacing": ".4px", "borderBottom": "2px solid #eaeded",
                       "border": "none"},
        style_data_conditional=[
            {"if": {"row_index": "odd"}, "backgroundColor": "#fafafa"},
        ],
        tooltip_data=tooltip_data,
        tooltip_duration=None,
        page_size=page_size,
    )


def code_block(text):
    return html.Pre(text, className="code-block")


def info_tip(text):
    return dbc.Alert(f"\U0001f4a1  {text}", className="info-tip mt-2")


def wire_animation(phases=None, active_phase=None, connected=False, message=None):
    """Loading spinner for connection state."""
    import dash_bootstrap_components as dbc
    if connected:
        msg = "✅  Connected"
        color = "success"
    else:
        msg = message or (phases[active_phase] if phases and active_phase is not None and active_phase < len(phases) else "Loading DocumentDB fleet data...")
        color = "warning"
    return html.Div([
        dbc.Spinner(size="sm", color=color, spinner_class_name="me-2"),
        html.Span(msg, style={"fontSize": ".88rem", "color": "var(--text-muted)"}),
    ], className="d-flex align-items-center justify-content-center py-3")


def coffee_animation(phase_label="Loading cluster data...", pct=0):
    """Loading spinner for data loading state."""
    import dash_bootstrap_components as dbc
    return html.Div([
        dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2"),
        html.Span(phase_label, style={"fontSize": ".88rem", "color": "var(--text-muted)"}),
    ], className="d-flex align-items-center justify-content-center py-3")
