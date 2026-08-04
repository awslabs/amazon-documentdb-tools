"""WA Review v2 — shared UI components, constants, helpers."""
import re
import dash_bootstrap_components as dbc
from dash import html

# ── Pillar metadata ───────────────────────────────────────────────────────────

PILLAR_ICONS = {
    "Reliability":            "🛡️",
    "Security":               "🔒",
    "Operational Excellence": "⚙️",
    "Performance Efficiency": "🚀",
    "Cost Optimization":      "💰",
    "Sustainability":         "🌱",
}

PILLAR_COLORS = {
    "Reliability":            "#d45b07",
    "Security":               "#dd344c",
    "Operational Excellence": "#5e6b7a",
    "Performance Efficiency": "#8c4fff",
    "Cost Optimization":      "#067f68",
    "Sustainability":         "#0972d3",
}

STATUS_SYMBOL = {
    "pass": ("✓", "#1D8102"),
    "warn": ("!", "#906806"),
    "fail": ("✗", "#D13212"),
    "info": ("ℹ", "#687078"),
}

STATUS_BG = {
    "pass": "rgba(29,129,2,.04)",
    "warn": "rgba(144,104,6,.04)",
    "fail": "rgba(209,50,18,.04)",
    "info": "transparent",
}

PRI_COLORS = {
    "Critical": "#D13212",
    "High":     "#D13212",
    "Medium":   "#906806",
    "Low":      "#687078",
}

# ── Column widths — fixed so all rows align perfectly ─────────────────────────
_SYM_W  = "1.6rem"   # symbol column
_ID_W   = "4.8rem"   # ID column
_INDENT = "6.6rem"   # detail indent = sym + id + gap

# ── WA Question headlines ─────────────────────────────────────────────────────

WA_QUESTIONS = {
    # Reliability
    "REL1":  "REL 1 — RPO and RTO Requirements",
    "REL1a": "REL 1 — RPO and RTO Requirements",
    "REL1b": "REL 1 — RPO and RTO Requirements",
    "REL1c": "REL 1 — RPO and RTO Requirements",
    "REL1d": "REL 1 — RPO and RTO Requirements",
    "REL1e": "REL 1 — RPO and RTO Requirements",
    "REL1f": "REL 1 — RPO and RTO Requirements",
    "REL2":  "REL 1 — RPO and RTO Requirements",   # legacy DR-test note (raw REL2 → REL1e)
    "REL2a": "REL 2 — Exception Handling",
    "REL3":  "REL 3 — Connection Management",
    "REL3a": "REL 3 — Connection Management",
    "REL3b": "REL 3 — Connection Management",
    "REL3c": "REL 3 — Connection Management",
    "REL4":  "REL 4 — Cursor Management",
    "REL4a": "REL 4 — Cursor Management",
    "REL4b": "REL 4 — Cursor Management",
    "REL5":  "REL 5 — High Availability Architecture",
    "REL5a": "REL 5 — High Availability Architecture",
    "REL5b": "REL 5 — High Availability Architecture",
    "REL5c": "REL 5 — High Availability Architecture",
    "REL6":  "REL 6 — Reliability Monitoring",
    "REL6a": "REL 6 — Reliability Monitoring",
    "REL6b": "REL 6 — Reliability Monitoring",
    "REL6c": "REL 6 — Reliability Monitoring",
    "REL6d": "REL 6 — Reliability Monitoring",
    "REL6e": "REL 6 — Reliability Monitoring",
    # Legacy raw ids (fallback path only) kept mapped for safe grouping:
    "REL7":  "REL 6 — Reliability Monitoring",
    "REL7a": "REL 6 — Reliability Monitoring",
    "REL7b": "REL 6 — Reliability Monitoring",
    "REL8":  "REL 4 — Cursor Management",
    "REL9":  "REL 6 — Reliability Monitoring",
    # Security
    "SEC1":  "SEC 1 — Data Protection",
    "SEC1a": "SEC 1 — Data Protection",
    "SEC1b": "SEC 1 — Data Protection",
    "SEC1c": "SEC 1 — Data Protection",
    "SEC1d": "SEC 1 — Data Protection",
    "SEC6":  "SEC 6 — Audit & Monitoring",
    "SEC2":  "SEC 2 — Network Access",
    "SEC2a": "SEC 2 — Network Access",
    "SEC3a": "SEC 3 — Authentication",
    "SEC3b": "SEC 3 — Authentication",
    "SEC3":  "SEC 4 — Credential Management",
    "SEC4a": "SEC 4 — Credential Management",
    "SEC4b": "SEC 4 — Credential Management",
    "SEC5a": "SEC 5 — IAM Permissions",
    "SEC5":  "SEC 6 — Audit & Monitoring",
    "SEC6a": "SEC 6 — Audit & Monitoring",
    "SEC6b": "SEC 6 — Audit & Monitoring",
    "SEC6c": "SEC 6 — Audit & Monitoring",
    "SEC6d": "SEC 6 — Audit & Monitoring",
    "SEC7":  "SEC 7 — Automated Security",
    "SEC7a": "SEC 7 — Automated Security",
    "SEC7c": "SEC 7 — Automated Security",
    "SEC8":  "SEC 8 — Deletion Protection",
    "SEC8a": "SEC 8 — Deletion Protection",
    # Operational Excellence
    "OPS1":  "OPS 1 — Deployment & Configuration",
    "OPS1a": "OPS 1 — Deployment & Configuration",
    "OPS2":  "OPS 2 — Failover Planning",
    "OPS2a": "OPS 2 — Failover Planning",
    "OPS3":  "OPS 3 — Configuration Tracking",
    "OPS3a": "OPS 3 — Configuration Tracking",
    "OPS3b": "OPS 3 — Configuration Tracking",
    "OPS4":  "OPS 4 — Operational Readiness",
    "OPS4a": "OPS 4 — Operational Readiness",
    "OPS4b": "OPS 4 — Operational Readiness",
    "OPS5":  "OPS 5 — Monitoring & Observability",
    "OPS5a": "OPS 5 — Monitoring & Observability",
    "OPS5b": "OPS 5 — Monitoring & Observability",
    "OPS5c": "OPS 5 — Monitoring & Observability",
    "OPS5d": "OPS 5 — Monitoring & Observability",
    "OPS5e": "OPS 5 — Monitoring & Observability",
    "OPS6":  "OPS 6 — Snapshot Lifecycle",
    "OPS8":  "OPS 8 — Engine Version",
    # Performance
    "PERF1":  "PERF 1 — Query Access Patterns",
    "PERF1b": "PERF 1 — Query Access Patterns",
    "PERF1c": "PERF 1 — Query Access Patterns",
    "PERF1d": "PERF 1 — Query Access Patterns",
    "PERF2":  "PERF 2 — Data Access Patterns",
    "PERF2a": "PERF 2 — Data Access Patterns",
    "PERF2b": "PERF 5 — Performance Monitoring",
    "PERF5":  "PERF 3 — Connection Management",
    "PERF6":  "PERF 4 — Cluster Sizing & Cache",
    "PERF6a": "PERF 4 — Cluster Sizing & Cache",
    "PERF6b": "PERF 4 — Cluster Sizing & Cache",
    "PERF6c": "PERF 4 — Cluster Sizing & Cache",
    "PERF6d": "PERF 4 — Cluster Sizing & Cache",
    "PERF7":  "PERF 5 — Performance Monitoring",
    "PERF7a": "PERF 5 — Performance Monitoring",
    "PERF7b": "PERF 5 — Performance Monitoring",
    "PERF7c": "PERF 5 — Performance Monitoring",
    "PERF8":  "PERF 1 — Query Access Patterns",
    "PERF9":  "PERF 6 — Storage Efficiency",
    "PERF10": "PERF 1 — Query Access Patterns",
    "PERF11": "PERF 4 — Cluster Sizing & Cache",
    "PERF12": "PERF 4 — Cluster Sizing & Cache",
    "PERF13": "PERF 5 — Performance Monitoring",
    "PERF14": "PERF 4 — Cluster Sizing & Cache",
    "PERF15": "PERF 1 — Query Access Patterns",
    "PERF16": "PERF 1 — Query Access Patterns",
    # Cost
    "COST1":  "COST 1 — Instance Sizing",
    "COST1a": "COST 1 — Instance Sizing",
    "COST1b": "COST 1 — Instance Sizing",
    "COST1c": "COST 1 — Instance Sizing",
    "COST2a": "COST 6 — Query Optimization",
    # COST 3 — Storage: storage type (3/3a) + default compression (3b) only
    "COST3":  "COST 3 — Storage",
    "COST3a": "COST 3 — Storage",
    "COST3b": "COST 3 — Storage",
    # COST 4 — Index Efficiency: unused indexes (4a) + index count (4b)
    "COST4a": "COST 4 — Index Efficiency",
    "COST4b": "COST 4 — Index Efficiency",
    # COST 5 — Data Lifecycle (shifted from COST 4): TTL + stale snapshots
    "COST4":  "COST 5 — Data Lifecycle",
    "COST5":  "COST 5 — Data Lifecycle",
    "COST6":  "COST 2 — Cost Allocation",
    "COST7":  "COST 3 — Storage",
    "COST9":  "COST 1 — Instance Sizing",
    "COST10": "COST 1 — Instance Sizing",
    # Sustainability
    "SUST1":  "SUST 1 — Graviton Processors",
    "SUST2":  "SUST 2 — Storage Compression",
    "SUST3a": "SUST 3 — Resource Efficiency",
    "SUST4a": "SUST 4 — I/O Efficiency",
    "SUST5a": "SUST 5 — Backup Efficiency",
}

# Display ID remapping — internal ID → v2 display ID
ID_REMAP = {
    "REL2":  "REL1e",
    # Reliability checks now carry their final display ids directly (REL2a, REL3a/b,
    # REL4a/b, REL5a-c, REL6a-e). No REL→REL translation is applied, so the UI
    # (groups by raw id prefix) and the PDF (groups by remapped id prefix) stay in
    # sync via the explicit WA_QUESTIONS entries for each lettered id.
    "REL1d": None,   # suppressed — manual snapshot info not actionable in v2
    "REL6":  None,   # suppressed — raw IaC check covered by OPS1a (REL6a-e are distinct)
    "SEC3":  "SEC4a",
    "SEC5":  "SEC6a",
    "SEC7":  "SEC7c",
    # PERF v1 ID → v2 display ID (proper sub-numbering)
    "PERF11": "PERF6a",
    "PERF12": "PERF6b",
    "PERF14": "PERF6c",
    "PERF13": "PERF7c",
    # Suppressed in v2
    "OPS7":  None,
    "OPS8":  None,
    "COST9":  "COST1b",   # renumbered — idle reader check under COST 1
    "COST10": "COST1c",   # renumbered — serverless evaluation under COST 1
}

DETAIL_OVERRIDES = {
    "REL1e": ("Regularly test failover and restore procedures to validate RTO and RPO "
              "objectives. Use managed Global Cluster Failover to simulate regional "
              "failover events in non-production environments."),
}


# Guidance documentation links — keyed by display check-id. Rendered as a
# "Guidance doc" link under the check detail (UI) and in the PDF export.
DOC_LINKS = {
    "REL1c": "https://docs.aws.amazon.com/documentdb/latest/devguide/global-clusters.html",
    "REL2a": "https://aws.amazon.com/blogs/database/building-resilient-applications-with-amazon-documentdb-with-mongodb-compatibility-part-2-exception-handling/",
    "REL3a": "https://docs.aws.amazon.com/documentdb/latest/devguide/functional-differences.html#:~:text=%24sort%20stage.-,Retryable%20writes,-Starting%20with%20MongoDB",
    "REL4b": "https://aws.amazon.com/blogs/database/building-resilient-applications-with-amazon-documentdb-with-mongodb-compatibility-part-1-client-configuration/#:~:text=consistency%2C%20if%20desired.-,Cursor%20and%20connection%20limits,-When%20designing%20your",
    "SEC1b": "https://docs.aws.amazon.com/documentdb/latest/devguide/security.encryption.ssl.html",
    "SEC1c": "https://aws.amazon.com/blogs/database/introducing-client-side-field-level-encryption-and-mongodb-5-0-api-compatibility-in-amazon-documentdb/",
    "SEC3a": "https://aws.amazon.com/blogs/database/use-iam-authentication-with-amazon-documentdb-with-mongodb-compatibility/",
    "SEC6a": "https://aws.amazon.com/blogs/database/introducing-dml-auditing-for-amazon-documentdb-with-mongodb-compatibility/",
    "SEC8a": "https://docs.aws.amazon.com/documentdb/latest/devguide/db-instance-delete.html#db-instance-delete-deletion-protection",
    "OPS5d": "https://docs.aws.amazon.com/documentdb/latest/devguide/performance-insights.html",
    "COST4a": "https://docs.aws.amazon.com/documentdb/latest/devguide/user_diagnostics.html#user-diag-index-usage",
}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _id_prefix(check_id):
    cid = check_id or ""
    if cid in WA_QUESTIONS:
        return cid
    m = re.match(r"([A-Z]+\d+)", cid)
    return m.group(1) if m else cid


def _check_sort_key(check):
    """Order checks within a question group by display id (e.g. REL4a < REL4b,
    REL6a < REL6d). Keeps a check's order stable regardless of which path
    (plugin vs per-instance vs v2 extras) produced it."""
    cid = ID_REMAP.get(check.get("id", ""), check.get("id", "")) or ""
    m = re.match(r"([A-Za-z]+)(\d+)([a-z]*)", cid)
    if m:
        return (m.group(1), int(m.group(2)), m.group(3))
    return (cid, 0, "")


def _sort_key(headline):
    # Extract pillar prefix and number for correct ordering (e.g. PERF 1 < PERF 2 < PERF 5)
    m = re.search(r"(\w+)\s+(\d+)", headline)
    if m:
        prefix_order = {"REL": 1, "SEC": 2, "OPS": 3, "PERF": 4, "COST": 5, "SUST": 6}
        return (prefix_order.get(m.group(1), 9), int(m.group(2)))
    return (9, 99)


def group_by_question(checks):
    groups = {}
    for c in checks:
        raw_id = c.get("id", "")
        # Skip suppressed checks
        if ID_REMAP.get(raw_id) is None and raw_id in ID_REMAP:
            continue
        prefix   = _id_prefix(raw_id)
        headline = WA_QUESTIONS.get(prefix, prefix)
        if headline not in groups:
            groups[headline] = []
        groups[headline].append(c)
    return sorted(groups.items(), key=lambda x: _sort_key(x[0]))


def normalise_for_pdf(results):
    """Apply ID_REMAP, suppression, DETAIL_OVERRIDES and question grouping
    so the PDF matches the UI exactly. Returns a list of
    (pillar, question_headline, [normalised_check, ...]) tuples,
    ordered by pillar then question number."""
    STATUS_ORDER = {"fail": 0, "warn": 1, "pass": 2, "info": 3}
    PILLAR_ORDER = ["Reliability", "Security", "Operational Excellence",
                    "Performance Efficiency", "Cost Optimization", "Sustainability"]

    # Step 1 — remap IDs, suppress None, apply DETAIL_OVERRIDES, exclude Other pillar
    normalised = []
    for c in results:
        if c.get("pillar") == "Other":
            continue   # system messages, not WA checks
        raw_id   = c.get("id", "")
        remapped = ID_REMAP.get(raw_id, raw_id)
        if remapped is None:
            continue                          # suppressed
        detail = DETAIL_OVERRIDES.get(remapped, c.get("detail", ""))
        normalised.append({**c, "id": remapped, "detail": detail})

    # Step 2 — group by pillar, then by question headline (manual — no re-suppression)
    by_pillar = {}
    for c in normalised:
        by_pillar.setdefault(c["pillar"], []).append(c)

    ordered_pillars = [p for p in PILLAR_ORDER if p in by_pillar] + \
                      [p for p in by_pillar   if p not in PILLAR_ORDER]

    result = []
    for pillar in ordered_pillars:
        # Group by WA question headline using already-remapped IDs
        groups = {}
        for c in by_pillar[pillar]:
            cid      = c["id"]
            prefix   = _id_prefix(cid)
            headline = WA_QUESTIONS.get(prefix, prefix)
            groups.setdefault(headline, []).append(c)

        # Sort groups by question number, then checks within each group by display id
        for headline in sorted(groups, key=_sort_key):
            sorted_checks = sorted(groups[headline], key=_check_sort_key)
            result.append((pillar, headline, sorted_checks))

    return result


# ── Actionable counts (excl. info) — used by PDF cover ───────────────────────

def actionable_counts(results):
    """Return (n_pass, n_warn, n_fail, n_info, actionable, score) after suppression.
    Excludes the 'Other' pillar (system messages, not WA checks)."""
    visible = []
    for c in results:
        if c.get("pillar") == "Other":
            continue   # system messages, not WA checks
        raw_id   = c.get("id", "")
        remapped = ID_REMAP.get(raw_id, raw_id)
        if remapped is None:
            continue   # suppressed
        visible.append(c)
    n_pass = sum(1 for r in visible if r["status"] == "pass")
    n_warn = sum(1 for r in visible if r["status"] == "warn")
    n_fail = sum(1 for r in visible if r["status"] == "fail")
    n_info = sum(1 for r in visible if r["status"] == "info")
    actionable = n_pass + n_warn + n_fail
    score = int(n_pass / actionable * 100) if actionable else 0
    return n_pass, n_warn, n_fail, n_info, actionable, score


# ── UI components ─────────────────────────────────────────────────────────────

def check_row(check):
    """Single check row — fixed column widths ensure perfect alignment."""
    status = check["status"]
    sym, col = STATUS_SYMBOL.get(status, ("ℹ", "#687078"))
    bg       = STATUS_BG.get(status, "transparent")
    raw_id   = check.get("id", "")
    remapped = ID_REMAP.get(raw_id, raw_id)
    if remapped is None:
        return None   # suppressed in v2
    cid      = remapped
    label    = check.get("label", "")
    detail   = DETAIL_OVERRIDES.get(cid, check.get("detail", ""))
    doc_url  = DOC_LINKS.get(cid)

    return html.Div([
        # Main row — symbol | ID | label, all vertically centred
        html.Div([
            html.Span(sym, style={
                "color": col, "fontWeight": "700", "fontSize": ".9rem",
                "width": _SYM_W, "flexShrink": "0", "textAlign": "center",
            }),
            html.Span(cid, style={
                "fontSize": ".68rem", "fontWeight": "700",
                "color": "var(--text-muted)", "fontFamily": "monospace",
                "width": _ID_W, "flexShrink": "0",
            }),
            html.Span(label, style={
                "fontSize": ".84rem", "fontWeight": "600",
                "color": "var(--text-heading)", "flex": "1",
                "lineHeight": "1.35",
            }),
        ], style={
            "display": "flex", "alignItems": "center",
            "padding": ".3rem .6rem",
            "minHeight": "2rem",
        }),
        # Detail — indented to align under label
        html.Div(detail, style={
            "fontSize": ".75rem", "color": "var(--text-muted)",
            "paddingLeft": _INDENT, "paddingRight": ".6rem",
            "paddingBottom": ".3rem", "lineHeight": "1.4",
        }) if detail else None,
        # Guidance doc link (when curated for this check)
        html.Div(
            html.A(["\U0001F4D6  Guidance doc \u2192"], href=doc_url,
                   target="_blank", rel="noopener noreferrer",
                   style={"fontSize": ".72rem", "color": "var(--color-primary)",
                          "textDecoration": "none", "fontWeight": "600"}),
            style={"paddingLeft": _INDENT, "paddingRight": ".6rem",
                   "paddingBottom": ".35rem"},
        ) if doc_url else None,
    ], style={
        "background": bg,
        "borderBottom": "1px solid var(--border-default)",
    })


def question_group(headline, checks, active_filters):
    """WA question sub-heading + its check rows (collapsible)."""
    visible = [c for c in checks
               if not active_filters or c["status"] in active_filters]
    if not visible:
        return None
    visible = sorted(visible, key=_check_sort_key)

    n_fail = sum(1 for c in visible if c["status"] == "fail")
    n_warn = sum(1 for c in visible if c["status"] == "warn")
    n_pass = sum(1 for c in visible if c["status"] == "pass")
    # Colour: red if any fail, amber if any warn, neutral otherwise
    hl_col  = "#D13212" if n_fail else "#906806" if n_warn else "var(--text-muted)"
    hl_bg   = "rgba(209,50,18,.04)" if n_fail else \
              "rgba(144,104,6,.04)" if n_warn else "var(--bg-surface-alt)"

    # Status summary badges for the header
    status_badges = []
    if n_fail:
        status_badges.append(html.Span(f"✗{n_fail}", style={"fontSize": ".68rem", "color": "#D13212", "fontWeight": "700", "marginLeft": ".4rem"}))
    if n_warn:
        status_badges.append(html.Span(f"!{n_warn}", style={"fontSize": ".68rem", "color": "#906806", "fontWeight": "700", "marginLeft": ".4rem"}))
    if n_pass:
        status_badges.append(html.Span(f"✓{n_pass}", style={"fontSize": ".68rem", "color": "#1D8102", "fontWeight": "700", "marginLeft": ".4rem"}))

    # All subsections collapsed by default
    is_open = False

    return html.Details([
        html.Summary([
            html.Span(headline, style={
                "fontSize": ".72rem", "fontWeight": "700",
                "color": hl_col,
                "letterSpacing": ".3px",
            }),
            *status_badges,
        ], style={
            "padding": ".25rem .6rem",
            "background": hl_bg,
            "borderBottom": "1px solid var(--border-default)",
            "borderLeft": f"3px solid {hl_col}",
            "cursor": "pointer",
            "display": "flex", "alignItems": "center",
            "listStyle": "none",
        }),
        html.Div([r for r in [check_row(c) for c in visible] if r is not None]),
    ], open=is_open, className="wa-subsection")


def pillar_section(pillar_name, checks, active_filters):
    """Full pillar block: header + question groups."""
    color = PILLAR_COLORS.get(pillar_name, "#5e6b7a")
    icon  = PILLAR_ICONS.get(pillar_name, "📋")

    n_pass = sum(1 for c in checks if c["status"] == "pass")
    n_warn = sum(1 for c in checks if c["status"] == "warn")
    n_fail = sum(1 for c in checks if c["status"] == "fail")
    n_info = sum(1 for c in checks if c["status"] == "info")

    groups    = group_by_question(checks)
    group_els = [question_group(h, gc, active_filters) for h, gc in groups]
    group_els = [g for g in group_els if g is not None]

    if not group_els:
        return None

    # Pillar header — dark background, pillar colour bottom border
    header = html.Div([
        html.Span(f"{icon}  {pillar_name}", style={
            "fontWeight": "700", "fontSize": ".88rem", "color": "#ffffff",
        }),
        html.Div([
            html.Span(f"✓ {n_pass}", style={
                "fontSize": ".75rem", "color": "#6fcf97",
                "fontWeight": "700", "marginLeft": ".5rem",
            }) if n_pass else None,
            html.Span(f"! {n_warn}", style={
                "fontSize": ".75rem", "color": "#f2c94c",
                "fontWeight": "700", "marginLeft": ".5rem",
            }) if n_warn else None,
            html.Span(f"✗ {n_fail}", style={
                "fontSize": ".75rem", "color": "#ff7675",
                "fontWeight": "700", "marginLeft": ".5rem",
            }) if n_fail else None,
            html.Span(f"ℹ {n_info}", style={
                "fontSize": ".75rem", "color": "rgba(255,255,255,.55)",
                "fontWeight": "600", "marginLeft": ".5rem",
            }) if n_info else None,
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={
        "display": "flex", "justifyContent": "space-between",
        "alignItems": "center",
        "padding": ".4rem .6rem",
        "background": "var(--text-heading)",
        "borderBottom": f"2px solid {color}",
    })

    return html.Div([
        header,
        html.Div(group_els),
    ], style={
        "border": "1px solid var(--border-default)",
        "borderRadius": "6px", "overflow": "hidden",
        "marginBottom": ".75rem",
        "boxShadow": "0 1px 3px rgba(0,0,0,.06)",
    })


def score_bar(results):
    n_pass = sum(1 for r in results if r["status"] == "pass")
    n_warn = sum(1 for r in results if r["status"] == "warn")
    n_fail = sum(1 for r in results if r["status"] == "fail")
    n_info = sum(1 for r in results if r["status"] == "info")
    actionable = n_pass + n_warn + n_fail
    score  = int(n_pass / actionable * 100) if actionable else 0
    s_col  = "#1D8102" if score >= 80 else "#906806" if score >= 60 else "#D13212"

    def _stat(val, label, col):
        return html.Div([
            html.Div(str(val), style={
                "fontSize": ".95rem", "fontWeight": "800", "color": col,
                "lineHeight": "1",
            }),
            html.Div(label, style={
                "fontSize": ".65rem", "color": "var(--text-muted)",
                "fontWeight": "600", "marginTop": ".1rem",
            }),
        ], style={"textAlign": "center", "minWidth": "3rem"})

    return html.Div([
        # Score
        html.Div([
            html.Div(f"{score}%", style={
                "fontSize": "1.4rem", "fontWeight": "800", "color": s_col,
                "lineHeight": "1",
            }),
            html.Div("Health Score", style={
                "fontSize": ".65rem", "color": "var(--text-muted)",
                "fontWeight": "600", "marginTop": ".1rem",
            }),
        ], style={"textAlign": "center", "minWidth": "5rem",
                  "paddingRight": "1rem",
                  "borderRight": "1px solid var(--border-default)"}),
        # Counts
        html.Div([
            _stat(n_pass,     "Passed",     "#1D8102"),
            _stat(n_warn,     "Warnings",   "#906806"),
            _stat(n_fail,     "Failed",     "#D13212"),
            _stat(actionable, "Checks",     "var(--text-muted)"),
            html.Div([
                html.Div(str(n_info), style={
                    "fontSize": ".95rem", "fontWeight": "800", "color": "#687078",
                    "lineHeight": "1",
                }),
                html.Div("Info", style={
                    "fontSize": ".65rem", "color": "var(--text-muted)",
                    "fontWeight": "600", "marginTop": ".1rem",
                }),
            ], style={
                "textAlign": "center", "minWidth": "3rem",
                "paddingLeft": "1rem",
                "borderLeft": "1px solid var(--border-default)",
            }),
        ], style={"display": "flex", "gap": "1.2rem",
                  "paddingLeft": "1rem", "alignItems": "center"}),
    ], style={
        "display": "flex", "alignItems": "center",
        "padding": ".6rem 1rem",
        "background": "var(--bg-surface-alt)",
        "border": "1px solid var(--border-default)",
        "borderRadius": "6px", "marginBottom": ".6rem",
    })


def ai_section(ai_md):
    if not ai_md or not isinstance(ai_md, dict):
        return None

    cards = []
    for pillar, recs in ai_md.items():
        if not recs:
            continue
        color = PILLAR_COLORS.get(pillar, "#5e6b7a")
        icon  = PILLAR_ICONS.get(pillar, "📋")

        rec_rows = []
        for r in recs:
            pri  = r.get("priority", "Medium")
            pcol = PRI_COLORS.get(pri, "#687078")
            rec_rows.append(html.Div([
                html.Div([
                    html.Span(pri, style={
                        "fontSize": ".65rem", "fontWeight": "700",
                        "color": pcol, "width": "3.5rem", "flexShrink": "0",
                    }),
                    html.Span(r.get("action", ""), style={
                        "fontSize": ".82rem", "fontWeight": "600",
                        "color": "var(--text-heading)", "flex": "1",
                        "lineHeight": "1.35",
                    }),
                ], style={"display": "flex", "gap": ".4rem",
                          "alignItems": "flex-start"}),
                html.Div([
                    html.Span("Why: ", style={
                        "fontWeight": "700", "fontSize": ".72rem",
                        "color": "var(--text-muted)", "flexShrink": "0",
                    }),
                    html.Span(r.get("why", ""), style={
                        "fontSize": ".72rem", "color": "var(--text-muted)",
                        "lineHeight": "1.35",
                    }),
                ], style={"display": "flex", "gap": ".2rem",
                          "paddingLeft": "3.9rem",
                          "marginTop": ".15rem"}) if r.get("why") else None,
                html.Div([
                    html.Span("Impact: ", style={
                        "fontWeight": "700", "fontSize": ".72rem",
                        "color": "var(--text-muted)", "flexShrink": "0",
                    }),
                    html.Span(r.get("impact", ""), style={
                        "fontSize": ".72rem", "color": "var(--text-muted)",
                        "lineHeight": "1.35",
                    }),
                ], style={"display": "flex", "gap": ".2rem",
                          "paddingLeft": "3.9rem",
                          "marginTop": ".1rem"}) if r.get("impact") else None,
            ], style={
                "padding": ".3rem .6rem",
                "borderBottom": "1px solid var(--border-default)",
            }))

        cards.append(html.Div([
            html.Div([
                html.Span(f"{icon}  {pillar}", style={
                    "fontWeight": "700", "fontSize": ".84rem", "color": "#ffffff",
                }),
                dbc.Badge(str(len(recs)), color="secondary", className="ms-2",
                          style={"fontSize": ".62rem"}),
            ], style={
                "padding": ".35rem .6rem",
                "background": "var(--text-heading)",
                "borderBottom": f"2px solid {color}",
            }),
            html.Div(rec_rows),
        ], style={
            "border": "1px solid var(--border-default)",
            "borderRadius": "6px", "overflow": "hidden",
            "marginBottom": ".5rem",
        }))

    if not cards:
        return None

    return html.Div([
        html.Div("🤖  AI-Powered Recommendations", style={
            "fontWeight": "700", "fontSize": ".9rem",
            "color": "var(--text-heading)",
            "marginBottom": ".5rem", "paddingBottom": ".3rem",
            "borderBottom": "2px solid var(--color-primary)",
        }),
        html.Div(cards),
    ], style={"marginTop": "1.2rem"})
