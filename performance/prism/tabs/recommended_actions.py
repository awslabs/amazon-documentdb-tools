"""Recommended Actions — standalone cluster-level page."""
import logging
from dash import html, dcc, callback, Input, Output, State, no_update, ctx, ALL
import dash_bootstrap_components as dbc

logger = logging.getLogger(__name__)

_TH = {"padding": ".4rem .6rem", "fontSize": ".65rem", "fontWeight": "700",
       "textTransform": "uppercase", "letterSpacing": ".5px",
       "color": "var(--text-muted)", "borderBottom": "2px solid var(--border-default)",
       "background": "var(--bg-surface-alt)", "whiteSpace": "nowrap"}
_TD = {"padding": ".35rem .6rem", "fontSize": ".82rem", "color": "var(--text-body)",
       "borderBottom": "1px solid var(--border-default)", "verticalAlign": "top"}
_TABLE = {"width": "100%", "borderCollapse": "collapse", "borderRadius": "8px",
          "overflow": "hidden", "border": "1px solid var(--border-default)"}

_IMPACTS = {
    "encryption":       ("Security",        "Protects data at rest; required for HIPAA/SOC2/PCI-DSS."),
    "deletion_prot":    ("Disaster Recovery","Prevents accidental permanent cluster deletion."),
    "multi_az":         ("High Availability","Enables ~30s auto-failover on AZ outage."),
    "backup_retention": ("Disaster Recovery","Extends PITR window for late-discovered corruption."),
    "profiler_log":     ("Observability",    "Enables slow query analysis via CloudWatch."),
    "audit_log":        ("Compliance",       "Tracks data access for audit trails and forensics."),
    "compression":      ("Cost",             "Reduces storage 2-4x; cuts storage and I/O bill."),
    "downsize":         ("Cost",             "Instance over-provisioned; downsize saves compute cost."),
    "upsize_cache":     ("Performance",      "Low cache hit ratio causes excessive disk I/O."),
    "unused_index":     ("Cost + Writes",    "Wastes storage and slows every write with zero read benefit."),
    "low_cardinality":  ("Query Perf",       "Low-selectivity index doesn't narrow results; wastes I/O."),
    "redundant_index":  ("Cost + Writes",    "Prefix-duplicate of wider index; adds write overhead only."),
    "missing_index":    ("Query Perf",       "Full COLLSCAN on every query; index cuts latency 100-1000x."),
    "bloat":            ("Cost",             "Reclaimable wasted disk space; compacting improves cache."),
    "compression_coll": ("Cost",             "Uncompressed large collection; compression saves 50-75%."),
    "wa_fail":          ("Reliability",      "Well-Architected failure; gap that can cause outages."),
    "wa_warn":          ("Risk",             "Well-Architected warning; deviation from best practices."),
    "agent_finding":    ("Agent Analysis",   "Evidence-based finding from autonomous AI investigation."),
}


def _impact_cell(key, pillar_override=None, desc_override=None):
    if pillar_override:
        cat, desc = pillar_override, (desc_override or "")
    else:
        cat, desc = _IMPACTS.get(key, ("—", ""))
    return html.Td(
        [html.Strong(cat + ": ", style={"color": "var(--text-heading)", "fontSize": ".78rem"}),
         html.Span(desc, style={"fontSize": ".78rem", "color": "var(--text-muted)"})],
        style=_TD)


def _sev_badge(sev):
    return dbc.Badge(sev, color={"Critical": "danger", "High": "danger",
                                  "Medium": "warning", "Low": "info"}.get(sev, "secondary"),
                     style={"fontSize": ".65rem"})


def _section_header(label, section_id, count=None, open_=False):
    badge = dbc.Badge(str(count), color="secondary", className="ms-2",
                      style={"fontSize": ".6rem"}) if count else ""
    return html.Div(
        [html.Span("▾" if open_ else "▸",
                   id={"type": "ra-chevron", "s": section_id},
                   style={"marginRight": ".4rem", "color": "var(--text-muted)", "width": "1rem",
                          "display": "inline-block", "fontSize": ".85rem"}),
         html.Span(label, style={"fontWeight": "700", "fontSize": ".9rem"}),
         badge],
        id={"type": "ra-hdr", "s": section_id},
        style={"cursor": "pointer", "padding": ".5rem .7rem", "borderRadius": "8px",
               "background": "var(--bg-surface-alt)", "border": "1px solid var(--border-default)",
               "marginBottom": ".3rem", "userSelect": "none", "display": "flex",
               "alignItems": "center"})


# ── Storage pricing constants (fallback; overridden from cost_data) ───────────
_STORAGE_PRICE = {"standard": 0.10, "iopt": 0.30}   # $/GB/month


def _get_storage_price_per_gb():
    """Return (price_per_gb, storage_type_label) from live snapshot cost data."""
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            data = _snap.get("data") or {}
        cost = data.get("cost_data") or {}
        stype = data.get("cluster", {}).get("storage_type", "Standard")
        is_iopt = "optimized" in stype.lower() or "iopt" in stype.lower()
        if cost:
            costs = cost["iopt_costs"] if is_iopt else cost["standard_costs"]
            storage_gb = cost.get("storage_gb", 1) or 1
            # derive $/GB from the storage line item
            price_per_gb = costs["storage"] / storage_gb
        else:
            price_per_gb = _STORAGE_PRICE["iopt"] if is_iopt else _STORAGE_PRICE["standard"]
        return price_per_gb, stype
    except Exception:
        return _STORAGE_PRICE["standard"], "Standard"


def _calc_savings(analysis_data):
    """
    Returns dict with:
      unused_bytes, redundant_bytes, bloat_bytes,
      total_bytes, total_gb, monthly_savings_usd, storage_type
    """
    unused_b = redundant_b = bloat_b = 0
    if not analysis_data:
        return None

    for db_name, collections in analysis_data.items():
        if not isinstance(collections, dict):
            continue
        for coll_name, cd in collections.items():
            if not isinstance(cd, dict) or "error" in cd:
                continue
            ia = cd.get("index_analysis", {})
            idxs = cd.get("indexes", [])
            idx_map = {i["name"]: i for i in idxs}

            # Unused indexes
            for idx in ia.get("unused_indexes", []):
                name = idx if isinstance(idx, str) else idx.get("name", "")
                unused_b += idx_map.get(name, {}).get("size", 0)

            # Redundant (prefix-duplicate) indexes
            for target in idxs:
                if target["name"] in ("_id", "_id_"):
                    continue
                t_keys = list(target.get("fields", {}).keys())
                for other in idxs:
                    if other["name"] == target["name"]:
                        continue
                    o_keys = list(other.get("fields", {}).keys())
                    if len(t_keys) < len(o_keys) and o_keys[:len(t_keys)] == t_keys:
                        redundant_b += target.get("size", 0)
                        break

            # Bloat
            bloat_pct = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
            storage_sz = cd.get("storageSize", 0)
            if bloat_pct > 30:
                bloat_b += int(storage_sz * bloat_pct / 100)

    total_b = unused_b + redundant_b + bloat_b
    if total_b == 0:
        return None

    total_gb = total_b / (1024 ** 3)
    price_per_gb, stype = _get_storage_price_per_gb()
    monthly_usd = total_gb * price_per_gb

    return {
        "unused_bytes": unused_b,
        "redundant_bytes": redundant_b,
        "bloat_bytes": bloat_b,
        "total_gb": total_gb,
        "monthly_usd": monthly_usd,
        "storage_type": stype,
    }


def _fmt_bytes(b):
    if b >= 1024 ** 3:
        return f"{b / 1024**3:.2f} GB"
    if b >= 1024 ** 2:
        return f"{b / 1024**2:.1f} MB"
    return f"{b / 1024:.0f} KB"


def _build_savings_banner(savings):
    gb_str  = f"{savings['total_gb']:.2f} GB" if savings else "—"
    usd_str = f"${savings['monthly_usd']:,.2f}/mo" if savings else "—"
    gb_color  = "var(--accent-green)" if savings and savings["total_gb"] > 1 else "var(--text-muted)"
    usd_color = "var(--accent-green)" if savings and savings["monthly_usd"] > 10 else "var(--text-muted)"

    breakdown = ""
    if savings:
        parts = []
        if savings["unused_bytes"]:
            parts.append(f"Unused idx: {_fmt_bytes(savings['unused_bytes'])}")
        if savings["redundant_bytes"]:
            parts.append(f"Redundant idx: {_fmt_bytes(savings['redundant_bytes'])}")
        if savings["bloat_bytes"]:
            parts.append(f"Bloat: {_fmt_bytes(savings['bloat_bytes'])}")
        breakdown = " · ".join(parts)

    def _col(label, value, value_color, border=True):
        return html.Div([
            html.Div(label, style={"fontSize": ".65rem", "fontWeight": "700",
                                   "textTransform": "uppercase", "letterSpacing": ".4px",
                                   "color": "var(--text-muted)", "marginBottom": ".2rem"}),
            html.Div(value, style={"fontSize": ".88rem", "fontWeight": "700",
                                   "color": value_color, "lineHeight": "1.2"}),
        ], style={"flex": "1", "minWidth": "0", "padding": "0 .75rem",
                  **({"borderRight": "1px solid var(--border-default)"} if border else {})})

    return html.Div([
        html.Div("Impact Summary",
                 style={"fontSize": ".72rem", "fontWeight": "700", "color": "var(--text-muted)",
                        "textTransform": "uppercase", "letterSpacing": ".5px",
                        "marginBottom": ".4rem"}),
        html.Div([
            _col("Storage Savings",  gb_str,                              gb_color),
            _col("Cost Savings",     usd_str,                             usd_color),
            _col("Security",         "Encryption, TLS, access controls",  "var(--accent-red)"),
            _col("Performance",      "Faster queries, reduced I/O",       "var(--accent-blue)"),
            _col("Reliability",      "HA, backup, failover coverage",     "var(--accent-green)"),
            html.Div(breakdown or "Analyze databases to calculate",
                     style={"fontSize": ".72rem", "color": "var(--text-muted)",
                            "padding": "0 .75rem", "alignSelf": "center",
                            "flex": "1.5", "minWidth": "0"}),
        ], style={"display": "flex", "alignItems": "center"}),
    ], style={"padding": ".5rem .75rem", "marginBottom": ".75rem",
              "border": "1px solid var(--border-default)",
              "background": "var(--bg-surface-alt)"})
def _build_summary_bar(cluster_recs, db_recs):
    n_cluster = len(cluster_recs)
    n_db = sum(len(v) for v in db_recs.values())
    total = n_cluster + n_db

    # Count cluster recs by severity for pills
    counts = {"Critical": 0, "High": 0, "Medium": 0, "Low": 0}
    for sev, *_ in cluster_recs:
        counts[sev] = counts.get(sev, 0) + 1

    pills = []
    for sev, color in [("Critical", "var(--accent-red)"), ("High", "var(--accent-red)"),
                        ("Medium", "var(--color-warning)"), ("Low", "var(--accent-blue)")]:
        if counts[sev]:
            pills.append(dbc.Badge(
                f"{counts[sev]} {sev}",
                color={"Critical": "danger", "High": "danger",
                       "Medium": "warning", "Low": "info"}[sev],
                className="me-1",
                style={"fontSize": ".72rem"}
            ))

    return html.Div([
        html.Div([
            html.Span(str(total), style={"fontSize": "1.3rem", "fontWeight": "800",
                       "color": "var(--accent-red)" if total > 5
                       else "var(--color-warning)" if total > 0 else "var(--accent-green)"}),
            html.Span(" total actions", style={"fontSize": ".82rem", "color": "var(--text-muted)",
                                               "marginLeft": ".3rem"}),
        ]),
        html.Div([
            *pills,
            html.Span(f"  {n_cluster} cluster · {n_db} database",
                      style={"fontSize": ".75rem", "color": "var(--text-muted)", "marginLeft": ".3rem"}),
        ], style={"display": "flex", "alignItems": "center", "flexWrap": "wrap"}),
    ], style={"display": "flex", "justifyContent": "space-between", "alignItems": "center",
              "padding": ".5rem .8rem", "borderRadius": "8px", "marginBottom": ".6rem",
              "border": "1px solid var(--border-default)",
              "background": "var(--bg-surface-alt)"})


# ── Extraction ────────────────────────────────────────────────────────────────

def _extract_cluster_recs():
    recs = []
    try:
        from tabs.cluster_snapshot import _snap, _snap_lock
        with _snap_lock:
            snap = _snap.get("data")
            done = _snap.get("done", False)
        if snap and done:
            cl = snap.get("cluster", {})
            insts = snap.get("instances", [])
            azs = set(i.get("az", "") for i in insts) - {""}
            if not cl.get("storage_encrypted"):
                recs.append(("Critical", "Enable encryption at rest (requires new cluster)", "encryption"))
            if not cl.get("deletion_protection"):
                recs.append(("High", "Enable deletion protection", "deletion_prot"))
            if len(azs) < 2:
                recs.append(("High", "Deploy instances across 2+ AZs for failover", "multi_az"))
            if cl.get("backup_retention", 0) < 7:
                recs.append(("Medium", f"Increase backup retention to 7+ days (currently {cl.get('backup_retention',0)})", "backup_retention"))
            if not cl.get("profiler_log"):
                recs.append(("Medium", "Enable profiler log export for slow query analysis", "profiler_log"))
            if not cl.get("audit_log"):
                recs.append(("Low", "Enable audit log export for compliance tracking", "audit_log"))
            if str(cl.get("compression", "None")).lower() == "disabled":
                recs.append(("Medium", "Enable default collection compression (5.0: enabled; 8.0: zstd or lz4)", "compression"))
            for i in insts:
                if i.get("cpu_avg") is not None and i["cpu_avg"] < 5:
                    recs.append(("Low", f"Downsize {i['id']} — avg CPU {i['cpu_avg']:.1f}%", "downsize"))
                bchr = i.get("buffer_cache_hit_ratio")
                if bchr is not None and bchr < 85:
                    recs.append(("Medium", f"Upsize {i['id']} — cache hit ratio {bchr:.1f}%", "upsize_cache"))
    except Exception as e:
        logger.debug("Cluster recs failed: %s", e)

    try:
        from tabs.well_architected import _wa, _wa_lock
        with _wa_lock:
            wa_results = list(_wa.get("results", []))
            wa_done = _wa.get("done", False)
        if wa_results and wa_done:
            seen = {r[1] for r in recs}
            for check in wa_results:
                if check["status"] not in ("fail", "warn"):
                    continue
                detail = check.get("detail", "")
                label = check.get("label", "")
                text = f"{label}: {detail}" if detail else label
                if text not in seen:
                    sev = "High" if check["status"] == "fail" else "Medium"
                    key = "wa_fail" if check["status"] == "fail" else "wa_warn"
                    actual_pillar = check.get("pillar", "Well-Architected")
                    desc = "Well-Architected failure; gap that can cause outages." if check["status"] == "fail" else "Well-Architected warning; deviation from best practices."
                    recs.append((sev, text, key, actual_pillar, desc))
                    seen.add(text)
    except Exception as e:
        logger.debug("WA recs failed: %s", e)

    # ── Agentic findings: evidence-based recommendations from the AI agent ───
    try:
        from agent_orchestrator import _agent_state, _lock
        with _lock:
            agent_status = _agent_state.get("status", "idle")
            agent_findings = list(_agent_state.get("_agentic_findings", []))
        if agent_findings and agent_status == "complete":
            seen = {r[1] for r in recs}
            for finding in agent_findings:
                text = finding.get("finding", "")
                if not text or text in seen:
                    continue
                # Map agent severity to UI severity
                sev_map = {"critical": "Critical", "high": "High",
                           "medium": "Medium", "low": "Low"}
                sev = sev_map.get(finding.get("severity", "medium"), "Medium")
                # Map agent category to impact description
                cat = finding.get("category", "operational")
                rec_text = finding.get("recommendation", "")
                display = f"{text}: {rec_text}" if rec_text else text
                cat_label = {"performance": "Performance",
                             "cost": "Cost Optimization",
                             "reliability": "Reliability",
                             "security": "Security",
                             "operational": "Operational"}.get(cat, "Agent Analysis")
                evidence = finding.get("evidence", "")
                desc = evidence[:120] if evidence else "AI agent finding based on cluster investigation."
                recs.append((sev, display, "agent_finding", cat_label, desc))
                seen.add(text)
    except Exception as e:
        logger.debug("Agentic findings extraction failed: %s", e)

    recs.sort(key=lambda x: {"Critical": 0, "High": 1, "Medium": 2, "Low": 3}.get(x[0], 9))
    return recs


def _extract_db_recs(analysis_data):
    if not analysis_data or not isinstance(analysis_data, dict):
        return {}
    db_recs = {}
    for db_name, collections in analysis_data.items():
        if not isinstance(collections, dict):
            continue
        actions = []
        for coll_name, cd in collections.items():
            if not isinstance(cd, dict) or "error" in cd:
                continue
            ia = cd.get("index_analysis", {})
            idxs = cd.get("indexes", [])

            for idx in ia.get("unused_indexes", []):
                n = idx if isinstance(idx, str) else idx.get("name", "?")
                actions.append((f"Drop unused index `{n}` on {coll_name}", "unused_index"))

            for idx in ia.get("low_cardinality_indexes", []):
                n = idx if isinstance(idx, str) else idx.get("name", "?")
                actions.append((f"Review low-cardinality index `{n}` on {coll_name}", "low_cardinality"))

            for target in idxs:
                if target["name"] in ("_id", "_id_"):
                    continue
                t_keys = list(target.get("fields", {}).keys())
                for other in idxs:
                    if other["name"] == target["name"]:
                        continue
                    o_keys = list(other.get("fields", {}).keys())
                    if len(t_keys) < len(o_keys) and o_keys[:len(t_keys)] == t_keys:
                        actions.append((f"Drop redundant index `{target['name']}` on {coll_name} (covered by `{other['name']}`)", "redundant_index"))
                        break

            doc_count = cd.get("count", 0)
            non_id = [i for i in idxs if i.get("name") not in ("_id", "_id_")]
            if doc_count > 100_000 and len(non_id) == 0:
                actions.append((f"Add indexes on {coll_name} ({doc_count:,} docs, full scans)", "missing_index"))

            bloat_pct = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
            if bloat_pct > 30:
                actions.append((f"Compact {coll_name} — {bloat_pct:.0f}% storage bloat", "bloat"))

            if not cd.get("compression", {}).get("enabled", False) and cd.get("storageSize", 0) > 50 * 1024 * 1024:
                size_mb = cd.get("storageSize", 0) / (1024 * 1024)
                actions.append((f"Enable compression on {coll_name} ({size_mb:.0f} MB)", "compression_coll"))

        if actions:
            db_recs[db_name] = actions
    return db_recs


# ── Render helpers ────────────────────────────────────────────────────────────

_SEV_GROUPS = [
    ("Critical", "var(--accent-red)",     "#fff0f0"),
    ("High",     "var(--accent-red)",     "#fff4f0"),
    ("Medium",   "var(--color-warning)",  "#fffbf0"),
    ("Low",      "var(--accent-blue)",    "#f0f6ff"),
]


def _sev_table(recs_for_group):
    """Render a small table for one severity group (no Sev column)."""
    header = html.Tr([html.Th(h, style=_TH) for h in ["Recommendation", "Impact"]])
    rows = [html.Tr([
        html.Td(text, style=_TD),
        _impact_cell(key,
                     pillar_override=r[3] if len(r) > 3 else None,
                     desc_override=r[4] if len(r) > 4 else None),
    ]) for r in recs_for_group for _, text, key, *_ in [r]]
    return html.Table([html.Thead(header), html.Tbody(rows)], style=_TABLE)


# Severity groups that start collapsed by default (lower-priority noise).
_SEV_DEFAULT_COLLAPSED = {"Medium", "Low"}


def _cluster_table(recs, sev_open_state=None):
    if not recs:
        return html.Div("✅  No cluster-level issues found",
                        style={"fontSize": ".85rem", "color": "var(--accent-green)",
                               "fontWeight": "600", "padding": ".4rem 0"})

    sev_open_state = sev_open_state or {}
    children = []
    for sev, color, bg in _SEV_GROUPS:
        group = [r for r in recs if r[0] == sev]
        if not group:
            continue
        default_open = sev not in _SEV_DEFAULT_COLLAPSED
        is_open = sev_open_state.get(sev, default_open)
        children.append(html.Div(
            [html.Span("▾" if is_open else "▸",
                       id={"type": "ra-sev-chevron", "sev": sev},
                       style={"marginRight": ".4rem", "color": color, "width": ".9rem",
                              "display": "inline-block", "fontSize": ".8rem"}),
             html.Span(sev, style={"fontWeight": "700", "fontSize": ".78rem",
                                   "color": color, "textTransform": "uppercase",
                                   "letterSpacing": ".5px"}),
             dbc.Badge(str(len(group)), color="secondary", className="ms-2",
                       style={"fontSize": ".6rem"})],
            id={"type": "ra-sev-hdr", "sev": sev},
            style={"display": "flex", "alignItems": "center", "padding": ".3rem .6rem",
                   "borderRadius": "6px 6px 0 0", "background": bg,
                   "borderLeft": f"3px solid {color}",
                   "border": f"1px solid var(--border-default)",
                   "borderBottom": "none", "marginTop": ".5rem",
                   "cursor": "pointer", "userSelect": "none"}
        ))
        children.append(dbc.Collapse(
            _sev_table(group),
            id={"type": "ra-sev-collapse", "sev": sev}, is_open=is_open,
        ))
    return html.Div(children)


def _db_subheader(db_name, count, open_):
    """Collapsible sub-header for one database inside the Database Level section.

    Styled lighter/indented relative to the top-level section headers so the
    hierarchy reads clearly. Uses the `ra-db-*` pattern type so it is driven by
    cb_ra_db_toggle, independent of the top-level cb_ra_toggle.
    """
    return html.Div(
        [html.Span("▾" if open_ else "▸",
                   id={"type": "ra-db-chevron", "db": db_name},
                   style={"marginRight": ".4rem", "color": "var(--text-muted)",
                          "width": ".9rem", "display": "inline-block", "fontSize": ".8rem"}),
         html.Span(db_name, style={"fontWeight": "700", "fontFamily": "monospace",
                                   "fontSize": ".82rem"}),
         dbc.Badge(str(count), color="warning", className="ms-2",
                   style={"fontSize": ".6rem"})],
        id={"type": "ra-db-hdr", "db": db_name},
        style={"cursor": "pointer", "padding": ".35rem .6rem", "borderRadius": "6px",
               "background": "var(--bg-surface)", "border": "1px solid var(--border-default)",
               "marginBottom": ".25rem", "marginLeft": ".6rem", "userSelect": "none",
               "display": "flex", "alignItems": "center"})


def _db_recs_table(actions):
    """Render the Recommendation/Impact table for a single database's actions."""
    header = html.Tr([html.Th(h, style=_TH) for h in ["Recommendation", "Impact"]])
    rows = [html.Tr([html.Td(text, style=_TD), _impact_cell(key)])
            for (text, key) in actions[:15]]
    return html.Table([html.Thead(header), html.Tbody(rows)],
                      style={**_TABLE, "marginLeft": ".6rem", "width": "calc(100% - .6rem)"})


def _db_table(db_recs, analyzing_dbs=None, db_open_state=None):
    """Render the Database Level body as per-database collapsible blocks.

    Each database gets its own header (name + action count) and a dbc.Collapse
    holding that database's recommendations. Open/closed state per database is
    read from db_open_state (missing → collapsed) and toggled by cb_ra_db_toggle.
    """
    db_open_state = db_open_state or {}
    if not db_recs and not analyzing_dbs:
        return html.Div("Run analysis on databases to see recommendations.",
                        style={"fontSize": ".85rem", "color": "var(--text-muted)", "padding": ".4rem 0"})

    blocks = []
    for db_name in sorted(db_recs.keys()):
        actions = db_recs[db_name]
        # Individual databases are collapsed by default (missing → closed).
        is_open = db_open_state.get(db_name, False)
        blocks.append(_db_subheader(db_name, len(actions), is_open))
        blocks.append(dbc.Collapse(
            html.Div(_db_recs_table(actions), className="mb-2 mt-1"),
            id={"type": "ra-db-collapse", "db": db_name}, is_open=is_open,
        ))

    # Databases still being analyzed — shown as plain status lines (no collapse).
    if analyzing_dbs:
        for db_name in analyzing_dbs:
            if db_name not in db_recs:
                blocks.append(html.Div(
                    [dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2",
                                 spinner_style={"width": "12px", "height": "12px"}),
                     html.Span(db_name, style={"fontFamily": "monospace", "fontSize": ".8rem",
                                               "color": "var(--text-muted)"}),
                     html.Span(" — Analyzing…", style={"fontSize": ".8rem",
                                                       "color": "var(--text-muted)"})],
                    style={"display": "flex", "alignItems": "center", "padding": ".35rem .6rem",
                           "marginLeft": ".6rem"}))

    if not blocks:
        return html.Div("✅  No issues found across analyzed databases.",
                        style={"fontSize": ".85rem", "color": "var(--accent-green)",
                               "fontWeight": "600", "padding": ".4rem 0"})
    return html.Div(blocks)


# ── Slow Query Optimizations callout ──────────────────────────────────────────

def _slow_query_section(patterns):
    """Render the Slow Query Optimizations callout, grouped by database.

    Surfaces the AI recommendations produced by slow_query_recommender for the
    already-collected patterns. Schedules generation idempotently (the engine
    dedups, caches, and is suppressed while the agent is running) — it never
    re-scans CloudWatch. Patterns without a ready rec show a generating
    placeholder.
    """
    if not patterns:
        return None, False

    pattern_keys = [p.get("pattern_key") for p in patterns if p.get("pattern_key")]
    try:
        import slow_query_recommender as sqr
        # Reuse existing patterns; schedule is idempotent (cache + in-flight dedup,
        # gated while agent runs). Does NOT re-run the slow query scan.
        cluster_id = region = ""
        try:
            from tabs.cluster_slow_queries import _csq, _csq_lock
            with _csq_lock:
                cluster_id = _csq.get("cluster_id", "")
                region = _csq.get("region", "us-east-1")
        except Exception:
            pass
        sqr.schedule(patterns, cluster_id, region)
        states = sqr.get_state(pattern_keys)
    except Exception as e:
        logger.debug("slow query rec scheduling failed: %s", e)
        states = {}

    pending = _slow_recs_pending(patterns, states)

    # Group patterns by database.
    by_db = {}
    for p in patterns:
        ns = p.get("ns", "")
        db_name = ns.split(".", 1)[0] if "." in ns else "unknown"
        by_db.setdefault(db_name, []).append(p)

    header = html.Tr([html.Th(h, style=_TH) for h in
                      ["Database", "Namespace", "Slow Query (count · avg)", "AI Recommendation"]])
    rows = []
    for db_name in sorted(by_db.keys()):
        db_patterns = sorted(by_db[db_name],
                             key=lambda x: x.get("count", 0) * x.get("avg_time", 0),
                             reverse=True)
        for i, p in enumerate(db_patterns):
            pk = p.get("pattern_key")
            ns = p.get("ns", "")
            coll = ns.split(".", 1)[1] if "." in ns else ns
            op = p.get("operation", "—")
            cnt = p.get("count", 0)
            avg = p.get("avg_time", 0)
            st = states.get(pk) or {}
            status = st.get("status")
            rec = st.get("recommendation") or {}

            if status == "done" and rec:
                action = rec.get("action", "—")
                badge_color = {"Add index": "primary", "Rewrite query": "warning",
                               "Scale compute": "info"}.get(action, "secondary")
                rec_cell = html.Td([
                    dbc.Badge(action, color=badge_color, className="me-2",
                              style={"fontSize": ".65rem"}),
                    (dbc.Badge("⚠ unverified", color="danger", className="me-2",
                               style={"fontSize": ".6rem"}) if rec.get("unsafe") else ""),
                    dcc.Markdown(rec.get("markdown", ""), className="ra-slow-md",
                                 style={"fontSize": ".76rem"}),
                ], style=_TD)
            elif status == "unavailable":
                rec_cell = html.Td("AI recommendations unavailable",
                                   style={**_TD, "color": "var(--text-muted)",
                                          "fontStyle": "italic"})
            elif status == "failed":
                rec_cell = html.Td("Could not generate recommendation",
                                   style={**_TD, "color": "var(--accent-red)"})
            else:
                rec_cell = html.Td([
                    dbc.Spinner(size="sm", color="warning", spinner_class_name="me-2",
                                spinner_style={"width": "12px", "height": "12px"}),
                    html.Span("Generating AI recommendation — check back shortly",
                              style={"color": "var(--text-muted)", "fontStyle": "italic"}),
                ], style={**_TD, "fontSize": ".78rem"})

            db_cell = [
                html.Span(db_name, style={"fontWeight": "600", "fontFamily": "monospace",
                                          "fontSize": ".8rem"}),
                dbc.Badge(str(len(db_patterns)), color="warning", className="ms-1",
                          style={"fontSize": ".6rem"}),
            ] if i == 0 else ""

            rows.append(html.Tr([
                html.Td(db_cell, style={**_TD, "whiteSpace": "nowrap"}),
                html.Td(coll, style={**_TD, "fontFamily": "monospace", "fontSize": ".76rem"}),
                html.Td(f"{op} · {cnt}× · {avg:.0f}ms", style={**_TD, "fontSize": ".78rem"}),
                rec_cell,
            ]))

    table = html.Table([html.Thead(header), html.Tbody(rows)], style=_TABLE)
    return table, pending


# ── Helpers ───────────────────────────────────────────────────────────────────

def _get_slow_patterns():
    """Return already-collected slow query patterns. Does NOT re-scan CloudWatch.

    Prefers the agent's slow_query module result (populated whether or not the
    user opened the Slow Query tab); falls back to the tab's own cache.
    """
    try:
        from agent_orchestrator import _agent_state, _lock
        with _lock:
            mod = _agent_state["modules"].get("slow_query", {})
            if mod.get("status") == "done" and isinstance(mod.get("result"), list):
                return list(mod["result"])
    except Exception:
        pass
    try:
        from tabs.cluster_slow_queries import _csq, _csq_lock
        with _csq_lock:
            if _csq.get("done") and _csq.get("patterns"):
                return list(_csq["patterns"])
    except Exception:
        pass
    return []


def _slow_recs_pending(patterns, states):
    """True if any slow query pattern is still awaiting a terminal AI rec state."""
    for p in patterns:
        pk = p.get("pattern_key")
        status = (states.get(pk) or {}).get("status")
        if status not in ("done", "failed", "unavailable"):
            return True
    return False


def _get_merged_analysis(dash_data):
    merged = dict(dash_data) if dash_data else {}
    try:
        from agent_orchestrator import get_db_analysis_results
        live = get_db_analysis_results()
        if live:
            merged.update(live)
    except Exception:
        pass
    return merged


def _is_agent_active():
    try:
        from agent_orchestrator import get_agent_state
        return get_agent_state().get("status") in ("running", "observing", "reasoning")
    except Exception:
        return False


def _slow_recs_active():
    """True if slow query patterns exist with AI recs not yet in a terminal state,
    so the page should keep polling to surface them."""
    try:
        import slow_query_recommender as sqr
        patterns = _get_slow_patterns()
        if not patterns:
            return False
        states = sqr.get_state([p.get("pattern_key") for p in patterns if p.get("pattern_key")])
        return _slow_recs_pending(patterns, states)
    except Exception:
        return False


def _get_analyzing_dbs():
    try:
        from agent_orchestrator import get_agent_state, get_db_analysis_results
        state = get_agent_state()
        if state.get("status") not in ("running", "observing", "reasoning"):
            return []
        mod = state.get("modules", {}).get("db_analysis", {})
        if mod.get("status") in ("running", "pending"):
            scope = state.get("analysis_scope", {}).get("in_scope", [])
            done = set(get_db_analysis_results().keys())
            return [db for db in scope if db not in done]
    except Exception:
        pass
    return []


# ── Page content builder ──────────────────────────────────────────────────────

def _render_page_content(analysis_data, analyzing_dbs=None, open_cluster=True, open_db=True,
                         open_slow=True, db_open_state=None, sev_open_state=None):
    cluster_recs = _extract_cluster_recs()
    db_recs = _extract_db_recs(analysis_data)
    savings = _calc_savings(analysis_data)

    slow_patterns = _get_slow_patterns()
    slow_table, slow_pending = _slow_query_section(slow_patterns)
    n_slow = len(slow_patterns)

    children = [
        _build_summary_bar(cluster_recs, db_recs),
        _build_savings_banner(savings),

        # Cluster section
        _section_header("Cluster Level", "cluster", count=len(cluster_recs), open_=open_cluster),
        dbc.Collapse(
            html.Div(_cluster_table(cluster_recs, sev_open_state), className="mb-2 mt-1"),
            id={"type": "ra-collapse", "s": "cluster"}, is_open=open_cluster,
        ),

        html.Div(style={"height": ".4rem"}),

        # DB section
        _section_header("Database Level", "db",
                        count=sum(len(v) for v in db_recs.values()) + len(analyzing_dbs or []),
                        open_=open_db),
        dbc.Collapse(
            html.Div(_db_table(db_recs, analyzing_dbs, db_open_state), className="mb-2 mt-1"),
            id={"type": "ra-collapse", "s": "db"}, is_open=open_db,
        ),

        html.Div(style={"height": ".4rem"}),

        # Slow Query Optimizations callout — always rendered (even empty) so the
        # three collapsible sections stay a fixed set for the pattern-matching
        # toggle callback.
        _section_header("Slow Query Optimizations", "slow", count=n_slow, open_=open_slow),
        dbc.Collapse(
            html.Div(slow_table if slow_table is not None else html.Div(
                "No slow query patterns collected yet. Run the agent or open the "
                "Slow Query tab to populate them.",
                style={"fontSize": ".85rem", "color": "var(--text-muted)", "padding": ".4rem 0"}),
                className="mb-2 mt-1"),
            id={"type": "ra-collapse", "s": "slow"}, is_open=open_slow,
        ),
    ]

    return html.Div(children)


# ── Page renderer ─────────────────────────────────────────────────────────────

def render_recommended_actions(cluster_id, region, conn_str):
    merged = _get_merged_analysis(None)
    analyzing = _get_analyzing_dbs()
    agent_active = _is_agent_active()

    return html.Div([
        html.Div([
            html.Div([
                html.Span("Recommended Actions", className="section-title",
                          style={"marginBottom": "0", "borderBottom": "none", "paddingBottom": "0"}),
                html.Span("  ·  ", style={"color": "var(--text-muted)", "margin": "0 .3rem"}),
                html.Span(cluster_id or "", style={"fontSize": ".88rem", "fontWeight": "600",
                                                    "color": "var(--text-body)", "fontFamily": "monospace"}),
                html.Span(f"  ({region})", style={"fontSize": ".8rem", "color": "var(--text-muted)"}),
            ], style={"display": "flex", "alignItems": "baseline", "flexWrap": "wrap"}),
        ], style={"marginBottom": ".75rem", "paddingBottom": ".4rem",
                  "borderBottom": "2px solid var(--color-primary)"}),

        # Sections state store
        dcc.Store(id="ra-sections-state", data={"cluster": True, "db": True, "slow": True}),
        # Per-database collapse state (db_name -> is_open). Missing → collapsed.
        dcc.Store(id="ra-db-state", data={}),
        # Per-severity collapse state for the cluster section (sev -> is_open).
        # Missing → default (Medium collapsed, others open).
        dcc.Store(id="ra-sev-state", data={}),
        dcc.Store(id="rec-page-meta", data={"cluster_id": cluster_id, "region": region}),

        html.Div(id="rec-page-content",
                 children=_render_page_content(merged, analyzing)),

        dcc.Interval(id="rec-page-poll", interval=5000,
                     disabled=not (agent_active or _slow_recs_active())),
    ])


# ── Callbacks ─────────────────────────────────────────────────────────────────

@callback(
    Output("ra-sections-state", "data"),
    Output({"type": "ra-collapse", "s": ALL}, "is_open"),
    Output({"type": "ra-chevron", "s": ALL}, "children"),
    Input({"type": "ra-hdr", "s": ALL}, "n_clicks"),
    State("ra-sections-state", "data"),
    prevent_initial_call=True,
)
def cb_ra_toggle(clicks, state):
    if not ctx.triggered_id or not any(c for c in clicks if c):
        return no_update, [no_update, no_update, no_update], [no_update, no_update, no_update]
    s = ctx.triggered_id["s"]
    state = dict(state) if state else {"cluster": True, "db": True, "slow": True}
    state[s] = not state.get(s, True)
    order = ["cluster", "db", "slow"]
    return state, [state.get(k, True) for k in order], ["▾" if state.get(k, True) else "▸" for k in order]


@callback(
    Output("ra-db-state", "data"),
    Output({"type": "ra-db-collapse", "db": ALL}, "is_open"),
    Output({"type": "ra-db-chevron", "db": ALL}, "children"),
    Input({"type": "ra-db-hdr", "db": ALL}, "n_clicks"),
    State("ra-db-state", "data"),
    prevent_initial_call=True,
)
def cb_ra_db_toggle(clicks, state):
    """Toggle an individual database's collapse inside the Database Level section.

    The set of databases is dynamic, so the output order is derived from the
    live outputs list rather than a hardcoded order.
    """
    # db names in the order Dash will apply the ALL outputs.
    out_dbs = [o["id"]["db"] for o in ctx.outputs_list[1]]
    if not ctx.triggered_id or not any(c for c in (clicks or []) if c):
        return no_update, [no_update] * len(out_dbs), [no_update] * len(out_dbs)
    db = ctx.triggered_id["db"]
    state = dict(state) if state else {}
    state[db] = not state.get(db, False)
    is_open = [state.get(d, False) for d in out_dbs]
    chevrons = ["▾" if state.get(d, False) else "▸" for d in out_dbs]
    return state, is_open, chevrons


@callback(
    Output("ra-sev-state", "data"),
    Output({"type": "ra-sev-collapse", "sev": ALL}, "is_open"),
    Output({"type": "ra-sev-chevron", "sev": ALL}, "children"),
    Input({"type": "ra-sev-hdr", "sev": ALL}, "n_clicks"),
    State("ra-sev-state", "data"),
    prevent_initial_call=True,
)
def cb_ra_sev_toggle(clicks, state):
    """Toggle a severity group's collapse inside the Cluster Level section.

    Only non-empty severity groups are rendered, so the set is dynamic. The
    output order is derived from the live outputs list rather than a fixed
    order. Missing state falls back to the per-severity default (Medium and Low
    collapsed, Critical/High open).
    """
    out_sevs = [o["id"]["sev"] for o in ctx.outputs_list[1]]
    if not ctx.triggered_id or not any(c for c in (clicks or []) if c):
        return no_update, [no_update] * len(out_sevs), [no_update] * len(out_sevs)
    sev = ctx.triggered_id["sev"]
    state = dict(state) if state else {}
    # Default open state for a severity that hasn't been toggled yet.
    default_open = sev not in _SEV_DEFAULT_COLLAPSED
    state[sev] = not state.get(sev, default_open)

    def _is_open(s):
        return state.get(s, s not in _SEV_DEFAULT_COLLAPSED)

    is_open = [_is_open(s) for s in out_sevs]
    chevrons = ["▾" if _is_open(s) else "▸" for s in out_sevs]
    return state, is_open, chevrons


@callback(
    Output("rec-page-content", "children"),
    Output("rec-page-poll", "disabled"),
    Input("rec-page-poll", "n_intervals"),
    State("analysis-store", "data"),
    State("ra-sections-state", "data"),
    State("ra-db-state", "data"),
    State("ra-sev-state", "data"),
    prevent_initial_call=True,
)
def cb_rec_page_poll(n, analysis_data, sections_state, db_state, sev_state):
    merged = _get_merged_analysis(analysis_data)
    analyzing = _get_analyzing_dbs()
    agent_active = _is_agent_active()
    s = sections_state or {"cluster": True, "db": True, "slow": True}
    content = _render_page_content(merged, analyzing,
                                   s.get("cluster", True), s.get("db", True),
                                   s.get("slow", True), db_open_state=db_state or {},
                                   sev_open_state=sev_state or {})
    # Keep polling while the agent runs OR while slow query AI recs are still
    # being generated, so placeholders get replaced as recs complete.
    keep_polling = agent_active or _slow_recs_active()
    return content, not keep_polling


from tabs.registry import register_tab
register_tab("rec_actions", "", "Recommended Actions", "cluster", render_recommended_actions)
