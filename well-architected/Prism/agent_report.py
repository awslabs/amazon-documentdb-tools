"""Agent Report Generator — Markdown report from completed agent state."""
import time
from datetime import datetime, timezone


def generate_report(agent_state: dict, conn_data: dict) -> str:
    """Generate full Markdown report from agent state."""
    cluster_id = conn_data.get("cluster_id", "unknown")
    region = conn_data.get("region", "us-east-1")
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    sections = []
    sections.append(f"# Amazon DocumentDB Prism — Analysis Report")
    sections.append(f"**Cluster:** {cluster_id} | **Region:** {region} | **Generated:** {now}")
    sections.append(f"**Analysis Mode:** Autonomous Agent (Observe→Reason→Decide→Act)\n")

    # Executive Summary
    sections.append(_executive_summary(agent_state))

    # Reasoning Log
    sections.append(_reasoning_log(agent_state))

    # Analysis Scope
    sections.append(_scope_section(agent_state))

    # Module results
    modules = agent_state.get("modules", {})

    if modules.get("cluster_snapshot", {}).get("result"):
        sections.append(_cluster_section(modules["cluster_snapshot"]["result"]))

    if modules.get("db_analysis", {}).get("result"):
        sections.append(_db_analysis_section(modules["db_analysis"]["result"]))

    if modules.get("slow_query", {}).get("result"):
        sections.append(_slow_query_section(modules["slow_query"]["result"]))

    if modules.get("compression", {}).get("result"):
        sections.append(_compression_section(modules["compression"]["result"]))

    if modules.get("well_architected", {}).get("result"):
        sections.append(_wa_section(modules["well_architected"]["result"]))

    if modules.get("instance_recommender", {}).get("result"):
        sections.append(_instance_section(modules["instance_recommender"]["result"]))

    if modules.get("storage_recommender", {}).get("result"):
        sections.append(_storage_section(modules["storage_recommender"]["result"]))

    # Live activity observations
    alerts = agent_state.get("live_alerts", [])
    if alerts:
        sections.append(_activity_section(alerts))

    # Skipped modules
    skipped = agent_state.get("skipped_modules", [])
    if skipped:
        sections.append(_skipped_section(skipped))

    return "\n\n".join(sections)


def _executive_summary(state):
    modules = state.get("modules", {})
    done_count = sum(1 for m in modules.values() if m.get("status") == "done")
    total = len(modules)
    insights = state.get("correlated_insights", [])

    # Count issues
    issues = 0
    for m in modules.values():
        r = m.get("result")
        if isinstance(r, list):
            issues += sum(1 for x in r if isinstance(x, dict) and x.get("status") == "fail")

    lines = ["## Executive Summary"]
    lines.append(f"**Modules Completed:** {done_count}/{total}")
    lines.append(f"**Correlated Insights:** {len(insights)}")

    if insights:
        lines.append("\n### Top Actions (Agent-Correlated)")
        for i, insight in enumerate(insights[:5], 1):
            lines.append(f"{i}. {insight}")

    return "\n".join(lines)


def _reasoning_log(state):
    log = state.get("reasoning_log", [])
    if not log:
        return "## Agent Reasoning Log\nNo reasoning log available."

    lines = ["## Agent Reasoning Log"]
    lines.append("| Step | Module | Reasoning |")
    lines.append("|------|--------|-----------|")
    for entry in log:
        lines.append(f"| {entry.get('step', '—')} | {entry.get('module', '—')} | {entry.get('reasoning', '—')} |")

    skipped = state.get("skipped_modules", [])
    for s in skipped:
        lines.append(f"| — | {s.get('module', '—')} | Skipped: {s.get('reason', '—')} |")

    return "\n".join(lines)


def _scope_section(state):
    scope = state.get("analysis_scope", {})
    in_scope = scope.get("in_scope", [])
    skipped = scope.get("skipped", {})

    lines = ["## Analysis Scope"]
    lines.append(f"**Databases analysed:** {len(in_scope)}")
    lines.append(f"**Databases skipped:** {len(skipped)}")

    if skipped:
        lines.append("\n| Database | Reason |")
        lines.append("|----------|--------|")
        for db, reason in list(skipped.items())[:20]:
            lines.append(f"| {db} | {reason} |")

    return "\n".join(lines)


def _cluster_section(data):
    if not isinstance(data, dict):
        return "## Cluster Configuration\nNo data available."
    cl = data.get("cluster", {})
    lines = ["## Cluster Configuration"]
    lines.append(f"- **Engine:** {cl.get('engine_version', '—')}")
    lines.append(f"- **Instances:** {len(data.get('instances', []))}")
    lines.append(f"- **Storage:** {data.get('storage_gb', 0):.2f} GB")
    lines.append(f"- **Encryption:** {'Yes' if cl.get('storage_encrypted') else 'No'}")
    lines.append(f"- **Deletion Protection:** {'Yes' if cl.get('deletion_protection') else 'No'}")
    lines.append(f"- **Backup Retention:** {cl.get('backup_retention', 0)} days")
    return "\n".join(lines)


def _db_analysis_section(data):
    if not isinstance(data, dict):
        return "## Database Analysis\nNo data available."
    lines = ["## Database Analysis"]
    for db_name, colls in data.items():
        if not isinstance(colls, dict):
            continue
        lines.append(f"\n### Database: {db_name}")
        lines.append(f"Collections: {len(colls)}")
        lines.append("| Collection | Docs | Size (MB) | Indexes | Unused Idx | Bloat % |")
        lines.append("|------------|------|-----------|---------|------------|---------|")
        for coll, cd in list(colls.items())[:30]:
            if not isinstance(cd, dict):
                continue
            count = cd.get("count", 0)
            size = cd.get("storageSize", cd.get("size", 0)) / (1024 ** 2)
            ia = cd.get("index_analysis", {})
            n_idx = ia.get("total_indexes", 0)
            unused = len(ia.get("unused_indexes", []))
            bloat = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
            lines.append(f"| {coll} | {count:,} | {size:.1f} | {n_idx} | {unused} | {bloat:.1f} |")
    return "\n".join(lines)


def _slow_query_section(data):
    lines = ["## Slow Query Findings"]
    if not data:
        lines.append("No slow query data available.")
        return "\n".join(lines)
    patterns = data if isinstance(data, list) else data.get("patterns", [])
    if not patterns:
        lines.append("No slow query patterns found.")
        return "\n".join(lines)
    lines.append("| Namespace | Avg (ms) | Count | Pattern |")
    lines.append("|-----------|----------|-------|---------|")
    for p in patterns[:20]:
        ns = p.get("ns", p.get("namespace", "—"))
        avg = p.get("avg_ms", p.get("avgMs", 0))
        count = p.get("count", 0)
        pattern = str(p.get("filter", p.get("query_shape", "")))[:50]
        lines.append(f"| {ns} | {avg:.0f} | {count} | {pattern} |")
    return "\n".join(lines)


def _compression_section(data):
    lines = ["## Compression Opportunities"]
    if not data:
        lines.append("No compression data.")
        return "\n".join(lines)
    lines.append("| Collection | Current Size | LZ4 Est. | ZSTD Est. | Savings % |")
    lines.append("|------------|-------------|----------|-----------|-----------|")
    for key, val in list(data.items())[:20]:
        if not isinstance(val, dict):
            continue
        curr = val.get("current_size_mb", val.get("original_size", 0))
        lz4 = val.get("lz4_size_mb", val.get("lz4_estimate", 0))
        zstd = val.get("zstd_size_mb", val.get("zstd_estimate", 0))
        savings = val.get("savings_pct", 0)
        lines.append(f"| {key} | {curr:.1f} MB | {lz4:.1f} MB | {zstd:.1f} MB | {savings:.0f}% |")
    return "\n".join(lines)


def _wa_section(data):
    lines = ["## Well-Architected Assessment"]
    if not data or not isinstance(data, list):
        lines.append("No WA data.")
        return "\n".join(lines)
    n_pass = sum(1 for r in data if r.get("status") == "pass")
    n_fail = sum(1 for r in data if r.get("status") == "fail")
    n_warn = sum(1 for r in data if r.get("status") == "warn")
    lines.append(f"**Pass:** {n_pass} | **Warn:** {n_warn} | **Fail:** {n_fail}")
    fails = [r for r in data if r.get("status") == "fail"]
    if fails:
        lines.append("\n**Failed Checks:**")
        for r in fails:
            lines.append(f"- ❌ {r.get('label', '—')} — {r.get('detail', '')}")
    return "\n".join(lines)


def _instance_section(data):
    lines = ["## Instance Sizing"]
    if not data or not isinstance(data, list):
        lines.append("No instance data.")
        return "\n".join(lines)
    lines.append("| Instance | Type | Recommendation |")
    lines.append("|----------|------|----------------|")
    for item in data:
        inst = item.get("instance", "—")
        itype = item.get("type", "—")
        rec = item.get("recommendation", {})
        rec_obj = rec.get("recommendation", {}) if isinstance(rec, dict) else {}
        rec_text = rec_obj.get("instance", "No change") if isinstance(rec_obj, dict) else "No change"
        lines.append(f"| {inst} | {itype} | {rec_text} |")
    return "\n".join(lines)


def _storage_section(data):
    lines = ["## Storage Recommendation"]
    if not data or not isinstance(data, dict):
        lines.append("No storage data.")
        return "\n".join(lines)
    sc = data.get("standard_costs", {})
    ic = data.get("iopt_costs", {})
    lines.append(f"- **Standard Total:** ${sc.get('total', 0):,.2f}/mo")
    lines.append(f"- **I/O Optimized Total:** ${ic.get('total', 0):,.2f}/mo")
    savings = data.get("potential_savings", 0)
    if savings > 0:
        lines.append(f"- **Potential Savings:** ${savings:,.2f}/mo")
    return "\n".join(lines)


def _activity_section(alerts):
    lines = ["## Live Activity Observations"]
    lines.append("During analysis, the following slow/blocked queries were detected:")
    lines.append("| PID | Namespace | Duration | User | Status |")
    lines.append("|-----|-----------|----------|------|--------|")
    seen = set()
    for a in alerts:
        key = (a.get("opid"), a.get("ns"))
        if key in seen:
            continue
        seen.add(key)
        us = a.get("us", 0)
        dur = f"{us / 1_000_000:.1f}s" if us > 1_000_000 else f"{us / 1_000:.0f}ms"
        lines.append(f"| {a.get('opid', '—')} | {a.get('ns', '—')} | {dur} | {a.get('user', '—')} | {a.get('status', '—')} |")
        if len(seen) >= 10:
            break
    return "\n".join(lines)


def _skipped_section(skipped):
    lines = ["## Skipped Modules"]
    lines.append("| Module | Reason |")
    lines.append("|--------|--------|")
    for s in skipped:
        lines.append(f"| {s.get('module', '—')} | {s.get('reason', '—')} |")
    return "\n".join(lines)
