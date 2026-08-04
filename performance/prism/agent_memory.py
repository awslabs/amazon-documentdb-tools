"""Agent Memory — persistent observation layer for cross-run learning.

Storage: .prism_cache/{cluster_id}/
  snapshot.json          — cluster config facts
  databases.json         — per-db summary (sizes, indexes, bloat)
  activity/{ts}.json     — 15-min activity summaries
  daily/{date}.json      — daily rollups
  last_analysis.json     — last agent run key findings
"""
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

logger = logging.getLogger(__name__)

_CACHE_ROOT = os.path.join(os.path.dirname(__file__), ".prism_cache")
_LEGACY_CACHE = os.path.join(os.path.dirname(__file__), ".doculens_cache")
_MAX_DAYS = 30

# Auto-migrate legacy cache directory on first access
if not os.path.isdir(_CACHE_ROOT) and os.path.isdir(_LEGACY_CACHE):
    os.rename(_LEGACY_CACHE, _CACHE_ROOT)
    logger.info("Migrated .doculens_cache → .prism_cache")


def _cluster_dir(cluster_id):
    safe_id = cluster_id.replace("/", "_").replace("\\", "_")
    d = os.path.join(_CACHE_ROOT, safe_id)
    os.makedirs(d, exist_ok=True)
    return d


def _write(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, default=str, separators=(",", ":"))


def _read(path):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return json.load(f)
        except Exception:
            pass
    return None


def _content_hash(data):
    """Hash data content ignoring timestamps for change detection."""
    import hashlib
    def _strip_ts(obj):
        if isinstance(obj, dict):
            return {k: _strip_ts(v) for k, v in obj.items()
                    if k not in ("ts", "first_seen", "last_seen", "updated_at")}
        if isinstance(obj, list):
            return [_strip_ts(i) for i in obj]
        return obj
    clean = json.dumps(_strip_ts(data), sort_keys=True, default=str)
    return hashlib.md5(clean.encode(), usedforsecurity=False).hexdigest()


def _write_versioned(path, data, max_versions=3):
    """Write only if content changed. Keep up to max_versions, preserve oldest first_seen.

    Files: {name}.json (current), {name}.v1.json, {name}.v2.json
    v1 = previous, v2 = oldest. Oldest first_seen timestamp is preserved.
    """
    base, ext = os.path.splitext(path)

    # Read current
    current = _read(path)
    if current:
        # Compare content (ignoring timestamps)
        new_hash = _content_hash(data)
        old_hash = _content_hash(current)
        if new_hash == old_hash:
            # No change — just update last_seen timestamp
            current["last_seen"] = datetime.now(timezone.utc).isoformat()
            _write(path, current)
            return False  # no new version

    # Content changed — rotate versions
    # Shift v(n-1) → v(n), drop oldest beyond max_versions
    for i in range(max_versions - 1, 0, -1):
        src = f"{base}.v{i}{ext}" if i > 1 else f"{base}.v1{ext}"
        dst = f"{base}.v{i+1}{ext}" if i + 1 <= max_versions else None
        if os.path.exists(src):
            if dst and i + 1 <= max_versions:
                os.replace(src, dst)
            elif not dst or i + 1 > max_versions:
                os.remove(src)

    # Current → v1
    if current and os.path.exists(path):
        os.replace(path, f"{base}.v1{ext}")

    # Preserve first_seen from the oldest known version
    first_seen = None
    if current:
        first_seen = current.get("first_seen", current.get("ts"))
    data["first_seen"] = first_seen or data.get("ts", datetime.now(timezone.utc).isoformat())
    data["last_seen"] = datetime.now(timezone.utc).isoformat()

    _write(path, data)
    return True  # new version written


# ═══ SNAPSHOT — cluster config ════════════════════════════════════════════════

def save_snapshot(cluster_id, observation):
    """Save lightweight cluster facts."""
    d = _cluster_dir(cluster_id)
    _write_versioned(os.path.join(d, "snapshot.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "cluster_id": cluster_id,
        "engine_version": observation.get("engine_version", ""),
        "instance_types": observation.get("instance_types", []),
        "deletion_protection": observation.get("deletion_protection", False),
        "backup_retention": observation.get("backup_retention", 0),
        "log_exports": observation.get("log_exports", []),
        "db_count": len(observation.get("databases", [])),
        "has_slow_query_logs": observation.get("has_slow_query_logs", False),
    })


def load_snapshot(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "snapshot.json"))


# ═══ DATABASES — per-db summary ═══════════════════════════════════════════════

def save_databases(cluster_id, db_analysis_result):
    """Save compact per-db summary from db_analysis results."""
    if not isinstance(db_analysis_result, dict):
        return
    summary = {}
    for db_name, colls in db_analysis_result.items():
        if not isinstance(colls, dict):
            continue
        total_docs = 0
        total_size = 0
        total_indexes = 0
        unused_indexes = 0
        bloat_sum = 0
        n_colls = 0
        for coll, cd in colls.items():
            if not isinstance(cd, dict):
                continue
            n_colls += 1
            total_docs += cd.get("count", 0)
            total_size += cd.get("storageSize", cd.get("size", 0))
            ia = cd.get("index_analysis", {})
            total_indexes += ia.get("total_indexes", 0)
            unused_indexes += len(ia.get("unused_indexes", []))
            bloat_sum += cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
        summary[db_name] = {
            "collections": n_colls,
            "docs": total_docs,
            "size_mb": round(total_size / (1024 ** 2), 1),
            "indexes": total_indexes,
            "unused_indexes": unused_indexes,
            "avg_bloat_pct": round(bloat_sum / n_colls, 1) if n_colls else 0,
        }
    d = _cluster_dir(cluster_id)
    _write_versioned(os.path.join(d, "databases.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "databases": summary,
    })


def load_databases(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "databases.json"))


# ═══ ACTIVITY — 15-min summaries ══════════════════════════════════════════════

def save_activity_summary(cluster_id, summary):
    """Write a 15-min activity summary."""
    d = _cluster_dir(cluster_id)
    activity_dir = os.path.join(d, "activity")
    os.makedirs(activity_dir, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    _write(os.path.join(activity_dir, f"{ts}.json"), summary)
    # Trigger daily rollup if needed
    _maybe_rollup(cluster_id)


def build_activity_summary(samples):
    """Build a 15-min summary from accumulated currentOp samples.
    
    samples: list of dicts, each from one poll cycle:
      {"active": int, "idle": int, "slow": int, "users": set, "apps": set,
       "namespaces": list, "longest_ms": float}
    """
    if not samples:
        return None
    n = len(samples)
    all_users = set()
    all_apps = set()
    ns_counts = {}
    total_slow = 0
    peak_active = 0
    active_sum = 0
    longest = 0

    for s in samples:
        active_sum += s.get("active", 0)
        peak_active = max(peak_active, s.get("active", 0))
        total_slow += s.get("slow", 0)
        longest = max(longest, s.get("longest_ms", 0))
        all_users.update(s.get("users", []))
        all_apps.update(s.get("apps", []))
        for ns in s.get("namespaces", []):
            ns_counts[ns] = ns_counts.get(ns, 0) + 1

    top_ns = sorted(ns_counts.items(), key=lambda x: -x[1])[:5]

    return {
        "ts": datetime.now(timezone.utc).isoformat(),
        "samples": n,
        "avg_active": round(active_sum / n, 1),
        "peak_active": peak_active,
        "total_slow": total_slow,
        "longest_ms": round(longest, 0),
        "unique_users": sorted(all_users),
        "unique_apps": sorted(all_apps),
        "top_namespaces": [{"ns": ns, "hits": c} for ns, c in top_ns],
    }


def load_recent_activity(cluster_id, hours=24):
    """Load activity summaries from the last N hours."""
    d = os.path.join(_cluster_dir(cluster_id), "activity")
    if not os.path.isdir(d):
        return []
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    cutoff_str = cutoff.strftime("%Y%m%d_%H%M")
    results = []
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".json"):
            continue
        if fname.replace(".json", "") >= cutoff_str:
            data = _read(os.path.join(d, fname))
            if data:
                results.append(data)
    return results


# ═══ DAILY ROLLUP ═════════════════════════════════════════════════════════════

def _maybe_rollup(cluster_id):
    """If yesterday has >4 activity files and no daily rollup, create one."""
    yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d")
    d = _cluster_dir(cluster_id)
    daily_dir = os.path.join(d, "daily")
    daily_file = os.path.join(daily_dir, f"{yesterday}.json")
    if os.path.exists(daily_file):
        return

    activity_dir = os.path.join(d, "activity")
    if not os.path.isdir(activity_dir):
        return

    day_files = [f for f in os.listdir(activity_dir)
                 if f.startswith(yesterday) and f.endswith(".json")]
    if len(day_files) < 4:
        return

    # Build daily rollup
    summaries = []
    for fname in sorted(day_files):
        data = _read(os.path.join(activity_dir, fname))
        if data:
            summaries.append(data)

    if not summaries:
        return

    all_users = set()
    all_apps = set()
    ns_counts = {}
    total_slow = 0
    peak_active = 0
    active_sum = 0
    longest = 0
    hour_activity = {}

    for s in summaries:
        active_sum += s.get("avg_active", 0)
        peak_active = max(peak_active, s.get("peak_active", 0))
        total_slow += s.get("total_slow", 0)
        longest = max(longest, s.get("longest_ms", 0))
        all_users.update(s.get("unique_users", []))
        all_apps.update(s.get("unique_apps", []))
        for ns_entry in s.get("top_namespaces", []):
            ns = ns_entry.get("ns", "")
            ns_counts[ns] = ns_counts.get(ns, 0) + ns_entry.get("hits", 0)
        # Track busiest hour
        ts = s.get("ts", "")
        if len(ts) >= 13:
            hour = ts[11:13]
            hour_activity[hour] = hour_activity.get(hour, 0) + s.get("avg_active", 0)

    n = len(summaries)
    top_ns = sorted(ns_counts.items(), key=lambda x: -x[1])[:5]
    busiest_hour = max(hour_activity, key=hour_activity.get) if hour_activity else "—"

    os.makedirs(daily_dir, exist_ok=True)
    _write(daily_file, {
        "date": yesterday,
        "summaries_count": n,
        "avg_active": round(active_sum / n, 1),
        "peak_active": peak_active,
        "total_slow_queries": total_slow,
        "longest_query_ms": longest,
        "unique_users": sorted(all_users),
        "unique_apps": sorted(all_apps),
        "busiest_hour": busiest_hour,
        "top_namespaces": [{"ns": ns, "hits": c} for ns, c in top_ns],
    })

    # Delete the 15-min files for that day (rolled up)
    for fname in day_files:
        try:
            os.remove(os.path.join(activity_dir, fname))
        except Exception:
            pass

    logger.info("Daily rollup created for %s: %d summaries", yesterday, n)


def load_daily_summaries(cluster_id, days=30):
    """Load daily rollups for the last N days."""
    d = os.path.join(_cluster_dir(cluster_id), "daily")
    if not os.path.isdir(d):
        return []
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).strftime("%Y%m%d")
    results = []
    for fname in sorted(os.listdir(d)):
        if not fname.endswith(".json"):
            continue
        if fname.replace(".json", "") >= cutoff:
            data = _read(os.path.join(d, fname))
            if data:
                results.append(data)
    return results


# ═══ LAST ANALYSIS — key findings only ════════════════════════════════════════

def save_last_analysis(cluster_id, agent_state):
    """Save compact summary of last agent run."""
    modules = agent_state.get("modules", {})
    summary = {}
    for mod_name, mod_data in modules.items():
        summary[mod_name] = {
            "status": mod_data.get("status", "pending"),
            "issues": mod_data.get("error") or None,
            "ts": mod_data.get("ts"),
        }
    d = _cluster_dir(cluster_id)
    _write(os.path.join(d, "last_analysis.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "modules": summary,
        "correlated_insights": agent_state.get("correlated_insights", [])[:10],
        "skipped_modules": agent_state.get("skipped_modules", []),
        "reasoning_log": agent_state.get("reasoning_log", [])[:15],
    })


def load_last_analysis(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "last_analysis.json"))


# ═══ MODULE RESULTS — persist actual outputs locally ═══════════════════════════════

def save_wa_results(cluster_id, results):
    """Save Well-Architected review results."""
    if not results or not isinstance(results, list):
        return
    d = _cluster_dir(cluster_id)
    _write_versioned(os.path.join(d, "wa_results.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "checks": [{
            "pillar": r.get("pillar", ""),
            "id": r.get("id", ""),
            "label": r.get("label", ""),
            "status": r.get("status", ""),
            "detail": r.get("detail", ""),
        } for r in results],
        "summary": {
            "total": len(results),
            "pass": sum(1 for r in results if r.get("status") == "pass"),
            "warn": sum(1 for r in results if r.get("status") == "warn"),
            "fail": sum(1 for r in results if r.get("status") == "fail"),
        },
    })


def load_wa_results(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "wa_results.json"))


def load_wa_results_previous(cluster_id):
    """Load the previous (v1) WA results for drift comparison."""
    return _read(os.path.join(_cluster_dir(cluster_id), "wa_results.v1.json"))


def save_slow_queries(cluster_id, patterns):
    """Save slow query patterns."""
    if not patterns:
        return
    d = _cluster_dir(cluster_id)
    _write_versioned(os.path.join(d, "slow_queries.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "count": len(patterns),
        "patterns": [{
            "ns": p.get("ns", ""),
            "operation": p.get("operation", ""),
            "count": p.get("count", 0),
            "avg_time": p.get("avg_time", 0),
            "max_time": p.get("max_time", 0),
            "example_query": p.get("example_query", {}),
        } for p in patterns[:50]],
    })


def load_slow_queries(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "slow_queries.json"))


def save_index_health(cluster_id, db_analysis_result):
    """Save index health summary from db_analysis results."""
    if not isinstance(db_analysis_result, dict):
        return
    d = _cluster_dir(cluster_id)
    summary = {}
    for db_name, colls in db_analysis_result.items():
        if not isinstance(colls, dict):
            continue
        db_indexes = []
        for coll, cd in colls.items():
            if not isinstance(cd, dict):
                continue
            for idx in cd.get("indexes", []):
                u = idx.get("usage", {})
                c = idx.get("cardinality", {})
                b = idx.get("bloat", {})
                flags = []
                if u.get("potential_unused"):
                    flags.append("unused")
                if c.get("is_low"):
                    flags.append("low_cardinality")
                if b.get("unusedPercent", 0) > 20:
                    flags.append("bloated")
                if flags:
                    db_indexes.append({
                        "collection": coll,
                        "name": idx.get("name", ""),
                        "fields": list(idx.get("fields", {}).keys()),
                        "ops": u.get("ops_count", 0),
                        "flags": flags,
                    })
        if db_indexes:
            summary[db_name] = db_indexes
    _write_versioned(os.path.join(d, "index_health.json"), {
        "ts": datetime.now(timezone.utc).isoformat(),
        "databases": summary,
    })


def load_index_health(cluster_id):
    return _read(os.path.join(_cluster_dir(cluster_id), "index_health.json"))


# ═══ COMPARE — detect changes between runs ═══════════════════════════════════

def compare_with_previous(cluster_id, current_observation, current_db_summary=None):
    """Compare current state with previous snapshot. Returns list of change strings."""
    changes = []

    prev_snap = load_snapshot(cluster_id)
    if prev_snap:
        if prev_snap.get("engine_version") and current_observation.get("engine_version"):
            if prev_snap["engine_version"] != current_observation["engine_version"]:
                changes.append(f"Engine upgraded: {prev_snap['engine_version']} → {current_observation['engine_version']}")
        prev_types = set(prev_snap.get("instance_types", []))
        curr_types = set(current_observation.get("instance_types", []))
        added = curr_types - prev_types
        removed = prev_types - curr_types
        if added:
            changes.append(f"New instance types: {', '.join(added)}")
        if removed:
            changes.append(f"Removed instance types: {', '.join(removed)}")
        prev_dbs = prev_snap.get("db_count", 0)
        curr_dbs = len(current_observation.get("databases", []))
        if curr_dbs != prev_dbs and prev_dbs > 0:
            changes.append(f"Database count: {prev_dbs} → {curr_dbs}")

    prev_dbs = load_databases(cluster_id)
    if prev_dbs and current_db_summary and isinstance(current_db_summary, dict):
        prev_db_data = prev_dbs.get("databases", {})
        for db_name, curr in current_db_summary.items():
            prev = prev_db_data.get(db_name)
            if not prev:
                changes.append(f"New database: {db_name}")
                continue
            # Bloat change
            prev_bloat = prev.get("avg_bloat_pct", 0)
            curr_bloat = curr.get("avg_bloat_pct", 0)
            if curr_bloat - prev_bloat > 10:
                changes.append(f"{db_name} bloat: {prev_bloat}% → {curr_bloat}%")
            # Size change >50%
            prev_size = prev.get("size_mb", 0)
            curr_size = curr.get("size_mb", 0)
            if prev_size > 0 and curr_size / prev_size > 1.5:
                changes.append(f"{db_name} size grew: {prev_size}MB → {curr_size}MB")

    # WA changes
    prev_wa = load_wa_results(cluster_id)
    if prev_wa:
        prev_fails = sum(1 for c in prev_wa.get("checks", []) if c.get("status") == "fail")
        prev_warns = sum(1 for c in prev_wa.get("checks", []) if c.get("status") == "warn")
        changes.append(f"Previous WA: {prev_fails} failures, {prev_warns} warnings (from {prev_wa.get('ts', '?')[:10]})")

    # Slow query changes
    prev_sq = load_slow_queries(cluster_id)
    if prev_sq:
        prev_count = prev_sq.get("count", 0)
        prev_patterns = prev_sq.get("patterns", [])
        if prev_patterns:
            worst = max(prev_patterns, key=lambda p: p.get("avg_time", 0))
            changes.append(f"Previous slow queries: {prev_count} patterns, worst {worst.get('ns','?')} avg={worst.get('avg_time',0):.0f}ms")

    # Index health changes
    prev_idx = load_index_health(cluster_id)
    if prev_idx:
        prev_dbs_idx = prev_idx.get("databases", {})
        total_issues = sum(len(idxs) for idxs in prev_dbs_idx.values())
        if total_issues:
            changes.append(f"Previous index issues: {total_issues} flagged indexes across {len(prev_dbs_idx)} databases")

    return changes[:15]


# ═══ CLEANUP — enforce 30-day retention ═══════════════════════════════════════

def cleanup_old_files(cluster_id):
    """Delete activity and daily files older than 30 days."""
    d = _cluster_dir(cluster_id)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=_MAX_DAYS)).strftime("%Y%m%d")
    removed = 0
    for subdir in ("activity", "daily"):
        path = os.path.join(d, subdir)
        if not os.path.isdir(path):
            continue
        for fname in os.listdir(path):
            if not fname.endswith(".json"):
                continue
            date_part = fname[:8]
            if date_part < cutoff:
                try:
                    os.remove(os.path.join(path, fname))
                    removed += 1
                except Exception:
                    pass
    if removed:
        logger.info("Cleaned up %d files older than %d days for %s", removed, _MAX_DAYS, cluster_id)
