"""Recommendation_Engine — AI-powered optimization recommendations for slow query patterns.

Generates per-pattern recommendations (Add index / Rewrite query / Scale compute)
in background threads, grounded in three inputs:
  1. Namespace_Stats   — the database analysis (indexes + collection stats) for the pattern's namespace
  2. Pattern context   — pattern_key, ns, operation, count, avg_time, max_time, example_query
  3. Advisor_Context   — documentdb-advisor/SKILL.md + every file under references/

Design constraints (see .kiro/specs/ai-slow-query-recommendations/):
  - Never blocks the UI thread. All generation runs on daemon threads.
  - Gated on db_analysis availability; collision-safe with the autonomous agent.
  - Per-database concurrency throttling + FIFO queue.
  - Persistent cache keyed by (cluster_id, pattern_key, stats_digest).
  - Graceful "AI recommendations unavailable" fallback when Bedrock is unreachable.
"""
import os
import json
import time
import queue
import hashlib
import logging
import threading

logger = logging.getLogger(__name__)

# ── Constants ────────────────────────────────────────────────────────────────
CONCURRENCY_LIMIT = 5          # Req 10.2 — max simultaneous Bedrock calls per database (clamped 1..20 per Req 10.1)
GEN_TIMEOUT_SEC = 120          # Req 3.6 — max time for one recommendation generation
QUEUE_TIMEOUT_SEC = 300        # Req 10.5 — max time a job may sit queued before being dropped
ONDEMAND_TIMEOUT_SEC = 30      # Req 6.2 — max time for an on-demand db analysis fallback
MAX_GENERATION_ATTEMPTS = 2    # Req 13.2 — consecutive Bedrock failures before Unavailable_State

PLACEHOLDER_TEXT = "Analysing for optimisations..."   # Req 2.1
UNAVAILABLE_TEXT = "AI recommendations unavailable"   # Req 13.1

_ACTION_LABELS = {"Add index", "Rewrite query", "Scale compute"}

_CACHE_DIR = os.path.join(os.path.dirname(__file__), ".prism_cache", "slow_query_recs")

# ── Module state ─────────────────────────────────────────────────────────────
# Per-pattern recommendation state, keyed by pattern_key. Entry shape:
#   {
#     "status": "placeholder"|"generating"|"done"|"failed"|"unavailable",
#     "recommendation": {"action", "markdown", "unsafe", "unsafe_reason"} | None,
#     "error": str | None,
#     "stats_digest": str | None,
#     "started_ts": float | None,
#     "fail_count": int,
#   }
_rec_state = {}
_rec_lock = threading.RLock()

# Patterns currently scheduled or in-flight (dedup guard). set of pattern_key.
_inflight = set()

# Per-database concurrency control. db_name -> threading.Semaphore.
_db_semaphores = {}
_sem_lock = threading.Lock()

# Global FIFO job queue + single dispatcher thread (lazily started).
_job_queue = queue.Queue()
_dispatcher_started = False
_dispatcher_lock = threading.Lock()


# ── Cache key helpers (Task 1) ───────────────────────────────────────────────

def _stats_digest(namespace_stats):
    """Deterministic digest of the namespace stats fed to the LLM (Req 11.2).

    Only the fields that influence the recommendation are included, so unrelated
    agent-state churn does not invalidate the cache while any change to these
    inputs does.
    """
    if not namespace_stats:
        material = {}
    else:
        material = {
            "count": namespace_stats.get("count"),
            "storageSize": namespace_stats.get("storageSize"),
            "indexes": namespace_stats.get("indexes"),
            "index_analysis": namespace_stats.get("index_analysis"),
        }
    canonical = json.dumps(material, sort_keys=True, default=str)
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _cache_key(cluster_id, pattern_key, stats_digest):
    """Compose the Cache_Key from cluster id, pattern key, and stats digest (Req 11.2)."""
    raw = f"{cluster_id}\x1f{pattern_key}\x1f{stats_digest}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


# ── Public API (Task 1) ──────────────────────────────────────────────────────

def get_state(pattern_keys):
    """Return a shallow copy of _rec_state entries for the given pattern_keys.

    Non-blocking read used by the tab's render/poll path (Correctness Property 1).
    """
    out = {}
    with _rec_lock:
        for pk in pattern_keys:
            entry = _rec_state.get(pk)
            if entry is not None:
                # Shallow copy; nested recommendation dict is treated as immutable once set.
                out[pk] = dict(entry)
    return out


def reset():
    """Clear all in-memory recommendation state, including Unavailable_State and
    fail counts, plus the in-flight set (Req 12.2, 13.6).

    Pending queued jobs become no-ops because their pattern_key is no longer in
    _inflight (workers re-check before generating).
    """
    with _rec_lock:
        _rec_state.clear()
        _inflight.clear()
    logger.info("slow_query_recommender state reset")


# ── Namespace gating + collision-safe fallback (Task 3) ──────────────────────

def _db_analysis_snapshot():
    """Return (result_dict, module_status) for the agent's db_analysis module.

    Reads under the agent's own lock. Returns ({}, "pending") on any failure so
    callers treat analysis as unavailable.
    """
    try:
        from agent_orchestrator import _agent_state, _lock
        with _lock:
            mod = _agent_state["modules"]["db_analysis"]
            result = mod.get("result")
            status = mod.get("status") or "pending"
            result = dict(result) if isinstance(result, dict) else {}
        return result, status
    except Exception as e:  # pragma: no cover - defensive
        logger.debug("db_analysis snapshot failed: %s", e)
        return {}, "pending"


def is_idle_for_analysis(db_name):
    """True iff on-demand analysis is permitted: the agent is not running AND the
    db_analysis module is not pending/running (Req 6.1, 6.2)."""
    try:
        from agent_orchestrator import is_agent_running
        if is_agent_running():
            return False
    except Exception:  # pragma: no cover - defensive
        return False
    _, status = _db_analysis_snapshot()
    return status not in ("pending", "running")


def _namespace_stats(db_name, collection):
    """Return (available, stats) for a namespace.

    Availability is determined solely by the presence of db_name in the
    db_analysis result (Req 5.2, 5.3). When the database is present but the
    specific collection is absent, stats is an empty dict but available is True.
    Returns (False, None) when the database has not been analyzed.
    """
    result, _ = _db_analysis_snapshot()
    if db_name not in result:
        return False, None
    db_result = result.get(db_name)
    if not isinstance(db_result, dict):
        return True, {}
    return True, db_result.get(collection, {})


def _ensure_db_analyzed_bounded(conn_str, db_name):
    """Invoke ensure_db_analyzed under a watchdog bounded to ONDEMAND_TIMEOUT_SEC.

    Returns the namespace stats dict for db_name on success, or None on
    error/empty/timeout (Req 6.2, 6.4). Abandons the result if the agent starts
    running mid-call (Req 6.5).
    """
    holder = {"result": None, "error": None}

    def _run():
        try:
            from agent_orchestrator import ensure_db_analyzed
            res = ensure_db_analyzed(conn_str, db_name)
            holder["result"] = res
        except Exception as e:
            holder["error"] = str(e)

    t = threading.Thread(target=_run, daemon=True)
    t.start()
    t.join(timeout=ONDEMAND_TIMEOUT_SEC)

    if t.is_alive():
        logger.warning("on-demand analysis for %s exceeded %ss watchdog", db_name, ONDEMAND_TIMEOUT_SEC)
        return None  # Req 6.4 — timeout; abandoned thread is daemon

    # Req 6.5 — if the agent began running while we waited, abandon the result.
    try:
        from agent_orchestrator import is_agent_running
        if is_agent_running():
            logger.info("agent started during on-demand analysis for %s; abandoning", db_name)
            return None
    except Exception:  # pragma: no cover - defensive
        pass

    if holder["error"] or not holder["result"]:
        return None  # Req 6.4
    res = holder["result"]
    if isinstance(res, dict) and db_name in res:
        return res[db_name]
    return None


# ── Advisor context + prompt + Bedrock (Task 4) ──────────────────────────────

_ADVISOR_DIR = os.path.join(os.path.dirname(__file__), "documentdb-advisor")


def _strip_frontmatter(text):
    if text.startswith("---"):
        end = text.find("---", 3)
        if end > 0:
            return text[end + 3:].strip()
    return text


def _load_advisor_context():
    """Load documentdb-advisor/SKILL.md + every file under references/ (Req 7.3).

    Returns the combined text, or None if nothing could be loaded (Req 7.4).
    """
    parts = []
    skill_path = os.path.join(_ADVISOR_DIR, "SKILL.md")
    try:
        with open(skill_path, "r", encoding="utf-8") as f:
            parts.append(_strip_frontmatter(f.read()))
    except Exception as e:
        logger.warning("advisor SKILL.md load failed: %s", e)

    ref_dir = os.path.join(_ADVISOR_DIR, "references")
    try:
        for fname in sorted(os.listdir(ref_dir)):
            if not fname.endswith(".md"):
                continue
            fpath = os.path.join(ref_dir, fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    parts.append(f"\n--- Reference: {fname} ---\n{f.read()}")
            except Exception as e:
                logger.warning("advisor reference %s load failed: %s", fname, e)
    except Exception as e:
        logger.warning("advisor references dir load failed: %s", e)

    if not parts:
        return None  # Req 7.4
    return "\n\n".join(parts)


def _build_prompt(pattern, namespace_stats, advisor_context):
    """Build (system_prompt, user_msg) for the Bedrock request (Req 7.1, 7.2)."""
    system_prompt = f"""{advisor_context}

You are an Amazon DocumentDB performance expert reviewing a single slow query
pattern. Base every recommendation on the DocumentDB best practices and the
actual index/collection statistics provided. Do NOT give generic database
advice — every point must be grounded in Amazon DocumentDB behavior and the
data shown.

Tone and guidance rules:
- Use measured, non-absolute language. Avoid strong or dismissive wording such
  as "No change needed", "No action required", or "nothing to do".
- Always try to offer a constructive path forward. If you cannot identify a
  query rewrite or an index that would help (for example, a deliberate full
  collection scan such as an unfiltered count over millions of documents),
  say that no query-level optimisations were identified and recommend that
  scaling up compute (a larger instance class / more memory) can help reduce
  the runtime, rather than implying the situation is fine as-is.

Choose ACTION as follows:
- "Add index"     — an index would reduce the scan.
- "Rewrite query" — restructuring the query/pipeline would help.
- "Scale compute" — no query-level optimisation is available; larger/faster
                    compute is the realistic lever.

Respond in EXACTLY this format and nothing else:

ACTION: <one of: Add index | Rewrite query | Scale compute>
RATIONALE: <1-3 sentences, DocumentDB-specific, referencing the stats. Measured tone, no absolute wording.>
SUGGESTION:
<For "Add index": a concrete db.<collection>.createIndex({{...}}) command.
 For "Rewrite query": the rewritten filter or aggregation pipeline.
 For "Scale compute": briefly note that no query optimisations were identified
   for this pattern and that scaling up compute (e.g. a larger instance class
   with more memory) can help reduce the runtime.>
"""

    # Compact the namespace stats to the fields that matter for the decision.
    stats_view = {}
    if namespace_stats:
        stats_view = {
            "count": namespace_stats.get("count"),
            "storageSize": namespace_stats.get("storageSize"),
            "avgObjSize": namespace_stats.get("avgObjSize"),
            "indexes": namespace_stats.get("indexes"),
            "index_analysis": namespace_stats.get("index_analysis"),
        }

    user_msg = f"""Slow query pattern:
- pattern_key: {pattern.get('pattern_key')}
- namespace (ns): {pattern.get('ns')}
- operation: {pattern.get('operation')}
- occurrences (count): {pattern.get('count')}
- avg_time_ms: {pattern.get('avg_time')}
- max_time_ms: {pattern.get('max_time')}
- example_query:
```json
{json.dumps(pattern.get('example_query', {}), indent=2, default=str)}
```

Collection statistics and indexes for this namespace:
```json
{json.dumps(stats_view, indent=2, default=str)}
```

Provide the optimization recommendation in the required format."""
    return system_prompt, user_msg


def _invoke_bedrock(system_prompt, user_msg, region):
    """Call MODEL_ID then FALLBACK_MODEL_ID (Req 7.5, 7.6). Returns text or None (Req 7.7)."""
    import boto3
    from bedrock_advisor import MODEL_ID, FALLBACK_MODEL_ID

    try:
        client = boto3.client("bedrock-runtime", region_name=region)
    except Exception as e:
        logger.error("bedrock client init failed: %s", e)
        return None

    for model_id in (MODEL_ID, FALLBACK_MODEL_ID):
        try:
            resp = client.invoke_model(
                modelId=model_id,
                body=json.dumps({
                    "messages": [{"role": "user", "content": user_msg}],
                    "max_tokens": 1500,
                    "temperature": 0.2,
                    "system": system_prompt,
                    "anthropic_version": "bedrock-2023-05-31",
                }),
            )
            result = json.loads(resp["body"].read())
            return result["content"][0]["text"].strip()
        except Exception as e:
            logger.warning("Bedrock %s failed: %s", model_id, e)
            continue
    return None  # Req 7.7 — both models failed


# ── Output parsing + safety validation (Task 5) ──────────────────────────────

def _parse_recommendation(text):
    """Extract the ACTION label and return (action, markdown) (Req 8.1, 8.2, 8.5)."""
    action = "Other"
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.upper().startswith("ACTION:"):
            label = stripped.split(":", 1)[1].strip()
            # Case-insensitive match against the allowed set.
            for allowed in _ACTION_LABELS:
                if label.lower() == allowed.lower():
                    action = allowed
                    break
            break
    return action, text


def _extract_suggestion_filters(text):
    """Extract candidate JSON objects from the SUGGESTION block for safety checking.

    Returns a list of parsed dicts. Best-effort: scans fenced code blocks and any
    inline {...} JSON object. Non-JSON suggestions (e.g. createIndex commands)
    yield the index key spec when parseable.
    """
    candidates = []
    # Fenced code blocks first.
    import re
    for block in re.findall(r"```(?:json)?\s*(.*?)```", text, re.DOTALL):
        candidates.append(block.strip())
    # Inline createIndex({...}) / find({...}) argument objects.
    for obj in re.findall(r"\{(?:[^{}]|\{[^{}]*\})*\}", text):
        candidates.append(obj.strip())

    filters = []
    for cand in candidates:
        try:
            parsed = json.loads(cand)
            if isinstance(parsed, dict):
                filters.append(parsed)
        except Exception:
            continue
    return filters


def _validate_safety(text):
    """Run _check_query_safety over any suggested filters (Req 9).

    Returns (unsafe: bool, reason: str|None). Fail-safe: any exception or a
    reported rejection -> unsafe=True (Req 9.5). No filters present -> safe (Req 9.4).
    """
    filters = _extract_suggestion_filters(text)
    if not filters:
        return False, None  # Req 9.4
    try:
        from bedrock_advisor import _check_query_safety
    except Exception as e:  # pragma: no cover - defensive
        logger.warning("could not import _check_query_safety: %s", e)
        return True, "Safety validator unavailable"  # Req 9.5 fail-safe

    for flt in filters:
        try:
            reason = _check_query_safety(flt)
        except Exception as e:
            logger.warning("query safety check raised: %s", e)
            return True, "Safety check error"  # Req 9.5 fail-safe
        if reason:
            return True, reason  # Req 9.2
    return False, None  # Req 9.3


# ── State helpers ────────────────────────────────────────────────────────────

def _set_state(pattern_key, **fields):
    with _rec_lock:
        entry = _rec_state.get(pattern_key, {
            "status": "placeholder", "recommendation": None, "error": None,
            "stats_digest": None, "started_ts": None, "fail_count": 0,
        })
        entry.update(fields)
        _rec_state[pattern_key] = entry


def _get_fail_count(pattern_key):
    with _rec_lock:
        entry = _rec_state.get(pattern_key)
        return entry.get("fail_count", 0) if entry else 0


# ── Cache read/write layer (Task 2) ──────────────────────────────────────────

def _cache_path(cache_key):
    return os.path.join(_CACHE_DIR, f"{cache_key}.json")


def _cache_save(cluster_id, pattern_key, stats_digest, recommendation):
    """Persist a recommendation atomically (Req 11.1). Returns True on success."""
    try:
        os.makedirs(_CACHE_DIR, exist_ok=True)
        key = _cache_key(cluster_id, pattern_key, stats_digest)
        path = _cache_path(key)
        payload = {
            "cluster_id": cluster_id,
            "pattern_key": pattern_key,
            "stats_digest": stats_digest,
            "action": recommendation["action"],
            "markdown": recommendation["markdown"],
            "unsafe": recommendation["unsafe"],
            "unsafe_reason": recommendation["unsafe_reason"],
            "_timestamp": time.strftime("%Y%m%d_%H%M%S"),
        }
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, default=str)
        os.replace(tmp, path)
        return True
    except Exception as e:
        logger.warning("cache save failed for %s: %s", pattern_key, e)
        return False  # Req 3.8 — caller discards


def _cache_load(cluster_id, pattern_key, stats_digest):
    """Load a cached recommendation matching the exact key (Req 11.3).

    Returns the recommendation dict or None. Corrupt/unreadable entries are
    treated as absent (Req 11.6).
    """
    key = _cache_key(cluster_id, pattern_key, stats_digest)
    path = _cache_path(key)
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if data.get("stats_digest") != stats_digest:
            return None  # Req 11.4 — stale
        return {
            "action": data["action"],
            "markdown": data["markdown"],
            "unsafe": data.get("unsafe", False),
            "unsafe_reason": data.get("unsafe_reason"),
        }
    except Exception as e:
        logger.warning("cache load failed (treating as absent) for %s: %s", pattern_key, e)
        return None  # Req 11.6


# ── Generation orchestration (Task 6) ────────────────────────────────────────

def _generate_one(pattern, cluster_id, region, force=False):
    """Generate a recommendation for one pattern. Runs on a worker thread.

    Returns nothing; updates _rec_state. Increments fail_count only on Bedrock
    connectivity failures (Req 13.7).
    """
    pattern_key = pattern.get("pattern_key")
    ns = pattern.get("ns", "")
    db_name = ns.split(".", 1)[0] if "." in ns else ns
    collection = ns.split(".", 1)[1] if "." in ns else ""

    # 2. Namespace gating + collision-safe fallback (Req 5, 6)
    available, stats = _namespace_stats(db_name, collection)
    if not available:
        if is_idle_for_analysis(db_name):
            try:
                from app import get_connection_string
                conn_str = get_connection_string() or ""
            except Exception:
                conn_str = ""
            db_result = _ensure_db_analyzed_bounded(conn_str, db_name)
            if db_result is None:
                _set_state(pattern_key, status="placeholder",
                           error=f"db analysis unavailable for {db_name}")  # Req 6.4
                return
            stats = db_result.get(collection, {}) if isinstance(db_result, dict) else {}
        else:
            # Agent busy / analysis pending — stay placeholder, retry later (Req 6.1)
            _set_state(pattern_key, status="placeholder")
            return

    # 1. Cache check (unless force) — needs stats_digest (Req 11.3)
    stats_digest = _stats_digest(stats)
    if not force:
        cached = _cache_load(cluster_id, pattern_key, stats_digest)
        if cached is not None:
            _set_state(pattern_key, status="done", recommendation=cached,
                       stats_digest=stats_digest, error=None)
            logger.info("slow_query rec done: %s ns=%s action=%s (cached)",
                        pattern_key, ns, cached.get("action"))
            return

    # 3. Build advisor context (Req 7.3, 7.4)
    advisor_context = _load_advisor_context()
    if advisor_context is None:
        _set_state(pattern_key, status="placeholder",
                   error="advisor context unavailable")  # Req 7.4 — not a Bedrock failure
        return

    # 4. Invoke Bedrock (Req 7.5-7.7)
    _set_state(pattern_key, status="generating", started_ts=time.time(),
               stats_digest=stats_digest)
    logger.debug("slow_query rec generating: %s ns=%s (invoking Bedrock)", pattern_key, ns)
    system_prompt, user_msg = _build_prompt(pattern, stats, advisor_context)
    text = _invoke_bedrock(system_prompt, user_msg, region)

    if text is None:
        # Bedrock unreachable / both models failed -> count toward Unavailable_State (Req 7.7, 13.1)
        fail_count = _get_fail_count(pattern_key) + 1
        if fail_count >= MAX_GENERATION_ATTEMPTS:
            _set_state(pattern_key, status="unavailable", fail_count=fail_count,
                       error="Bedrock unreachable")  # Req 13.1, 13.4
        else:
            _set_state(pattern_key, status="placeholder", fail_count=fail_count,
                       error="Bedrock invocation failed")
        return

    # 5. Parse (Req 8) + 6. Safety (Req 9)
    action, markdown = _parse_recommendation(text)
    unsafe, unsafe_reason = _validate_safety(text)
    recommendation = {
        "action": action,
        "markdown": markdown,
        "unsafe": unsafe,
        "unsafe_reason": unsafe_reason,
    }

    # 7. Persist + store (Req 11.1, 3.5, 3.8)
    if not _cache_save(cluster_id, pattern_key, stats_digest, recommendation):
        # Store failed — discard, leave no cached rec; mark placeholder for retry (Req 3.8)
        _set_state(pattern_key, status="placeholder", error="cache store failed")
        return

    _set_state(pattern_key, status="done", recommendation=recommendation,
               stats_digest=stats_digest, error=None, fail_count=0)
    logger.info("slow_query rec done: %s ns=%s action=%s%s",
                pattern_key, ns, action, " [unsafe]" if unsafe else "")


# ── Scheduler + per-db concurrency + timeouts (Task 7) ───────────────────────

def _get_semaphore(db_name):
    with _sem_lock:
        sem = _db_semaphores.get(db_name)
        if sem is None:
            limit = max(1, min(20, CONCURRENCY_LIMIT))  # Req 10.1 clamp
            sem = threading.Semaphore(limit)
            _db_semaphores[db_name] = sem
        return sem


def _run_job(db_name, pattern, cluster_id, region, force):
    """Worker body for one job: acquire per-db slot, run generation under timeout,
    always release the slot and clear in-flight (Req 10.4)."""
    pattern_key = pattern.get("pattern_key")
    sem = _get_semaphore(db_name)
    sem.acquire()
    try:
        # Enforce GEN_TIMEOUT_SEC by running generation in a nested thread (Req 3.6).
        worker = threading.Thread(
            target=_generate_one, args=(pattern, cluster_id, region, force), daemon=True)
        worker.start()
        worker.join(timeout=GEN_TIMEOUT_SEC)
        if worker.is_alive():
            logger.warning("generation for %s exceeded %ss; discarding", pattern_key, GEN_TIMEOUT_SEC)
            # Req 3.6 — discard partial result, leave eligible for regen.
            _set_state(pattern_key, status="placeholder", error="generation timeout")
    except Exception as e:
        logger.error("generation worker for %s crashed: %s", pattern_key, e, exc_info=True)
        _set_state(pattern_key, status="failed", error=str(e))  # Req 3.7
    finally:
        sem.release()  # Req 10.4
        with _rec_lock:
            _inflight.discard(pattern_key)
            drained = not _inflight
            if drained:
                # Batch complete: this was the last in-flight pattern. Summarize
                # the current rec states so completion is observable in the log.
                # (May fire more than once if patterns are scheduled in waves,
                # e.g. some gated until the agent finishes.)
                counts = {}
                for entry in _rec_state.values():
                    counts[entry.get("status")] = counts.get(entry.get("status"), 0) + 1
        if drained:
            logger.info("slow_query recommendations complete: %d done, %d failed, "
                        "%d unavailable, %d pending (total %d patterns)",
                        counts.get("done", 0), counts.get("failed", 0),
                        counts.get("unavailable", 0),
                        counts.get("placeholder", 0) + counts.get("generating", 0),
                        sum(counts.values()))


def _dispatcher_loop():
    """Single dispatcher: pulls FIFO jobs, drops stale ones, spawns workers (Req 10.5)."""
    while True:
        db_name, pattern, cluster_id, region, force, enqueue_ts = _job_queue.get()
        try:
            pattern_key = pattern.get("pattern_key")
            # Drop jobs that waited too long (Req 10.5).
            if time.time() - enqueue_ts > QUEUE_TIMEOUT_SEC:
                logger.warning("job for %s dropped after >%ss in queue", pattern_key, QUEUE_TIMEOUT_SEC)
                _set_state(pattern_key, status="placeholder", error="queue timeout")
                with _rec_lock:
                    _inflight.discard(pattern_key)
                continue
            # If reset cleared the in-flight marker, skip (job is stale).
            with _rec_lock:
                if pattern_key not in _inflight:
                    continue
            threading.Thread(
                target=_run_job,
                args=(db_name, pattern, cluster_id, region, force),
                daemon=True,
            ).start()
        finally:
            _job_queue.task_done()


def _ensure_dispatcher():
    global _dispatcher_started
    with _dispatcher_lock:
        if not _dispatcher_started:
            threading.Thread(target=_dispatcher_loop, daemon=True).start()
            _dispatcher_started = True


def _agent_busy():
    """True only while the agent's db_analysis module is still pending/running.

    Automatic recommendation scheduling must not contend with db_analysis (the
    heavy writer thread) for the agent's shared lock — scheduling gated patterns
    on every 1.5s slow-query poll while db_analysis ran caused heavy lock
    contention that slowed analysis to a crawl.

    Gating on the *entire* agent run (is_agent_running) needlessly delayed
    recommendations until later modules (compression, recommenders) finished
    too — and those don't contend with db_analysis. So we gate only on
    db_analysis itself: once db_analysis is done, recommendations may be
    scheduled even while the agent runs other modules (e.g. compression).

    Returns True (suppress) only when the agent is running AND db_analysis has
    not finished. Returns False on any error (fail open)."""
    try:
        from agent_orchestrator import is_agent_running
        if not is_agent_running():
            return False
    except Exception:
        return False
    try:
        _, status = _db_analysis_snapshot()
        return status in ("pending", "running")
    except Exception:
        # Couldn't determine db_analysis state; stay conservative and suppress.
        return True


def schedule(patterns, cluster_id, region, force=False):
    """Schedule background recommendation generation for the given patterns.

    Non-blocking. Idempotent via _inflight. Skips patterns that already have a
    valid in-memory recommendation (unless force) or are in the Unavailable_State
    (unless force/on-demand). Returns immediately (Req 3.1, 3.2, 3.3, 4.2, 4.5).

    Automatic (non-forced) scheduling is suppressed while the autonomous agent is
    running, to avoid contending with db_analysis for the agent lock. Explicit
    on-demand/re-analyze requests (force=True) are always honoured.
    """
    _ensure_dispatcher()
    if not patterns:
        return

    # Suppress automatic scheduling during active analysis (see _agent_busy).
    if not force and _agent_busy():
        return

    _queued = 0
    for pattern in patterns:
        pattern_key = pattern.get("pattern_key")
        if not pattern_key:
            continue
        ns = pattern.get("ns", "")
        db_name = ns.split(".", 1)[0] if "." in ns else ns

        with _rec_lock:
            entry = _rec_state.get(pattern_key)
            if pattern_key in _inflight:
                continue  # Req 3.2, 4.5 — already scheduled/in-flight
            if not force and entry is not None:
                status = entry.get("status")
                if status == "done":
                    continue  # already have a recommendation
                if status == "unavailable":
                    continue  # Req 13.4 — stop auto-reschedule until force/on-demand
            if force and entry is not None:
                # Reset failure tracking on explicit re-analyze/on-demand (Req 13.5)
                entry["fail_count"] = 0
                entry["status"] = "placeholder"
                entry["error"] = None
            # Mark placeholder + in-flight, then enqueue.
            _inflight.add(pattern_key)
            _rec_state[pattern_key] = entry or {
                "status": "placeholder", "recommendation": None, "error": None,
                "stats_digest": None, "started_ts": None, "fail_count": 0,
            }

        _job_queue.put((db_name, pattern, cluster_id, region, force, time.time()))
        _queued += 1

    # Batch-start marker — only when this call actually queued new work, so the
    # idle render/poll calls (which dedup to zero) stay quiet. Pairs with the
    # "slow_query recommendations complete" log.
    if _queued:
        logger.info("slow_query recommendations scheduled: %d pattern(s) queued%s",
                    _queued, " (force)" if force else "")


def request_one(pattern, cluster_id, region):
    """On-demand request for a single (possibly non-displayed) pattern (Req 4.2).

    Treated like a forced schedule so it bypasses the Unavailable_State guard and
    resets fail tracking, but still reuses a valid cache entry inside _generate_one
    (cache check runs only when not force) — so we pass force=False to preserve the
    cache short-circuit (Req 4.4) while explicitly clearing any unavailable status.
    """
    pattern_key = pattern.get("pattern_key")
    with _rec_lock:
        entry = _rec_state.get(pattern_key)
        if entry and entry.get("status") == "unavailable":
            entry["status"] = "placeholder"
            entry["fail_count"] = 0
            entry["error"] = None
            _rec_state[pattern_key] = entry
    schedule([pattern], cluster_id, region, force=False)
