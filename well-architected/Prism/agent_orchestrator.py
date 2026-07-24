"""Autonomous Agent Orchestrator — Observe→Reason→Decide→Act loop."""
import re
import json
import logging
import threading
import time

logger = logging.getLogger(__name__)

AGENT_ENABLED = True  # Kill switch

_lock = threading.Lock()
_stop_event = threading.Event()
_threads = []

_agent_state = {
    "status": "idle",
    "current_module": "",
    "current_detail": "",
    "current_reasoning": "",
    "pct": 0,
    "modules": {
        "cluster_snapshot": {"status": "pending", "result": None, "error": None, "ts": None},
        "db_analysis": {"status": "pending", "result": None, "error": None, "ts": None},
        "slow_query": {"status": "pending", "result": None, "error": None, "ts": None},
        "compression": {"status": "pending", "result": None, "error": None, "ts": None},
        "bloat": {"status": "pending", "result": None, "error": None, "ts": None},
        "well_architected": {"status": "pending", "result": None, "error": None, "ts": None},
        "instance_recommender": {"status": "pending", "result": None, "error": None, "ts": None},
        "storage_recommender": {"status": "pending", "result": None, "error": None, "ts": None},
    },
    "skipped_modules": [],
    "correlated_insights": [],
    "live_alerts": [],
    "report": {"ready": False, "markdown": "", "generated_at": None},
    "new_analysis_available": None,
    "reasoning_log": [],
    "analysis_scope": {"in_scope": [], "skipped": {}},
}

# Fixed fallback order if Bedrock planning fails
_FALLBACK_ORDER = [
    "cluster_snapshot", "well_architected", "db_analysis",
    "slow_query", "compression", "bloat",
    "instance_recommender", "storage_recommender",
]

# ═══ PUBLIC API ═══════════════════════════════════════════════════════════════

def start_agent(conn_data: dict) -> None:
    """Start the autonomous agent. Non-blocking."""
    if not AGENT_ENABLED:
        return
    stop_agent()
    _stop_event.clear()
    _reset_state()
    t1 = threading.Thread(target=_agent_loop, args=(conn_data,), daemon=True)
    t2 = threading.Thread(target=_monitor_activity, args=(conn_data.get("connection_string", ""),), daemon=True)
    _threads.extend([t1, t2])
    t1.start()
    t2.start()


def stop_agent() -> None:
    """Stop the agent."""
    _stop_event.set()
    for t in _threads:
        t.join(timeout=3)
    _threads.clear()


def get_agent_state() -> dict:
    """Thread-safe read of current agent state — lightweight, no full results."""
    with _lock:
        # Return summary only — full results accessed via get_db_analysis_results()
        modules_summary = {}
        for name, mod in _agent_state["modules"].items():
            modules_summary[name] = {
                "status": mod.get("status", "pending"),
                "ts": mod.get("ts"),
                "error": mod.get("error"),
            }
        return {
            "status": _agent_state["status"],
            "current_module": _agent_state["current_module"],
            "current_reasoning": _agent_state["current_reasoning"],
            "pct": _agent_state["pct"],
            "modules": modules_summary,
            "skipped_modules": list(_agent_state["skipped_modules"]),
            "correlated_insights": list(_agent_state["correlated_insights"]),
            "live_alerts": list(_agent_state["live_alerts"]),
            "report": dict(_agent_state["report"]),
            "reasoning_log": list(_agent_state["reasoning_log"]),
            "analysis_scope": dict(_agent_state["analysis_scope"]),
            "_db_analysis_version": _agent_state.get("_db_analysis_version", 0),
        }


def get_db_analysis_results() -> dict:
    """Return the full db_analysis results dict. Called by merge callback."""
    with _lock:
        r = _agent_state["modules"]["db_analysis"].get("result")
        if r and isinstance(r, dict):
            return dict(r)
        return {}


def get_live_alerts() -> list:
    with _lock:
        return list(_agent_state["live_alerts"])


def is_agent_running() -> bool:
    with _lock:
        return _agent_state["status"] in ("observing", "reasoning", "running")


def ensure_db_analyzed(conn_str: str, db_name: str) -> dict:
    """If db not yet analyzed, run db_analysis synchronously. Returns result."""
    with _lock:
        mod = _agent_state["modules"]["db_analysis"]
        if mod["status"] == "done" and mod["result"] and db_name in mod["result"]:
            return mod["result"]
    try:
        from db_analyzer import get_documentdb_stats
        result = get_documentdb_stats({"connection_string": conn_str, "database_name": db_name})
        # Unwrap: returns {db_name: {coll: data}}
        if isinstance(result, dict) and db_name in result:
            result = result[db_name]
        with _lock:
            if _agent_state["modules"]["db_analysis"]["result"] is None:
                _agent_state["modules"]["db_analysis"]["result"] = {}
            _agent_state["modules"]["db_analysis"]["result"][db_name] = result
            _agent_state["new_analysis_available"] = {db_name: result}
        return {db_name: result}
    except Exception as e:
        logger.error("ensure_db_analyzed failed: %s", e)
        return {}


def ensure_slow_queries_analyzed(cluster_id: str, region: str, log_group: str = "") -> list:
    """If slow queries not yet analyzed, run analysis synchronously. Returns patterns list."""
    with _lock:
        mod = _agent_state["modules"]["slow_query"]
        if mod["status"] == "done" and mod["result"]:
            r = mod["result"]
            return r if isinstance(r, list) else []
    if not log_group and cluster_id:
        log_group = f"/aws/docdb/{cluster_id}/profiler"
    if not log_group:
        return []
    try:
        from query_analyzer import get_query_patterns
        result = get_query_patterns("*", "*", hours=168, log_group_name=log_group, aws_region=region)
        with _lock:
            _agent_state["modules"]["slow_query"]["status"] = "done"
            _agent_state["modules"]["slow_query"]["result"] = result
            _agent_state["modules"]["slow_query"]["ts"] = time.time()
        return result or []
    except Exception as e:
        logger.error("ensure_slow_queries_analyzed failed: %s", e)
        return []


# ═══ INTERNAL HELPERS ═════════════════════════════════════════════════════════

def _reset_state():
    with _lock:
        _agent_state["status"] = "idle"
        _agent_state["current_module"] = ""
        _agent_state["current_detail"] = ""
        _agent_state["current_reasoning"] = ""
        _agent_state["pct"] = 0
        _agent_state["skipped_modules"] = []
        _agent_state["correlated_insights"] = []
        _agent_state["live_alerts"] = []
        _agent_state["report"] = {"ready": False, "markdown": "", "generated_at": None}
        _agent_state["new_analysis_available"] = None
        _agent_state["reasoning_log"] = []
        _agent_state["analysis_scope"] = {"in_scope": [], "skipped": {}}
        _agent_state["_agentic_findings"] = []
        for m in _agent_state["modules"].values():
            m.update(status="pending", result=None, error=None, ts=None)

    # Clear tab-level caches
    try:
        from tabs.cluster_slow_queries import reset_cache as reset_sq_cache
        reset_sq_cache()
    except Exception:
        pass
    try:
        import index_usage_cluster
        index_usage_cluster.reset_cache()
    except Exception:
        pass


def _set_status(status, module="", reasoning=""):
    with _lock:
        _agent_state["status"] = status
        _agent_state["current_module"] = module
        _agent_state["current_reasoning"] = reasoning


def _calc_pct(completed, total):
    if total == 0:
        return 0
    return min(int(completed / total * 100), 99)


# ═══ SAFETY & PRIORITISATION ═════════════════════════════════════════════════

def _get_prioritisation_cfg():
    from prism_cfg import get_config
    return get_config().get("agent_prioritisation", {})


def _safety_check(module, target_db, target_collection, conn_data):
    """Returns (safe: bool, reason: str)."""
    cfg = _get_prioritisation_cfg()
    system_dbs = {"admin", "local", "config"}

    if target_db and target_db.lower() in system_dbs:
        return False, f"'{target_db}' is a system database"

    skip_dbs = cfg.get("skip_databases", ["staging", "local", "admin", "config"])
    if target_db and any(target_db.lower() == s.lower() or target_db.lower().startswith(s.lower()) for s in skip_dbs):
        return False, f"Database '{target_db}' is on skip list"

    skip_colls = cfg.get("skip_collections", [])
    if target_collection and any(target_collection.lower().startswith(s.lower()) for s in skip_colls):
        return False, f"Collection '{target_collection}' matches skip pattern"

    return True, ""


def _build_analysis_scope(all_databases, db_sizes, active_databases=None):
    """Build prioritised scope from config. Active databases (from currentOp) get top priority."""
    cfg = _get_prioritisation_cfg()
    priority_list = cfg.get("priority_databases", [])
    skip_list = cfg.get("skip_databases", ["staging", "local", "admin", "config"])
    max_dbs = cfg.get("max_databases_to_analyse", 20)
    active_dbs = set(active_databases or [])

    skipped = {}
    candidates = []
    for db in all_databases:
        if any(db.lower() == s.lower() or db.lower().startswith(s.lower()) for s in skip_list):
            skipped[db] = "on skip list"
            continue
        candidates.append(db)

    def sort_key(db):
        # Active databases first, then priority list, then by size
        if db in active_dbs:
            return (0, 0)
        if db in priority_list:
            return (1, priority_list.index(db))
        return (2, -(db_sizes.get(db, 0)))

    candidates.sort(key=sort_key)
    in_scope = candidates[:max_dbs]
    for db in candidates[max_dbs:]:
        skipped[db] = f"exceeds max_databases_to_analyse ({max_dbs})"

    return {"in_scope": in_scope, "skipped": skipped}


def _iter_databases(databases):
    """Yield databases with pacing from config."""
    from prism_cfg import get_config
    cfg = get_config()
    delay = cfg.get("database_tree", {}).get("delay_between_seconds", 2)
    for db in databases:
        if _stop_event.is_set():
            break
        yield db
        _stop_event.wait(delay)


# ═══ OBSERVE PHASE ════════════════════════════════════════════════════════════

def _observe(conn_data):
    """Collect fast cluster facts for reasoning."""
    import boto3
    import pymongo

    obs = {
        "cluster_id": conn_data.get("cluster_id", ""),
        "region": conn_data.get("region", "us-east-1"),
        "databases": [],
        "instance_types": [],
        "engine_version": "",
        "deletion_protection": False,
        "backup_retention": 0,
        "log_exports": [],
        "db_sizes": {},
        "collection_counts": {},
        "has_slow_query_logs": False,
        "priority_signals": [],
        "completed_modules": [],
    }

    # AWS metadata
    try:
        region = conn_data.get("region", "us-east-1")
        cluster_id = conn_data.get("cluster_id", "")
        if cluster_id:
            docdb = boto3.client("docdb", region_name=region)
            cl = docdb.describe_db_clusters(DBClusterIdentifier=cluster_id)["DBClusters"][0]
            obs["engine_version"] = cl.get("EngineVersion", "")
            obs["deletion_protection"] = cl.get("DeletionProtection", False)
            obs["backup_retention"] = cl.get("BackupRetentionPeriod", 0)
            obs["log_exports"] = cl.get("EnabledCloudwatchLogsExports", [])

            insts = docdb.describe_db_instances(
                Filters=[{"Name": "db-cluster-id", "Values": [cluster_id]}])["DBInstances"]
            obs["instance_types"] = [i["DBInstanceClass"] for i in insts]
    except Exception as e:
        logger.warning("Observe AWS metadata failed: %s", e)

    # Database sizes via pymongo
    conn_str = conn_data.get("connection_string", "")
    if conn_str:
        try:
            client = pymongo.MongoClient(conn_str, serverSelectionTimeoutMS=5000, appname='DocDB-Prism')
            dbs = [d for d in client.list_database_names() if d not in ("admin", "local", "config")]
            obs["databases"] = dbs
            from prism_cfg import get_config
            batch = get_config().get("database_tree", {}).get("initial_batch", 10)
            for db_name in dbs[:batch]:
                if _stop_event.is_set():
                    break
                try:
                    stats = client[db_name].command("dbStats")
                    obs["db_sizes"][db_name] = stats.get("dataSize", 0) / (1024 * 1024)
                    obs["collection_counts"][db_name] = stats.get("collections", 0)
                except Exception:
                    pass
            client.close()
        except Exception as e:
            logger.warning("Observe pymongo failed: %s", e)

    # Check CloudWatch log group — derive default from cluster_id if not provided
    log_group = conn_data.get("log_group", "")
    if not log_group and obs["cluster_id"]:
        log_group = f"/aws/docdb/{obs['cluster_id']}/profiler"
    obs["log_group"] = log_group
    try:
        if log_group:
            logs_client = boto3.client("logs", region_name=obs["region"])
            logs_client.describe_log_streams(logGroupName=log_group, limit=1)
            obs["has_slow_query_logs"] = True
    except Exception:
        obs["has_slow_query_logs"] = False

    # Detect active databases from currentOp to prioritize them
    obs["active_databases"] = []
    if conn_str:
        try:
            from tabs.current_activity import _fetch_current_ops
            ops, err = _fetch_current_ops(conn_str)
            if not err and ops:
                active_dbs = set()
                for op in ops:
                    ns = op.get("ns", "")
                    if "." in ns:
                        db_part = ns.split(".", 1)[0]
                        if db_part not in ("admin", "local", "config", ""):
                            active_dbs.add(db_part)
                obs["active_databases"] = list(active_dbs)
        except Exception:
            pass

    # Load previous state and detect changes
    cluster_id = obs["cluster_id"]
    if cluster_id:
        try:
            import agent_memory as mem
            mem.cleanup_old_files(cluster_id)
            changes = mem.compare_with_previous(cluster_id, obs)
            obs["changes_from_previous"] = changes
            prev_analysis = mem.load_last_analysis(cluster_id)
            if prev_analysis:
                obs["previous_insights"] = prev_analysis.get("correlated_insights", [])[:5]
            recent_activity = mem.load_recent_activity(cluster_id, hours=24)
            if recent_activity:
                obs["recent_activity_summary"] = {
                    "periods": len(recent_activity),
                    "peak_active": max((a.get("peak_active", 0) for a in recent_activity), default=0),
                    "total_slow": sum(a.get("total_slow", 0) for a in recent_activity),
                }
        except Exception as e:
            logger.debug("Memory load failed: %s", e)

    return obs


# ═══ REASON PHASE — Bedrock as Planner ════════════════════════════════════════

_PLANNER_ADDENDUM = """

## Agent Planner Role
Decide what to analyze next. Return JSON only.

Available modules: cluster_snapshot, well_architected, db_analysis, slow_query, compression, bloat, instance_recommender, storage_recommender

Rules:
1. Prioritize modules most likely to find actionable issues.
2. If a collection has 0 indexes and millions of docs, prioritize slow_query.
3. If CPU avg < 5%, deprioritize instance_recommender.
4. If no slow query logs exist, skip slow_query.
5. If avg bloat < 5%, skip bloat.
6. Always run cluster_snapshot and well_architected first.
7. Return "done" when all high-priority modules complete.

JSON format:
{"next_module": "<module_name>|done", "target_database": "<db>", "target_collection": null, "reasoning": "<one sentence>", "priority": "high|medium|low", "skip_modules": [], "skip_reasons": {}}
"""


def _reason(observation, completed_modules, findings_signals):
    """Call Bedrock to decide next module. Falls back to fixed order on failure."""
    try:
        from bedrock_advisor import _load_skill, _call_bedrock_simple

        skill_text = _load_skill()
        system = skill_text + _PLANNER_ADDENDUM

        user_msg = json.dumps({
            "observation": {k: v for k, v in observation.items() if k != "priority_signals" or v},
            "completed_modules": completed_modules,
            "findings_signals": findings_signals[:20],
            "available_modules": [m for m in _FALLBACK_ORDER if m not in completed_modules],
            "changes_from_previous": observation.get("changes_from_previous", []),
            "previous_insights": observation.get("previous_insights", []),
            "recent_activity": observation.get("recent_activity_summary"),
        }, default=str)

        import boto3
        from bedrock_advisor import MODEL_ID, FALLBACK_MODEL_ID
        region = observation.get("region", "us-east-1")

        for model_id in [MODEL_ID, FALLBACK_MODEL_ID]:
            try:
                client = boto3.client("bedrock-runtime", region_name=region)
                resp = client.invoke_model(
                    modelId=model_id,
                    body=json.dumps({
                        "messages": [{"role": "user", "content": user_msg}],
                        "max_tokens": 500,
                        "temperature": 0.1,
                        "system": system,
                        "anthropic_version": "bedrock-2023-05-31",
                    }),
                )
                result = json.loads(resp["body"].read())
                text = result["content"][0]["text"].strip()
                # Strip markdown fences if present
                text = re.sub(r"^```[a-z]*\s*", "", text)
                text = re.sub(r"\s*```$", "", text.strip())
                # Extract first complete JSON object
                m = re.search(r"\{.*\}", text, re.DOTALL)
                if m:
                    return json.loads(m.group())
            except Exception as e:
                logger.warning("Bedrock planner %s failed: %s", model_id, e)
                continue
    except Exception as e:
        logger.warning("Reason phase failed entirely: %s", e)

    # Fallback: next uncompleted module in fixed order
    return _fallback_decision(completed_modules)


def _fallback_decision(completed_modules):
    """Fixed priority order fallback."""
    for m in _FALLBACK_ORDER:
        if m not in completed_modules:
            return {"next_module": m, "target_database": None, "target_collection": None,
                    "reasoning": "Fallback: sequential order", "priority": "medium",
                    "skip_modules": [], "skip_reasons": {}}
    return {"next_module": "done", "reasoning": "All modules complete"}


# ═══ DECIDE PHASE ═════════════════════════════════════════════════════════════

def _decide(decision, completed):
    """Validate Bedrock's decision. Returns module name or 'done'."""
    module = decision.get("next_module", "done")
    if module == "done":
        # Verify critical modules ran — slow_query is critical when profiler logs exist
        critical = {"cluster_snapshot", "well_architected", "db_analysis", "slow_query"}
        missing = critical - set(completed)
        # Skip slow_query from critical if no log group available
        if "slow_query" in missing:
            scope = _agent_state.get("analysis_scope", {})
            # Check if observation found slow query logs
            obs_has_logs = bool(_agent_state.get("_observation", {}).get("has_slow_query_logs"))
            if not obs_has_logs:
                missing.discard("slow_query")
        if missing:
            return missing.pop()
        return "done"
    if module in completed:
        return _fallback_decision(completed).get("next_module", "done")
    if module not in _agent_state["modules"]:
        return _fallback_decision(completed).get("next_module", "done")
    return module


# ═══ ACT PHASE ════════════════════════════════════════════════════════════════

_MODULE_TIMEOUT = 120  # seconds max per module

# Per-database wall-clock budget for db_analysis. A degraded/half-dead tunnel can
# make a single DB's analysis (esp. the cardinality find/$sample) block far past
# socketTimeoutMS; this bounds it so the run continues to the next database.
# Set above the observed slow-but-completing case (~85s on a degraded tunnel) so
# a recovering DB finishes rather than being skipped at the edge.
_DB_ANALYSIS_PER_DB_TIMEOUT = 120  # seconds


def _run_with_timeout(fn, timeout=_MODULE_TIMEOUT):
    """Run fn() in a thread with timeout. Returns (result, error)."""
    result_box = [None, None]  # [result, error]

    def _wrapper():
        try:
            result_box[0] = fn()
        except Exception as e:
            result_box[1] = e

    t = threading.Thread(target=_wrapper, daemon=True)
    t.start()
    t.join(timeout=timeout)
    if t.is_alive():
        return None, TimeoutError(f"Module timed out after {timeout}s")
    if result_box[1]:
        return None, result_box[1]
    return result_box[0], None


def _act(module, target_db, target_collection, conn_data):
    """Execute chosen module. Returns structured findings.

    Tries the analyzer plugin system first. Falls back to legacy hardcoded dispatch.
    """
    conn_str = conn_data.get("connection_string", "")
    cluster_id = conn_data.get("cluster_id", "")
    region = conn_data.get("region", "us-east-1")
    log_group = conn_data.get("log_group", "")
    if not log_group and cluster_id:
        log_group = f"/aws/docdb/{cluster_id}/profiler"
    result = None
    key_findings = []
    priority_signals = []

    # ── Legacy hardcoded dispatch ────────────────────────────────────────────

    try:
        if module == "cluster_snapshot":
            from tabs.cluster_snapshot import _run_snapshot, _snap, _snap_lock
            with _snap_lock:
                if _snap["done"] and _snap["data"]:
                    result = _snap["data"]
                elif _snap["running"]:
                    result = "_waiting"
            # Wait for in-progress snapshot to finish
            if result == "_waiting":
                for _ in range(90):
                    if _stop_event.is_set():
                        break
                    with _snap_lock:
                        if _snap["done"]:
                            result = _snap["data"]
                            break
                    time.sleep(1)
                else:
                    result = None
            if not result or result == "_waiting":
                res, err = _run_with_timeout(
                    lambda: _run_snapshot(cluster_id, region, conn_str), timeout=90)
                if err:
                    logger.warning("cluster_snapshot timed out: %s", err)
                with _snap_lock:
                    result = _snap["data"]
            if result:
                for inst in result.get("instances", []):
                    if inst.get("cpu_avg") is not None and inst["cpu_avg"] < 5:
                        priority_signals.append(f"{inst['id']} CPU avg {inst['cpu_avg']:.1f}% — may be oversized")

        elif module == "well_architected":
            from tabs.well_architected import _run_wa_checks, _wa, _wa_lock
            with _wa_lock:
                if _wa["done"] and _wa["results"]:
                    result = _wa["results"]
                elif _wa["running"]:
                    result = "_waiting"
            if result == "_waiting":
                for _ in range(90):
                    if _stop_event.is_set():
                        break
                    with _wa_lock:
                        if _wa["done"]:
                            result = _wa["results"]
                            break
                    time.sleep(1)
                else:
                    result = None
            if not result or result == "_waiting":
                _run_with_timeout(
                    lambda: _run_wa_checks(cluster_id, region), timeout=90)
                with _wa_lock:
                    result = _wa["results"]
            if result:
                fails = [r for r in result if r["status"] == "fail"]
                key_findings = [r["label"] for r in fails[:5]]
            # Also trigger v2 extra checks so merged results are available
            try:
                from tabs.wa_v2 import _run_all_extra, _v2_state, _v2_lock
                with _v2_lock:
                    already_done = _v2_state["done"]
                if not already_done:
                    threading.Thread(
                        target=_run_all_extra,
                        args=(cluster_id, region, conn_str),
                        daemon=True,
                    ).start()
            except Exception as e:
                logger.debug("v2 extra checks trigger failed: %s", e)

        elif module == "db_analysis":
            from db_analyzer import get_documentdb_stats
            scope = _agent_state.get("analysis_scope", {}).get("in_scope", [])
            dbs = scope if scope else ([target_db] if target_db else [])
            # Resolve a reader/secondary ONCE per run for cardinality sampling
            # offload (not per-database). None → primary-only sampling.
            _reader_conn = None
            try:
                if cluster_id:
                    from index_usage_cluster import get_reader_connection_string
                    _reader_conn = get_reader_connection_string(cluster_id, region, conn_str)
            except Exception as e:
                logger.debug("db_analysis: reader offload unavailable: %s", e)
            # Get already-analyzed databases to skip them
            with _lock:
                existing = _agent_state["modules"]["db_analysis"].get("result")
            already_done = set(existing.keys()) if isinstance(existing, dict) else set()
            result = dict(existing) if isinstance(existing, dict) else {}
            _dbs_to_do = [d for d in _iter_databases(dbs)]
            logger.info("db_analysis: module start — %d database(s) in scope, %d already done",
                        len(_dbs_to_do), len(already_done))
            _analyzed_count = 0
            for _idx, db_name in enumerate(_dbs_to_do, 1):
                if _stop_event.is_set():
                    logger.info("db_analysis: stop event set, halting at %s (%d/%d)",
                                db_name, _idx, len(_dbs_to_do))
                    break
                if db_name in already_done:
                    continue
                safe, reason = _safety_check(module, db_name, None, conn_data)
                if not safe:
                    continue
                logger.info("db_analysis: starting %s (%d/%d)", db_name, _idx, len(_dbs_to_do))
                _t0 = time.time()
                try:
                    # Per-DB wall-clock watchdog. On a half-dead tunnel, a single
                    # pymongo operation (notably the cardinality find/$sample) can
                    # block far past socketTimeoutMS, stalling the whole run for
                    # many minutes. The watchdog abandons a DB that exceeds the
                    # budget (its daemon thread dies with the process) and lets the
                    # loop continue to the next database.
                    db_result, _err = _run_with_timeout(
                        lambda: get_documentdb_stats(
                            {"connection_string": conn_str, "database_name": db_name,
                             "reader_connection_string": _reader_conn}),
                        timeout=_DB_ANALYSIS_PER_DB_TIMEOUT)
                    if _err is not None:
                        logger.warning("db_analysis for %s aborted after %.1fs: %s",
                                       db_name, time.time() - _t0, _err)
                        continue
                    # get_documentdb_stats returns {db_name: {coll: {...}}} — unwrap
                    if isinstance(db_result, dict) and db_name in db_result:
                        db_result = db_result[db_name]
                    elif isinstance(db_result, dict) and "error" in db_result:
                        logger.warning("db_analysis for %s returned error: %s", db_name, db_result["error"])
                        continue
                    result[db_name] = db_result
                    # Emit lazily per-database so UI updates incrementally
                    with _lock:
                        if _agent_state["modules"]["db_analysis"]["result"] is None:
                            _agent_state["modules"]["db_analysis"]["result"] = {}
                        _agent_state["modules"]["db_analysis"]["result"][db_name] = db_result
                        _agent_state["_db_analysis_version"] = _agent_state.get("_db_analysis_version", 0) + 1
                    _analyzed_count += 1
                    logger.info("db_analysis: %s complete (%.1fs)", db_name, time.time() - _t0)
                    # Extract signals
                    for coll, cd in db_result.items():
                        if not isinstance(cd, dict):
                            continue
                        ia = cd.get("index_analysis", {})
                        if ia.get("total_indexes", 1) == 0 and cd.get("count", 0) > 100000:
                            priority_signals.append(f"{db_name}.{coll}: 0 indexes, {cd['count']:,} docs")
                        bloat = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
                        if bloat > 30:
                            priority_signals.append(f"{db_name}.{coll}: {bloat:.0f}% bloat")
                except Exception as e:
                    logger.warning("db_analysis for %s failed after %.1fs: %s",
                                   db_name, time.time() - _t0, e)
            logger.info("db_analysis: module loop finished — %d analyzed this run", _analyzed_count)

        elif module == "slow_query":
            if log_group:
                from query_analyzer import get_query_patterns
                result = get_query_patterns("*", "*", 168, log_group, region)
                if result:
                    patterns = result if isinstance(result, list) else result.get("patterns", [])
                    for p in patterns[:5]:
                        ns = p.get("ns", p.get("namespace", ""))
                        avg_ms = p.get("avg_ms", p.get("avgMs", 0))
                        if avg_ms > 200:
                            priority_signals.append(f"Slow query on {ns}: avg {avg_ms:.0f}ms")

        elif module == "compression":
            from compression_analyzer import analyze_collection_compression
            from prism_cfg import get_config
            comp_cfg = get_config().get("compression_analysis", {})
            batch_size = comp_cfg.get("collections_per_batch", 5)
            comp_delay = comp_cfg.get("delay_between_seconds", 3)
            result = {}
            # Offload compression sampling to a reader/secondary when available;
            # fall back to the writer connection if none is reachable.
            comp_conn = conn_str
            try:
                if cluster_id:
                    from index_usage_cluster import get_reader_connection_string
                    _reader_conn = get_reader_connection_string(cluster_id, region, conn_str)
                    if _reader_conn:
                        comp_conn = _reader_conn
                        logger.info("compression: sampling from reader")
            except Exception as e:
                logger.debug("compression: reader offload unavailable: %s", e)
            scope = _agent_state.get("analysis_scope", {}).get("in_scope", [])
            dbs = scope if scope else ([target_db] if target_db else [])
            for db_name in dbs:
                if _stop_event.is_set():
                    break
                db_result = _agent_state["modules"]["db_analysis"].get("result", {})
                collections = list((db_result.get(db_name, {}) if isinstance(db_result, dict) else {}).keys())[:batch_size]
                for coll in collections:
                    if _stop_event.is_set():
                        break
                    safe, reason = _safety_check(module, db_name, coll, conn_data)
                    if not safe:
                        continue
                    try:
                        cr = analyze_collection_compression(comp_conn, db_name, coll)
                        result[f"{db_name}.{coll}"] = cr
                    except Exception as e:
                        logger.warning("Compression %s.%s failed: %s", db_name, coll, e)
                _stop_event.wait(comp_delay)

        elif module == "bloat":
            # Bloat data is already in db_analysis results
            db_result = _agent_state["modules"]["db_analysis"].get("result", {})
            result = {}
            if isinstance(db_result, dict):
                for db_name, colls in db_result.items():
                    if not isinstance(colls, dict):
                        continue
                    for coll, cd in colls.items():
                        if not isinstance(cd, dict):
                            continue
                        bloat = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
                        if bloat > 5:
                            result[f"{db_name}.{coll}"] = bloat

        elif module == "instance_recommender":
            from cloudwatch_analyzer import get_cpu_metrics
            from instance_recommender import analyze_workload_statistics, recommend_instance_type
            result = []
            snap_data = _agent_state["modules"]["cluster_snapshot"].get("result")
            if snap_data and isinstance(snap_data, dict):
                for inst in snap_data.get("instances", []):
                    dp = inst.get("cpu_datapoints", [])
                    wa = analyze_workload_statistics(dp)
                    rec = recommend_instance_type(inst["type"], wa, inst.get("buffer_cache_hit_ratio"))
                    result.append({"instance": inst["id"], "type": inst["type"], "recommendation": rec})

        elif module == "storage_recommender":
            from storage_cost_analyzer import analyze_storage_costs
            result, err = _run_with_timeout(lambda: analyze_storage_costs(cluster_id, region), timeout=60)
            if err:
                logger.warning("storage_recommender timed out or failed: %s", err)
                result = None

    except Exception as e:
        logger.error("Act phase failed for %s: %s", module, e, exc_info=True)
        with _lock:
            _agent_state["modules"][module]["error"] = str(e)
            _agent_state["modules"][module]["status"] = "error"
        return {"module": module, "result": None, "key_findings": [], "priority_signals": [], "issues_found": 0}

    return {
        "module": module,
        "result": result,
        "key_findings": key_findings,
        "priority_signals": priority_signals,
        "issues_found": len(priority_signals),
    }


# ═══ CORRELATE PHASE ══════════════════════════════════════════════════════════

def _correlate(all_findings):
    """Connect dots across modules. Returns list of insight strings."""
    insights = []

    db_findings = all_findings.get("db_analysis", {}).get("result", {})
    slow_findings = all_findings.get("slow_query", {}).get("result")
    snap_findings = all_findings.get("cluster_snapshot", {}).get("result")

    # Slow query + missing indexes
    if slow_findings and db_findings:
        patterns = slow_findings if isinstance(slow_findings, list) else slow_findings.get("patterns", [])
        for p in patterns[:10]:
            ns = p.get("ns", p.get("namespace", ""))
            if "." in ns:
                db, coll = ns.split(".", 1)
                coll_data = db_findings.get(db, {}).get(coll, {})
                if isinstance(coll_data, dict):
                    ia = coll_data.get("index_analysis", {})
                    if ia.get("total_indexes", 1) <= 1:
                        insights.append(f"🔴 {ns}: slow queries AND missing indexes — create index on query filter fields")

    # CPU low + large instance
    if snap_findings and isinstance(snap_findings, dict):
        for inst in snap_findings.get("instances", []):
            if inst.get("cpu_avg") is not None and inst["cpu_avg"] < 5:
                if "4xlarge" in inst["type"] or "8xlarge" in inst["type"]:
                    insights.append(f"🟡 {inst['id']} ({inst['type']}): avg CPU {inst['cpu_avg']:.1f}% — may be 4x oversized")

    # Bloat + no compression
    if db_findings:
        for db_name, colls in db_findings.items():
            if not isinstance(colls, dict):
                continue
            for coll, cd in colls.items():
                if not isinstance(cd, dict):
                    continue
                bloat = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
                comp = cd.get("compression", {}).get("enabled", False)
                if bloat > 30 and not comp:
                    insights.append(f"🟡 {db_name}.{coll}: {bloat:.0f}% bloat AND no compression — compact + enable compression")

    return insights[:10]


# ═══ MAIN AGENT LOOP (Thread 1) ══════════════════════════════════════════════

def _agent_loop(conn_data):
    """Main agent entry point — Observe→Reason→Decide→Act loop."""
    try:
        _set_status("observing", "Collecting cluster facts…")
        observation = _observe(conn_data)

        # Build analysis scope — prioritize databases with current activity
        scope = _build_analysis_scope(
            observation["databases"], observation["db_sizes"],
            active_databases=observation.get("active_databases", []))
        with _lock:
            _agent_state["analysis_scope"] = scope
            _agent_state["_observation"] = observation  # Store for _decide() to check has_slow_query_logs

        # ── Reason→Decide→Act loop ──────────────────────────────────────────

        completed = []
        all_findings = {}
        total_estimate = len([m for m in _FALLBACK_ORDER])

        while not _stop_event.is_set():
            # REASON
            _set_status("reasoning", "Evaluating findings…")
            signals = []
            for f in all_findings.values():
                signals.extend(f.get("priority_signals", []))
            decision = _reason(observation, completed, signals)

            # Handle skipped modules
            for skip_mod in decision.get("skip_modules", []):
                skip_reason = decision.get("skip_reasons", {}).get(skip_mod, "Bedrock decided to skip")
                with _lock:
                    _agent_state["skipped_modules"].append({"module": skip_mod, "reason": skip_reason})
                    _agent_state["modules"].get(skip_mod, {}).update(status="skipped")

            # DECIDE
            module = _decide(decision, completed)
            if module == "done":
                break

            # Log reasoning
            with _lock:
                _agent_state["reasoning_log"].append({
                    "step": len(completed) + 1,
                    "module": module,
                    "reasoning": decision.get("reasoning", ""),
                })

            # ACT
            _set_status("running", module, decision.get("reasoning", ""))
            with _lock:
                _agent_state["modules"][module]["status"] = "running"
                _agent_state["pct"] = _calc_pct(len(completed), total_estimate)

            findings = _act(module, decision.get("target_database"),
                           decision.get("target_collection"), conn_data)

            # Store result
            with _lock:
                if findings["result"] is not None:
                    _agent_state["modules"][module]["status"] = "done"
                    _agent_state["modules"][module]["result"] = findings["result"]
                elif _agent_state["modules"][module]["status"] != "error":
                    _agent_state["modules"][module]["status"] = "done"
                _agent_state["modules"][module]["ts"] = time.time()

            all_findings[module] = findings
            completed.append(module)

            # CORRELATE
            insights = _correlate(all_findings)
            with _lock:
                _agent_state["correlated_insights"] = insights
                _agent_state["pct"] = _calc_pct(len(completed), total_estimate)

            # Update observation for next cycle
            observation["priority_signals"] = findings.get("priority_signals", [])
            observation["completed_modules"] = completed

        # Generate report and persist state
        _set_status("complete", "Generating report…")
        from agent_report import generate_report
        report_md = generate_report(_agent_state, conn_data)
        with _lock:
            _agent_state["report"] = {"ready": True, "markdown": report_md, "generated_at": time.time()}
            _agent_state["status"] = "complete"
            _agent_state["pct"] = 100

        # Save to persistent memory
        cluster_id = conn_data.get("cluster_id", "")
        if cluster_id:
            try:
                import agent_memory as mem
                mem.save_snapshot(cluster_id, observation)
                db_result = _agent_state["modules"]["db_analysis"].get("result")
                if db_result:
                    mem.save_databases(cluster_id, db_result)
                    mem.save_index_health(cluster_id, db_result)
                wa_result = _agent_state["modules"]["well_architected"].get("result")
                if wa_result:
                    # Save merged v1+v2 results for drift detection
                    try:
                        from tabs.wa_v2 import _v2_state, _v2_lock
                        with _v2_lock:
                            v2_extra = list(_v2_state["results"])
                        mem.save_wa_results(cluster_id, wa_result + v2_extra)
                    except Exception:
                        mem.save_wa_results(cluster_id, wa_result)
                sq_result = _agent_state["modules"]["slow_query"].get("result")
                if sq_result:
                    patterns = sq_result if isinstance(sq_result, list) else []
                    mem.save_slow_queries(cluster_id, patterns)
                mem.save_last_analysis(cluster_id, _agent_state)
            except Exception as e:
                logger.debug("Memory save failed: %s", e)

    except Exception as e:
        logger.error("Agent loop failed: %s", e, exc_info=True)
        with _lock:
            _agent_state["status"] = "error"
            _agent_state["current_detail"] = str(e)


# ═══ ACTIVITY MONITOR (Thread 2) ═════════════════════════════════════════════

def _monitor_activity(conn_str, interval=60):
    """Poll currentOp every ~45s. Accumulate samples, flush 15-min summary to disk."""
    if not conn_str:
        return

    # Resolve cluster_id lazily
    cluster_id = ""
    activity_samples = []
    last_flush = time.time()
    FLUSH_INTERVAL = 900  # 15 minutes

    while not _stop_event.is_set():
        try:
            # Lazy resolve cluster_id from snapshot result
            if not cluster_id:
                with _lock:
                    snap = _agent_state["modules"]["cluster_snapshot"].get("result")
                    if isinstance(snap, dict) and "cluster" in snap:
                        cluster_id = snap["cluster"].get("id", "")

            from tabs.current_activity import _fetch_current_ops
            ops, error = _fetch_current_ops(conn_str)
            if not error and ops:
                slow = [op for op in ops
                        if op.get("active") and op.get("microsecs_running", 0) > 500_000]

                # Collect sample for 15-min summary
                users_set = set()
                apps_set = set()
                namespaces = []
                active_count = 0
                longest_ms = 0
                for op in ops:
                    eu = op.get("effectiveUsers", [])
                    if eu:
                        users_set.add(eu[0].get("user", ""))
                    meta = op.get("clientMetaData", op.get("clientMetadata", {})) or {}
                    app_name = (meta.get("application", {}) or {}).get("name", "")
                    if app_name:
                        apps_set.add(app_name)
                    if op.get("active"):
                        active_count += 1
                        ns = op.get("ns", "")
                        if ns and "." in ns:
                            namespaces.append(ns)
                        us = op.get("microsecs_running", 0)
                        longest_ms = max(longest_ms, us / 1000)

                activity_samples.append({
                    "active": active_count,
                    "idle": len(ops) - active_count,
                    "slow": len(slow),
                    "users": list(users_set),
                    "apps": list(apps_set),
                    "namespaces": namespaces[:5],
                    "longest_ms": longest_ms,
                })

                # Build alerts for UI
                alerts = []
                for op in slow[:5]:
                    us = op.get("microsecs_running", 0)
                    ns = op.get("ns", "\u2014")
                    opid = op.get("opid", "\u2014")
                    op_users = op.get("effectiveUsers", [])
                    user = op_users[0].get("user", "\u2014") if op_users else "\u2014"

                    if op.get("waitingForLock"):
                        status = "blocked"
                    elif us > 30_000_000:
                        status = f"long-running ({us / 1_000_000:.0f}s)"
                    else:
                        status = f"running ({us / 1_000:.0f}ms)"

                    hint = _correlate_with_indexes(ns)
                    alerts.append({
                        "ns": ns, "opid": opid, "user": user,
                        "us": us, "status": status, "hint": hint,
                        "ts": time.time(),
                    })

                with _lock:
                    existing = [a for a in _agent_state["live_alerts"] if time.time() - a["ts"] < 30]
                    _agent_state["live_alerts"] = (existing + alerts)[-20:]

            # Flush 15-min summary to disk
            if time.time() - last_flush >= FLUSH_INTERVAL and activity_samples:
                try:
                    import agent_memory as mem
                    summary = mem.build_activity_summary(activity_samples)
                    if summary and cluster_id:
                        mem.save_activity_summary(cluster_id, summary)
                except Exception as e:
                    logger.debug("Activity flush failed: %s", e)
                activity_samples.clear()
                last_flush = time.time()

        except Exception as e:
            logger.debug("Activity monitor error: %s", e)
            # Auto-heal: if connection failed and we're in tunnel mode, try reconnect
            if "connection" in str(e).lower() or "timeout" in str(e).lower():
                try:
                    from ssh_tunnel import ensure_tunnel
                    ensure_tunnel()
                except Exception:
                    pass

        _stop_event.wait(interval)

def _correlate_with_indexes(ns):
    """If agent found missing indexes for this collection, return hint."""
    if "." not in ns:
        return ""
    parts = ns.split(".", 1)
    if len(parts) != 2:
        return ""
    db, coll = parts

    with _lock:
        db_result = _agent_state["modules"]["db_analysis"].get("result")
    if not db_result or not isinstance(db_result, dict):
        return ""

    coll_data = db_result.get(db, {}).get(coll, {})
    if not isinstance(coll_data, dict):
        return ""

    ia = coll_data.get("index_analysis", {})
    if ia.get("total_indexes", 1) <= 1:
        return f"No indexes on {coll} — consider adding index on query filter fields"

    # Check for low cardinality
    low_card = ia.get("low_cardinality_indexes", [])
    if low_card:
        return f"Low cardinality index detected on {coll} — may not be selective"

    return ""
