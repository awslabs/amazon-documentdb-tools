"""AI Chat Advisor — two-step Bedrock agent: classify → fetch → answer."""
import boto3
import json
import logging
import threading

logger = logging.getLogger(__name__)

_chat = {"messages": [], "pending": False, "response": None, "error": None}
_chat_lock = threading.Lock()

# Session cache — reuse fetched data within same chat session
_data_cache = {}

# Load SKILL.md as the base system prompt + DocumentDB domain knowledge
try:
    from bedrock_advisor import _load_skill, _load_references
    _SKILL_TEXT = _load_skill()
except Exception:
    _SKILL_TEXT = ""

_ADVISOR_ADDENDUM = """

## Chat Advisor Role
You are Amazon DocumentDB Prism AI Advisor with access to real cluster data provided below.
Rules:
1. Answer using the ANALYSIS DATA provided. Do not guess or make up data.
2. If the question is ambiguous or could apply to multiple databases, ASK which database.
3. Reference actual collection names, index names, namespaces, and numbers.
4. Use valid MongoDB/DocumentDB syntax for commands.
5. If slow query patterns exist, proactively mention worst offenders when relevant.
6. If data is insufficient, say what is missing and suggest how to get it.
7. Be concise, specific, and actionable.
8. Format with markdown: **bold**, `code`, bullet lists."""

SYSTEM_PROMPT = _SKILL_TEXT + _ADVISOR_ADDENDUM if _SKILL_TEXT else _ADVISOR_ADDENDUM.strip()

# Data sources the advisor can request
_DATA_SOURCES = {
    "collection_stats": "Per-collection stats: doc count, size, storageSize, avgObjSize, compression, bloat bytes and %",
    "index_health": "Per-index: name, field definitions, ops_count, unused status, cardinality %, low_cardinality flag, bloat %, redundancy (covered-by list)",
    "slow_queries": "Slow query patterns from CloudWatch profiler logs: namespace, operation, avg/max ms, count, example query",
    "live_activity": "Live currentOp: active/idle counts, connected users, app names, slow operations with PID/namespace/duration/blocked status",
    "cluster_config": "Instances (type, AZ, role, CPU/memory/connections/cache metrics), engine version, encryption, deletion protection, backup, storage type",
    "well_architected": "Well-Architected checks per pillar: pass/warn/fail status with recommendations",
    "agent_insights": "Cross-module correlations from the autonomous agent (e.g. slow query + missing index)",
}

CLASSIFIER_PROMPT = """You are a data routing classifier for a DocumentDB advisor tool.
Given the user's question, decide what data is needed to answer it.

Available data sources:
{sources}

Available databases: {databases}

Currently selected database: {selected_db}

Return ONLY valid JSON:
{{
  "scope": "cluster" | "database" | "ask_user",
  "target_database": "<db_name or null>",
  "sources_needed": ["<source1>", "<source2>"],
  "clarification": "<question to ask user, only if scope=ask_user>"
}}

Rules:
1. If the question clearly targets a specific database or collection, set scope=database and target_database.
2. If the question is about the cluster (instances, config, connections, overall health), set scope=cluster.
3. If ambiguous and multiple databases exist, set scope=ask_user with a clarification question.
4. For broad questions (optimize, health check, what's wrong, CPU high), request multiple sources — better to have too much data than too little.
5. For performance/query questions, ALWAYS include slow_queries and index_health.
6. For cost/sizing questions, ALWAYS include cluster_config and collection_stats.
7. agent_insights is always cheap — include it for any recommendation or health question.
8. When in doubt, include more sources rather than fewer."""


def _classify_question(question, databases, selected_db, region):
    """Step 1: Ask Bedrock what data is needed. Returns classification dict."""
    sources_desc = "\n".join(f"  - {k}: {v}" for k, v in _DATA_SOURCES.items())
    prompt = CLASSIFIER_PROMPT.format(
        sources=sources_desc,
        databases=", ".join(databases[:20]) if databases else "none discovered",
        selected_db=selected_db or "none",
    )

    for attempt in range(2):
        try:
            client = boto3.client("bedrock-runtime", region_name=region)
            resp = client.invoke_model(
                modelId="us.anthropic.claude-haiku-4-5-20251001-v1:0",
                body=json.dumps({
                    "messages": [{"role": "user", "content": question}],
                    "max_tokens": 300,
                    "temperature": 0.0,
                    "system": prompt,
                    "anthropic_version": "bedrock-2023-05-31",
                }),
            )
            text = json.loads(resp["body"].read())["content"][0]["text"]
            if "{" in text:
                return json.loads(text[text.index("{"):text.rindex("}") + 1])
        except Exception as e:
            logger.warning("Classifier attempt %d failed: %s", attempt + 1, e)

    # Fallback: best guess based on selected_db
    sources = ["collection_stats", "agent_insights"]
    if selected_db:
        return {"scope": "database", "target_database": selected_db, "sources_needed": sources}
    return {"scope": "cluster", "target_database": None,
            "sources_needed": ["cluster_config", "agent_insights"]}


def _fetch_data(classification, analysis_data, database_name, conn_meta, conn_str):
    """Step 2: Fetch only the data sources Bedrock requested."""
    needed = classification.get("sources_needed", [])
    target_db = classification.get("target_database") or database_name
    lines = []

    if target_db:
        lines.append(f"Target Database: {target_db}")

    for source in needed:
        cache_key = f"{source}:{target_db or 'cluster'}"

        # Check session cache first
        if cache_key in _data_cache:
            lines.append(_data_cache[cache_key])
            continue

        data = _fetch_source(source, target_db, analysis_data, conn_meta, conn_str)
        if data:
            _data_cache[cache_key] = data
            lines.append(data)

    if not lines:
        # Fallback: always provide at least cluster config and agent insights
        for fallback_src in ["cluster_config", "agent_insights", "collection_stats"]:
            data = _fetch_source(fallback_src, target_db, analysis_data, conn_meta, conn_str)
            if data:
                lines.append(data)

    return "\n".join(lines) if lines else "Analysis is still in progress. The agent is collecting data from the cluster."


def _fetch_source(source, target_db, analysis_data, conn_meta, conn_str):
    """Fetch a single data source. Returns formatted string."""

    if source == "collection_stats":
        if not target_db:
            # Cluster-level: summarize all databases from agent results
            try:
                from agent_orchestrator import get_db_analysis_results
                all_results = get_db_analysis_results()
                if all_results:
                    lines = ["\nDatabase Size Summary (all databases):"]
                    db_sizes = []
                    for db_name, colls in all_results.items():
                        if not isinstance(colls, dict):
                            continue
                        total_size = sum(cd.get("storageSize", cd.get("size", 0))
                                        for cd in colls.values() if isinstance(cd, dict))
                        total_docs = sum(cd.get("count", 0)
                                        for cd in colls.values() if isinstance(cd, dict))
                        n_colls = sum(1 for cd in colls.values() if isinstance(cd, dict) and "error" not in cd)
                        db_sizes.append((db_name, total_size, total_docs, n_colls))
                    db_sizes.sort(key=lambda x: -x[1])
                    for db_name, size, docs, nc in db_sizes:
                        size_mb = size / (1024 ** 2)
                        lines.append(f"  {db_name}: {size_mb:.1f}MB, {docs:,} docs, {nc} collections")
                    return "\n".join(lines)
            except Exception:
                pass
            # Fallback: use analysis_data if available
            if analysis_data:
                lines = ["\nDatabase Size Summary:"]
                for db_name, colls in analysis_data.items():
                    if not isinstance(colls, dict):
                        continue
                    total_size = sum(cd.get("storageSize", cd.get("size", 0))
                                    for cd in colls.values() if isinstance(cd, dict))
                    lines.append(f"  {db_name}: {total_size / (1024**2):.1f}MB")
                return "\n".join(lines) if len(lines) > 1 else None
            return None
        # Database-level: specific database
        db = (analysis_data or {}).get(target_db, {})
        if not db and target_db and conn_str:
            try:
                from agent_orchestrator import ensure_db_analyzed
                result = ensure_db_analyzed(conn_str, target_db)
                db = result.get(target_db, {})
            except Exception:
                pass
        if not db:
            return None
        lines = [f"\nCollection Stats for {target_db}:"]
        for coll, cd in list(db.items())[:20]:
            if not isinstance(cd, dict) or "error" in cd:
                continue
            size_mb = cd.get("storageSize", cd.get("size", 0)) / (1024 ** 2)
            bloat = cd.get("unusedStorageSize", {}).get("unusedPercent", 0)
            comp = "yes" if cd.get("compression", {}).get("enabled") else "no"
            lines.append(f"  {coll}: {cd.get('count', 0):,} docs, {size_mb:.1f}MB, bloat={bloat:.0f}%, compression={comp}")
        return "\n".join(lines)

    elif source == "index_health":
        # Cluster-level: summarize issues across all databases
        if not target_db:
            try:
                from agent_orchestrator import get_db_analysis_results
                all_results = get_db_analysis_results()
                if not all_results:
                    all_results = analysis_data or {}
                if all_results:
                    lines = ["\nIndex Health Summary (all databases):"]
                    for db_name, colls in all_results.items():
                        if not isinstance(colls, dict):
                            continue
                        n_unused = 0
                        n_low = 0
                        n_total = 0
                        for cd in colls.values():
                            if not isinstance(cd, dict):
                                continue
                            ia = cd.get("index_analysis", {})
                            n_total += ia.get("total_indexes", 0)
                            n_unused += len(ia.get("unused_indexes", []))
                            n_low += len(ia.get("low_cardinality_indexes", []))
                        if n_total > 0:
                            issues = []
                            if n_unused:
                                issues.append(f"{n_unused} unused")
                            if n_low:
                                issues.append(f"{n_low} low-card")
                            issue_str = f" [{', '.join(issues)}]" if issues else ""
                            lines.append(f"  {db_name}: {n_total} indexes{issue_str}")
                    return "\n".join(lines) if len(lines) > 1 else None
            except Exception:
                pass
            return None
        # Database-level
        db = (analysis_data or {}).get(target_db, {})
        if not db and target_db and conn_str:
            try:
                from agent_orchestrator import ensure_db_analyzed
                result = ensure_db_analyzed(conn_str, target_db)
                db = result.get(target_db, {})
            except Exception:
                pass
        if not db:
            return None
        lines = [f"\nIndex Health for {target_db}:"]
        for coll, cd in list(db.items())[:15]:
            if not isinstance(cd, dict):
                continue
            for idx in cd.get("indexes", []):
                fields = ", ".join(idx.get("fields", {}).keys())
                u = idx.get("usage", {})
                c = idx.get("cardinality", {})
                b = idx.get("bloat", {})
                flags = []
                if u.get("potential_unused"):
                    flags.append("UNUSED")
                if c.get("is_low"):
                    flags.append(f"LOW_CARD({c.get('percentage', 0):.1f}%)")
                if b.get("unusedPercent", 0) > 20:
                    flags.append(f"BLOAT({b.get('unusedPercent', 0):.0f}%)")
                flag_str = f" [{', '.join(flags)}]" if flags else ""
                lines.append(f"  {coll}.{idx['name']}: {{{fields}}} ops={u.get('ops_count', 0)}{flag_str}")
        return "\n".join(lines)

    elif source == "slow_queries":
        try:
            from agent_orchestrator import ensure_slow_queries_analyzed
            cluster_id = (conn_meta or {}).get("cluster_id", "")
            region = (conn_meta or {}).get("region", "us-east-1")
            log_group = (conn_meta or {}).get("log_group", "")
            patterns = ensure_slow_queries_analyzed(cluster_id, region, log_group)
            if not patterns:
                return "\nSlow Queries: No slow query patterns found in profiler logs."
            lines = [f"\nSlow Query Patterns ({len(patterns)} found):"]
            for p in patterns[:15]:
                ns = p.get("ns", p.get("namespace", ""))
                avg = p.get("avg_time", 0)
                mx = p.get("max_time", 0)
                cnt = p.get("count", 0)
                op = p.get("operation", "")
                query = p.get("example_query", {})
                query_str = json.dumps(query, default=str)[:5120] if query else ""
                lines.append(f"  {ns}: {op} avg={avg:.0f}ms max={mx:.0f}ms count={cnt}")
                if query_str:
                    lines.append(f"    query: {query_str}")
            return "\n".join(lines)
        except Exception as e:
            logger.warning("Failed to fetch slow queries: %s", e)
            return None

    elif source == "live_activity":
        if not conn_str:
            return None
        try:
            from tabs.current_activity import _fetch_current_ops
            ops, err = _fetch_current_ops(conn_str)
            if err or not ops:
                return "\nLive Activity: No operations detected."
            users = set()
            apps = set()
            active = 0
            slow_ops = []
            for op in ops:
                eu = op.get("effectiveUsers", [])
                if eu:
                    users.add(eu[0].get("user", ""))
                meta = op.get("clientMetaData", op.get("clientMetadata", {})) or {}
                app_name = (meta.get("application", {}) or {}).get("name", "")
                if app_name:
                    apps.add(app_name)
                if op.get("active"):
                    active += 1
                    us = op.get("microsecs_running", 0)
                    if us > 500_000:
                        slow_ops.append(f"  PID:{op.get('opid','-')} {op.get('ns','-')} {us/1_000_000:.1f}s user:{eu[0].get('user','-') if eu else '-'}")
            lines = [f"\nLive Activity: {len(ops)} connections, {active} active, {len(ops)-active} idle"]
            lines.append(f"Users: {', '.join(sorted(users)) if users else 'none'}")
            if apps:
                lines.append(f"Applications: {', '.join(sorted(apps))}")
            if slow_ops:
                lines.append("Slow operations (>500ms):")
                lines.extend(slow_ops[:5])
            return "\n".join(lines)
        except Exception:
            return None

    elif source == "cluster_config":
        try:
            from agent_orchestrator import _agent_state, _lock
            with _lock:
                snap = _agent_state["modules"]["cluster_snapshot"].get("result")
            if not snap or not isinstance(snap, dict):
                return None
            cl = snap.get("cluster", {})
            insts = snap.get("instances", [])
            lines = ["\nCluster Configuration:"]
            lines.append(f"  Engine: {cl.get('engine_version', '-')}, Storage: {snap.get('storage_gb', 0):.1f}GB")
            lines.append(f"  Encryption: {'yes' if cl.get('storage_encrypted') else 'no'}, Deletion Protection: {'yes' if cl.get('deletion_protection') else 'no'}")
            lines.append(f"  Backup: {cl.get('backup_retention', 0)} days, Compression: {cl.get('compression', '-')}")
            for i in insts:
                cpu = f"CPU={i['cpu_avg']:.1f}%" if i.get("cpu_avg") is not None else "CPU=-"
                conn = f"Conns={i['conn_avg']:.0f}" if i.get("conn_avg") is not None else "Conns=-"
                cache = f"Cache={i['buffer_cache_hit_ratio']:.1f}%" if i.get("buffer_cache_hit_ratio") is not None else ""
                lines.append(f"  {i['id']}: {i['type']} {i['role']} {i['az']} {cpu} {conn} {cache}")
            return "\n".join(lines)
        except Exception:
            return None

    elif source == "well_architected":
        try:
            from tabs.well_architected import _wa, _wa_lock
            with _wa_lock:
                results = list(_wa["results"]) if _wa["done"] else []
            if not results:
                return None
            fails = [r for r in results if r["status"] in ("fail", "warn")]
            if not fails:
                return "\nWell-Architected: All checks passed."
            lines = [f"\nWell-Architected Issues ({len(fails)}):"]
            for r in fails[:10]:
                lines.append(f"  [{r['status'].upper()}] {r['label']}: {r.get('detail', '')}")
            return "\n".join(lines)
        except Exception:
            return None

    elif source == "agent_insights":
        try:
            from agent_orchestrator import _agent_state, _lock
            with _lock:
                insights = list(_agent_state.get("correlated_insights", []))
            if not insights:
                return None
            lines = ["\nAgent Insights:"]
            for ins in insights[:5]:
                lines.append(f"  - {ins}")
            return "\n".join(lines)
        except Exception:
            return None

    return None


def get_chat_messages():
    with _chat_lock:
        return list(_chat["messages"])


def get_pending_state():
    with _chat_lock:
        return _chat["pending"], _chat["response"], _chat["error"]


def clear_chat():
    with _chat_lock:
        _chat.update(messages=[], pending=False, response=None, error=None)
    _data_cache.clear()


def send_message(user_msg, analysis_data, database_name, conn_meta=None, region='us-east-1'):
    """Two-step: classify what data is needed, fetch it, then answer."""
    with _chat_lock:
        _chat["messages"].append({"role": "user", "content": user_msg})
        _chat["pending"] = True
        _chat["response"] = None
        _chat["error"] = None

    def _call():
        try:
            conn_str = conn_meta.get("connection_string", "") if conn_meta else ""
            databases = conn_meta.get("databases", []) if conn_meta else []

            # Step 1: Classify
            classification = _classify_question(user_msg, databases, database_name, region)
            logger.info("Chat classifier: %s", json.dumps(classification, default=str))

            # If Bedrock says ask the user
            if classification.get("scope") == "ask_user":
                reply = classification.get("clarification", "Could you clarify which database you're asking about?")
                with _chat_lock:
                    _chat["messages"].append({"role": "assistant", "content": reply})
                    _chat["response"] = reply
                    _chat["pending"] = False
                return

            # Step 2: Fetch only needed data
            context = _fetch_data(classification, analysis_data, database_name, conn_meta, conn_str)
            logger.info("Chat context sources: %s, context_length: %d chars",
                        classification.get("sources_needed", []), len(context))
            logger.info("Chat context preview: %s", context)

            # Step 3: Answer with context + topic-matched references
            refs = ""
            try:
                refs = _load_references(user_msg)
                if refs:
                    logger.info("Chat injecting references for question")
            except Exception:
                pass
            system_text = f"{SYSTEM_PROMPT}{refs}\n\n--- ANALYSIS DATA ---\n{context}"

            with _chat_lock:
                history = list(_chat["messages"])

            logger.info("Chat sending to Bedrock: system=%d chars, messages=%d",
                        len(system_text), len(history))

            api_msgs = [{"role": m["role"], "content": m["content"]} for m in history]

            from bedrock_advisor import MODEL_ID, FALLBACK_MODEL_ID
            client = boto3.client("bedrock-runtime", region_name=region)

            for model_id in [MODEL_ID, FALLBACK_MODEL_ID]:
                try:
                    resp = client.invoke_model(
                        modelId=model_id,
                        body=json.dumps({
                            "anthropic_version": "bedrock-2023-05-31",
                            "system": system_text,
                            "messages": api_msgs,
                            "max_tokens": 2048,
                            "temperature": 0.2,
                        }),
                    )
                    result = json.loads(resp["body"].read())
                    reply = result["content"][0]["text"]

                    with _chat_lock:
                        _chat["messages"].append({"role": "assistant", "content": reply})
                        _chat["response"] = reply
                        _chat["pending"] = False
                    return
                except Exception as e:
                    logger.warning("Chat model %s failed: %s", model_id, e)
                    continue

            with _chat_lock:
                _chat["error"] = "Unable to reach Bedrock. Please try again."
                _chat["pending"] = False

        except Exception as e:
            logger.error("Chat advisor error: %s", e, exc_info=True)
            with _chat_lock:
                _chat["error"] = str(e)
                _chat["pending"] = False

    threading.Thread(target=_call, daemon=True).start()
