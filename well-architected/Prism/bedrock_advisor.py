"""Bedrock AI Advisor — agentic tool-use with MCP operations."""
import json
import logging
import boto3

logger = logging.getLogger(__name__)

MODEL_ID = "us.anthropic.claude-sonnet-4-20250514-v1:0"
FALLBACK_MODEL_ID = "us.anthropic.claude-haiku-4-5-20251001-v1:0"

# Load system prompt from SKILL.md
import os as _os

_SKILL_DIR = _os.path.join(_os.path.dirname(__file__), "documentdb-well-architected-review")
_REF_DIR = _os.path.join(_SKILL_DIR, "references")


def _load_skill():
    """Load advisor-prompt.md as system prompt for the chat advisor."""
    skill_path = _os.path.join(_SKILL_DIR, "advisor-prompt.md")
    if _os.path.exists(skill_path):
        with open(skill_path) as f:
            # Strip YAML frontmatter
            text = f.read()
            if text.startswith("---"):
                end = text.find("---", 3)
                if end > 0:
                    text = text[end + 3:].strip()
            return text
    return "You are an Amazon DocumentDB expert advisor."


# Each entry: list of word stems/variants → reference files to inject.
# A question matches a topic if ANY of its stems appear as a substring in the lowercased question.
_TOPIC_MAP = [
    (
        ["perform", "slow", "latenc", "speed", "fast", "bottleneck", "throughput", "respon"],
        ["performance-improvement-tips.md", "query-plan-and-troubleshooting.md"],
    ),
    (
        ["optim", "improv", "opportunit", "recommend", "suggest", "better", "enhanc", "tuning", "tune"],
        ["best-practices.md", "performance-improvement-tips.md", "anti-patterns.md"],
    ),
    (
        ["index", "cardinality", "unused", "redundant", "bloat", "compound",
         "createindex", "dropindex", "reindex", "sparse", "unique", "ttl",
         "multikey", "text index", "2dsphere", "geospatial", "background"],
        ["index-management.md", "best-practices.md", "anti-patterns.md"],
    ),
    (
        ["query", "explain", "plan", "collscan", "scan", "filter", "aggregat"],
        ["query-plan-and-troubleshooting.md", "supported-operators.md"],
    ),
    (
        ["cost", "pric", "bill", "saving", "cheap", "expens", "spend"],
        ["pricing-and-cost-optimization.md"],
    ),
    (
        ["instance", "sizing", "size", "memory", "cpu", "scale", "right-siz", "rightsiz", "graviton", "r6g", "r8g"],
        ["pricing-and-cost-optimization.md", "best-practices.md"],
    ),
    (
        ["serverless", "dcu", "variable workload", "spiky"],
        ["serverless.md", "pricing-and-cost-optimization.md"],
    ),
    (
        ["backup", "restore", "snapshot", "recover", "retention", "pitr", "point-in-time"],
        ["backup-and-restore.md"],
    ),
    (
        ["compress", "lz4", "zstd", "storage", "disk", "space"],
        ["best-practices.md", "performance-improvement-tips.md"],
    ),
    (
        ["mongodb", "migrat", "compat", "differ", "operator", "support", "retrywrit"],
        ["functional-differences.md", "supported-operators.md"],
    ),
    (
        ["anti-pattern", "long running", "long-running", "array", "multi-key"],
        ["anti-patterns.md"],
    ),
    (
        ["schema", "data model", "embed", "reference", "document design"],
        ["performance-improvement-tips.md"],
    ),
    (
        ["best practice", "health", "well-architect", "checklist", "review"],
        ["best-practices.md"],
    ),
    (
        ["connect", "user", "session", "who", "client", "driver", "currentop",
         "current op", "active", "idle", "running", "blocked", "lock", "operation",
         "pid", "opid", "thread", "monitor", "live", "activity"],
        ["live-operations-monitoring.md"],
    ),
]

# Fallback: broad advisory questions that don't hit a specific topic
_ADVISORY_STEMS = [
    "analys", "assess", "audit", "diagnos", "evaluat", "examin", "inspect",
    "issue", "problem", "concern", "help", "advice", "what", "how", "why", "should",
]
_FALLBACK_FILES = ["best-practices.md", "anti-patterns.md"]


def _load_references(question):
    """Load relevant reference files based on question intent."""
    if not _os.path.isdir(_REF_DIR):
        return ""

    q_lower = question.lower()
    files_to_load = set()

    # Topic-specific matching
    for stems, files in _TOPIC_MAP:
        if any(stem in q_lower for stem in stems):
            files_to_load.update(files)

    # Fallback: general advisory question with no specific topic matched
    if not files_to_load and any(stem in q_lower for stem in _ADVISORY_STEMS):
        files_to_load.update(_FALLBACK_FILES)

    if not files_to_load:
        return ""

    # Load files (cap at 3 to avoid token overflow)
    refs = []
    for fname in sorted(files_to_load)[:3]:
        fpath = _os.path.join(_REF_DIR, fname)
        if _os.path.exists(fpath):
            with open(fpath) as f:
                refs.append(f"\n--- Reference: {fname} ---\n{f.read()}")

    if refs:
        return "\n\nREFERENCE DOCUMENTATION:\n" + "\n".join(refs)
    return ""


SYSTEM_PROMPT = _load_skill()
logger.info("SKILL loaded: %d chars, %d lines", len(SYSTEM_PROMPT), SYSTEM_PROMPT.count("\n"))

# Tool definitions for Bedrock tool_use
TOOLS = [
    {
        "name": "list_databases",
        "description": "List all databases in the DocumentDB cluster",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "list_collections",
        "description": "List all collections in a specific database",
        "input_schema": {
            "type": "object",
            "properties": {"db_name": {"type": "string", "description": "Database name"}},
            "required": ["db_name"],
        },
    },
    {
        "name": "get_database_stats",
        "description": "Get database statistics. NOTE: This may fail with serialization errors. Prefer using context data when available.",
        "input_schema": {
            "type": "object",
            "properties": {"db_name": {"type": "string", "description": "Database name"}},
            "required": ["db_name"],
        },
    },
    {
        "name": "get_collection_stats",
        "description": "Get collection statistics including document count, size, index info",
        "input_schema": {
            "type": "object",
            "properties": {
                "db_name": {"type": "string", "description": "Database name"},
                "collection": {"type": "string", "description": "Collection name"},
            },
            "required": ["db_name", "collection"],
        },
    },
    {
        "name": "analyze_schema",
        "description": "Analyze collection schema by sampling documents to find field types and coverage",
        "input_schema": {
            "type": "object",
            "properties": {
                "db_name": {"type": "string", "description": "Database name"},
                "collection": {"type": "string", "description": "Collection name"},
            },
            "required": ["db_name", "collection"],
        },
    },
    {
        "name": "find_documents",
        "description": "Query documents from a collection with optional filter",
        "input_schema": {
            "type": "object",
            "properties": {
                "db_name": {"type": "string", "description": "Database name"},
                "collection": {"type": "string", "description": "Collection name"},
                "query": {"type": "object", "description": "Query filter (MongoDB syntax)"},
                "limit": {"type": "integer", "description": "Max documents to return", "default": 5},
            },
            "required": ["db_name", "collection"],
        },
    },
    {
        "name": "count_documents",
        "description": "Count documents in a collection with optional filter",
        "input_schema": {
            "type": "object",
            "properties": {
                "db_name": {"type": "string", "description": "Database name"},
                "collection": {"type": "string", "description": "Collection name"},
                "query": {"type": "object", "description": "Optional query filter"},
            },
            "required": ["db_name", "collection"],
        },
    },
    {
        "name": "explain_query",
        "description": "Explain the execution plan for a find query",
        "input_schema": {
            "type": "object",
            "properties": {
                "db_name": {"type": "string", "description": "Database name"},
                "collection": {"type": "string", "description": "Collection name"},
                "query": {"type": "object", "description": "Query to explain"},
            },
            "required": ["db_name", "collection"],
        },
    },
]


# ── Query safety checks (prevent NoSQL injection from LLM-generated queries) ──

_DENIED_OPERATORS = {"$where", "$function", "$accumulator"}
_MAX_REGEX_LEN = 200
_MAX_QUERY_DEPTH = 10


def _check_query_safety(query, depth=0):
    """Check a query dict for dangerous operators. Returns rejection reason or None."""
    if depth > _MAX_QUERY_DEPTH:
        return f"Query nesting too deep (>{_MAX_QUERY_DEPTH} levels)"
    if not isinstance(query, dict):
        return None
    for key, value in query.items():
        if key in _DENIED_OPERATORS:
            return f"Operator {key} is not allowed"
        if key == "$regex" and isinstance(value, str) and len(value) > _MAX_REGEX_LEN:
            return f"$regex pattern too long ({len(value)} chars, max {_MAX_REGEX_LEN})"
        # Recurse into nested dicts and lists
        if isinstance(value, dict):
            result = _check_query_safety(value, depth + 1)
            if result:
                return result
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    result = _check_query_safety(item, depth + 1)
                    if result:
                        return result
    return None


def _execute_tool(tool_name, tool_input, conn_str):
    """Execute a tool call. MCP is disabled — always returns error to trigger fallback."""
    return {"error": "Direct database access via MCP is disabled. Use context data to answer."}


def ask_advisor(question, conn_str="", db_context=None, analysis_summary="", region="us-east-1"):
    """Agentic advisor — Bedrock decides which tools to call, executes them, synthesizes answer.
    
    Supports multi-turn tool use: Bedrock can call multiple tools in sequence.
    """
    # Build initial prompt with context
    prompt = question
    ctx_parts = []
    if db_context:
        if db_context.get("cluster_id"):
            ctx_parts.append(f"Cluster: {db_context['cluster_id']}")
        if db_context.get("database"):
            ctx_parts.append(f"Current database: {db_context['database']}")
        if db_context.get("collection"):
            ctx_parts.append(f"Current collection: {db_context['collection']}")
    if analysis_summary:
        ctx_parts.append(f"\nCached analysis data:\n{analysis_summary}")
    if ctx_parts:
        prompt = "\n".join(ctx_parts) + f"\n\nQuestion: {question}"

    messages = [{"role": "user", "content": prompt}]

    # Load relevant reference docs based on the question
    refs = _load_references(question)
    if refs:
        ref_names = [l.split("--- Reference: ")[1].split(" ---")[0] for l in refs.splitlines() if "--- Reference:" in l]
        logger.info("SKILL references injected for question %r: %s", question[:60], ref_names)
    else:
        logger.info("SKILL no references matched for question %r", question[:60])
    enriched_prompt = SYSTEM_PROMPT + refs if refs else SYSTEM_PROMPT

    # Agentic loop — keep calling tools until Bedrock gives a final text answer
    max_iterations = 3
    for iteration in range(max_iterations):
        response = _call_bedrock_with_tools(messages, region, enriched_prompt)
        if not response:
            return "Unable to reach Bedrock. Please try again."

        # Check if response contains tool_use blocks
        tool_calls = [b for b in response.get("content", []) if b.get("type") == "tool_use"]
        text_blocks = [b for b in response.get("content", []) if b.get("type") == "text"]

        if not tool_calls:
            # No tool calls — return the text response
            return "\n".join(b.get("text", "") for b in text_blocks).strip()

        # Execute tool calls and feed results back
        messages.append({"role": "assistant", "content": response["content"]})

        tool_results = []
        n_errors = 0
        for tc in tool_calls:
            tool_name = tc["name"]
            tool_input = tc.get("input", {})
            logger.info("Bedrock tool call: %s(%s)", tool_name, json.dumps(tool_input)[:100])

            result = _execute_tool(tool_name, tool_input, conn_str)
            result_str = json.dumps(result, indent=2, default=str)
            logger.info("Tool result: %s", result_str[:200])

            # If tool failed, add guidance to use context data
            if isinstance(result, dict) and "error" in result:
                n_errors += 1
                result_str += ("\n\nNOTE: This tool call failed. "
                              "Use the context data provided earlier to answer. "
                              "Do NOT retry the same tool — it will fail again.")

            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tc["id"],
                "content": result_str,
            })

        messages.append({"role": "user", "content": tool_results})

        # If all tools failed, break out of the loop early
        if n_errors == len(tool_calls):
            logger.warning("All %d tool calls failed, stopping agentic loop", n_errors)
            # Ask Bedrock one more time with explicit instruction to use context
            messages.append({"role": "user", "content": 
                "All tool calls failed due to MCP server issues. "
                "Please answer using ONLY the context data provided in my original question. "
                "Do not attempt any more tool calls."})
            response = _call_bedrock_with_tools(messages, region, enriched_prompt)
            if response:
                text_blocks = [b for b in response.get("content", []) if b.get("type") == "text"]
                if text_blocks:
                    return "\n".join(b.get("text", "") for b in text_blocks).strip()
            return "Unable to get data from the cluster. Please try again or check the MCP connection."

    return "Reached maximum tool iterations. Please try a more specific question."


def explain_results(action_name, results, context=None, region="us-east-1"):
    """Simple explanation of pre-fetched results (no tool use)."""
    prompt = f"Action: {action_name}\n\nResults:\n```json\n{json.dumps(results, indent=2, default=str)}\n```"
    if context:
        prompt += f"\n\nContext: {context}"
    prompt += "\n\nExplain these results concisely and provide recommendations."
    return _call_bedrock_simple(prompt, region)


def _call_bedrock_with_tools(messages, region="us-east-1", system_prompt=None):
    """Call Bedrock with tool definitions for agentic behavior."""
    if system_prompt is None:
        system_prompt = SYSTEM_PROMPT
    for model_id in [MODEL_ID, FALLBACK_MODEL_ID]:
        try:
            bedrock = boto3.client("bedrock-runtime", region_name=region)
            body = {
                "messages": messages,
                "max_tokens": 4000,
                "temperature": 0.1,
                "system": system_prompt,
                "anthropic_version": "bedrock-2023-05-31",
                "tools": TOOLS,
            }
            response = bedrock.invoke_model(modelId=model_id, body=json.dumps(body))
            result = json.loads(response["body"].read())
            logger.info("Bedrock %s: stop_reason=%s, content_types=%s",
                        model_id, result.get("stop_reason"),
                        [b.get("type") for b in result.get("content", [])])
            return result
        except Exception as e:
            logger.warning("Bedrock %s failed: %s", model_id, e)
            continue
    return None


def _call_bedrock_simple(prompt, region="us-east-1"):
    """Simple Bedrock call without tools."""
    for model_id in [MODEL_ID, FALLBACK_MODEL_ID]:
        try:
            bedrock = boto3.client("bedrock-runtime", region_name=region)
            response = bedrock.invoke_model(
                modelId=model_id,
                body=json.dumps({
                    "messages": [{"role": "user", "content": prompt}],
                    "max_tokens": 2000,
                    "temperature": 0.2,
                    "system": SYSTEM_PROMPT,
                    "anthropic_version": "bedrock-2023-05-31",
                }),
            )
            result = json.loads(response["body"].read())
            return result["content"][0]["text"]
        except Exception as e:
            logger.warning("Bedrock %s failed: %s", model_id, e)
            continue
    return None
