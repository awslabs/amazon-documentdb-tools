---
name: prism-dash
display_name: Prism Dash Platform
description: "Top-level guide to the Prism Dash platform — an autonomous Amazon DocumentDB analysis and advisory tool built on Python/Dash. Covers the Observe→Reason→Decide→Act agent loop, the AnalyzerPlugin system, the 8-module analysis pipeline, cross-module correlation, cross-run memory, the Bedrock AI advisory stack, the code review engine, and Markdown report generation. Use when you need to understand how Prism works end-to-end, where a capability lives, or how the sub-skills (documentdb-advisor, documentdb-well-architected-review) plug in."
icon: "🔭"
metadata:
  author: Amazon DocumentDB SSA Team
  version: "1.0.0"
---

# Prism Dash Platform

Prism Dash is an autonomous Amazon DocumentDB optimization platform written in
Python on top of Dash 2.17 (with Dash Bootstrap Components + Plotly). It connects
to a DocumentDB cluster, runs an autonomous agent that analyzes the cluster for
performance, cost, reliability, and architectural issues, correlates the findings
across modules, and surfaces AI-grounded recommendations through a tabbed UI and a
Markdown report.

This is the **orchestration-level** skill. It describes how the whole system fits
together and where each capability lives. Two focused sub-skills handle the
conversational and review surfaces:

- **`documentdb-advisor`** — the read-only conversational advisor (chat, agentic
  tool-use, per-slow-query recommendations). See `documentdb-advisor/SKILL.md`.
- **`documentdb-well-architected-review`** — the 6-pillar Well-Architected review
  (38-check catalog, scoring, exports). See
  `documentdb-well-architected-review/SKILL.md`.

When a task is specifically about chat/advice or about a WA review, defer to those
sub-skills. Use this skill for cross-cutting questions about the platform.

## 1. Tech Stack

- **UI:** Dash 2.17 + Dash Bootstrap Components + Plotly
- **Backend:** Python 3, PyMongo 4.5 (`appname="DocDB-Agent"`), boto3
- **AI:** AWS Bedrock — `us.anthropic.claude-sonnet-4-20250514-v1:0` (primary),
  `us.anthropic.claude-haiku-4-5-20251001-v1:0` (fallback)
- **Target:** Amazon DocumentDB clusters (public or private via SSH tunnel)

## 2. Architecture — The Autonomous Agent (`agent_orchestrator.py`)

The agent runs an **Observe → Reason → Decide → Act** loop on a background daemon
thread (started by `start_agent(conn_data)`; stopped by `stop_agent()`). A second
daemon thread (`_monitor_activity`) polls `currentOp` for live activity. All shared
state lives in the module-level `_agent_state` dict guarded by a `threading.Lock`;
the UI polls a lightweight summary via `get_agent_state()` and `dcc.Interval`.

| Phase | Function | What it does |
|-------|----------|--------------|
| Observe | `_observe(conn_data)` | Collects fast facts: AWS cluster metadata (engine version, deletion protection, backup retention, log exports, instance types) via boto3; database sizes/collection counts via PyMongo (`dbStats`, capped by `initial_batch`); CloudWatch profiler log-group existence; active databases from `currentOp`; previous state + config drift from `agent_memory`. |
| Reason | `_reason(...)` | Calls Bedrock as a **planner** with the advisor system prompt + a planner addendum. Returns JSON `{next_module, target_database, target_collection, reasoning, priority, skip_modules, skip_reasons}`. On any failure it falls back to a fixed order via `_fallback_decision`. |
| Decide | `_decide(decision, completed)` | Validates the plan: dedups already-completed modules, enforces that critical modules ran (`cluster_snapshot`, `well_architected`, `db_analysis`, and `slow_query` when profiler logs exist), and returns the next module or `"done"`. |
| Act | `_act(module, ...)` | Executes the chosen module under a per-module timeout (`_run_with_timeout`, default 120s) and records results into `_agent_state["modules"][name]`. |

**Fixed fallback order** (`_FALLBACK_ORDER`): `cluster_snapshot → well_architected →
db_analysis → slow_query → compression → bloat → instance_recommender →
storage_recommender`.

### Safety & prioritisation (driven by `lazy_load_config.yaml`)

- System databases `admin`, `local`, `config` are **always** excluded.
- `_safety_check()` enforces `skip_databases` and `skip_collections` (prefix match).
- `_build_analysis_scope()` ranks candidates: **active databases first** (from
  `currentOp`), then `priority_databases`, then largest-by-size, truncated to
  `max_databases_to_analyse`. Skipped databases and their reasons are recorded in
  `analysis_scope.skipped`.
- `_iter_databases()` paces work with `database_tree.delay_between_seconds` to keep
  cluster load gentle and honors the `_stop_event`.

### Cross-module correlation

After modules complete, the agent correlates findings across modules (for example
slow query on a collection with a missing/low-cardinality index, or high bloat on a
collection with compression disabled) and stores human-readable strings in
`_agent_state["correlated_insights"]`. These feed the report's executive summary and
are exposed to the chat advisor as the `agent_insights` data source.

## 3. Plugin System (`analyzer_base.py`)

Analysis modules are designed as plugins implementing the `AnalyzerPlugin` ABC.
Concrete subclasses **auto-register** at import time via `__init_subclass__` into
`_ANALYZER_REGISTRY` (accessed through `get_analyzer_registry()`,
`get_analyzer(name)`, `get_execution_order()`).

```python
class AnalyzerPlugin(ABC):
    name: str            # Unique ID; matches a key in agent_state["modules"]
    display_name: str    # Human-readable name for the UI
    scope: str           # "database" (per-db) or "cluster" (once)
    dependencies: list   # Analyzer names that must complete first
    priority: int = 50   # Lower runs earlier
    enabled: bool = True # Kill switch

    def run(self, conn_data, progress_callback=None, previous_results=None) -> Any: ...
    def should_skip(self, conn_data, previous_results=None) -> Optional[str]: ...
    def validate_prerequisites(self, conn_data) -> Optional[str]: ...
    def get_report_section(self, result, conn_data) -> Optional[str]: ...
```

`conn_data` keys: `connection_string`, `database_name`, `cluster_id`, `region`,
`log_group`. `progress_callback(phase: str, detail: str, pct: int)` is the standard
progress contract across the platform.

`analyzers/__init__.py` imports the **7** plugin implementations so they register:
`ClusterSnapshotAnalyzer`, `DbAnalysisAnalyzer`, `SlowQueryAnalyzer`,
`CompressionAnalyzer`, `WellArchitectedAnalyzer`, `InstanceRecommenderAnalyzer`,
`StorageRecommenderAnalyzer`.

> **Current execution reality:** the orchestrator's `_act()` keeps
> `_PLUGIN_READY_MODULES` empty and runs a **legacy hardcoded dispatch** for every
> module, because the UI reads from shared-state dicts (`_snap`, `_wa`,
> `_agent_state`) that the plugins don't yet fully populate. The plugin registry is
> fully wired and discovered at import; modules graduate to the plugin path by being
> added to `_PLUGIN_READY_MODULES`. Document modules by their behavior, not by
> assuming the plugin path is active.

To add a new analyzer, follow the **`prism-analyzer-module`** skill.

## 4. Module Catalog (8 agent-state slots)

`_agent_state["modules"]` has **8** slots. Seven map to registered analyzer plugins;
`bloat` is a slot whose analysis is produced as part of `db_analysis` (index/collection
bloat percentages), not a separate registered plugin.

| Module | Scope | Depends on | Produces |
|--------|-------|-----------|----------|
| `cluster_snapshot` | cluster | — | AWS cluster config + per-instance CloudWatch metrics (CPU, cache hit ratio, connections, memory). Backed by `cloudwatch_analyzer.py` and `tabs/cluster_snapshot.py`. |
| `well_architected` | cluster | — | 38-check WA assessment across 6 pillars + Bedrock recommendations. See the WA review sub-skill. |
| `db_analysis` | database | — | Per-database, per-collection stats: doc counts, sizes, indexes, usage (`$indexStats`), cardinality, bloat. Backed by `db_analyzer.py`, `cardinality_analyzer.py`. Also yields the `bloat` slot data. |
| `slow_query` | cluster | — | Slow query patterns from CloudWatch profiler logs (namespace, operation, avg/max ms, count, example query). Backed by `query_analyzer.py`. |
| `compression` | database | db_analysis | Per-collection compression analysis (LZ4/ZSTD savings estimation). Backed by `compression_analyzer.py`. |
| `bloat` | database | db_analysis | Index/collection bloat detection (surfaced from `db_analysis` results). |
| `instance_recommender` | cluster | cluster_snapshot | Statistical workload sizing recommendations. Backed by `instance_recommender.py`. |
| `storage_recommender` | cluster | cluster_snapshot | Standard vs I/O-Optimized cost comparison. Backed by `storage_cost_analyzer.py`. |

Each module slot holds `{status, result, error, ts}` where `status ∈ {pending,
running, done, skipped}`.

### Instance recommender (`instance_recommender.py`)

`analyze_workload_statistics()` computes mean, std, **coefficient of variation (CV)**,
spike frequency (values above mean + 2·std), idle frequency (values < 20%), and P5/P95
from CloudWatch CPU datapoints, then classifies the workload as `highly_spiky`,
`moderately_spiky`, or `sustained`. `recommend_instance_type()` then proposes one of:
**Serverless** (DCU-based, for spiky workloads with high idle + spike frequency),
**NVMe** (`db.r6gd.*`, when buffer cache hit ratio < 85% and not CPU-bound),
**downgrade** (over-provisioned), or **upgrade** (under-provisioned). Catalogs:
`STANDARD_INSTANCES` (db.t4g, db.r5, db.r6g, db.r8g) and `NVME_INSTANCES` (db.r6gd).
Burstable `db.t*` types are never recommended for scale up/down targets.

### Storage cost analyzer (`storage_cost_analyzer.py`)

Pulls live AWS Pricing API data and compares **Standard** vs **I/O-Optimized** monthly
cost (instance compute + storage + I/O), region-specific. I/O-Optimized is favored
when per-operation I/O cost is the dominant component of the bill.

## 5. Memory System (`agent_memory.py`)

Persistent, cross-run learning under `.prism_cache/{cluster_id}/`:

| File | Contents |
|------|----------|
| `snapshot.json` | Lightweight cluster facts (engine version, instance types, deletion protection, backup retention, log exports, db count). |
| `databases.json` | Per-db summary: collections, docs, size, index counts, unused indexes, avg bloat. |
| `activity/{YYYYMMDD_HHMM}.json` | 15-minute `currentOp` activity summaries. |
| `daily/{YYYYMMDD}.json` | Daily rollups (auto-created from ≥4 activity files; the 15-min files are then removed). |
| `last_analysis.json` | Compact summary of the last agent run (module statuses, top correlated insights, reasoning log). |
| `wa_results.json`, `slow_queries.json`, `index_health.json` | Versioned module outputs for trend/drift comparison. |

- **Change detection:** `_content_hash()` hashes content while ignoring timestamp
  fields, so unchanged data only refreshes `last_seen` rather than writing a new
  version. `compare_with_previous()` emits human-readable drift strings (engine
  upgrades, instance-type changes, db count changes, bloat/size growth, prior WA/slow
  query/index summaries).
- **Versioning:** `_write_versioned()` keeps up to **3** versions
  (`name.json`, `name.v1.json`, `name.v2.json`) and preserves the oldest `first_seen`.
- **Retention:** `cleanup_old_files()` deletes activity/daily files older than **30
  days**.

## 6. AI Advisory Stack

Three Bedrock-backed components share the same model pair and degrade gracefully.
Detailed behavior lives in the sub-skills; the wiring is:

- **`bedrock_advisor.py`** — agentic tool-use advisor. Loads its system prompt from
  `documentdb-well-architected-review/advisor-prompt.md` (via `_load_skill()`) and
  injects topic-matched reference docs from that directory's `references/`. Defines
  **8** tools but `_execute_tool()` returns an error (MCP disabled), so answers are
  grounded in injected context. See `documentdb-advisor/SKILL.md`.
- **`chat_advisor.py`** — two-step chat (classify → fetch → answer). Step 1 (Haiku)
  classifies which data sources are needed; step 2 fetches those sources from agent
  state and answers with the advisor prompt + matched references. Data sources:
  `collection_stats`, `index_health`, `slow_queries`, `live_activity`,
  `cluster_config`, `well_architected`, `agent_insights`.
- **`slow_query_recommender.py`** — per-pattern recommendations (`Add index` /
  `Rewrite query` / `Scale compute`), grounded in `documentdb-advisor/SKILL.md` + all
  of its `references/`. Background generation, per-database concurrency limit of 5,
  persistent cache keyed by `(cluster_id, pattern_key, stats_digest)`, lifecycle
  `placeholder → generating → done | failed | unavailable`.
- **`bedrock_parallel.py`** — parallel per-pattern index suggestions (Haiku) with
  collection context (existing indexes, doc count), ESR-rule prompting, JSON output.

The same Bedrock pair also powers the **agent planner** (`_reason`) and the WA
**recommendation generation** (`tabs/well_architected._generate_ai_recommendations`
via `documentdb-well-architected-review/wa-advisor-prompt.md`).

## 7. Code Review Engine (`code_review_engine.py`)

Scans **application source code** (not the database) for DocumentDB client best
practices. `start_code_review(target_dir, output_dir)` runs on a background thread:
`_discover_files()` finds source files (JS/TS, Java, Python, Go, C#, Ruby, PHP, R) and
config files (docker-compose, .env, serverless.yml, CDK, etc.); `_scan_patterns()`
detects `MongoClient` usage, connection strings, pool/timeout/retry config, and index
creation; `_evaluate_checklist()` scores **54** checklist items; `_write_report()`
emits a Markdown report with a compliance percentage. Surfaced via
`tabs/code_review_panel.py`.

## 8. Report Generation (`agent_report.py`)

`generate_report(agent_state, conn_data)` produces a Markdown report with: title +
metadata, **Executive Summary** (modules completed, correlated insights, top actions),
**Agent Reasoning Log** (step/module/reasoning table), **Analysis Scope** (in-scope vs
skipped databases), per-module sections (cluster config, database analysis, slow
queries, compression, Well-Architected, instance sizing, storage), **Live Activity
Observations**, and **Skipped Modules**. The Well-Architected review additionally
supports a PDF export via `wa_pdf.generate_wa_pdf(...)`.

## 9. Configuration (`lazy_load_config.yaml`)

Loaded at runtime through `lazy_load_cfg.get_config()`. Key sections:

- `database_tree.initial_batch` (10) — databases fully populated at connect; the rest
  load in the background.
- `database_tree.delay_between_seconds` (2) — pacing between background db loads and
  agent iterations.
- `compression_analysis.collections_per_batch` (5), `delay_between_seconds` (3).
- `agent_prioritisation.priority_databases` ([]), `skip_databases`
  (`staging, local, admin, config`), `max_databases_to_analyse` (20),
  `skip_collections` (`audit_log, tmp_, _archive` — prefix match),
  `max_collection_size_gb` (50), `max_compression_sample_docs` (1,000,000).

Always respect these limits; never bypass `skip_*` lists or analyze system databases.

## 10. Other Components

- `aws_discovery.py` — discovers DocumentDB clusters/instances in an account/region.
- `ssh_tunnel.py` — manages SSH tunnels for private clusters (with orphan reaping).
- `tabs/` — self-registering Dash UI tabs (`tabs/registry.py`); `tabs/wa_v2/` is the
  next-gen WA UI that contributes additional checks (e.g. `PERF1`, `PERF1c`).

## 11. Severity / Priority Schema (shared across all Prism skills)

To keep findings traceable across the platform, all three skills use the same scale:

- **Critical** — data loss, security breach, or unavailability risk.
- **High** — significant performance degradation or cost waste happening now.
- **Medium** — best-practice gap with moderate risk.
- **Low** — optimization opportunity with low urgency.

WA check rows additionally carry a status of `pass | warn | fail | info`; `info` is
excluded from the health score denominator.

## 12. Safety Constraints (platform-wide)

- Prism is **read-only** with respect to user data. Never insert/update/delete user
  documents. The advisor may recommend index/config changes but always with a
  "test in non-production first" caveat.
- **Never** recommend dropping the `_id` index.
- LLM-generated queries are safety-checked (`bedrock_advisor._check_query_safety`):
  `$where`, `$function`, `$accumulator` are denied; `$regex` length and nesting depth
  are capped.
- Treat AWS resources as production unless proven otherwise; prefer read/describe/list
  operations and never disable safety protections without explicit confirmation.
