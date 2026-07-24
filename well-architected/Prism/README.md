# Amazon DocumentDB Prism Dash — Amazon DocumentDB Autonomous Analysis Platform

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Dash](https://img.shields.io/badge/Dash-2.17-119DFF)
![Amazon DocumentDB](https://img.shields.io/badge/Amazon-DocumentDB-527FFF)
![Amazon Bedrock](https://img.shields.io/badge/Amazon-Bedrock-FF9900)

Amazon DocumentDB Prism is an AI-powered analysis and advisory tool for Amazon DocumentDB. It gives you
a fleet-wide view of every cluster in a region and lets you drill all the way down to
individual index optimization opportunities on a single collection. Connect to a cluster
and an autonomous **Observe → Reason → Decide → Act** agent analyzes it across multiple
modules, correlates the findings, and produces a consolidated report and an interactive
AI advisor.

Amazon DocumentDB Prism examines both sides of the workload. It runs a **Well-Architected review** of
the cluster across all 6 AWS pillars, and it can also **scan your application source
code** — the code that connects to and queries DocumentDB — to surface client-side best
practices and optimization opportunities such as connection pooling, timeouts, retries,
and query and cost patterns.

Built with Dash (Plotly), Amazon Bedrock (Claude Sonnet 4 / Haiku), boto3, and PyMongo.

---

## Contents

|  |  |
|---------|-------------|
| [Why Amazon DocumentDB Prism](#why-prism) | Key capabilities and what sets it apart |
| [Setup & Installation](#setup--installation) | First-time setup (see the Setup Guide) |
| [What You Can See](#what-you-can-see--fleet-down-to-index) | Fleet → cluster → database → collection → index views |
| [Architecture](#architecture) | Components and how they fit together |
| [Analysis Modules](#analysis-modules) | The modules the autonomous agent runs |
| [Running the Server](#running-the-server) | Start, stop, and operational notes |
| [User Guide](#user-guide) | Step-by-step usage once running |
| [Configuration](#configuration) | Runtime config and IAM policy files |
| [AI-Powered Features](#ai-powered-features) | Bedrock planning, advisor, and recommendations |
| [Well-Architected Review](#well-architected-review) | 75 automated checks across the 6 pillars |
| [Skills — How the AI Is Grounded](#skills--how-the-ai-is-grounded) | Packaged skills that ground every AI feature |
| [Project Structure](#project-structure) | Repository layout |

---

## Why Amazon DocumentDB Prism

- **Fleet to field** — Start at a region-wide fleet overview and drill down through
  cluster → database → collection → index without leaving the tool.
- **Autonomous agent** — A Bedrock planner decides what to analyze next from live cluster
  state, then runs the analysis modules in priority order (with a fixed-order fallback if
  planning is unavailable).
- **Cross-module correlation** — Connects related signals (slow query + missing index +
  bloat on the same collection) into a single actionable insight instead of scattered
  findings.
- **Well-Architected review** — 75 automated checks across all 6 AWS WAF pillars with
  remediation guidance and Bedrock-generated recommendations, exportable as a PDF report.
- **Application code review** — Scans your app source for DocumentDB client anti-patterns
  (54-item checklist, 10+ languages) with AI recommendations.
- **AI chat advisor** — A multi-turn Bedrock advisor grounded in your cluster's data and
  curated DocumentDB references, with query-safety guardrails.
- **Persistent memory** — Cross-run learning with configuration-drift detection.
- **Read-only by design** — Connects with read-only permissions and never writes to,
  modifies, or deletes anything on your cluster or AWS resources.

---

## Setup & Installation

For first-time setup (prerequisites, installation, IAM, security groups, verification),
follow the **[Setup Guide](SETUP.md)**.

---

## What You Can See — Fleet Down to Index

| Level | View | What it shows |
|-------|------|---------------|
| **Fleet** | Landing page | Every DocumentDB cluster in the region: counts, instance-type / engine-version / Performance Insights distribution, and per-cluster actions (Review, Connect). |
| **Cluster** | Cluster Overview, Activity, Slow Queries, Well-Architected, Recommended Actions | Configuration + CloudWatch metrics, live operations, slow-query patterns, WA findings, and a prioritized action list. |
| **Database** | Overview, Indexes, Compression | Per-collection stats and database-wide index and compression analysis. |
| **Collection** | Collection detail | Document counts, size, storage, and bloat for a single collection. |
| **Index** | Index detail | Per-index cardinality, usage (`$indexStats`), redundancy, and bloat — the index optimization opportunities. |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Dash Web UI (tabs/)                       │
├─────────────────────────────────────────────────────────────┤
│  Agent Orchestrator (Observe → Reason → Decide → Act)         │
├────────────┬──────────────┬──────────────┬───────────────────┤
│ Analyzers  │ WA Checks    │ AI Advisory  │ Code Review        │
│            │              │ (Bedrock)    │ Engine             │
├────────────┴──────────────┴──────────────┴───────────────────┤
│  Data Layer: pymongo │ boto3 │ CloudWatch │ Agent Memory       │
└─────────────────────────────────────────────────────────────┘
```

- **Orchestrator** (`agent_orchestrator.py`) drives the analysis loop on background
  threads and publishes thread-safe state the UI polls via `dcc.Interval`.
- **AI Advisory** (`bedrock_advisor.py`, `chat_advisor.py`, `slow_query_recommender.py`)
  grounds Bedrock answers in collected data and **skill** reference docs (see Skills).
- **Agent Memory** (`agent_memory.py`) persists results under `.prism_cache/` for
  trend and drift analysis.

---

## Analysis Modules

After you connect, the autonomous agent runs these modules automatically (you don't
trigger them individually). You can also analyze a specific database on demand — see the
User Guide.

| Module | Scope | What it produces |
|--------|-------|------------------|
| `cluster_snapshot` | Cluster | AWS cluster metadata + CloudWatch metrics (CPU, memory, connections, cache hit ratio, IOPS, replica lag). |
| `well_architected` | Cluster | 75 checks across all 6 pillars with remediation guidance. |
| `db_analysis` | Database | Per-collection stats: doc count, size, indexes, usage, cardinality, bloat, compression. |
| `slow_query` | Cluster | CloudWatch profiler log pattern extraction + AI per-pattern recommendations. |
| `compression` | Database | Collection-level LZ4/ZSTD readiness and savings estimates. |
| `bloat` | Database | Index and collection bloat detection (surfaced from `db_analysis`). |
| `instance_recommender` | Cluster | Statistical workload analysis → sizing (scale up/down, Graviton, Serverless, NVMe). |
| `storage_recommender` | Cluster | Standard vs I/O-Optimized cost comparison using the live AWS Pricing API. |

---

## Running the Server

Installation and the server lifecycle are separate steps: setup verifies the environment
but does not launch the application. Start the server only after preflight has passed (see
the [Setup Guide](SETUP.md)).

**Start (macOS / Linux, local or EC2):**

```bash
./start_server.sh
```

`start_server.sh` runs preflight, launches the server bound to `0.0.0.0:8501`, and prints
the access URL — `http://localhost:8501` locally, or the instance's public URL on EC2. On
Windows, run `python app.py` instead (see the [Setup Guide](SETUP.md)).

**Stop:**

```bash
./stop_server.sh
```

**Operational notes**

- A single Gunicorn worker is used intentionally: the analysis agent maintains state
  in-process, so multiple workers would fragment it.
- Each start writes a timestamped log to `logs/prism_<timestamp>.log`.
- Configurable via environment variables: `PRISM_PORT` (default `8501`),
  `PRISM_HOST` (default `0.0.0.0`), `PRISM_DEBUG` (default `0`).

---

## User Guide

This section assumes Amazon DocumentDB Prism is already installed and running (see the [Setup Guide](SETUP.md)).

### 1. Open the app

Browse to the URL printed by `start_server.sh` (or `http://localhost:8501`). You'll see
a **sign-in screen** — enter the credentials you created with `create_user.py` (see
[§4 in the Setup Guide](SETUP.md#4-run-the-server)). Sessions last 15 minutes of
inactivity (sliding window); active use keeps you logged in.

### 2. Review the fleet

The landing page auto-discovers every DocumentDB cluster in the selected region (change
the region from the top-nav selector). You get summary metrics, distribution charts, and
a per-cluster table. From here you can:

- **Review** — run a Well-Architected review for a cluster directly, without connecting
  (uses AWS APIs and CloudWatch only). Results open in a modal with a PDF download.
- **Connect →** — open the connection dialog to analyze the cluster's data.

### 3. Connect to a cluster

The connection dialog has three steps:

1. **Cluster** — select the AWS region and pick a discovered cluster (or Discover again).
2. **Connection** — choose how to reach the cluster. The toggle **defaults to SSH
   Tunnel**; pick the mode for where you run Amazon DocumentDB Prism:

   | Where you run Amazon DocumentDB Prism | Use | Why |
   |------------------------|-----|-----|
   | EC2 in the same VPC as DocumentDB | **Direct** | The instance reaches the cluster endpoint directly over the VPC — no bastion needed. |
   | Laptop / outside the VPC (private cluster) | **SSH Tunnel** | DocumentDB isn't publicly reachable, so connect through a bastion host: provide the bastion IP, SSH user (default `ec2-user`), and `.pem` key path. Amazon DocumentDB Prism opens and manages the tunnel (with auto-reconnect). |

   > **On EC2, switch to "Direct" — don't leave the default "SSH Tunnel."** Leaving it on
   > SSH Tunnel tries to open a bastion connection you don't need and the connect fails.
3. **Credentials** — DocumentDB username, password, and TLS on/off.

Click **Connect & Load Databases**. The database tree loads (first batch immediately, the
rest in the background) and the autonomous agent starts.

### 4. Let the agent analyze (autonomous)

Once connected, the agent runs on its own — observing the cluster, planning the next
module, and analyzing in priority order. Watch progress in the sidebar; databases in the
tree show ⏳ while analyzing and ✅ when done. Findings populate the cluster- and
database-level tabs as they're produced.

The agent auto-analyzes up to **10 databases** (configurable via `max_databases_to_analyse`
in `prism_config.yaml`). Any databases beyond that limit aren't analyzed automatically —
you can analyze them on demand (see the next step).

### 5. Analyze a database on demand (manual)

You don't have to wait for the agent to reach a given database. Click a database in the
sidebar tree to analyze it directly, then open its **Overview**, **Indexes**, or
**Compression** tabs. Drill into a collection, then into an individual index, to see
cardinality, usage, redundancy, and bloat for that index.

### 6. Run a Well-Architected review

Open the **Well-Architected** tab for a connected cluster (or use **Review** from the
fleet page). The review evaluates 75 checks across Reliability, Security, Operational
Excellence, Performance Efficiency, Cost Optimization, and Sustainability, produces a
health score, and generates Bedrock recommendations for failing/warning checks. Export
the findings with **Download PDF** (or **Download PDF + AI** to include the AI
recommendations).

### 7. Ask the AI advisor

The **AI Advisor** tab is a chat grounded in your cluster's collected data and curated
DocumentDB references. Ask about slow queries, indexing, sizing, cost, compression, or
MongoDB compatibility. It cites the data it used and never runs write operations.

### 8. Scan your application code

The **Application Code Review** scans the source code of an application that connects to
DocumentDB and checks it against a 54-item client best-practice checklist (connection
config, pooling, timeouts, failover/HA, exception handling, idempotency, cursor
management, Lambda, security, monitoring, and query/cost), then adds AI recommendations.

> **Important:** the scan runs against the filesystem of the machine where Amazon DocumentDB Prism is
> running. The application source must be present on that machine, and the path you enter
> must be a directory readable by the Amazon DocumentDB Prism process. If you run Amazon DocumentDB Prism on a bastion
> or EC2 host, clone or copy the application code there first.

Enter the absolute path (for example `/home/ec2-user/my-app`) and click **Scan**. You can
reopen past runs from the history dropdown and download the report as a Markdown (`.md`) file.

---

## Configuration

| File | Purpose |
|------|---------|
| [`prism_config.yaml`](prism_config.yaml) | Runtime tuning: initial database batch size, agent pacing (`delay_between_seconds`), prioritised/skip database and collection lists, `max_databases_to_analyse`, compression batch limits. |
| [`iam_policy.json`](iam_policy.json) | Minimum IAM policy for the execution role. |

The agent always excludes the `admin`, `local`, and `config` system databases and honors
the skip lists and size limits in `prism_config.yaml`.

---

## AI-Powered Features

- **Autonomous planning** — Bedrock decides analysis order from observed cluster state;
  falls back to a fixed priority order on any failure.
- **Chat advisor** (`chat_advisor.py`) — A classify → fetch → answer pipeline: it
  determines which data sources are needed, fetches them from agent state, and answers
  with topic-matched reference docs.
- **Slow query recommendations** (`slow_query_recommender.py`) — Per-pattern advice
  (Add index / Rewrite query / Scale compute) with per-database concurrency limits and a
  persistent cache.
- **WA remediation advice** — Pillar-specific Bedrock recommendations from failing and
  warning checks.
- **Code review recommendations** — DocumentDB-specific fixes for findings from the
  application code scan.

Models: `us.anthropic.claude-sonnet-4-20250514-v1:0` (primary),
`us.anthropic.claude-haiku-4-5-20251001-v1:0` (fallback). Bedrock-driven queries are
guarded: `$where`/`$function`/`$accumulator` are denied and regex length and nesting
depth are capped.

---

## Well-Architected Review

75 automated checks across the 6 AWS Well-Architected pillars:

| Pillar | Example checks |
|--------|----------------|
| Reliability | Backup retention, AWS Backup plan, global cluster, instance count, multi-AZ, replica lag, failover events, cursor timeouts, MVCC ID availability |
| Security | Encryption at rest, TLS enabled, security group exposure, Secrets Manager usage, audit logging, TLS minimum version, deletion protection |
| Operational Excellence | Subnet group AZ span, profiler logging, CloudWatch alarms, custom parameter group, maintenance window, engine version |
| Performance Efficiency | Connection utilization, buffer cache hit ratio, index-to-data ratio, storage bloat, freeable memory, swap usage, disk queue depth, large collections without indexes, redundant indexes |
| Cost Optimization | CPU utilization, unused indexes, cost allocation tags, storage type, idle reader detection |
| Sustainability | Graviton processor adoption, compression enablement |

Checks run both per-cluster and per-instance (with writer-only / reader-only routing for
metrics like MVCC and idle readers). **Output:** a health score, the findings shown in
the UI, Bedrock recommendations for failing/warning checks, and a downloadable PDF report
(`wa_pdf.py`).

---

## Skills — How the AI Is Grounded

Amazon DocumentDB Prism does not rely on the model's training data for DocumentDB knowledge. Every AI
feature is grounded by **packaged skills** — folders containing a `SKILL.md` system
prompt plus curated DocumentDB reference docs. Recommendations stay engine-specific and
current, and you update them by editing Markdown (no retraining).

| Skill | Contents | Grounds |
|-------|----------|---------|
| [`documentdb-advisor/`](documentdb-advisor) | `SKILL.md` + `references/` (11 DocumentDB docs: anti-patterns, backup/restore, best-practices, functional-differences, index-management, live-operations-monitoring, performance-improvement-tips, pricing/cost, query-plan/troubleshooting, serverless, supported-operators) | Slow-query recommendations (`slow_query_recommender.py` loads `SKILL.md` + all references) and the code-review advice (`SKILL.md` + best-practices) |
| [`documentdb-well-architected-review/`](documentdb-well-architected-review) | `SKILL.md`, `advisor-prompt.md`, `wa-advisor-prompt.md`, `pillars/`, `references/` | The AI chat advisor (`advisor-prompt.md` system prompt + topic-matched references) and the WA recommendation generator (`wa-advisor-prompt.md`) |
| [`prism-dash/`](prism-dash) | `SKILL.md` | Platform overview skill — documentation and onboarding for the whole tool (not loaded at runtime) |

**Topic-aware reference injection:** when you ask the advisor a question, it matches the
topic (performance, indexing, cost, backup, serverless, compatibility, …) and injects up
to 3 of the reference docs above into the prompt, so answers cite curated DocumentDB
guidance rather than generic database advice.

### Run the skills in Kiro or Amazon Q (headless, no UI)

The skills are portable, framework-agnostic Markdown, so an agent in **Kiro** or **Amazon
Q** can load them directly and deliver these capabilities without the Amazon DocumentDB Prism UI:

- **Well-Architected review (end to end).** Point the agent at the
  `documentdb-well-architected-review` skill. Its frontmatter declares the inputs
  (`cluster_identifier`, `aws_region`) and the tools it needs (`call_aws`, `run_python`,
  `file_write`), and the body contains the full step-by-step workflow, the check catalog
  with thresholds and remediation CLI, the scoring formula, and the export schema. Given
  AWS API access, the agent runs the complete review for a cluster and produces a health
  score, prioritized findings, a Universal WAR Export JSON, and an HTML dashboard.

  > Example prompt: *"Using the documentdb-well-architected-review skill, run a
  > Well-Architected review of cluster `my-docdb` in `us-east-1` and export the report."*

- **DocumentDB advisory.** Load the `documentdb-advisor` skill (`SKILL.md` + its 11
  reference docs) as the agent's grounding to get engine-specific, reference-cited
  guidance on indexing, instance sizing, cost optimization, compression, query plans,
  anti-patterns, backup/restore, and MongoDB compatibility — with the same topic-aware
  reference selection used in the app.

  > Example prompt: *"Using the documentdb-advisor skill, review these indexes and slow
  > query patterns and recommend changes following the ESR rule."*

Both skills carry the same DocumentDB safety guidance used in the app (read-only advice,
never drop the `_id` index, test changes in non-production first).

---

## Project Structure

```
prism-dash/
├── app.py                          # Entry point (exposes server = app.server)
├── agent_orchestrator.py           # Autonomous agent loop
├── agent_memory.py                 # Persistent cross-run memory (.prism_cache/)
├── agent_report.py                 # Markdown report generation
├── bedrock_advisor.py              # AI advisor (Bedrock)
├── chat_advisor.py                 # Two-step chat (classify→fetch→answer)
├── slow_query_recommender.py       # Per-pattern AI recommendations
├── code_review_engine.py           # 54-check application source scanner
├── instance_recommender.py         # Statistical workload analysis
├── storage_cost_analyzer.py        # Pricing API cost comparison
├── wa_pdf.py                       # WA review PDF export
├── ssh_tunnel.py                   # SSH tunnel management for private clusters
├── analyzers/                      # Analysis module implementations
├── wa_checks/                      # Well-Architected checks
├── tabs/                           # Dash UI (fleet, cluster, database, code review)
├── scripts/                        # setup_local.sh, setup_ec2.sh, preflight.sh
├── documentdb-advisor/             # Skill: advisor SKILL.md + 11 reference docs
├── documentdb-well-architected-review/  # Skill: SKILL.md, advisor + WA prompts, pillars, references
├── prism-dash/                  # Skill: platform overview (documentation)
├── iam_policy.json                 # Required IAM permissions
├── prism_config.yaml           # Runtime config
├── SETUP.md                        # Setup & installation (single source of truth)
├── start_server.sh / stop_server.sh
└── requirements.txt
```
