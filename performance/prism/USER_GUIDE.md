# Prism for Amazon DocumentDB — User Guide

Step-by-step guide to using Prism once it is installed and running.

> **Before you start:** this guide assumes Prism is already installed and the server
> is running. For prerequisites, installation, IAM, security groups, and creating a
> login user, see the **[Setup Guide](SETUP.md)**. For a feature and architecture
> overview, see the **[README](README.md)**.

---

## Contents

1. [Open the app](#1-open-the-app)
2. [Review the fleet](#2-review-the-fleet)
3. [Connect to a cluster](#3-connect-to-a-cluster)
4. [Let the agent analyze (autonomous)](#4-let-the-agent-analyze-autonomous)
5. [Analyze a database on demand (manual)](#5-analyze-a-database-on-demand-manual)
6. [Run a Well-Architected review](#6-run-a-well-architected-review)
7. [Ask the AI advisor](#7-ask-the-ai-advisor)
8. [Scan your application code](#8-scan-your-application-code)
9. [Tab reference](#tab-reference)
10. [Architecture](#architecture)
11. [Analysis Modules](#analysis-modules)
12. [Well-Architected Review](#well-architected-review)
13. [Skills — How the AI Is Grounded](#skills--how-the-ai-is-grounded)
14. [Project Structure](#project-structure)
15. [Tips & troubleshooting](#tips--troubleshooting)

---

## 1. Open the app

Browse to the URL printed by `start_server.sh` (or `http://localhost:8501`). You'll see
a **sign-in screen** — enter the credentials you created with `create_user.py` (see
[§4 in the Setup Guide](SETUP.md#4-run-the-server)). Sessions last 15 minutes of
inactivity (sliding window); active use keeps you logged in.

---

## 2. Review the fleet

The landing page auto-discovers every DocumentDB cluster in the selected region (change
the region from the top-nav selector). You get summary metrics, distribution charts, and
a per-cluster table. From here you can:

- **Review** — run a Well-Architected review for a cluster directly, without connecting
  (uses AWS APIs and CloudWatch only). Results open in a modal with a PDF download.
- **Connect →** — open the connection dialog to analyze the cluster's data.

---

## 3. Connect to a cluster

The connection dialog has three steps:

1. **Cluster** — select the AWS region and pick a discovered cluster (or Discover again).
2. **Connection** — choose how to reach the cluster. The toggle **defaults to SSH
   Tunnel**; pick the mode for where you run Prism:

   | Where you run Prism | Use | Why |
   |------------------------|-----|-----|
   | EC2 in the same VPC as DocumentDB | **Direct** | The instance reaches the cluster endpoint directly over the VPC — no bastion needed. |
   | Laptop / outside the VPC (private cluster) | **SSH Tunnel** | DocumentDB isn't publicly reachable, so connect through a bastion host: provide the bastion IP, SSH user (default `ec2-user`), and `.pem` key path. Prism opens and manages the tunnel (with auto-reconnect). |

   > **On EC2, switch to "Direct" — don't leave the default "SSH Tunnel."** Leaving it on
   > SSH Tunnel tries to open a bastion connection you don't need and the connect fails.
3. **Credentials** — DocumentDB username, password, and TLS on/off.

Click **Connect & Load Databases**. The database tree loads (first batch immediately, the
rest in the background) and the autonomous agent starts.

---

## 4. Let the agent analyze (autonomous)

Once connected, the agent runs on its own — observing the cluster, planning the next
module, and analyzing in priority order. Watch progress in the sidebar; databases in the
tree show ⏳ while analyzing and ✅ when done. Findings populate the cluster- and
database-level tabs as they're produced.

The agent auto-analyzes up to **10 databases** (configurable via `max_databases_to_analyse`
in `prism_config.yaml`). Any databases beyond that limit aren't analyzed automatically —
you can analyze them on demand (see the next step).

---

## 5. Analyze a database on demand (manual)

You don't have to wait for the agent to reach a given database. Click a database in the
sidebar tree to analyze it directly, then open its **Overview**, **Indexes**, or
**Compression** tabs. Drill into a collection, then into an individual index, to see
cardinality, usage, redundancy, and bloat for that index.

---

## 6. Run a Well-Architected review

Open the **Well-Architected** tab for a connected cluster (or use **Review** from the
fleet page). The review evaluates 75 checks across Reliability, Security, Operational
Excellence, Performance Efficiency, Cost Optimization, and Sustainability, produces a
health score, and generates Bedrock recommendations for failing/warning checks. Export
the findings with **Download PDF** (or **Download PDF + AI** to include the AI
recommendations).

---

## 7. Ask the AI advisor

The **AI Advisor** tab is a chat grounded in your cluster's collected data and curated
DocumentDB references. Ask about slow queries, indexing, sizing, cost, compression, or
MongoDB compatibility. It cites the data it used and never runs write operations.

---

## 8. Scan your application code

The **Application Code Review** scans the source code of an application that connects to
DocumentDB and checks it against a 54-item client best-practice checklist (connection
config, pooling, timeouts, failover/HA, exception handling, idempotency, cursor
management, Lambda, security, monitoring, and query/cost), then adds AI recommendations.

> **Important:** the scan runs against the filesystem of the machine where Prism is
> running. The application source must be present on that machine, and the path you enter
> must be a directory readable by the Prism process. If you run Prism on a bastion
> or EC2 host, clone or copy the application code there first.

Enter the absolute path (for example `/home/ec2-user/my-app`) and click **Scan**. You can
reopen past runs from the history dropdown and download the report as a Markdown (`.md`) file.

---

## Tab reference

Once connected, these tabs are available. Cluster-level tabs apply to the whole cluster;
database-level tabs apply to the database currently selected in the sidebar tree.

| Tab | Level | What it shows |
|-----|-------|---------------|
| **Cluster Overview** | Cluster | Configuration and CloudWatch metrics (CPU, memory, connections, cache hit ratio, IOPS, replica lag), instance list, storage, and instance/storage recommendations. |
| **Activity** | Cluster | Live operations (`currentOp`) — active/idle sessions, connected users, and long-running operations. |
| **Slow Queries** | Cluster | Slow-query patterns extracted from CloudWatch profiler logs with per-pattern AI recommendations. |
| **Well-Architected** | Cluster | 75 checks across the 6 pillars, a health score, AI remediation advice, and a PDF export. |
| **Recommended Actions** | Cluster | A prioritized, consolidated action list correlated across modules. |
| **AI Advisor** | Cluster | Chat grounded in your collected cluster data and DocumentDB references. |
| **Application Code Review** | Cluster | 54-item client best-practice scan of your app source with AI recommendations. |
| **Overview** | Database | Per-collection stats for the selected database. |
| **Indexes** | Database | Database-wide index analysis — usage, cardinality, redundancy, and bloat. |
| **Compression** | Database | Collection-level LZ4/ZSTD readiness and savings estimates. |
| **Collection detail** | Collection | Document count, size, storage, and bloat for a single collection. |
| **Index detail** | Index | Per-index cardinality, usage (`$indexStats`), redundancy, and bloat. |

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
[step-by-step usage](#5-analyze-a-database-on-demand-manual) above.

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

Prism does not rely on the model's training data for DocumentDB knowledge. Every AI
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
Q** can load them directly and deliver these capabilities without the Prism UI:

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
├── USER_GUIDE.md                   # Step-by-step usage guide
├── start_server.sh / stop_server.sh
└── requirements.txt
```

---

## Tips & troubleshooting

- **Region selector** — the fleet page discovers clusters in the region chosen in the
  top-nav selector. Switch regions there to see clusters elsewhere.
- **Direct vs SSH Tunnel** — on EC2 in the same VPC, use **Direct**. From a laptop or
  outside the VPC, use **SSH Tunnel** with a bastion. Leaving the default SSH Tunnel on
  an in-VPC EC2 host will cause the connect to fail.
- **Read-only by design** — Prism connects with read-only permissions and never writes
  to, modifies, or deletes anything on your cluster or AWS resources.
- **System databases** — `admin`, `local`, and `config` are always excluded, along with
  any skip lists and size limits set in [`prism_config.yaml`](prism_config.yaml).
- **Sessions** — you're logged out after 15 minutes of inactivity; active use keeps the
  session alive.
- **Analysis limit** — only the first `max_databases_to_analyse` databases are analyzed
  automatically; analyze the rest on demand by clicking them in the sidebar tree.
- **Server lifecycle and logs** — see [Running the Application](README.md#running-the-application)
  in the README. Each start writes a timestamped log under `logs/`.

---

**Related docs:** [README](README.md) · [Setup Guide](SETUP.md)
