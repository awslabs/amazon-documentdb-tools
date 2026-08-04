# Prism for Amazon DocumentDB

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Dash](https://img.shields.io/badge/Dash-2.17-119DFF)
![Amazon DocumentDB](https://img.shields.io/badge/Amazon-DocumentDB-527FFF)
![Amazon Bedrock](https://img.shields.io/badge/Amazon-Bedrock-FF9900)

Prism for Amazon DocumentDB is an AI-powered analysis and advisory tool for Amazon DocumentDB. It gives you
a fleet-wide view of every cluster in a region and lets you drill all the way down to
individual index optimization opportunities on a single collection. Connect to a cluster
and an autonomous **Observe → Reason → Decide → Act** agent analyzes it across multiple
modules, correlates the findings, and produces a consolidated report and an interactive
AI advisor.

Prism examines both sides of the workload. It runs a **Well-Architected review** of
the cluster across all 6 AWS pillars, and it can also **scan your application source
code** — the code that connects to and queries DocumentDB — to surface client-side best
practices and optimization opportunities such as connection pooling, timeouts, retries,
and query and cost patterns.

Built with Dash (Plotly), Amazon Bedrock (Claude Sonnet 4 / Haiku), boto3, and PyMongo.

---

## Contents

|  |  |
|---------|-------------|
| [Why Prism](#why-prism) | Key capabilities and what sets it apart |
| [What You Can See](#what-you-can-see--fleet-down-to-index) | Fleet → cluster → database → collection → index views |
| [AI-Powered Features](#ai-powered-features) | Bedrock planning, advisor, and recommendations |
| [Setup & Installation](#setup--installation) | First-time setup (see the Setup Guide) |
| [Running the Application](#running-the-application) | Start, stop, and operational notes |
| [User Guide](USER_GUIDE.md) | Step-by-step usage, architecture, analysis modules, Well-Architected review, skills, and project structure |
| [Configuration](#configuration) | Runtime config and IAM policy files |

---

## Why Prism

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

## What You Can See — Fleet Down to Index

| Level | View | What it shows |
|-------|------|---------------|
| **Fleet** | Landing page | Every DocumentDB cluster in the region: counts, instance-type / engine-version / Performance Insights distribution, and per-cluster actions (Review, Connect). |
| **Cluster** | Cluster Overview, Activity, Slow Queries, Well-Architected, Recommended Actions | Configuration + CloudWatch metrics, live operations, slow-query patterns, WA findings, and a prioritized action list. |
| **Database** | Overview, Indexes, Compression | Per-collection stats and database-wide index and compression analysis. |
| **Collection** | Collection detail | Document counts, size, storage, and bloat for a single collection. |
| **Index** | Index detail | Per-index cardinality, usage (`$indexStats`), redundancy, and bloat — the index optimization opportunities. |

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

## Setup & Installation

For first-time setup (prerequisites, installation, IAM, security groups, verification),
follow the **[Setup Guide](SETUP.md)**.

---

## Running the Application

Installation and the application lifecycle are separate steps: setup verifies the environment
but does not launch the application. Start the application only after preflight has passed (see
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

Prism for Amazon DocumentDB has a dedicated, step-by-step **[User Guide](USER_GUIDE.md)** that
walks through everything once the server is running — opening the app, reviewing the
fleet, connecting to a cluster, the autonomous agent, on-demand database analysis, the
Well-Architected review, the AI advisor, and the application code scan. It also includes a
full [tab reference](USER_GUIDE.md#tab-reference) and
[tips & troubleshooting](USER_GUIDE.md#tips--troubleshooting).

➡️ **Read the full [User Guide](USER_GUIDE.md).**

---

## Configuration

| File | Purpose |
|------|---------|
| [`prism_config.yaml`](prism_config.yaml) | Runtime tuning: initial database batch size, agent pacing (`delay_between_seconds`), prioritised/skip database and collection lists, `max_databases_to_analyse`, compression batch limits. |
| [`iam_policy.json`](iam_policy.json) | Minimum IAM policy for the execution role. |

The agent always excludes the `admin`, `local`, and `config` system databases and honors
the skip lists and size limits in `prism_config.yaml`.
