---
name: documentdb-well-architected-review
display_name: DocumentDB Well-Architected Review
description: "Performs a comprehensive Well-Architected review of Amazon DocumentDB clusters. Evaluates cluster configuration against 6 pillars (Reliability, Security, Operational Excellence, Performance Efficiency, Cost Optimization, Sustainability), generates findings with remediation guidance, and produces a universal JSON export + interactive HTML dashboard."
icon: "🏗️"
trigger: I need to do a DocumentDB well-architected review
inputs:
  - name: cluster_identifier
    description: "The DocumentDB cluster identifier to review"
    type: string
    required: true
  - name: aws_region
    description: "AWS region where the cluster resides"
    type: string
    required: true
    default: us-east-1
depends-on:
  - aws-api
tools:
  - call_aws
  - run_python
  - file_write
---

## 1. Overview

Performs a comprehensive Well-Architected review of an Amazon DocumentDB cluster across 6 AWS Well-Architected pillars: Reliability, Security, Operational Excellence, Performance Efficiency, Cost Optimization, and Sustainability. Evaluates **38 automated checks** against live cluster configuration, CloudWatch metrics, and security posture. Produces a health score, prioritized findings with remediation CLI commands, Bedrock-generated pillar recommendations, a Universal WAR Export JSON file, an interactive HTML dashboard, and (inside Prism) a PDF export.

This is a **sub-skill** of the Prism platform (see the top-level `prism-dash`
skill). It owns cluster assessment and pillar scoring. Conversational advice and
per-query tuning are owned by the `documentdb-advisor` skill — defer chat/advisory
questions there.

### Two execution modes

1. **Prism Dash plugin execution** — the live app runs the checks via the
   `wa_checks/` plugin registry (Section 4a) and renders results in the UI, with
   Bedrock recommendations and PDF export. This is the primary mode.
2. **Standalone manual execution** — an agent with AWS CLI access can perform the
   review directly using the 13-step workflow in Section 3 and the check catalog in
   Section 4. Both modes share the **same check IDs** so findings are traceable.

## 2. DocumentDB Engine Context

The reviewing agent MUST know these facts:

- DocumentDB is NOT MongoDB — API-compatible but purpose-built storage engine with B-tree indexes
- Only 2/3 of instance RAM is available for buffer cache (1/3 reserved by DocumentDB)
- Connection limits by instance type: db.t3/t4g.medium=1000, db.r5/r6g.large=3400, db.r5/r6g.xlarge=7000, db.r5/r6g.2xlarge=14200, db.r5/r6g.4xlarge=28400, db.r5/r6g.8xlarge+=60000
- Compression: LZ4 available all versions; ZSTD available 8.0+ only; NOT enabled by default on 5.0 (must set default_compression parameter)
- IAM authentication: available on 5.0+ instance-based clusters only; primary user always uses password auth
- Query Planner: v1 (3.6/4.0), v2 (5.0 — up to 10x improvement), v3 (8.0)
- retryWrites must be false in connection strings
- I/O-Optimized storage: no per-operation I/O charges; beneficial when I/O cost exceeds ~25% of total cluster bill
- Graviton instances (r6g, r7g, r8g, t4g, r6gd): better price-performance; r8g is latest generation (Graviton4, engine 5.0/8.0 only)
- 6-way replication across 3 AZs regardless of instance count
- Automatic failover requires minimum 2 instances; typical RTO ~30 seconds with multi-AZ
- Global clusters: cross-region RPO less than 1 second, RTO ~1 minute
- Backup: continuous to S3, point-in-time recovery to any second within retention window
- Index best practices: <=5 indexes per collection, compound indexes <=3 fields, follow ESR (Equality-Sort-Range) rule
- Long-running queries (>30 min) block MVCC garbage collection causing cascading bloat and CPU pressure
- MVCC total capacity: 1.4 billion IDs

## 3. Workflow

### Step 1: Discover cluster configuration
- call_aws: `aws docdb describe-db-clusters --db-cluster-identifier {cluster_identifier} --region {aws_region}`
- Extract: EngineVersion, BackupRetentionPeriod, DeletionProtection, StorageEncrypted, StorageType, DBClusterParameterGroup, DBSubnetGroup, VpcSecurityGroups, EnabledCloudwatchLogsExports, DBClusterMembers, DBClusterArn, Endpoint

### Step 2: Fetch instance details
- call_aws: `aws docdb describe-db-instances --filters Name=db-cluster-id,Values={cluster_identifier} --region {aws_region}`
- Extract: DBInstanceIdentifier, DBInstanceClass, AvailabilityZone, IsClusterWriter (from DBClusterMembers)

### Step 3: Fetch parameter group settings
- call_aws: `aws docdb describe-db-cluster-parameters --db-cluster-parameter-group-name {parameter_group_name} --region {aws_region}`
- Extract: tls (enabled/disabled), tls_version

### Step 4: Fetch security groups
- For each VpcSecurityGroupId from Step 1:
- call_aws: `aws ec2 describe-security-groups --group-ids {sg_id} --region {aws_region}`
- Check: IpPermissions for 0.0.0.0/0 or ::/0 CidrIp rules

### Step 5: Fetch CloudWatch metrics (7-day window, 1-hour period)
- For each instance from Step 2, fetch these metrics with Dimensions=[{Name=DBInstanceIdentifier, Value={instance_id}}]:
  - CPUUtilization (Statistics: Average)
  - FreeableMemory (Statistics: Minimum)
  - BufferCacheHitRatio (Statistics: Average)
  - DatabaseConnections (Statistics: Maximum)
  - SwapUsage (Statistics: Maximum)
  - DiskQueueDepth (Statistics: Average)
  - IndexBufferCacheHitRatio (Statistics: Average)
  - DatabaseCursorsTimedOut (Statistics: Sum, Period: 86400)
  - AvailableMVCCIds (Statistics: Minimum) — writer instance only
  - ReadIOPS (Statistics: Average) — reader instances only
- For cluster-level: DBClusterReplicaLagMaximum with Dimensions=[{Name=DBClusterIdentifier, Value={cluster_identifier}}]

### Step 6: Fetch events (last 13 days)
- call_aws: `aws docdb describe-events --source-identifier {cluster_identifier} --source-type db-cluster --start-time {13_days_ago} --region {aws_region}`
- Filter for: "failover" in Message or EventCategories

### Step 7: Check Secrets Manager
- call_aws: `aws secretsmanager list-secrets --region {aws_region}`
- Search: cluster_identifier or cluster endpoint in secret Name or Description

### Step 8: Check CloudWatch alarms
- call_aws: `aws cloudwatch describe-alarms --alarm-name-prefix {cluster_identifier} --region {aws_region}`
- Count: MetricAlarms

### Step 9: Evaluate all checks against collected data
- Apply each check from the Check Catalog (Section 4) against the data collected in Steps 1-8
- For per-instance checks: iterate over each instance, identify writer vs reader role

### Step 10: Score results
- health_score = pass_count / (pass_count + warn_count + fail_count) * 100
- Exclude "info" status from scoring
- Color: >=80% green, 60-79% warning, <60% red

### Step 11: Generate Universal WAR Export JSON
- Use the schema defined in Section 6
- Write to file: `war_export_{cluster_identifier}_{timestamp}.json`

### Step 12: Generate interactive HTML dashboard
- Self-contained HTML with embedded CSS
- 6-pillar grid layout with pillar colors
- Health score header bar
- Per-pillar panels grouped by status (fail first, then warn, then pass)
- Write to file: `war_dashboard_{cluster_identifier}_{timestamp}.html`

### Step 13: Present findings summary
- Report health score
- List all CRITICAL and HIGH severity findings
- Provide top 3 remediation actions

## 4. Check Catalog

**38 checks across 6 pillars.** Of these, **37 are registered in the `wa_checks/`
plugin registry** via the `@register_check` decorator; **PERF1b** is contributed by
the Prism database-level path (`tabs/well_architected._run_db_checks`) and merged
into the same result set. All check IDs are **stable** — use exactly what the code
defines (e.g. `REL1a` not `REL-1`, `PERF1b` not `PERF-1b`).

The complete catalog is broken down by pillar in the `pillars/` directory. Each file
contains the full check table with IDs, thresholds, API calls, and remediation commands.

- **[pillars/reliability.md](pillars/reliability.md)** — 9 checks: REL1a, REL1b, REL1c, REL5a, REL5b, REL5c, REL7, REL8, REL9
- **[pillars/security.md](pillars/security.md)** — 7 checks: SEC1a, SEC1b, SEC2, SEC3, SEC5, SEC6, SEC8
- **[pillars/operational-excellence.md](pillars/operational-excellence.md)** — 6 checks: OPS2, OPS5a, OPS5b, OPS5c, OPS7, OPS8
- **[pillars/performance-efficiency.md](pillars/performance-efficiency.md)** — 9 checks: PERF5, PERF6, PERF8, PERF9, PERF11, PERF12, PERF13, PERF15, PERF1b
- **[pillars/cost-optimization.md](pillars/cost-optimization.md)** — 5 checks: COST1, COST3, COST6, COST7, COST9
- **[pillars/sustainability.md](pillars/sustainability.md)** — 2 checks: SUST1, SUST2

Read each pillar file for the detailed evaluation logic.

> Note: the Prism UI may surface *additional* exploratory checks beyond this
> canonical catalog (e.g. `PERF1`, `PERF1c`, `PERF10`, `PERF16`, `COST4`, plus
> `tabs/wa_v2/` checks) from the legacy/next-gen paths. Those are app extras; the
> **38-check catalog above is the source of truth** for exports and scoring.

### 4a. The `wa_checks/` Plugin Registry (`wa_checks/registry.py`)

In Prism, checks self-register with a decorator and are executed by `run_checks()`.
This is the authoritative implementation of the catalog.

**Registration** — `@register_check(check_id, pillar, label, source=..., ...)`:

```python
@register_check("REL1a", "Reliability", "Backup retention period",
                source="infrastructure", priority=10)
def check_backup_retention(ctx: CheckContext) -> list[dict]:
    retention = ctx.cluster.get("BackupRetentionPeriod", 1)
    return [{"pillar": "Reliability", "id": "REL1a",
             "label": f"Backup retention period ({retention} days)",
             "status": "pass" if retention >= 7 else "warn" if retention >= 3 else "fail",
             "detail": ""}]
```

**`CheckDefinition`** fields: `check_id`, `pillar`, `label`, `description`, `source`
(`"infrastructure" | "database" | "cloudwatch"`), `func`, `per_instance`,
`writer_only`, `reader_only`, `requires_analysis`, `priority`.

**`CheckContext`** fields passed to every check: `cluster_id`, `region`, `cluster`
(describe_db_clusters dict), `instances` (describe_db_instances list), `analysis_data`,
`conn_str`, shared boto3 clients `docdb_client` / `cw_client` / `ec2_client`, and the
per-instance fields `current_instance` and `is_writer`.

**`run_checks(...)` execution model:**
1. Determines writer instance IDs from `cluster["DBClusterMembers"]` where
   `IsClusterWriter` is true.
2. Runs all **non-per-instance** checks first, pillar by pillar (in fixed pillar order),
   sorted by `priority`. A check with `requires_analysis=True` and no `analysis_data`
   emits an `info` row ("requires Analyze") instead of running.
3. Then iterates each instance, setting `ctx.current_instance` / `ctx.is_writer`, and
   runs **per-instance** checks, honoring `writer_only` (e.g. REL9 MVCC) and
   `reader_only` (e.g. COST9 idle readers).
4. A check that raises is caught and recorded as a `warn` row rather than aborting the
   run.

Returns a flat list of result dicts: `{"pillar", "id", "label", "status", "detail"}`
with `status ∈ {pass, warn, fail, info}`.

`tabs/well_architected._run_wa_checks` calls `run_checks()` (plugin path), then appends
the database-level checks from `_run_db_checks(analysis_data)` (which contributes
`PERF1b` plus app extras), then triggers AI recommendations (Section 4b). If the plugin
import fails, it falls back to a legacy inline path producing the same IDs.

### 4b. Bedrock Recommendation Generation

After checks run, `_generate_ai_recommendations(check_results, cluster_id, region,
analysis_data)` sends the failing/warning checks to Bedrock using the system prompt in
**`wa-advisor-prompt.md`** (loaded via `_load_wa_advisor_prompt()`). Models:
`us.anthropic.claude-sonnet-4-20250514-v1:0` (primary) →
`us.anthropic.claude-haiku-4-5-20251001-v1:0` (fallback). The prompt returns **JSON
keyed by pillar**, each entry `{check_id, action, why, impact, priority}` where
`priority ∈ {Critical, High, Medium, Low}`. Only pillars with failing/warning checks
appear; the model must not invent checks not present in the input. Recommendations are
generated asynchronously and stored in `_wa["ai_md"]`.
## 5. Reference Constants

### CONN_LIMITS (instance type → max connections)
```
db.t3.medium: 1000      db.t4g.medium: 1000
db.r5.large: 3400       db.r6g.large: 3400       db.r6gd.large: 3400       db.r8g.large: 3400
db.r5.xlarge: 7000      db.r6g.xlarge: 7000      db.r6gd.xlarge: 7000      db.r8g.xlarge: 7000
db.r5.2xlarge: 14200    db.r6g.2xlarge: 14200    db.r6gd.2xlarge: 14200    db.r8g.2xlarge: 14200
db.r5.4xlarge: 28400    db.r6g.4xlarge: 28400    db.r6gd.4xlarge: 28400    db.r8g.4xlarge: 28400
db.r5.8xlarge: 60000    db.r6g.8xlarge: 60000    db.r6gd.8xlarge: 60000    db.r8g.8xlarge: 60000
db.r5.12xlarge: 60000   db.r6g.12xlarge: 60000   db.r6gd.12xlarge: 60000   db.r8g.12xlarge: 60000
db.r5.16xlarge: 60000   db.r6g.16xlarge: 60000   db.r6gd.16xlarge: 60000   db.r8g.16xlarge: 60000
db.r5.24xlarge: 60000
```

### INSTANCE_RAM_GIB (instance type → RAM in GiB)
```
db.t3.medium: 4         db.t4g.medium: 4
db.r5.large: 16         db.r6g.large: 16         db.r6gd.large: 16         db.r8g.large: 16
db.r5.xlarge: 32        db.r6g.xlarge: 32        db.r6gd.xlarge: 32        db.r8g.xlarge: 32
db.r5.2xlarge: 64       db.r6g.2xlarge: 64       db.r6gd.2xlarge: 64       db.r8g.2xlarge: 64
db.r5.4xlarge: 128      db.r6g.4xlarge: 128      db.r6gd.4xlarge: 128      db.r8g.4xlarge: 128
db.r5.8xlarge: 256      db.r6g.8xlarge: 256      db.r6gd.8xlarge: 256      db.r8g.8xlarge: 256
db.r5.12xlarge: 384     db.r6g.12xlarge: 384     db.r6gd.12xlarge: 384     db.r8g.12xlarge: 384
db.r5.16xlarge: 512     db.r6g.16xlarge: 512     db.r6gd.16xlarge: 512     db.r8g.16xlarge: 512
db.r5.24xlarge: 768
```

### GRAVITON_FAMILIES
```
r6g, r7g, r8g, t4g, r6gd
```

### Other Constants
- MVCC total capacity: 1,400,000,000 IDs
- Engine version classification: deprecated (3.x, 4.x), current (5.0), latest (8.0)
- CloudWatch lookback: 7 days for all metrics, 13 days for failover events
- CloudWatch period: 3600 seconds (1 hour) for all metrics except DatabaseCursorsTimedOut (86400 = daily)

## 6. Universal WAR Export Schema

```json
{
  "schema_version": "1.0",
  "metadata": {
    "service": "documentdb",
    "cluster_id": "",
    "account_id": "",
    "region": "",
    "engine_version": "",
    "timestamp": "",
    "skill_name": "documentdb-well-architected-review",
    "checks_evaluated": 0,
    "instances_reviewed": 0
  },
  "scoring": {
    "overall_health_score": 0,
    "by_status": {"pass": 0, "warn": 0, "fail": 0, "info": 0},
    "by_pillar": {}
  },
  "findings": [
    {
      "finding_id": "<unique UUID>",
      "check_id": "REL1a",
      "pillar": "reliability",
      "check_name": "Backup retention period",
      "status": "pass|warn|fail|info",
      "severity": "CRITICAL|HIGH|MEDIUM|LOW",
      "detail": "Human-readable finding with actual values",
      "instance_id": null,
      "remediation_cli": "aws docdb modify-db-cluster ...",
      "recommendations": []
    }
  ]
}
```

Note: check_id uses the SAME IDs as the Dash app (REL1a, SEC2, PERF5, etc.). The pillar field uses lowercase with underscores (reliability, security, operational_excellence, performance_efficiency, cost_optimization, sustainability).

## 7. Dashboard Specification

- Self-contained HTML file with embedded CSS (no external dependencies except CDN-loaded Plotly)
- Dark theme with AWS-inspired color palette
- Header: cluster_id, region, engine version, timestamp, health score (large, colored)
- 6-pillar grid (2 columns x 3 rows)
- Pillar colors: Reliability=#d45b07, Security=#dd344c, Operational Excellence=#5e6b7a, Performance Efficiency=#8c4fff, Cost Optimization=#067f68, Sustainability=#0972d3
- Each pillar panel: header with icon + name + status counts, body grouped by status (fail first with red left border, warn with orange, pass with green)
- Each finding row: status symbol + check_id (monospace) + label + detail (muted)
- Responsive layout (collapses to single column on mobile)

## 8. Priority Criteria (shared Prism severity schema)

These four levels are used consistently across all Prism skills. The Bedrock
recommendation output (Section 4b) uses these exact `priority` values.

- **Critical**: Data loss risk, security breach risk, cluster unavailability (e.g. deletion protection off, encryption disabled, single instance with no failover, MVCC IDs <25%)
- **High**: Significant performance degradation or cost waste actively occurring (e.g. buffer cache <95%, no CloudWatch alarms, swap usage, TLS disabled)
- **Medium**: Best practice gap with moderate risk (e.g. missing Secrets Manager, non-Graviton instances, no profiler logging, oversized instances)
- **Low**: Optimization opportunity with low urgency (e.g. compression not enabled, cost allocation tags missing, engine upgrade available, storage type informational)

Separately, each check **row** carries a `status` of `pass | warn | fail | info`. The
health score (Section 3, Step 10) excludes `info` rows from the denominator.

## 9. Lessons Learned / Anti-Patterns

- Always check engine version before recommending ZSTD compression (8.0+ only; use LZ4 for 5.0)
- Connection limit checks must match the EXACT instance type (db.r6g.large ≠ db.r6g.xlarge)
- BufferCacheHitRatio below 99% does NOT automatically mean the instance is undersized — check if the working set genuinely exceeds available cache
- NEVER recommend dropping the _id index
- Per-instance checks MUST identify writer vs reader roles. In the registry this is the `writer_only` / `reader_only` flags on `@register_check`, resolved from `DBClusterMembers.IsClusterWriter` (REL9/MVCC is `writer_only`, COST9/idle is `reader_only`)
- Use EXACT check IDs from the code — never invent new numbering (REL1a not REL-1, PERF1b not PERF-1b)
- Silent checks (REL7, REL8) should only produce findings when problems are detected — do not emit a "pass" row
- FreeableMemory is compared against INSTANCE_RAM_GIB lookup — not a fixed threshold
- The health score formula excludes "info" status findings from the denominator
- `requires_analysis=True` checks (COST3, PERF8, PERF9, PERF15, SUST2) emit an `info` "requires Analyze" row when no `analysis_data` is present — they do not fail
- PERF1b is not a registry check — it comes from the database-level path; keep it in the catalog but don't expect a `@register_check("PERF1b", ...)` decorator

## 10. Relation to Prism Dash App

This skill is executed by the Prism Dash app through the `wa_checks/` plugin
registry (Section 4a), which is the authoritative implementation of the check catalog.
The app adds capabilities on top of the raw checks:

- **Live database-level analysis** (index health, compression, bloat) feeds
  `analysis_data`, enabling `requires_analysis` checks (COST3, PERF8, PERF9, PERF15,
  SUST2) and the `PERF1b` redundant-index check.
- **AI recommendations** via Amazon Bedrock and `wa-advisor-prompt.md` (Section 4b).
- **Historical trend tracking** via `agent_memory` (`wa_results.json` + `.v1`/`.v2`
  versions, compared by `load_wa_results_previous`).
- **PDF export** via `wa_pdf.generate_wa_pdf(results, ai_data, cluster_id, region)` —
  a cover page with health score, per-pillar sections grouped by status, and an AI
  recommendations section.
- Optional **Universal WAR Export** (Section 6) for cross-tool interoperability.

The autonomous agent (top-level `prism-dash` skill) runs this review as its
`well_architected` module. Both the standalone skill and the Dash app share the same
check IDs (REL1a, SEC1b, PERF5, …) so findings are traceable across execution modes.

<!-- Check catalog must stay in sync with: wa_checks/*.py implementations -->
<!-- IDs are stable — use exactly what @register_check defines; PERF1b comes from tabs/well_architected._run_db_checks -->

Consult the `references/` directory for detailed DocumentDB-specific knowledge when generating recommendations. (These are the same reference docs used by the `documentdb-advisor` skill; do not duplicate their content here.)

## 11. Required IAM Permissions

```json
{
  "Effect": "Allow",
  "Action": [
    "rds:DescribeDBClusters",
    "rds:DescribeDBInstances",
    "rds:DescribeDBClusterParameters",
    "rds:DescribeDBSubnetGroups",
    "rds:DescribeEvents",
    "rds:ListTagsForResource",
    "ec2:DescribeSecurityGroups",
    "cloudwatch:GetMetricStatistics",
    "cloudwatch:DescribeAlarms",
    "secretsmanager:ListSecrets",
    "backup:ListBackupPlans",
    "backup:ListBackupSelections",
    "backup:GetBackupSelection",
    "backup:GetBackupPlan"
  ],
  "Resource": "*"
}
```

Note: DocumentDB uses the `rds` namespace for API calls (not a separate `docdb` namespace in IAM).
