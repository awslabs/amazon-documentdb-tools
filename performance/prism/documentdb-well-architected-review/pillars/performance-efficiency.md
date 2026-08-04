# Performance Efficiency Pillar

<!-- Check IDs must stay in sync with: wa_checks/performance.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates query access patterns, connection management, buffer cache efficiency, memory pressure, I/O performance, index health, and storage efficiency.

## Infrastructure Checks (per instance, from CloudWatch)

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| PERF5 | Connection utilization | Peak connections vs instance type limit over 7 days | cloudwatch:GetMetricStatistics (DatabaseConnections, Max, 7d, 1h) | <70% of CONN_LIMIT | 70-89% of CONN_LIMIT | ≥90% of CONN_LIMIT | Increase application maxPoolSize, add connection pooling, or upsize instance | HIGH | true |
| PERF6 | Buffer cache hit ratio | Average buffer cache hit ratio over 7 days | cloudwatch:GetMetricStatistics (BufferCacheHitRatio, Avg, 7d, 1h) | ≥99% | 95-98% | <95% | Upsize instance (more RAM = more cache) or reduce working set size | HIGH | true |
| PERF11 | FreeableMemory | Minimum freeable memory vs instance RAM over 7 days | cloudwatch:GetMetricStatistics (FreeableMemory, Min, 7d, 1h) | ≥10% of INSTANCE_RAM_GIB | 5-9% of INSTANCE_RAM_GIB | <5% of INSTANCE_RAM_GIB | Upsize instance or optimize queries to reduce memory pressure | HIGH | true |
| PERF12 | Swap usage | Maximum swap usage over 7 days (any swap = critical) | cloudwatch:GetMetricStatistics (SwapUsage, Max, 7d, 1h) | 0 bytes (no swap) | — | Any swap >0 | Instance critically undersized — upsize immediately | CRITICAL | true |
| PERF13 | Disk queue depth | Average disk queue depth over 7 days | cloudwatch:GetMetricStatistics (DiskQueueDepth, Avg, 7d, 1h) | ≤5 | >5 | — | Evaluate I/O-Optimized storage type or upsize instance | MEDIUM | true |

## Database-Level Checks (require analysis_data — from live DB connection)

| check_id | check_name | description | data_source | pass_condition | warn_condition | fail_condition | remediation | severity | per_instance |
|----------|-----------|-------------|-------------|----------------|----------------|----------------|-------------|----------|--------------|
| PERF8 | Index-to-data ratio | Total index size vs total data size across all collections | Sum of totalIndexSize / sum of dataSize | ≤50% | >50% | — | Review for unused/redundant indexes consuming excessive space | MEDIUM | false |
| PERF9 | Storage bloat | Collections with >30% unused storage (reclaimable space) | Per-collection unusedStorageSize.unusedPercent | No collections >30% | 1+ collections >30% unused | — | Run `db.runCommand({compact: "collection_name"})` on affected collections | MEDIUM | false |
| PERF15 | Large collections without indexes | Collections with >100K documents and no secondary indexes (only _id) | Per-collection: count >100K AND indexes.length ≤1 | All large collections have secondary indexes | 1+ flagged | — | Create indexes following ESR (Equality-Sort-Range) rule | HIGH | false |
| PERF1b | Redundant indexes | Indexes that are prefix subsets of other compound indexes | Per-collection: index A is prefix of index B | None found | 1+ redundant pairs | — | Drop the shorter prefix index (it's covered by the compound) after stakeholder verification | MEDIUM | false |

## Evaluation Notes

- PERF5 requires looking up the instance type in CONN_LIMITS to get the denominator.
- PERF11 requires looking up the instance type in INSTANCE_RAM_GIB to calculate the percentage.
- PERF12 has no "warn" state — any swap at all is a critical failure (instance is thrashing).
- Database-level checks (PERF8, PERF9, PERF15, PERF1b) require a live database connection to gather collection stats. In standalone mode (Mode B), these can be skipped with an "info" note, or the agent can connect via pymongo if credentials are available.
- For PERF5, if the instance type is not in CONN_LIMITS, emit "info" with the raw connection count.
