# Cost Optimization Pillar

<!-- Check IDs must stay in sync with: wa_checks/cost_optimization.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates instance right-sizing (CPU utilization), storage type selection, idle reader detection, unused index overhead, and cost allocation tagging.

## Infrastructure Checks

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| COST1 | CPU utilization | CPU P95 utilization indicates potential oversizing (7d) | cloudwatch:GetMetricStatistics (CPUUtilization, Avg, 7d, 1h) — compute P95 from sorted datapoints | P95 ≥10% | P95 <10% (likely oversized) | — | `aws docdb modify-db-instance --db-instance-identifier {id} --db-instance-class {smaller_type}` | MEDIUM | true |
| COST6 | Cost allocation tags | Cluster has cost allocation tags for expense tracking | docdb:ListTagsForResource (ResourceName=DBClusterArn) | ≥2 tags in TagList | <2 tags | — | `aws docdb add-tags-to-resource --resource-name {arn} --tags Key=Environment,Value=prod Key=Team,Value=platform` | LOW | false |
| COST7 | Storage type | Current storage type (informational — evaluate I/O-Optimized) | docdb:DescribeDBClusters → StorageType | — (always info) | — | — | Evaluate I/O-Optimized (`iopt1`) if I/O costs exceed 25% of total cluster bill | LOW | false |
| COST9 | Idle reader detection | Reader instances with minimal activity over 7 days | cloudwatch:GetMetricStatistics (DatabaseConnections Avg + ReadIOPS Avg, 7d, 1h) | Active: avg connections ≥2 OR avg ReadIOPS ≥5 | Idle: avg connections <2 AND avg ReadIOPS <5 | — | `aws docdb delete-db-instance --db-instance-identifier {reader_id}` (after confirming no scheduled workloads) | MEDIUM | true (reader only) |

## Database-Level Checks (require analysis_data)

| check_id | check_name | description | data_source | pass_condition | warn_condition | fail_condition | remediation | severity | per_instance |
|----------|-----------|-------------|-------------|----------------|----------------|----------------|-------------|----------|--------------|
| COST3 | Unused indexes | Indexes with zero operations in $indexStats (potential_unused flag) | Per-collection index analysis | None with potential_unused=true | 1+ unused indexes | — | `db.collection.dropIndex("index_name")` — verify with stakeholders first, check for monthly/quarterly report queries | MEDIUM | false |

## Evaluation Notes

- COST1 computes P95 from sorted Average datapoints: `sorted(dps)[int(len(dps) * 0.95)]`. A P95 <10% means the instance is idle 95% of the time — strong signal for downsizing.
- COST7 is always "info" — it's a prompt to evaluate, not a pass/fail check.
- COST9 runs on **reader instances only** — skip the writer. Both conditions (connections AND IOPS) must be low to flag as idle.
- COST3 requires live database analysis data. In standalone mode, skip with "info" note.
- When recommending downsizing (COST1), suggest the next smaller instance in the same family (e.g., r6g.2xlarge → r6g.xlarge). Never recommend t-class for production workloads.
