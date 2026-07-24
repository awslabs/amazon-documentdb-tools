# Reliability Pillar

<!-- Check IDs must stay in sync with: wa_checks/reliability.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates RPO/RTO requirements, backup strategy, global clusters, multi-AZ deployment, failover testing, replica lag, cursor management, and MVCC health.

## Checks

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| REL1a | Backup retention period | Evaluates backup retention days | docdb:DescribeDBClusters → BackupRetentionPeriod | ≥7 days | 3-6 days | <3 days | `aws docdb modify-db-cluster --db-cluster-identifier {id} --backup-retention-period 7` | HIGH | false |
| REL1b | AWS Backup plan | Checks if cluster is covered by an AWS Backup plan with optional cross-region copy | backup:ListBackupPlans, backup:ListBackupSelections, backup:GetBackupSelection, backup:GetBackupPlan | Plan covers cluster | No plan found | — | `aws backup create-backup-plan --backup-plan '...'` | MEDIUM | false |
| REL1c | Global cluster | Checks if cluster is part of a global cluster for cross-region DR (<1 min RTO) | docdb:DescribeGlobalClusters | Global cluster configured | — | — (info) | `aws docdb create-global-cluster --global-cluster-identifier {name} --source-db-cluster-identifier {arn}` | LOW | false |
| REL5a | Instance count | Minimum 2 instances required for automatic failover | docdb:DescribeDBInstances (count) | ≥2 instances | — | <2 instances | `aws docdb create-db-instance --db-cluster-identifier {id} --db-instance-class {type}` | CRITICAL | false |
| REL5b | Multi-AZ deployment | Instances spread across availability zones for failover | Derived from instance AvailabilityZone values | ≥2 AZs | — | Single AZ | Deploy additional instance in different AZ | CRITICAL | false |
| REL5c | Replica lag | Maximum replica lag over 7 days (healthy <20ms) | cloudwatch:GetMetricStatistics (DBClusterReplicaLagMaximum, Avg+Max, 7d, 1h) | Max <100ms | 100ms-1s | >1s | Investigate write-heavy workload or instance sizing | HIGH | false |
| REL7 | Failover events | Recent failover events in last 13 days (silent if none) | docdb:DescribeEvents (SourceType=db-cluster, 13 days) | 0 failovers (silent — no output) | 1+ failovers | — | Investigate root cause of failovers | MEDIUM | false |
| REL8 | Cursor timeouts | DatabaseCursorsTimedOut over 7 days (silent if zero) | cloudwatch:GetMetricStatistics (DatabaseCursorsTimedOut, Sum, Period=86400) | 0 timeouts (silent — no output) | Any timeouts >0 | — | Fix application cursor management (close cursors, set maxIdleTimeMS) | MEDIUM | true |
| REL9 | MVCC ID availability | Available MVCC IDs on writer instance (total capacity: 1.4 billion) | cloudwatch:GetMetricStatistics (AvailableMVCCIds, Min, 7d, 1h) | ≥50% of 1.4B (≥700M) | 25-49% (350M-700M) | <25% (<350M) | Kill long-running queries (>30 min) blocking GC; monitor LongestRunningGCProcess metric | CRITICAL | true (writer only) |

## Evaluation Notes

- REL7 and REL8 are **silent checks** — only produce findings when problems are detected. Do NOT emit a "pass" row.
- REL9 runs on the **writer instance only** — skip reader instances.
- REL5c uses cluster-level dimension (DBClusterIdentifier), not per-instance.
- REL1b requires checking multiple AWS Backup APIs in sequence: list plans → list selections → get selection details → check if cluster ARN is covered.
- Replica lag metric is in microseconds from CloudWatch — divide by 1000 for milliseconds.
