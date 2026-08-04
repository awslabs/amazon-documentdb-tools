# Operational Excellence Pillar

<!-- Check IDs must stay in sync with: wa_checks/operational_excellence.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates IaC deployment, subnet group span, profiler logging, CloudWatch alarms, custom parameter groups, maintenance windows, and engine version currency.

## Checks

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| OPS2 | Subnet group AZ span | DB subnet group spans multiple AZs for failover flexibility | docdb:DescribeDBSubnetGroups → Subnets[].SubnetAvailabilityZone | ≥3 AZs | <3 AZs | — | Add subnets in additional AZs to the subnet group | MEDIUM | false |
| OPS5a | Profiler logging | Profiler log export enabled for slow query analysis | docdb:DescribeDBClusters → EnabledCloudwatchLogsExports | "profiler" in exports list | Disabled (not in list) | — | `aws docdb modify-db-cluster --db-cluster-identifier {id} --enable-cloudwatch-logs-exports profiler` | MEDIUM | false |
| OPS5b | CloudWatch alarms | Sufficient CloudWatch alarms configured for the cluster | cloudwatch:DescribeAlarms (AlarmNamePrefix=cluster_id) | ≥3 MetricAlarms | 1-2 MetricAlarms | 0 MetricAlarms | Create alarms for CPUUtilization, FreeableMemory, DatabaseConnections at minimum | HIGH | false |
| OPS5c | Custom parameter group | Using non-default parameter group for workload tuning | docdb:DescribeDBClusters → DBClusterParameterGroup | Does NOT start with "default." | Starts with "default." | — | `aws docdb create-db-cluster-parameter-group --db-cluster-parameter-group-name {name} --db-parameter-group-family docdb5.0` then modify cluster | MEDIUM | false |
| OPS7 | Maintenance window | Maintenance window configured (informational) | docdb:DescribeDBClusters → PreferredMaintenanceWindow | — (always info) | — | — | Verify window aligns with lowest-traffic period | LOW | false |
| OPS8 | Engine version | Engine version currency — EOL versions flagged | docdb:DescribeDBClusters → EngineVersion | v8.x (info — latest) | v3.x or v4.x (EOL — end of life) | — | `aws docdb modify-db-cluster --db-cluster-identifier {id} --engine-version 5.0.0` | MEDIUM | false |

## Evaluation Notes

- OPS7 is always "info" status — it's informational only, never pass/warn/fail.
- OPS8: v5.0 shows as "info" with a note that 8.0 is available. v3.x/v4.x shows as "warn" (EOL).
- OPS5b searches alarms by prefix match on cluster_id — may find alarms for other resources if naming overlaps.
- OPS2 requires the DBSubnetGroup name from the cluster description, then a separate DescribeDBSubnetGroups call.
