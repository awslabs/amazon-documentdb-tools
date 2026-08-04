# Security Pillar

<!-- Check IDs must stay in sync with: wa_checks/security.py -->
<!-- IDs are stable — use exactly what @register_check defines -->

## Pillar Definition

Evaluates encryption at rest/in-transit, TLS version, network access (VPC/SGs), Secrets Manager usage, audit logging, and deletion protection.

## Checks

| check_id | check_name | description | aws_api | pass_condition | warn_condition | fail_condition | remediation_cli | severity | per_instance |
|----------|-----------|-------------|---------|----------------|----------------|----------------|-----------------|----------|--------------|
| SEC1a | Encryption at rest | Storage encryption enabled (KMS) | docdb:DescribeDBClusters → StorageEncrypted | Enabled (true) | — | Disabled (false) | Cannot enable on existing cluster — requires creating new cluster with encryption | CRITICAL | false |
| SEC1b | TLS enabled | TLS parameter in cluster parameter group | docdb:DescribeDBClusterParameters → ParameterName="tls" | ParameterValue="enabled" | — | Not "enabled" | `aws docdb modify-db-cluster-parameter-group --db-cluster-parameter-group-name {pg} --parameters ParameterName=tls,ParameterValue=enabled,ApplyMethod=pending-reboot` | CRITICAL | false |
| SEC2 | Security groups | VPC security groups not open to internet (0.0.0.0/0) | ec2:DescribeSecurityGroups for each VpcSecurityGroupId | All SGs have no 0.0.0.0/0 or ::/0 inbound rules | — | Any SG allows 0.0.0.0/0 or ::/0 | `aws ec2 revoke-security-group-ingress --group-id {sg} --protocol tcp --port 27017 --cidr 0.0.0.0/0` | CRITICAL | false |
| SEC3 | Secrets Manager usage | Cluster credentials stored in Secrets Manager (search by cluster_id or endpoint in secret name/description) | secretsmanager:ListSecrets (paginated) | Cluster ID or endpoint found in a secret's Name or Description | Not found | — | `aws secretsmanager create-secret --name docdb/{cluster_id} --secret-string '{"username":"...","password":"..."}'` | MEDIUM | false |
| SEC5 | Audit logging | Audit log export to CloudWatch enabled | docdb:DescribeDBClusters → EnabledCloudwatchLogsExports | "audit" in exports list | — | — (info — not a failure) | `aws docdb modify-db-cluster --db-cluster-identifier {id} --enable-cloudwatch-logs-exports audit` | LOW | false |
| SEC6 | TLS minimum version | TLS version parameter restricts to 1.2+ (no TLS 1.0/1.1) | docdb:DescribeDBClusterParameters → ParameterName="tls_version" | Contains "1.2" AND does NOT contain "1.0" or "1.1" | Older protocols allowed | — | `aws docdb modify-db-cluster-parameter-group --parameters ParameterName=tls_version,ParameterValue=TLSv1.2,ApplyMethod=pending-reboot` | MEDIUM | false |
| SEC8 | Deletion protection | Cluster deletion protection enabled | docdb:DescribeDBClusters → DeletionProtection | true | — | false | `aws docdb modify-db-cluster --db-cluster-identifier {id} --deletion-protection` | HIGH | false |

## Evaluation Notes

- SEC6 is **silent** if tls_version parameter cannot be determined (returns empty list).
- SEC2 checks ALL VpcSecurityGroups attached to the cluster — emit one finding per offending SG.
- SEC3 paginates through all secrets — search is case-insensitive substring match.
- SEC5 is informational ("info" status) when disabled — it's optional for compliance, not a security failure.
- SEC1a cannot be remediated on an existing cluster — encryption must be set at creation time.
