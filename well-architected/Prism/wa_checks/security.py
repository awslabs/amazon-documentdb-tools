"""WA Checks — Security Pillar.

Check IDs and thresholds must stay in sync with:
  documentdb-well-architected-review/SKILL.md § Check Catalog → Security
IDs are stable — do not rename (SEC1a, SEC1b, SEC2, SEC3, SEC5, SEC6, SEC8).
"""
import logging
from wa_checks.registry import register_check

logger = logging.getLogger(__name__)


@register_check("SEC1a", "Security", "Encryption at rest",
                source="infrastructure", priority=10)
def check_encryption(ctx):
    encrypted = ctx.cluster.get("StorageEncrypted", False)
    return [{"pillar": "Security", "id": "SEC1a",
             "label": f"Encryption at rest ({'enabled' if encrypted else 'disabled'})",
             "status": "pass" if encrypted else "fail",
             "detail": "" if encrypted else "Enable encryption at rest (requires new cluster)"}]


@register_check("SEC1b", "Security", "TLS enabled",
                source="infrastructure", priority=11)
def check_tls(ctx):
    tls_val = "unknown"
    try:
        pg_name = ctx.cluster.get("DBClusterParameterGroup", "")
        if pg_name:
            params = ctx.docdb_client.describe_db_cluster_parameters(
                DBClusterParameterGroupName=pg_name)
            for p in params.get("Parameters", []):
                if p.get("ParameterName") == "tls":
                    tls_val = p.get("ParameterValue", "enabled")
                    break
    except Exception as e:
        tls_val = f"check failed: {e}"
    return [{"pillar": "Security", "id": "SEC1b",
             "label": f"TLS ({tls_val})",
             "status": "pass" if tls_val == "enabled" else "fail",
             "detail": "" if tls_val == "enabled" else "TLS should be enabled"}]


@register_check("SEC2a", "Security", "Security groups",
                source="infrastructure", priority=20)
def check_security_groups(ctx):
    vpc_sgs = ctx.cluster.get("VpcSecurityGroups", [])
    results = []
    sg_open = False
    sg_checked = 0

    for vsg in vpc_sgs:
        sg_id = vsg.get("VpcSecurityGroupId", "")
        if not sg_id:
            continue
        try:
            sg_detail = ctx.ec2_client.describe_security_groups(
                GroupIds=[sg_id])["SecurityGroups"][0]
            sg_checked += 1
            for rule in sg_detail.get("IpPermissions", []):
                for ip_range in rule.get("IpRanges", []):
                    if ip_range.get("CidrIp") in ("0.0.0.0/0", "::/0"):
                        sg_open = True
                        results.append({"pillar": "Security", "id": "SEC2a",
                                       "label": f"Security group {sg_id} open to 0.0.0.0/0",
                                       "status": "fail",
                                       "detail": "Restrict to specific CIDR ranges"})
                        break
        except Exception as e:
            results.append({"pillar": "Security", "id": "SEC2a",
                           "label": f"Cannot check SG {sg_id}: {e}",
                           "status": "warn", "detail": ""})

    if not sg_open and sg_checked > 0:
        results.append({"pillar": "Security", "id": "SEC2a",
                       "label": f"Security groups properly restricted ({sg_checked} checked)",
                       "status": "pass", "detail": ""})
    return results


@register_check("SEC3", "Security", "Secrets Manager usage",
                source="infrastructure", priority=30)
def check_secrets_manager(ctx):
    import boto3
    cluster_endpoint = ctx.cluster.get("Endpoint", "")
    try:
        sm = boto3.client("secretsmanager", region_name=ctx.region)
        found = False
        for page in sm.get_paginator("list_secrets").paginate():
            for s in page.get("SecretList", []):
                name_lower = (s.get("Name", "") or "").lower()
                desc_lower = (s.get("Description", "") or "").lower()
                if (ctx.cluster_id.lower() in name_lower
                        or ctx.cluster_id.lower() in desc_lower
                        or cluster_endpoint.lower() in desc_lower):
                    found = True
                    break
            if found:
                break
        return [{"pillar": "Security", "id": "SEC3",
                 "label": f"Secrets Manager {'references' if found else 'does not reference'} this cluster",
                 "status": "pass" if found else "warn",
                 "detail": "" if found else "Store credentials in Secrets Manager"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC3",
                 "label": f"Cannot check Secrets Manager: {e}",
                 "status": "warn", "detail": ""}]


@register_check("SEC5", "Security", "Audit logging",
                source="infrastructure", priority=40)
def check_audit_logging(ctx):
    logs = ctx.cluster.get("EnabledCloudwatchLogsExports", [])
    enabled = "audit" in logs
    return [{"pillar": "Security", "id": "SEC5",
             "label": f"Audit logging ({'enabled' if enabled else 'disabled'})",
             "status": "pass" if enabled else "info",
             "detail": "" if enabled else "Optional — enable for compliance or access tracking"}]


@register_check("SEC1d", "Security", "TLS minimum version",
                source="infrastructure", priority=12)
def check_tls_version(ctx):
    tls_version = "unknown"
    try:
        pg_name = ctx.cluster.get("DBClusterParameterGroup", "")
        if pg_name:
            params = ctx.docdb_client.describe_db_cluster_parameters(
                DBClusterParameterGroupName=pg_name)
            for p in params.get("Parameters", []):
                if p.get("ParameterName") == "tls_version":
                    tls_version = p.get("ParameterValue", "unknown")
                    break
    except Exception:
        pass

    if tls_version == "unknown":
        return []  # Can't determine, skip silently

    is_tls12 = "1.2" in tls_version and "1.0" not in tls_version and "1.1" not in tls_version
    return [{"pillar": "Security", "id": "SEC1d",
             "label": f"TLS minimum version: {tls_version}",
             "status": "pass" if is_tls12 else "warn",
             "detail": "" if is_tls12 else "Set tls_version to TLSv1.2 to disable older protocols"}]


@register_check("SEC8a", "Security", "Deletion protection",
                source="infrastructure", priority=50)
def check_deletion_protection(ctx):
    enabled = ctx.cluster.get("DeletionProtection", False)
    return [{"pillar": "Security", "id": "SEC8a",
             "label": f"Deletion protection ({'enabled' if enabled else 'disabled'})",
             "status": "pass" if enabled else "fail",
             "detail": "" if enabled else "Enable deletion protection for production clusters"}]


# ── Migrated from tabs/wa_v2/security.py (Phase 3B) ──────────────────────────

@register_check("SEC1c", "Security", "Client-side field level encryption",
                source="infrastructure", priority=12)
def check_fle(ctx):
    """SEC 1 — Client-side FLE info note."""
    return [{"pillar": "Security", "id": "SEC1c",
             "label": "Client-side field level encryption (FLE) — app-side",
             "status": "info",
             "detail": "Evaluate FLE for PII, PHI, or financial data. Encrypts sensitive fields "
                       "in the client before transfer. Not detectable via AWS API."}]


@register_check("SEC3a", "Security", "IAM database authentication",
                source="infrastructure", priority=31)
def check_iam_auth(ctx):
    """SEC 3 — IAM database authentication."""
    iam = ctx.cluster.get("IAMDatabaseAuthenticationEnabled", False)
    return [{"pillar": "Security", "id": "SEC3a",
             "label": f"IAM database authentication ({'enabled' if iam else 'not enabled'})",
             "status": "pass" if iam else "warn",
             "detail": "" if iam else "Enable IAM auth to eliminate long-lived static passwords"}]


@register_check("SEC4b", "Security", "Secrets Manager rotation",
                source="infrastructure", priority=35)
def check_secrets_rotation(ctx):
    """SEC 4 — Secrets Manager automatic rotation."""
    import boto3
    try:
        sm = boto3.client("secretsmanager", region_name=ctx.region)
        rotation_enabled = False
        for page in sm.get_paginator("list_secrets").paginate():
            for s in page.get("SecretList", []):
                if (ctx.cluster_id.lower() in (s.get("Name", "") or "").lower() or
                        ctx.cluster_id.lower() in (s.get("Description", "") or "").lower()):
                    detail_s = sm.describe_secret(SecretId=s["ARN"])
                    rotation_enabled = detail_s.get("RotationEnabled", False)
                    break
            if rotation_enabled:
                break
        return [{"pillar": "Security", "id": "SEC4b",
                 "label": f"Secrets Manager rotation {'enabled' if rotation_enabled else 'not enabled'}",
                 "status": "pass" if rotation_enabled else "warn",
                 "detail": "" if rotation_enabled else
                 "Enable automatic rotation — DocumentDB supports zero-downtime rotation"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC4b",
                 "label": f"Cannot check rotation: {e}", "status": "warn", "detail": str(e)}]


@register_check("SEC5a", "Security", "IAM resource-level permissions",
                source="infrastructure", priority=41)
def check_iam_permissions(ctx):
    """SEC 5 — IAM resource-level permissions info note."""
    return [{"pillar": "Security", "id": "SEC5a",
             "label": "IAM resource-level permissions — review recommended",
             "status": "info",
             "detail": "Scope IAM policies to specific cluster ARNs. "
                       "Use IAM Access Analyzer to identify overly permissive policies."}]


@register_check("SEC6b", "Security", "CloudTrail enabled",
                source="infrastructure", priority=42)
def check_cloudtrail(ctx):
    """SEC 6 — CloudTrail for DocumentDB API calls."""
    import boto3
    try:
        ct = boto3.client("cloudtrail", region_name=ctx.region)
        trails = ct.describe_trails(includeShadowTrails=False)["trailList"]
        active = [t for t in trails
                  if t.get("IsMultiRegionTrail") or t.get("HomeRegion", "") == ctx.region]
        return [{"pillar": "Security", "id": "SEC6b",
                 "label": f"CloudTrail {'enabled' if active else 'not found in region'}",
                 "status": "pass" if active else "warn",
                 "detail": "" if active else "Enable CloudTrail to capture all DocumentDB API calls"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC6b",
                 "label": f"Cannot check CloudTrail: {e}", "status": "warn", "detail": str(e)}]


@register_check("SEC6c", "Security", "Security CloudWatch alarms",
                source="cloudwatch", priority=43)
def check_security_alarms(ctx):
    """SEC 6 — CW alarms for connections/failed logins."""
    try:
        alarms = ctx.cw_client.describe_alarms()["MetricAlarms"]
        metrics = {a["MetricName"] for a in alarms
                   if any(ctx.cluster_id in str(d) for d in a.get("Dimensions", []))}
        has_alarm = any(m in metrics for m in ("DatabaseConnections", "FailedLoginAttempts"))
        return [{"pillar": "Security", "id": "SEC6c",
                 "label": f"Security CloudWatch alarms {'configured' if has_alarm else 'not configured'}",
                 "status": "pass" if has_alarm else "warn",
                 "detail": "" if has_alarm else
                 "Configure alarms for unexpected connection spikes and failed auth attempts"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC6c",
                 "label": f"Cannot check security alarms: {e}", "status": "warn", "detail": str(e)}]


@register_check("SEC7a", "Security", "AWS Config rules",
                source="infrastructure", priority=55)
def check_config_rules(ctx):
    """SEC 7 — AWS Config rules for DocumentDB."""
    import boto3
    try:
        cfg = boto3.client("config", region_name=ctx.region)
        rules = cfg.describe_config_rules()["ConfigRules"]
        docdb_rules = [r for r in rules
                       if any(kw in r.get("ConfigRuleName", "").lower()
                              for kw in ("docdb", "documentdb", "rds"))]
        return [{"pillar": "Security", "id": "SEC7a",
                 "label": f"AWS Config: {len(docdb_rules)} relevant rule(s)" if docdb_rules
                 else "No AWS Config rules found for DocumentDB",
                 "status": "pass" if docdb_rules else "warn",
                 "detail": "" if docdb_rules else
                 "Use AWS Config rules to enforce encryption, TLS, and VPC placement"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC7a",
                 "label": f"Cannot check Config rules: {e}", "status": "warn", "detail": str(e)}]


# ── Migrated from tabs/wa_v2/security.py (Phase 3C) ──────────────────────────

# Roles that indicate broad / superuser access
_BROAD_ROLES = {
    "root", "dbAdminAnyDatabase", "readWriteAnyDatabase",
    "readAnyDatabase", "userAdminAnyDatabase", "clusterAdmin",
    "clusterManager", "restore", "backup",
}


@register_check("SEC3b", "Security", "RBAC user configuration",
                source="infrastructure", priority=32)
def check_rbac(ctx):
    """SEC 3 — Live RBAC check using the cluster connection string."""
    if not ctx.conn_str:
        return [{"pillar": "Security", "id": "SEC3b",
                 "label": "RBAC user check — no connection string available",
                 "status": "info",
                 "detail": "Create per-application DB users with minimum required permissions. "
                           "Do not use administrator credentials in application connection strings."}]
    try:
        import pymongo
        client = pymongo.MongoClient(ctx.conn_str, serverSelectionTimeoutMS=5000,
                                     appname="DocDB-Agent-WA")
        try:
            resp = client["admin"].command("usersInfo", 1)
            users = resp.get("users", [])
        except Exception as e:
            client.close()
            return [{"pillar": "Security", "id": "SEC3b",
                     "label": f"RBAC check — insufficient permissions: {e}",
                     "status": "info",
                     "detail": "Connect as primary user to inspect database users and roles"}]

        # Query $external for IAM users (5.0+, optional)
        iam_users = []
        try:
            ext_resp = client["$external"].command("usersInfo", 1)
            iam_users = [u for u in ext_resp.get("users", [])
                         if "MONGODB-AWS" in str(u.get("mechanisms", []))
                         or u.get("db") == "$external"]
        except Exception:
            pass

        client.close()

        # Classify users
        app_users = [u for u in users if u.get("user") != "serviceadmin"]
        primary = [u for u in app_users
                   if any(r.get("role") == "root" and r.get("db") == "admin"
                          for r in u.get("roles", []))]
        primary_names = {u["user"] for u in primary}
        non_primary = [u for u in app_users if u["user"] not in primary_names]

        def _is_broad(user):
            return any(r.get("role") in _BROAD_ROLES for r in user.get("roles", []))

        def _is_scoped(user):
            roles = user.get("roles", [])
            return (roles and
                    all(r.get("role") in ("read", "readWrite", "dbAdmin", "dbOwner")
                        and r.get("db", "admin") not in ("admin", "")
                        for r in roles))

        broad = [u["user"] for u in non_primary if _is_broad(u)]
        scoped = [u["user"] for u in non_primary if _is_scoped(u)]
        other = [u["user"] for u in non_primary
                 if u["user"] not in broad and u["user"] not in scoped]
        total_app = len(non_primary)
        iam_count = len(iam_users)

        if total_app == 0 and iam_count == 0:
            return [{"pillar": "Security", "id": "SEC3b",
                     "label": "No application users found — only primary user",
                     "status": "warn",
                     "detail": "Only the primary user exists — create per-application users "
                               "with minimum required roles (read/readWrite scoped to specific databases). "
                               "Do not use administrator credentials in application connection strings."}]

        parts = []
        if total_app > 0:
            parts.append(f"{total_app} password user(s)")
        if iam_count > 0:
            parts.append(f"{iam_count} IAM user(s) in $external")
        summary = ", ".join(parts)

        detail_parts = []
        if scoped:
            detail_parts.append(f"Scoped (least privilege): {', '.join(scoped)}")
        if broad:
            detail_parts.append(f"Broad roles (review): {', '.join(broad)}")
        if other:
            detail_parts.append(f"Other: {', '.join(other)}")
        if iam_count > 0:
            iam_names = [u.get("user", "").split("/")[-1] for u in iam_users]
            detail_parts.append(f"IAM: {', '.join(iam_names[:5])}")
        detail = ". ".join(detail_parts)

        if broad and not scoped:
            status = "warn"
            detail += ". All app users have broad roles — scope to specific databases"
        elif broad and scoped:
            status = "warn"
            detail += ". Some users have broad roles — review and restrict"
        else:
            status = "pass"
            detail += ". Ensure least-privilege principle is maintained"

        return [{"pillar": "Security", "id": "SEC3b",
                 "label": f"RBAC: {summary} configured", "status": status, "detail": detail}]

    except Exception as e:
        return [{"pillar": "Security", "id": "SEC3b",
                 "label": f"RBAC check — could not connect: {e}",
                 "status": "info",
                 "detail": "Ensure connection string is available when running WA Review"}]


@register_check("SEC6d", "Security", "AWS Security Hub findings",
                source="infrastructure", priority=44)
def check_security_hub(ctx):
    """SEC 6 — Security Hub findings for this cluster."""
    import boto3
    try:
        cluster_arn = ctx.cluster.get("DBClusterArn", "")
        sh = boto3.client("securityhub", region_name=ctx.region)
        try:
            sh.describe_hub()
        except Exception as he:
            if "InvalidAccess" in type(he).__name__ or "InvalidAccess" in str(he):
                return [{"pillar": "Security", "id": "SEC6d",
                         "label": "AWS Security Hub not enabled in this region",
                         "status": "info",
                         "detail": "Enable Security Hub for centralized, standards-based security "
                                   "findings against your DocumentDB cluster"}]
            return [{"pillar": "Security", "id": "SEC6d",
                     "label": f"Cannot check Security Hub: {he}",
                     "status": "warn", "detail": str(he)}]

        findings = []
        paginator = sh.get_paginator("get_findings")
        flt = {
            "ResourceId": [{"Value": cluster_arn, "Comparison": "EQUALS"}],
            "RecordState": [{"Value": "ACTIVE", "Comparison": "EQUALS"}],
        }
        for page in paginator.paginate(Filters=flt,
                                       PaginationConfig={"MaxItems": 200}):
            findings.extend(page.get("Findings", []))
        sev_labels = [f.get("Severity", {}).get("Label", "INFORMATIONAL")
                      for f in findings]
        n_crit_high = sum(1 for s in sev_labels if s in ("CRITICAL", "HIGH"))
        if not findings:
            return [{"pillar": "Security", "id": "SEC6d",
                     "label": "Security Hub: no active findings for this cluster",
                     "status": "pass", "detail": ""}]
        if n_crit_high:
            return [{"pillar": "Security", "id": "SEC6d",
                     "label": f"Security Hub: {n_crit_high} CRITICAL/HIGH finding(s) for this cluster",
                     "status": "fail",
                     "detail": f"{len(findings)} active finding(s) total — remediate critical/high first"}]
        return [{"pillar": "Security", "id": "SEC6d",
                 "label": f"Security Hub: {len(findings)} active finding(s) for this cluster",
                 "status": "warn",
                 "detail": "Review and remediate active Security Hub findings"}]
    except Exception as e:
        return [{"pillar": "Security", "id": "SEC6d",
                 "label": f"Cannot check Security Hub: {e}", "status": "warn", "detail": str(e)}]
