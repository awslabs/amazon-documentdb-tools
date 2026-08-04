"""WA Check Registry — plugin system for Well-Architected checks.

Checks self-register using the @wa_check decorator or register_check().
The orchestrator calls run_checks() to execute all registered checks.

Each check function receives a context dict and returns a list of check results.
"""
import logging
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass
class CheckDefinition:
    """Definition of a single WA check."""
    check_id: str
    pillar: str
    label: str
    description: str
    source: str  # "infrastructure", "database", "cloudwatch"
    func: Callable  # Function that runs the check
    per_instance: bool = False  # Runs once per instance
    writer_only: bool = False  # Only runs on writer instance
    reader_only: bool = False  # Only runs on reader instances
    requires_analysis: bool = False  # Needs analysis_data
    priority: int = 50  # Execution order within pillar


# ── Registry Storage ──────────────────────────────────────────────────────────

_CHECKS: dict[str, CheckDefinition] = {}
_PILLAR_ORDER = [
    "Reliability", "Security", "Operational Excellence",
    "Performance Efficiency", "Cost Optimization", "Sustainability",
]


def register_check(
    check_id: str,
    pillar: str,
    label: str,
    source: str = "infrastructure",
    description: str = "",
    per_instance: bool = False,
    writer_only: bool = False,
    reader_only: bool = False,
    requires_analysis: bool = False,
    priority: int = 50,
):
    """Decorator to register a WA check function.

    The decorated function receives a CheckContext and returns a list of
    check result dicts: [{"pillar", "id", "label", "status", "detail"}]

    Example:
        @register_check("REL1a", "Reliability", "Backup retention",
                        source="infrastructure")
        def check_backup_retention(ctx):
            retention = ctx.cluster.get("BackupRetentionPeriod", 1)
            return [{"pillar": "Reliability", "id": "REL1a",
                     "label": f"Backup retention ({retention} days)",
                     "status": "pass" if retention >= 7 else "warn",
                     "detail": ""}]
    """
    def decorator(func):
        defn = CheckDefinition(
            check_id=check_id,
            pillar=pillar,
            label=label,
            description=description,
            source=source,
            func=func,
            per_instance=per_instance,
            writer_only=writer_only,
            reader_only=reader_only,
            requires_analysis=requires_analysis,
            priority=priority,
        )
        _CHECKS[check_id] = defn
        logger.debug("Registered WA check: %s (%s)", check_id, pillar)
        return func
    return decorator


def get_all_checks() -> dict[str, CheckDefinition]:
    """Return all registered checks."""
    return dict(_CHECKS)


def get_checks_by_pillar(pillar: str) -> list[CheckDefinition]:
    """Return checks for a specific pillar, sorted by priority."""
    checks = [c for c in _CHECKS.values() if c.pillar == pillar]
    return sorted(checks, key=lambda c: c.priority)


def get_checks_by_source(source: str) -> list[CheckDefinition]:
    """Return checks by data source type."""
    return [c for c in _CHECKS.values() if c.source == source]


@dataclass
class CheckContext:
    """Context passed to each check function."""
    cluster_id: str
    region: str
    cluster: dict  # describe_db_clusters result
    instances: list  # describe_db_instances result
    analysis_data: Optional[dict] = None
    conn_str: str = ""
    # Boto3 clients (shared across checks to avoid re-creation)
    docdb_client: Any = None
    cw_client: Any = None
    ec2_client: Any = None
    # Per-instance context (set when running per-instance checks)
    current_instance: Optional[dict] = None
    is_writer: bool = False


def run_checks(
    cluster_id: str,
    region: str,
    cluster: dict,
    instances: list,
    analysis_data: Optional[dict] = None,
    conn_str: str = "",
    docdb_client=None,
    cw_client=None,
    ec2_client=None,
) -> list[dict]:
    """Run all registered checks and return results.

    Args:
        cluster_id: DocumentDB cluster identifier
        region: AWS region
        cluster: Result of describe_db_clusters (single cluster dict)
        instances: Result of describe_db_instances (list)
        analysis_data: Cached analysis data (optional)
        conn_str: Connection string (for live checks)
        docdb_client: Shared boto3 docdb client
        cw_client: Shared boto3 cloudwatch client
        ec2_client: Shared boto3 ec2 client

    Returns:
        List of check result dicts: [{"pillar", "id", "label", "status", "detail"}]
    """
    import boto3

    # Create clients if not provided
    if not docdb_client:
        docdb_client = boto3.client("docdb", region_name=region)
    if not cw_client:
        cw_client = boto3.client("cloudwatch", region_name=region)
    if not ec2_client:
        ec2_client = boto3.client("ec2", region_name=region)

    ctx = CheckContext(
        cluster_id=cluster_id,
        region=region,
        cluster=cluster,
        instances=instances,
        analysis_data=analysis_data,
        conn_str=conn_str,
        docdb_client=docdb_client,
        cw_client=cw_client,
        ec2_client=ec2_client,
    )

    results = []

    # Determine writer instance
    writer_ids = set()
    for member in cluster.get("DBClusterMembers", []):
        if member.get("IsClusterWriter"):
            writer_ids.add(member.get("DBInstanceIdentifier"))

    # Run non-per-instance checks first
    for pillar in _PILLAR_ORDER:
        checks = get_checks_by_pillar(pillar)
        for check_def in checks:
            if check_def.per_instance:
                continue  # handled below
            if check_def.requires_analysis and not analysis_data:
                results.append({
                    "pillar": check_def.pillar,
                    "id": check_def.check_id,
                    "label": f"{check_def.label} — requires Analyze",
                    "status": "info",
                    "detail": "Run database analysis first",
                })
                continue
            try:
                check_results = check_def.func(ctx)
                if check_results:
                    results.extend(check_results)
            except Exception as e:
                logger.warning("Check %s failed: %s", check_def.check_id, e)
                results.append({
                    "pillar": check_def.pillar,
                    "id": check_def.check_id,
                    "label": f"{check_def.label} — check failed: {e}",
                    "status": "warn",
                    "detail": str(e),
                })

    # Run per-instance checks
    for inst in instances:
        iid = inst.get("DBInstanceIdentifier", "")
        is_writer = iid in writer_ids

        ctx.current_instance = inst
        ctx.is_writer = is_writer

        for pillar in _PILLAR_ORDER:
            checks = get_checks_by_pillar(pillar)
            for check_def in checks:
                if not check_def.per_instance:
                    continue
                if check_def.writer_only and not is_writer:
                    continue
                if check_def.reader_only and is_writer:
                    continue
                try:
                    check_results = check_def.func(ctx)
                    if check_results:
                        results.extend(check_results)
                except Exception as e:
                    logger.warning("Check %s failed for %s: %s",
                                 check_def.check_id, iid, e)
                    results.append({
                        "pillar": check_def.pillar,
                        "id": check_def.check_id,
                        "label": f"{check_def.label} for {iid} — failed: {e}",
                        "status": "warn",
                        "detail": str(e),
                    })

    ctx.current_instance = None
    return results
