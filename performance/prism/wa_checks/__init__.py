"""Well-Architected Check Plugins — self-registering check modules.

Each pillar module defines check functions that auto-register via the
wa_check_registry. This allows adding new checks without modifying
the orchestrator or the main well_architected.py module.

Usage:
    from wa_checks import get_all_checks, get_checks_by_pillar, run_checks

    # Run all registered checks:
    results = run_checks(cluster_id, region, analysis_data, conn_str)

    # Get checks for a specific pillar:
    reliability_checks = get_checks_by_pillar("Reliability")
"""

from wa_checks.registry import (
    register_check,
    get_all_checks,
    get_checks_by_pillar,
    run_checks,
    CheckDefinition,
)

# Import pillar modules to trigger registration
from wa_checks import reliability  # noqa: F401
from wa_checks import security  # noqa: F401
from wa_checks import operational_excellence  # noqa: F401
from wa_checks import performance  # noqa: F401
from wa_checks import cost_optimization  # noqa: F401
from wa_checks import sustainability  # noqa: F401

__all__ = [
    "register_check",
    "get_all_checks",
    "get_checks_by_pillar",
    "run_checks",
    "CheckDefinition",
]
