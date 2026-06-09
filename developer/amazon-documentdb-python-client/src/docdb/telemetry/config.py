"""
Telemetry configuration.

Namespace and dimension strategy:
- Namespace: auto-derived from the cluster endpoint as "DocumentDB/{cluster-id}"
  to provide per-cluster views by default.
- Dimensions: "service" (from app_name) and "environment" (if provided) are
  added to every metric automatically. Filter by service or see all
  services hitting a cluster in one namespace.
"""

import re
from dataclasses import dataclass, field
from typing import Any

from .backend import NoOpBackend, TelemetryBackend
from .types import MetricsBackend, TracesBackend

_CLUSTER_ID_PATTERN = re.compile(r"^([^.]+)\.cluster")


def _extract_cluster_id(host: str) -> str:
    """
    Extract the cluster identifier from an Amazon DocumentDB endpoint.

    Format: {cluster-id}.cluster-{hash}.{region}.docdb.amazonaws.com
    Returns the first dot-delimited segment, or the full host if
    the format doesn't match (e.g., localhost).
    """
    match = _CLUSTER_ID_PATTERN.match(host)
    if match:
        return match.group(1)
    return host.split(".")[0] or host


@dataclass
class TelemetryConfig:
    enabled: bool = False
    """Main switch. When False, a NoOpBackend is used regardless of other settings."""

    metrics_backend: MetricsBackend = MetricsBackend.LOG
    """Where metric data points are published."""

    traces_backend: TracesBackend = TracesBackend.LOG
    """Where trace spans are published."""

    cloudwatch_namespace: str | None = None
    """CloudWatch namespace. If None (default), auto-derived as
    'DocumentDB/{cluster-id}' from the host endpoint. Set explicitly
    to override (e.g., 'DocumentDB/my-custom-name')."""

    cloudwatch_flush_interval_seconds: float = 60
    """How often buffered CloudWatch metrics are flushed."""

    cloudwatch_client: Any = None
    """Optional pre-configured boto3 CloudWatch client. If None, one is created."""

    default_dimensions: dict[str, str] = field(default_factory=dict)
    """Additional dimensions added to every metric beyond the auto-derived ones.
    The 'service' dimension is automatically set from app_name if not provided here."""

    log_level: int = 10  # logging.DEBUG
    """Log level for the LOG backend."""

    enabled_listeners: frozenset[str] = frozenset({"command", "heartbeat", "topology", "pool"})
    """Which PyMongo monitoring listeners are active. Possible values:
    'command', 'heartbeat', 'topology', 'pool'. Remove 'command' in
    high-throughput environments to reduce overhead while keeping
    infrastructure visibility (heartbeat, pool, topology)."""

    emit_spans: bool = True
    """Whether to emit span start/end events. When False, only metrics
    are recorded — useful for production environments where per-operation
    trace logging is too noisy but you still want counters and durations."""


def build_backend(
    config: TelemetryConfig,
    host: str = "",
    app_name: str | None = None,
) -> TelemetryBackend:
    """
    Construct the appropriate backend from config.

    Args:
        config: TelemetryConfig instance
        host: Cluster endpoint (used to auto-derive namespace and cluster dimension)
        app_name: Service name (used as the 'service' dimension)
    """
    if not config.enabled:
        return NoOpBackend()

    # Build the effective dimensions: auto-derived + user-provided
    dimensions = {}
    if app_name:
        dimensions["service"] = app_name
    cluster_id = _extract_cluster_id(host) if host else None
    if cluster_id:
        dimensions["cluster"] = cluster_id
    # User-provided dimensions override auto-derived ones
    dimensions.update(config.default_dimensions)

    if config.metrics_backend == MetricsBackend.CLOUDWATCH:
        # Auto-derive namespace from cluster endpoint if not explicitly set
        namespace = config.cloudwatch_namespace
        if namespace is None:
            namespace = f"DocumentDB/{cluster_id}" if cluster_id else "DocumentDB/Client"

        from .cloudwatch_backend import CloudWatchBackend
        return CloudWatchBackend(
            namespace=namespace,
            cloudwatch_client=config.cloudwatch_client,
            flush_interval_seconds=config.cloudwatch_flush_interval_seconds,
            default_dimensions=dimensions,
        )

    if config.metrics_backend == MetricsBackend.LOG or config.traces_backend == TracesBackend.LOG:
        from .log_backend import LogBackend
        return LogBackend(level=config.log_level)

    return NoOpBackend()
