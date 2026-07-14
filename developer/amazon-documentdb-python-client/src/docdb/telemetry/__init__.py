"""
docdb.telemetry — Observability for Amazon DocumentDB connections.

Provides pluggable telemetry backends (logging, CloudWatch) that receive
metrics and trace spans from PyMongo's monitoring hooks. 

Quickstart:
    from docdb import DocumentDBConfig
    from docdb.telemetry import TelemetryConfig, MetricsBackend

    config = DocumentDBConfig(
        host="cluster.docdb.amazonaws.com",
        telemetry=TelemetryConfig(
            enabled=True,
            metrics_backend=MetricsBackend.LOG,
        ),
    )
"""

from .backend import NoOpBackend, TelemetryBackend
from .cloudwatch_backend import CloudWatchBackend
from .config import TelemetryConfig, build_backend
from .listeners import (
    DocumentDBCommandListener,
    DocumentDBHeartbeatListener,
    DocumentDBPoolListener,
    DocumentDBTopologyListener,
)
from .log_backend import LogBackend
from .types import (
    METRIC_COMMANDS_DURATION_MS,
    METRIC_COMMANDS_FAILED,
    METRIC_COMMANDS_TOTAL,
    METRIC_CONNECTIONS_CLOSED,
    METRIC_CONNECTIONS_OPENED,
    METRIC_SERVER_HEARTBEAT_DURATION_MS,
    METRIC_SERVER_HEARTBEAT_FAILED,
    METRIC_TOPOLOGY_CHANGED,
    MetricRecord,
    MetricType,
    MetricsBackend,
    SpanContext,
    TracesBackend,
)

__all__ = [
    # Config
    "TelemetryConfig",
    "build_backend",
    # Backends
    "TelemetryBackend",
    "NoOpBackend",
    "LogBackend",
    "CloudWatchBackend",
    # Listeners
    "DocumentDBCommandListener",
    "DocumentDBHeartbeatListener",
    "DocumentDBPoolListener",
    "DocumentDBTopologyListener",
    # Types
    "MetricsBackend",
    "TracesBackend",
    "MetricType",
    "MetricRecord",
    "SpanContext",
    # Metric names
    "METRIC_COMMANDS_TOTAL",
    "METRIC_COMMANDS_FAILED",
    "METRIC_COMMANDS_DURATION_MS",
    "METRIC_CONNECTIONS_OPENED",
    "METRIC_CONNECTIONS_CLOSED",
    "METRIC_SERVER_HEARTBEAT_DURATION_MS",
    "METRIC_SERVER_HEARTBEAT_FAILED",
    "METRIC_TOPOLOGY_CHANGED",
]
