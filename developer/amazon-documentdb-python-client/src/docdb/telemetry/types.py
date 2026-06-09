"""
Telemetry types: enums, metric descriptors, and span context.

- Named counters and gauges emitted by internal components
- Trace spans wrapping operations for distributed tracing visibility
- Pluggable backends (NONE, LOG, CLOUDWATCH, OTLP)
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class MetricsBackend(Enum):
    NONE = "none"
    LOG = "log"
    CLOUDWATCH = "cloudwatch"


class TracesBackend(Enum):
    NONE = "none"
    LOG = "log"


class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"


@dataclass
class MetricRecord:
    name: str
    value: float
    metric_type: MetricType
    unit: str = ""
    dimensions: dict[str, str] = field(default_factory=dict)


@dataclass
class SpanContext:
    name: str
    attributes: dict[str, Any] = field(default_factory=dict)
    parent: "SpanContext | None" = None


# Pre-defined metric names
#
# CloudWatch dimension layout:
#   Commands:   default_dimensions + {command}
#   Pool:       default_dimensions + {reason} (closed only) or default_dimensions only
#   Heartbeat:  default_dimensions only (aggregated across nodes)
#   Topology:   default_dimensions only
METRIC_COMMANDS_TOTAL = "docdb.commands.total"
METRIC_COMMANDS_FAILED = "docdb.commands.failed"
METRIC_COMMANDS_DURATION_MS = "docdb.commands.duration_ms"
METRIC_CONNECTIONS_OPENED = "docdb.connections.opened"
METRIC_CONNECTIONS_CLOSED = "docdb.connections.closed"
METRIC_SERVER_HEARTBEAT_DURATION_MS = "docdb.heartbeat.duration_ms"
METRIC_SERVER_HEARTBEAT_FAILED = "docdb.heartbeat.failed"
METRIC_TOPOLOGY_CHANGED = "docdb.topology.changed"
METRIC_TRACKER_POOL_RESET = "docdb.tracker.pool_reset"
METRIC_RETRY_TRIGGERED = "docdb.retry.triggered"
METRIC_RETRY_EXHAUSTED = "docdb.retry.exhausted"
