"""
PyMongo monitoring listeners that feed into the telemetry backend.

PyMongo provides four listener types:
- CommandListener: per-command start/success/failure
- ServerHeartbeatListener: SDAM heartbeat events
- TopologyListener: topology open/description-changed/closed
- ConnectionPoolListener: pool events (created, checked out, closed)

These listeners translate raw PyMongo events into the wrapper's
metric/span model. Metrics use a flat, consistent dimension set
designed for clean CloudWatch grouping:

    Commands:  {command}
    Pool:      {reason} (closed only) or no extra dimensions
    Heartbeat: no extra dimensions (aggregate across nodes)
    Topology:  no extra dimensions

The default_dimensions from TelemetryConfig (service, environment, etc.)
are appended automatically by the backend and listeners don't duplicate them.
"""

import threading
import time

from pymongo import monitoring

from .backend import TelemetryBackend
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
    SpanContext,
)


class DocumentDBCommandListener(monitoring.CommandListener):
    """Tracks command execution: counts, durations, failures."""

    def __init__(self, backend: TelemetryBackend, emit_spans: bool = True) -> None:
        self._backend = backend
        self._emit_spans = emit_spans
        self._pending: dict[int, tuple[SpanContext | None, float]] = {}
        self._pending_lock = threading.Lock()

    def started(self, event: monitoring.CommandStartedEvent) -> None:
        span = None
        if self._emit_spans:
            span = SpanContext(
                name=f"docdb.command.{event.command_name}",
                attributes={
                    "db": event.database_name,
                    "command": event.command_name,
                    "server": f"{event.connection_id[0]}:{event.connection_id[1]}",
                },
            )
            self._backend.start_span(span)
        with self._pending_lock:
            self._pending[event.request_id] = (span, time.perf_counter())
        self._backend.record_metric(MetricRecord(
            name=METRIC_COMMANDS_TOTAL,
            value=1,
            metric_type=MetricType.COUNTER,
            unit="count",
            dimensions={"command": event.command_name},
        ))

    def succeeded(self, event: monitoring.CommandSucceededEvent) -> None:
        with self._pending_lock:
            entry = self._pending.pop(event.request_id, None)
        if entry:
            span, start = entry
            duration_ms = event.duration_micros / 1000.0
            if self._emit_spans and span:
                self._backend.end_span(span, duration_ms)
            self._backend.record_metric(MetricRecord(
                name=METRIC_COMMANDS_DURATION_MS,
                value=duration_ms,
                metric_type=MetricType.HISTOGRAM,
                unit="ms",
                dimensions={"command": event.command_name},
            ))

    def failed(self, event: monitoring.CommandFailedEvent) -> None:
        with self._pending_lock:
            entry = self._pending.pop(event.request_id, None)
        duration_ms = event.duration_micros / 1000.0
        if entry:
            span, start = entry
            if self._emit_spans and span:
                self._backend.end_span(span, duration_ms, error=Exception(event.failure))
        self._backend.record_metric(MetricRecord(
            name=METRIC_COMMANDS_FAILED,
            value=1,
            metric_type=MetricType.COUNTER,
            unit="count",
            dimensions={"command": event.command_name},
        ))
        self._backend.record_metric(MetricRecord(
            name=METRIC_COMMANDS_DURATION_MS,
            value=duration_ms,
            metric_type=MetricType.HISTOGRAM,
            unit="ms",
            dimensions={"command": event.command_name},
        ))


class DocumentDBHeartbeatListener(monitoring.ServerHeartbeatListener):
    """Tracks heartbeat health."""

    def __init__(self, backend: TelemetryBackend) -> None:
        self._backend = backend

    def started(self, event: monitoring.ServerHeartbeatStartedEvent) -> None:
        pass

    def succeeded(self, event: monitoring.ServerHeartbeatSucceededEvent) -> None:
        self._backend.record_metric(MetricRecord(
            name=METRIC_SERVER_HEARTBEAT_DURATION_MS,
            value=event.duration * 1000.0,
            metric_type=MetricType.HISTOGRAM,
            unit="ms",
        ))

    def failed(self, event: monitoring.ServerHeartbeatFailedEvent) -> None:
        self._backend.record_metric(MetricRecord(
            name=METRIC_SERVER_HEARTBEAT_FAILED,
            value=1,
            metric_type=MetricType.COUNTER,
            unit="count",
        ))
        self._backend.record_metric(MetricRecord(
            name=METRIC_SERVER_HEARTBEAT_DURATION_MS,
            value=event.duration * 1000.0,
            metric_type=MetricType.HISTOGRAM,
            unit="ms",
        ))


class DocumentDBTopologyListener(monitoring.TopologyListener):
    """Tracks cluster topology changes."""

    def __init__(self, backend: TelemetryBackend) -> None:
        self._backend = backend

    def opened(self, event: monitoring.TopologyOpenedEvent) -> None:
        pass

    def description_changed(self, event: monitoring.TopologyDescriptionChangedEvent) -> None:
        prev_servers = set(event.previous_description.server_descriptions().keys())
        new_servers = set(event.new_description.server_descriptions().keys())

        added = new_servers - prev_servers
        removed = prev_servers - new_servers

        if added or removed:
            self._backend.record_metric(MetricRecord(
                name=METRIC_TOPOLOGY_CHANGED,
                value=len(added) + len(removed),
                metric_type=MetricType.COUNTER,
                unit="count",
            ))

    def closed(self, event: monitoring.TopologyClosedEvent) -> None:
        pass


class DocumentDBPoolListener(monitoring.ConnectionPoolListener):
    """Tracks connection pool activity."""

    def __init__(self, backend: TelemetryBackend) -> None:
        self._backend = backend

    def pool_created(self, event: monitoring.PoolCreatedEvent) -> None:
        pass

    def pool_ready(self, event: monitoring.PoolReadyEvent) -> None:
        pass

    def pool_cleared(self, event: monitoring.PoolClearedEvent) -> None:
        pass

    def pool_closed(self, event: monitoring.PoolClosedEvent) -> None:
        pass

    def connection_created(self, event: monitoring.ConnectionCreatedEvent) -> None:
        self._backend.record_metric(MetricRecord(
            name=METRIC_CONNECTIONS_OPENED,
            value=1,
            metric_type=MetricType.COUNTER,
            unit="count",
        ))

    def connection_ready(self, event: monitoring.ConnectionReadyEvent) -> None:
        pass

    def connection_closed(self, event: monitoring.ConnectionClosedEvent) -> None:
        self._backend.record_metric(MetricRecord(
            name=METRIC_CONNECTIONS_CLOSED,
            value=1,
            metric_type=MetricType.COUNTER,
            unit="count",
            dimensions={"reason": event.reason},
        ))

    def connection_check_out_started(self, event: monitoring.ConnectionCheckOutStartedEvent) -> None:
        pass

    def connection_check_out_failed(self, event: monitoring.ConnectionCheckOutFailedEvent) -> None:
        pass

    def connection_checked_out(self, event: monitoring.ConnectionCheckedOutEvent) -> None:
        pass

    def connection_checked_in(self, event: monitoring.ConnectionCheckedInEvent) -> None:
        pass
