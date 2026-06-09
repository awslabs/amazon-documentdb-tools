"""
Connection Tracker.

Tracks open connections per host and forcibly closes all connections
to a host when it is removed from the topology.

Without this, stale connections to removed instances linger until PyMongo's
server_selection_timeout_ms expires, causing delays during failover recovery.
The tracker eliminates this by proactively cleaning up the moment the driver
detects a topology change.

This is implemented as PyMongo monitoring listeners because
it operates at the connection lifecycle level, not the operation level.
"""

from __future__ import annotations

import logging
import threading
from typing import TYPE_CHECKING

from pymongo import monitoring

if TYPE_CHECKING:
    from .telemetry.backend import TelemetryBackend

logger = logging.getLogger(__name__)


class ConnectionTracker:
    """
    Tracks connections per host and closes them on topology removal.

    Registers as both a ConnectionPoolListener and TopologyListener.
    When a server disappears from the topology, all tracked connections
    to that host are immediately invalidated by clearing the pool.
    """

    def __init__(self, client=None, telemetry_backend: TelemetryBackend | None = None) -> None:
        self._client = client
        self._backend = telemetry_backend
        self._connections: dict[tuple, int] = {}
        self._lock = threading.Lock()

    def set_client(self, client) -> None:
        self._client = client

    def set_backend(self, backend: TelemetryBackend) -> None:
        self._backend = backend

    @property
    def pool_listener(self) -> "ConnectionTrackerPoolListener":
        return ConnectionTrackerPoolListener(self)

    @property
    def topology_listener(self) -> "ConnectionTrackerTopologyListener":
        return ConnectionTrackerTopologyListener(self)

    def connection_opened(self, address: tuple) -> None:
        with self._lock:
            self._connections[address] = self._connections.get(address, 0) + 1

    def connection_closed(self, address: tuple) -> None:
        with self._lock:
            count = self._connections.get(address, 0)
            if count > 1:
                self._connections[address] = count - 1
            elif count == 1:
                del self._connections[address]

    def server_removed(self, address: tuple) -> None:
        with self._lock:
            count = self._connections.pop(address, 0)

        if count > 0:
            server_str = f"{address[0]}:{address[1]}"
            logger.warning(
                "ConnectionTracker: host %s removed from topology, "
                "closing %d tracked connection(s)",
                server_str, count,
            )
            self._emit_metric("docdb.tracker.pool_reset", count)
            if self._client:
                try:
                    topology = self._client._topology
                    topology.reset_server(address)
                except Exception:
                    logger.debug(
                        "ConnectionTracker: could not reset server %s "
                        "(pool may already be closed)",
                        server_str,
                    )

    def _emit_metric(self, name: str, value: float = 1) -> None:
        if self._backend:
            from .telemetry.types import MetricRecord, MetricType
            self._backend.record_metric(MetricRecord(
                name=name,
                value=value,
                metric_type=MetricType.COUNTER,
                unit="count",
            ))

    def get_connection_count(self, address: tuple) -> int:
        with self._lock:
            return self._connections.get(address, 0)

    @property
    def total_connections(self) -> int:
        with self._lock:
            return sum(self._connections.values())

    @property
    def hosts(self) -> dict[tuple, int]:
        with self._lock:
            return dict(self._connections)


class ConnectionTrackerPoolListener(monitoring.ConnectionPoolListener):
    """Feeds connection open/close events to the ConnectionTracker."""

    def __init__(self, tracker: ConnectionTracker) -> None:
        self._tracker = tracker

    def pool_created(self, event: monitoring.PoolCreatedEvent) -> None:
        pass

    def pool_ready(self, event: monitoring.PoolReadyEvent) -> None:
        pass

    def pool_cleared(self, event: monitoring.PoolClearedEvent) -> None:
        pass

    def pool_closed(self, event: monitoring.PoolClosedEvent) -> None:
        pass

    def connection_created(self, event: monitoring.ConnectionCreatedEvent) -> None:
        self._tracker.connection_opened(event.address)

    def connection_ready(self, event: monitoring.ConnectionReadyEvent) -> None:
        pass

    def connection_closed(self, event: monitoring.ConnectionClosedEvent) -> None:
        self._tracker.connection_closed(event.address)

    def connection_check_out_started(self, event: monitoring.ConnectionCheckOutStartedEvent) -> None:
        pass

    def connection_check_out_failed(self, event: monitoring.ConnectionCheckOutFailedEvent) -> None:
        pass

    def connection_checked_out(self, event: monitoring.ConnectionCheckedOutEvent) -> None:
        pass

    def connection_checked_in(self, event: monitoring.ConnectionCheckedInEvent) -> None:
        pass


class ConnectionTrackerTopologyListener(monitoring.TopologyListener):
    """Triggers connection cleanup when servers are removed from topology."""

    def __init__(self, tracker: ConnectionTracker) -> None:
        self._tracker = tracker

    def opened(self, event: monitoring.TopologyOpenedEvent) -> None:
        pass

    def description_changed(self, event: monitoring.TopologyDescriptionChangedEvent) -> None:
        prev_servers = set(event.previous_description.server_descriptions().keys())
        new_servers = set(event.new_description.server_descriptions().keys())

        removed = prev_servers - new_servers
        for address in removed:
            self._tracker.server_removed(address)

    def closed(self, event: monitoring.TopologyClosedEvent) -> None:
        pass
