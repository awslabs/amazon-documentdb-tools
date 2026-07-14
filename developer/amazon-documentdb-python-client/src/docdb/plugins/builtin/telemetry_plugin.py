"""
TelemetryPlugin — operation-level spans and metrics through the plugin chain.

Supplements (does not replace) the PyMongo DocumentDBCommandListener:
- CommandListener: fires on every wire command, knows protocol details
- TelemetryPlugin: fires on user operations, knows logical context
  (collection name, method, duration including time in other plugins)

Weight 100 — runs outermost so it measures total time including
time spent by downstream plugins (e.g., retry delays).
"""

from __future__ import annotations

import time
from typing import Any, Callable

from docdb.telemetry.types import (
    METRIC_COMMANDS_DURATION_MS,
    METRIC_COMMANDS_FAILED,
    MetricRecord,
    MetricType,
    SpanContext,
)

from ..base import BaseConnectionPlugin, OperationContext, WILDCARD


class TelemetryPlugin(BaseConnectionPlugin):

    def __init__(self, backend: Any) -> None:
        self._backend = backend

    @property
    def plugin_code(self) -> str:
        return "telemetry"

    @property
    def weight(self) -> int:
        return 100

    def subscribed_methods(self) -> frozenset[str]:
        return frozenset({WILDCARD})

    def execute(
        self,
        ctx: OperationContext,
        next_fn: Callable[[OperationContext], Any],
    ) -> Any:
        span = SpanContext(
            name=f"docdb.plugin.{ctx.method}",
            attributes={
                "collection": ctx.collection_name,
                "db": ctx.database_name,
                "method": ctx.method,
            },
        )
        self._backend.start_span(span)
        start = time.perf_counter()
        try:
            result = next_fn(ctx)
            duration_ms = (time.perf_counter() - start) * 1000
            self._backend.end_span(span, duration_ms)
            self._backend.record_metric(MetricRecord(
                name=METRIC_COMMANDS_DURATION_MS,
                value=duration_ms,
                metric_type=MetricType.HISTOGRAM,
                unit="ms",
                dimensions={"method": ctx.method, "collection": ctx.collection_name},
            ))
            return result
        except Exception as exc:
            duration_ms = (time.perf_counter() - start) * 1000
            self._backend.end_span(span, duration_ms, error=exc)
            self._backend.record_metric(MetricRecord(
                name=METRIC_COMMANDS_FAILED,
                value=1,
                metric_type=MetricType.COUNTER,
                unit="count",
                dimensions={"method": ctx.method, "collection": ctx.collection_name},
            ))
            raise
