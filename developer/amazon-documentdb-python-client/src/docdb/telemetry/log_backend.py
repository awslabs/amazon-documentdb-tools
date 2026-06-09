"""
Logging-based telemetry backend.

Emits metrics and spans as structured log records. Suitable for
development, debugging, and environments where log aggregation
(CloudWatch Logs, Datadog, Splunk) provides observability.
"""

import logging

from .backend import TelemetryBackend
from .types import MetricRecord, MetricType, SpanContext

logger = logging.getLogger("docdb.telemetry")


class LogBackend(TelemetryBackend):
    """Emits all telemetry as structured log messages."""

    def __init__(self, level: int = logging.DEBUG) -> None:
        self._level = level

    def record_metric(self, metric: MetricRecord) -> None:
        extra = {
            "metric_name": metric.name,
            "metric_type": metric.metric_type.value,
            "value": metric.value,
        }
        if metric.unit:
            extra["unit"] = metric.unit
        if metric.dimensions:
            extra["dimensions"] = metric.dimensions

        if metric.metric_type == MetricType.COUNTER:
            logger.log(self._level, "[metric] %s += %s", metric.name, metric.value, extra=extra)
        elif metric.metric_type == MetricType.GAUGE:
            logger.log(self._level, "[metric] %s = %s", metric.name, metric.value, extra=extra)
        else:
            logger.log(self._level, "[metric] %s: %s", metric.name, metric.value, extra=extra)

    def start_span(self, span: SpanContext) -> None:
        logger.log(
            self._level,
            "[span:start] %s",
            span.name,
            extra={"span_name": span.name, "attributes": span.attributes},
        )

    def end_span(self, span: SpanContext, duration_ms: float, error: Exception | None = None) -> None:
        extra = {"span_name": span.name, "duration_ms": duration_ms}
        if error:
            extra["error"] = str(error)
            logger.log(
                self._level,
                "[span:end] %s duration=%.1fms error=%s",
                span.name,
                duration_ms,
                type(error).__name__,
                extra=extra,
            )
        else:
            logger.log(
                self._level,
                "[span:end] %s duration=%.1fms",
                span.name,
                duration_ms,
                extra=extra,
            )

    def flush(self) -> None:
        for handler in logger.handlers:
            handler.flush()

    def shutdown(self) -> None:
        pass
