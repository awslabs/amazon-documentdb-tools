"""
Abstract telemetry backend interface.

All backends implement TelemetryBackend. The wrapper calls record_metric()
and start_span()/end_span(). The backend decides where they go.
"""

from abc import ABC, abstractmethod

from .types import MetricRecord, SpanContext


class TelemetryBackend(ABC):
    @abstractmethod
    def record_metric(self, metric: MetricRecord) -> None:
        """Emit a single metric data point."""

    @abstractmethod
    def start_span(self, span: SpanContext) -> None:
        """Begin a trace span."""

    @abstractmethod
    def end_span(self, span: SpanContext, duration_ms: float, error: Exception | None = None) -> None:
        """End a trace span with duration and optional error."""

    @abstractmethod
    def flush(self) -> None:
        """Flush any buffered telemetry data."""

    @abstractmethod
    def shutdown(self) -> None:
        """Release resources held by the backend."""


class NoOpBackend(TelemetryBackend):
    """Disabled telemetry — all operations are no-ops."""

    def record_metric(self, metric: MetricRecord) -> None:
        pass

    def start_span(self, span: SpanContext) -> None:
        pass

    def end_span(self, span: SpanContext, duration_ms: float, error: Exception | None = None) -> None:
        pass

    def flush(self) -> None:
        pass

    def shutdown(self) -> None:
        pass
