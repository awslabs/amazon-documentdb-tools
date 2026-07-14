"""
CloudWatch Metrics telemetry backend.

Buffers metric data points and publishes them to Amazon CloudWatch
in batches. Only emits explicit metrics from record_metric() — span
lifecycle (start/end) is intentionally not converted to additional
metrics to avoid duplication and dimension fragmentation.

Requires: boto3 (install via `pip install amazon-documentdb-python-client[cloudwatch]`)
"""

import logging
import threading
import time
from collections import deque
from typing import Any

from .backend import TelemetryBackend
from .types import MetricRecord, MetricType, SpanContext

logger = logging.getLogger("docdb.telemetry.cloudwatch")

_BATCH_SIZE = 20  # CloudWatch put_metric_data limit
_FLUSH_INTERVAL_SECONDS = 60

class CloudWatchBackend(TelemetryBackend):
    """
    Publishes metrics to Amazon CloudWatch Metrics.

    Only record_metric() produces CloudWatch data points. Spans are
    silently ignored — the CommandListener already emits duration as
    an explicit metric, so converting spans would create duplicates.
    """

    def __init__(
        self,
        namespace: str = "DocumentDB/Client",
        cloudwatch_client: Any = None,
        flush_interval_seconds: float = _FLUSH_INTERVAL_SECONDS,
        default_dimensions: dict[str, str] | None = None,
    ) -> None:
        try:
            import boto3
        except ImportError as e:
            raise ImportError(
                "boto3 is required for CloudWatch telemetry. "
            ) from e

        self._namespace = namespace
        self._client = cloudwatch_client or boto3.client("cloudwatch")
        self._default_dimensions = default_dimensions or {}
        self._buffer: deque[dict] = deque()
        self._lock = threading.Lock()
        self._flush_interval = flush_interval_seconds
        self._running = True
        self._flush_thread = threading.Thread(
            target=self._periodic_flush, daemon=True, name="docdb-cw-flush"
        )
        self._flush_thread.start()

    def record_metric(self, metric: MetricRecord) -> None:
        dimensions = {**self._default_dimensions, **metric.dimensions}
        cw_dimensions = [{"Name": k, "Value": v} for k, v in dimensions.items()]

        unit = self._map_unit(metric.unit, metric.metric_type)

        datum: dict = {
            "MetricName": metric.name,
            "Value": metric.value,
            "Unit": unit,
            "Dimensions": cw_dimensions,
        }

        with self._lock:
            self._buffer.append(datum)

    def start_span(self, span: SpanContext) -> None:
        pass

    def end_span(self, span: SpanContext, duration_ms: float, error: Exception | None = None) -> None:
        pass

    def flush(self) -> None:
        with self._lock:
            batches = self._flush_buffer()
        self._send_batches(batches)

    def shutdown(self) -> None:
        self._running = False
        self.flush()

    def _flush_buffer(self) -> list[list[dict]]:
        # Drain buffer under lock, then send outside the lock
        batches = []
        while self._buffer:
            batch = []
            for _ in range(min(_BATCH_SIZE, len(self._buffer))):
                batch.append(self._buffer.popleft())
            batches.append(batch)
        return batches

    def _send_batches(self, batches: list) -> None:
        for batch in batches:
            try:
                self._client.put_metric_data(
                    Namespace=self._namespace, MetricData=batch
                )
            except Exception:
                logger.exception("Failed to publish metrics to CloudWatch")

    def _periodic_flush(self) -> None:
        while self._running:
            time.sleep(self._flush_interval)
            if self._buffer:
                self.flush()

    @staticmethod
    def _map_unit(unit: str, metric_type: MetricType) -> str:
        unit_map = {
            "ms": "Milliseconds",
            "s": "Seconds",
            "bytes": "Bytes",
            "count": "Count",
            "": "None",
        }
        if unit in unit_map:
            return unit_map[unit]
        if metric_type == MetricType.COUNTER:
            return "Count"
        return "None"
