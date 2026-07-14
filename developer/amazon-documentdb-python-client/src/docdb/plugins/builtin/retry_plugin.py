"""
Retry Plugin.

Retries failed operations with exponential backoff and jitter.
Configurable per method subscription — by default retries reads only,
since retrying writes can cause duplicates unless idempotency is guaranteed.

Follows the AWS Builders' Library guidance on timeouts, retries, and
backoff with jitter:
https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/
"""

from __future__ import annotations

import logging
import random
import time
from typing import Any, Callable

from pymongo.errors import (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)

from ..base import BaseConnectionPlugin, OperationContext

logger = logging.getLogger(__name__)

_DEFAULT_READ_METHODS = frozenset({
    "find",
    "find_one",
    "count_documents",
    "distinct",
    "aggregate",
})

_RETRYABLE_ERRORS = (
    AutoReconnect,
    ConnectionFailure,
    NetworkTimeout,
    ServerSelectionTimeoutError,
)


class RetryPlugin(BaseConnectionPlugin):
    """
    Retries failed operations with exponential backoff and jitter.

    Default behavior retries reads only (safe — no side effects).
    To retry writes, explicitly include write method names in the
    retry_methods option. Only do this if your writes are idempotent.
    """

    def __init__(
        self,
        max_attempts: int = 3,
        base_delay_ms: float = 100,
        max_delay_ms: float = 5000,
        retry_methods: frozenset[str] | None = None,
        backend=None,
        config=None,
        **kwargs: Any,
    ) -> None:
        if kwargs:
            logger.warning(
                "RetryPlugin: unrecognized options ignored: %s",
                ", ".join(sorted(kwargs)),
            )
        self._max_attempts = max_attempts
        self._base_delay_ms = base_delay_ms
        self._max_delay_ms = max_delay_ms
        self._retry_methods = retry_methods or _DEFAULT_READ_METHODS
        self._backend = backend

    @property
    def plugin_code(self) -> str:
        return "retry"

    @property
    def weight(self) -> int:
        return 500

    def subscribed_methods(self) -> frozenset[str]:
        return self._retry_methods

    def execute(
        self,
        ctx: OperationContext,
        next_fn: Callable[[OperationContext], Any],
    ) -> Any:
        session = ctx.kwargs.get("session")
        if session is not None and session.in_transaction:
            return next_fn(ctx)

        last_error: Exception | None = None
        for attempt in range(self._max_attempts):
            try:
                result = next_fn(ctx)
                if attempt > 0:
                    logger.info(
                        "RetryPlugin: %s.%s.%s succeeded on attempt %d",
                        ctx.database_name, ctx.collection_name, ctx.method,
                        attempt + 1,
                    )
                return result
            except _RETRYABLE_ERRORS as exc:
                last_error = exc
                self._emit_metric("docdb.retry.triggered")
                if attempt < self._max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        "RetryPlugin: %s.%s.%s attempt %d failed (%s), "
                        "retrying in %.0fms...",
                        ctx.database_name, ctx.collection_name, ctx.method,
                        attempt + 1, type(exc).__name__, delay,
                    )
                    time.sleep(delay / 1000)
                else:
                    logger.error(
                        "RetryPlugin: %s.%s.%s failed after %d attempts",
                        ctx.database_name, ctx.collection_name, ctx.method,
                        self._max_attempts,
                    )
                    self._emit_metric("docdb.retry.exhausted")
        raise last_error  # type: ignore[misc]

    def _emit_metric(self, name: str) -> None:
        if self._backend:
            from docdb.telemetry.types import MetricRecord, MetricType
            self._backend.record_metric(MetricRecord(
                name=name,
                value=1,
                metric_type=MetricType.COUNTER,
                unit="count",
            ))

    def _calculate_delay(self, attempt: int) -> float:
        """Exponential backoff with full jitter, capped at max_delay_ms."""
        exponential = self._base_delay_ms * (2 ** attempt)
        capped = min(exponential, self._max_delay_ms)
        return random.uniform(0, capped)
