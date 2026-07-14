"""Built-in plugin registrations."""

from ..registry import register_plugin
from .telemetry_plugin import TelemetryPlugin


def _telemetry_factory(**opts):
    backend = opts.get("backend")
    if backend is None:
        raise ValueError(
            "TelemetryPlugin requires a 'backend' option. "
            "This is normally injected automatically by DocumentDBClient."
        )
    return TelemetryPlugin(backend)


register_plugin("telemetry", _telemetry_factory)


from .retry_plugin import RetryPlugin


def _retry_factory(**opts):
    retry_methods = opts.get("retry_methods")
    if retry_methods and isinstance(retry_methods, (list, set)):
        retry_methods = frozenset(retry_methods)
    return RetryPlugin(
        max_attempts=opts.get("max_attempts", 3),
        base_delay_ms=opts.get("base_delay_ms", 100),
        max_delay_ms=opts.get("max_delay_ms", 5000),
        retry_methods=retry_methods,
        backend=opts.get("backend"),
    )


register_plugin("retry", _retry_factory)
