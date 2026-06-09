"""
Example: Enabling telemetry to observe DocumentDB client behavior

Telemetry emits named metrics and trace spans for:
- Every command (find, insert, update, etc.) — count, duration, failures
- Connection pool activity — opens, closes, pool size
- Server heartbeats — duration, failures
- Topology changes — servers added/removed from the replica set

Two backends are available:
- LOG: Emits structured log records (zero dependencies, use in dev/debug)
- CLOUDWATCH: Publishes to Amazon CloudWatch Metrics (requires boto3)
"""

import logging

import docdb
from docdb import DocumentDBConfig
from docdb.telemetry import MetricsBackend, TelemetryConfig

# Configure logging to see telemetry output
logging.basicConfig(level=logging.DEBUG, format="%(name)s | %(message)s")

# --- Option A: Log-based telemetry (zero dependencies) ---

config = DocumentDBConfig(
    host="your-cluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    username="admin",
    password="secret",
    app_name="inventory-service",
    telemetry=TelemetryConfig(
        enabled=True,
        metrics_backend=MetricsBackend.LOG,
    ),
)

docdb.init(config)
client = docdb.get_client()

# All operations now emit telemetry automatically:
# [metric] docdb.commands.total += 1
# [span:start] docdb.command.find
# [span:end] docdb.command.find duration=3.2ms
# [metric] docdb.commands.duration_ms: 3.2

db = client.db("inventory")
db.products.find_one({"sku": "ABC-123"})

# Flush before shutdown to ensure all metrics are delivered
client.telemetry.flush()
docdb.shutdown()


# --- Option B: CloudWatch Metrics (production) ---
# Uncomment below for CloudWatch publishing:
#
# docdb.reset()
# config = DocumentDBConfig(
#     host="your-cluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
#     username="admin",
#     password="secret",
#     app_name="inventory-service",
#     telemetry=TelemetryConfig(
#         enabled=True,
#         metrics_backend=MetricsBackend.CLOUDWATCH,
#         cloudwatch_namespace="MyApp/DocumentDB",
#         cloudwatch_flush_interval_seconds=30,
#         default_dimensions={
#             "service": "inventory-service",
#             "environment": "prod",
#         },
#     ),
# )
# docdb.init(config)


# --- Option C: Custom backend ---
# Implement TelemetryBackend for Datadog, Prometheus, OTLP, etc.
#
# from docdb.telemetry import TelemetryBackend, MetricRecord, SpanContext
#
# class DatadogBackend(TelemetryBackend):
#     def record_metric(self, metric: MetricRecord) -> None:
#         statsd.increment(metric.name, metric.value, tags=metric.dimensions)
#     def start_span(self, span: SpanContext) -> None: ...
#     def end_span(self, span, duration_ms, error=None) -> None: ...
#     def flush(self) -> None: ...
#     def shutdown(self) -> None: ...
