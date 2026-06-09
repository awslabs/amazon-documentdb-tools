# Overview

## Why this exists

Document databases are compelling because they match how developers think: 

- store JSON
- no schema
- iterate fast

MongoDB makes this even easier: no authentication required locally, no setup ceremony, just connect and start inserting. Especially with GenAI coding tools like Claude, you can go from zero to a working app in minutes.

But the simplicity that makes development fast masks the architectural decisions you'll need at scale. You build locally with no auth, no TLS, no connection pooling, no replica awareness and it works. So you promote to test, then to production, and it still works...until it doesn't.

Maybe you discover that your connection pool is exhausting the cluster because every Lambda invocation opens a new client. Or a primary failover takes down your service because the driver didn't know it was in a replica set. These aren't edge cases, they're the natural consequence of patterns that work perfectly at development scale but fail at production scale.

By the time you discover them, you're patching a production environment live on a Friday night.

**This library puts you in the right position from the start.** 

Dev to test to prod is seamless because the same best practices are enforced from your first line of code. You're well-architected before you even begin without needing to know the details.

#### What is enforced automatically

| Parameter | Value | Why |
| --- | --- | --- |
| `replicaSet` | `rs0` | Required for replica set routing and failover. |
| `directConnection` | `False` | Guards against connecting to a single node. |
| `retryWrites` | `False` | PyMongo defaults this to `True`. |
| Connection tracker | Always active | Forcibly closes connections to hosts removed from topology. |

#### Defaults (can be overridden via `DocumentDBConfig`)

| Parameter | Default | Why |
| --- | --- | --- |
| `tls` | `True` | Enabled by default on Amazon DocumentDB clusters. |
| `read_preference` | `secondaryPreferred` | Routes reads to replicas, reducing primary load. |
| `max_pool_size` | `100` | Sized for typical workloads. |
| `min_pool_size` | `5` | Keeps connections warm, reduces cold-start latency. |
| `max_idle_time_ms` | `10000` | Closes idle connections to prevent stale accumulation. |
| `server_selection_timeout_ms` | `30000` | Configured to handle a failover (~30 seconds). |
| `connect_timeout_ms` | `10000` | Timeout for new TCP connections. |

---

## How it works — two layers

### Layer 1 - Safe defaults

Under the hood, the wrapper enforces `replicaSet=rs0`, `directConnection=False`, `retryWrites=False`, proper TLS, connection pooling, and other best practices for Amazon DocumentDB. Your code reads exactly like standard PyMongo.

```python
import docdb
from docdb import DocumentDBConfig

config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    username="appuser",
    password="changeme",
    app_name="my-service",
)
docdb.init(config)

client = docdb.get_client()
db = client.db("mydb")
db.orders.find_one({"_id": order_id})
```

### Layer 2 - Plugin chain

When you need cross-cutting behavior such as logging, retries, telemetry, you can add plugins that transparently intercept operations:

```python
from docdb import DocumentDBConfig, PluginConfig
from docdb.telemetry import TelemetryConfig, MetricsBackend

config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    app_name="order-service",
    telemetry=TelemetryConfig(enabled=True, metrics_backend=MetricsBackend.CLOUDWATCH),
    plugins=[
        PluginConfig("retry"),
    ],
)
```

Your queries stay the same (`db.orders.find_one(...)`) but now every operation automatically gets telemetry and retry logic without touching your business code.

---

## Plugin design

Without plugins, retry logic is scattered across every file that talks to the database:

```python
for attempt in range(3):
    try:
        result = db.orders.find_one({"_id": oid})
        break
    except (AutoReconnect, ConnectionFailure):
        if attempt == 2:
            raise
        time.sleep(0.1 * 2**attempt)
```

Multiply that by every query in your service. Now add timing. Now add audit logging. Each concern wraps the others, and the nesting gets unmanageable.

With the retry plugin, all of that goes away. The retry logic lives in the plugin, configured once:

```python
plugins=[PluginConfig("retry")]
```

...and you just run your code like normal

```python
db.orders.find_one({"_id": oid})
```

The modular design allows you to add your own plugins (see [docs/plugins.md](plugins.md)).

---

## Performance

The wrapper is designed for zero measurable overhead. All Python-side work (proxy dispatch, plugin chain, telemetry recording, connection tracking) operates in the single-digit microsecond range which disappears into the noise of a typical network round-trip to Amazon DocumentDB (1–50ms depending on your VPC topology).

---

## Compatibility

- Python 3.10+
- PyMongo 4.6+ (tested against 4.x; pinned below 5.0)
- The wrapper does not introduce any incompatibility beyond what already exists between PyMongo and Amazon DocumentDB

---

## Project structure

```text
├── pyproject.toml                          # Package config, dependencies, extras
├── src/docdb/
│   ├── __init__.py                         # Public API (init, get_client, shutdown)
│   ├── client.py                           # DocumentDBClient singleton, MongoClient construction
│   ├── config.py                           # DocumentDBConfig dataclass
│   ├── connection_tracker.py               # Always-on connection tracking + pool cleanup
│   ├── cursor.py                           # managed_cursor, find_all helpers
│   ├── secrets.py                          # AWS Secrets Manager config loader
│   ├── plugins/
│   │   ├── __init__.py                     # Plugin public API exports
│   │   ├── base.py                         # ConnectionPlugin ABC, OperationContext
│   │   ├── chain.py                        # PluginChainBuilder, PluginPipeline
│   │   ├── proxy.py                        # PluginAwareDatabase, PluginAwareCollection
│   │   ├── registry.py                     # Plugin registry (register_plugin)
│   │   └── builtin/
│   │       ├── __init__.py                 # Built-in plugin registration
│   │       ├── default_plugin.py           # Terminal plugin (calls real PyMongo)
│   │       ├── retry_plugin.py             # Retry with exponential backoff
│   │       └── telemetry_plugin.py         # Operation-level spans and metrics
│   └── telemetry/
│       ├── __init__.py                     # Telemetry public API
│       ├── backend.py                      # TelemetryBackend ABC, NoOpBackend
│       ├── cloudwatch_backend.py           # CloudWatch metrics publisher
│       ├── config.py                       # TelemetryConfig, build_backend
│       ├── listeners.py                    # PyMongo event listeners
│       ├── log_backend.py                  # Structured logging backend
│       └── types.py                        # Metric names, enums, SpanContext
├── docs/                                   # Detailed documentation
└── examples/                               # Runnable demos and integration patterns
```
