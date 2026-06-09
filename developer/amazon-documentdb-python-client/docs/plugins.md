# Plugin System

## Overview

The plugin system intercepts collection operations (find, insert, update, delete, aggregate) and routes them through a composable chain of middleware without changing your application code.

---

## Quickstart

```python
from docdb import DocumentDBConfig, PluginConfig

config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    app_name="my-service",
    plugins=[
        PluginConfig("retry"),
    ],
)
```

---

## Configuration

### Built-in plugins

| Plugin | Code | Weight | What it does |
| --- | --- | --- | --- |
| [TelemetryPlugin](telemetry.md) | `"telemetry"` | 100 | Operation-level spans and duration metrics (auto-injected when telemetry is enabled) |
| [RetryPlugin](retry.md) | `"retry"` | 500 | Retries failed operations with exponential backoff and jitter |
| DefaultPlugin | `"_default"` | 1000 | Calls the PyMongo method directly (always last) |

### Method subscription

Plugins can intercept all operations or declare exactly what they care about:

```python
def subscribed_methods(self):
    return frozenset({"find", "find_one", "aggregate"})  # e.g. reads only
```

Operations not in your set bypass your plugin entirely so there is no overhead, not even a function call.

### What's intercepted

The 16 most common collection operations go through the plugin chain:

`find`, `find_one`, `insert_one`, `insert_many`, `update_one`, `update_many`, `replace_one`, `delete_one`, `delete_many`, `aggregate`, `bulk_write`, `find_one_and_update`, `find_one_and_replace`, `find_one_and_delete`, `count_documents`, `distinct`

Administrative methods like `create_index`, `list_indexes`, `rename` pass directly to PyMongo.

---

## Architecture

```text
db.orders.find_one({"_id": "123"})
         │
         ▼
┌────────────────────────────────────────────┐
│  Plugin Chain (sorted by weight)           │
│                                            │
│  TelemetryPlugin                           │  ← measures total time
│      → next ─┐                             │
│              ▼                             │
│  RetryPlugin                               │  ← catches errors, retries
│      → next ─┐                             │
│              ▼                             │
│  DefaultPlugin                             │  ← calls PyMongo
│      → collection.find_one({"_id": "123"}) │
└────────────────────────────────────────────┘
         │
         ▼
       Result
```

| Property | What it means | Why it matters |
| --- | --- | --- |
| **Weight-based ordering** | Lower weight = runs first | Add your plugin at any weight and it slots in automatically. |
| **Method subscription** | Each plugin declares what it intercepts | A retry plugin subscribes to reads only. Write operations skip it entirely with zero overhead. |
| **Pre-built dispatch** | Chain is assembled once at startup | Per-query cost is a single dict lookup + closure call. No iteration or filtering on the hot path. |
| **Transparent proxy** | `client.db()` returns a proxy that looks identical to `pymongo.Database` | Your code, tests, and IDE autocomplete all work unchanged. |

### OperationContext

Every intercepted call receives an `OperationContext`:

```python
@dataclass
class OperationContext:
    method: str                 # e.g. "find_one", "insert_one"
    collection_name: str        # e.g. "orders"
    database_name: str          # e.g. "shop"
    args: tuple                 # positional args to the PyMongo method
    kwargs: dict[str, Any]      # keyword args
    attributes: dict[str, Any]  # inter-plugin communication
```

`attributes` is a free-form dict for plugins to communicate. The key `_pymongo_collection` holds the PyMongo Collection.

| Capability | How |
| --- | --- |
| **Observe** | Record timing, log operations, emit metrics |
| **Modify** | Change arguments before they reach PyMongo, transform results after |
| **Retry** | Catch errors from `next_fn`, re-invoke it with backoff |
| **Short-circuit** | Return a cached result without calling `next_fn` at all |
| **Communicate** | Set `ctx.attributes["key"]` for downstream plugins to read |

### Writing a custom plugin

```python
from docdb import BaseConnectionPlugin, PluginConfig, register_plugin

class AuditPlugin(BaseConnectionPlugin):
    """Logs every database operation."""

    @property
    def plugin_code(self):
        return "audit"

    @property
    def weight(self):
        return 150  # runs after telemetry (100), before retry (500)

    def subscribed_methods(self):
        return frozenset({"*"})  # all operations

    def execute(self, ctx, next_fn):
        print(f"[audit] {ctx.collection_name}.{ctx.method}()")
        result = next_fn(ctx)  # call the next plugin in chain
        print(f"[audit] completed")
        return result

# Register so it can be referenced by name in config
register_plugin("audit", lambda **opts: AuditPlugin())

# Enable it
config = DocumentDBConfig(
    host="...",
    plugins=[PluginConfig("audit")],
)
```

Plugins that hold resources should implement a `close()` method. It will be called automatically when `docdb.shutdown()` is invoked.

---

## FAQ

### Do plugins add latency to every query?

The per-query overhead is a dict lookup and a closure call, an impact of nanoseconds. The chain is pre-built at startup, not assembled per-operation. If a plugin doesn't subscribe to a method, it's not in that method's chain.

### Can I use multiple plugins together?

Yes. The weight system handles ordering automatically. You don't need to wire them manually. Each plugin is independent and communicates only through `ctx.attributes` if needed.

### What if I want to intercept administrative operations (create_index, etc.)?

Those bypass the proxy for performance reasons. Use PyMongo's `event_listeners` directly via `extra_options` if you need to observe them, or access the raw client via `client.raw`.
