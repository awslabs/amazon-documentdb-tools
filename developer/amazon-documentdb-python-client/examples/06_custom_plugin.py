"""
Example: Writing and registering a custom plugin

This example shows how to create a plugin that:
1. Logs every operation with timing
2. Adds retry logic for reads
3. Integrates into the plugin chain via registration + config

Plugins are composabl. You can stack multiple plugins and they
execute in weight order (lower weight = outermost = runs first).
"""

import sys
import time

sys.path.insert(0, "src")

from docdb import (
    BaseConnectionPlugin,
    DocumentDBConfig,
    PluginConfig,
    register_plugin,
)
from docdb.plugins.base import WILDCARD, OperationContext


# ─── Example 1: Simple audit/logging plugin ─────────────────────────────────

class AuditPlugin(BaseConnectionPlugin):
    """Logs every intercepted operation with timing."""

    @property
    def plugin_code(self):
        return "audit"

    @property
    def weight(self):
        return 150  # runs early (after telemetry at 100)

    def subscribed_methods(self):
        return frozenset({WILDCARD})  # observe all operations

    def execute(self, ctx, next_fn):
        start = time.perf_counter()
        print(f"  [audit] {ctx.database_name}.{ctx.collection_name}.{ctx.method}()")
        try:
            result = next_fn(ctx)
            duration = (time.perf_counter() - start) * 1000
            print(f"  [audit] ✓ completed in {duration:.1f}ms")
            return result
        except Exception as exc:
            duration = (time.perf_counter() - start) * 1000
            print(f"  [audit] ✗ failed in {duration:.1f}ms: {exc}")
            raise


# Register the plugin so it can be referenced by code string
register_plugin("audit", lambda **opts: AuditPlugin())


# ─── Example 2: Retry plugin for reads ──────────────────────────────────────

class RetryPlugin(BaseConnectionPlugin):
    """Retries failed read operations with exponential backoff."""

    READ_METHODS = frozenset({
        "find", "find_one", "aggregate", "count_documents", "distinct",
    })

    def __init__(self, max_attempts: int = 3, base_delay_ms: float = 100):
        self._max_attempts = max_attempts
        self._base_delay_ms = base_delay_ms

    @property
    def plugin_code(self):
        return "retry"

    @property
    def weight(self):
        return 500  # runs in the middle

    def subscribed_methods(self):
        return self.READ_METHODS  # only intercept reads

    def execute(self, ctx, next_fn):
        last_error = None
        for attempt in range(self._max_attempts):
            try:
                result = next_fn(ctx)
                if attempt > 0:
                    print(f"  [retry] succeeded on attempt {attempt + 1}")
                return result
            except Exception as exc:
                last_error = exc
                if attempt < self._max_attempts - 1:
                    delay = self._base_delay_ms * (2 ** attempt) / 1000
                    print(f"  [retry] attempt {attempt + 1} failed, "
                          f"retrying in {delay * 1000:.0f}ms...")
                    time.sleep(delay)
        raise last_error


# Register with configurable options
register_plugin("retry", lambda **opts: RetryPlugin(
    max_attempts=opts.get("max_attempts", 3),
    base_delay_ms=opts.get("base_delay_ms", 100),
))


# ─── Usage ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import docdb

    # Configure with plugins, auto-sorted by weight. E.g.:
    #   telemetry (100) → audit (150) → retry (500) → DefaultPlugin (1000)
    config = DocumentDBConfig(
        host="localhost",
        port=27017,
        tls=False,
        app_name="plugin-demo",
        plugins=[
            PluginConfig("audit"),
            PluginConfig("retry", options={"max_attempts": 3}),
        ],
    )

    docdb.init(config)
    client = docdb.get_client()

    print("\n" + "=" * 60)
    print("  PLUGIN DEMO")
    print("  Chain: DefaultPlugin ← retry ← audit")
    print("=" * 60 + "\n")

    db = client.db("demo")

    print(">>> find_one (goes through audit + retry + default):")
    try:
        db.orders.find_one({"_id": "test-123"})
    except Exception as e:
        print(f"  Expected error (no DB running): {type(e).__name__}\n")

    print(">>> insert_one (goes through audit + default, skips retry):")
    try:
        db.orders.insert_one({"name": "widget"})
    except Exception as e:
        print(f"  Expected error (no DB running): {type(e).__name__}\n")

    print(">>> create_index (NOT intercepted — goes directly to PyMongo):")
    try:
        db.orders.create_index([("name", 1)])
    except Exception as e:
        print(f"  Expected error (no DB running): {type(e).__name__}\n")

    docdb.shutdown()
    print("Done.")
