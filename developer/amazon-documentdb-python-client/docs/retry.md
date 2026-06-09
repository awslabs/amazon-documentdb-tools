# Retry Plugin

## Overview

Without retries, transient network errors (connection resets during failover, brief DNS resolution failures, momentary overloads) surface directly to your application. The retry plugin handles this automatically with exponential backoff and jitter.

---

## Quickstart

```python
from docdb import DocumentDBConfig, PluginConfig

config = DocumentDBConfig(
    host="mycluster.cluster-abc123.us-east-1.docdb.amazonaws.com",
    app_name="order-service",
    plugins=[PluginConfig("retry")],
)
```

---

## Configuration

| Option | Default | Description |
| --- | --- | --- |
| `max_attempts` | 3 | Total attempts (1 initial + 2 retries) |
| `base_delay_ms` | 100 | Base delay for exponential backoff |
| `max_delay_ms` | 5000 | Maximum delay cap |
| `retry_methods` | reads only | Which methods to retry. Add write methods only if they're idempotent. |

```python
PluginConfig("retry", options={
    "max_attempts": 5,
    "base_delay_ms": 200,
    "max_delay_ms": 10000,
    "retry_methods": ["find", "find_one", "insert_one", "update_one"],
})
```

Default read methods retried: `find`, `find_one`, `count_documents`, `distinct`, `aggregate`.

---

## Behavior

Only PyMongo transient errors trigger a retry:

- `AutoReconnect` — connection dropped mid-operation
- `ConnectionFailure` — cannot establish connection
- `NetworkTimeout` — operation timed out at network level
- `ServerSelectionTimeoutError` — no suitable server found within timeout

Application errors (`DuplicateKeyError`, `ValidationError`, etc.) propagate immediately.

### Retries reads only by default

Retrying writes is unsafe unless your writes are idempotent. For example, your application calls `insert_one({"customer_id": auto_generated, "amount_owed": 100})`. The document is written successfully, but the acknowledgment is lost due to a network error. The retry would then insert a duplicate document for the specified `customer_id`.

### Backoff strategy

Full jitter (per [AWS Builders' Library](https://aws.amazon.com/builders-library/timeouts-retries-and-backoff-with-jitter/)):

```text
delay = random(0, min(base_delay_ms * 2^attempt, max_delay_ms))
```

Each retry picks a random delay between 0 and the exponential ceiling. This prevents "thundering herds" when multiple clients fail simultaneously during failover.

| Attempt | Max possible delay |
| --- | --- |
| 1st retry | 0–100ms |
| 2nd retry | 0–200ms |
| 3rd retry | 0–400ms |
| 4th retry | 0–800ms |
| 5th retry | 0–1600ms |
| 6th+ retry | 0–5000ms (capped) |

### Retrying writes safely

Idempotent (safe to retry):

- `update_one` with `$set` — setting a field to the same value twice is harmless
- `insert_one` with a deterministic `_id` — duplicate insert fails with `DuplicateKeyError` (which is not a retryable error, so it propagates immediately)
- `delete_one` — deleting an already-deleted document is a no-op

Not idempotent (do not retry):

- `insert_one` with auto-generated `_id` — retry creates a duplicate
- `update_one` with `$inc` — retry increments twice
- `insert_many` without explicit `_id` values — partial retry duplicates some documents

For non-idempotent writes, use application-level deduplication keys rather than automatic retries.

### Interaction with other plugins

The retry plugin runs inside the telemetry plugin because it has a higher weight. Telemetry measures total time including retries, giving you visibility into the full user-side latency.

### Session state across retries

When a retry connects to a different host (e.g., after failover to a new primary), your driver parameters (like read preference) are preserved automatically. These settings are sent per-operation in the wire protocol since they're properties of your Python objects (`Database`, `Collection`), not of the physical TCP connection. Amazon DocumentDB also maintains server-side session state across the cluster, so `ClientSession` continuity is preserved even when the underlying connection changes. A retry to a new host behaves identically to the original attempt and no session state is lost.

### Metrics emitted

| Metric | When |
| --- | --- |
| `docdb.retry.triggered` | Each retry attempt (transient error caught) |
| `docdb.retry.exhausted` | All attempts failed, error propagated to application |

---

## FAQ

### Will retries hide real failures from me?

No. If all retry attempts fail, the original error propagates to your application code. The `docdb.retry.exhausted` metric fires so you see it in CloudWatch. Retries only mask *transient* issues that resolve quickly.

### Should I retry writes?

Only if they're idempotent. If you use deterministic `_id` values or `$set` operations, it's safe. If you use `$inc` or auto-generated IDs, don't.

### What if the retry succeeds but takes too long?

The telemetry plugin wraps the retry plugin, so your `docdb.commands.duration_ms` metric shows the full latency including retry delays. You'll see the spike in your dashboard.
