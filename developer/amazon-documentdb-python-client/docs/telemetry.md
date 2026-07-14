# Telemetry Plugin

## Overview

The telemetry system adds automatic observability to every Amazon DocumentDB operation without changing application code. When enabled, it hooks into PyMongo's built-in monitoring system and translates raw events into structured metrics and traces.

```terminal
Your App Code (find, insert, etc.)
        │
        ▼
┌─────────────────────────────────┐
│  PyMongo MongoClient            │  ← already emits internal events
│  (CommandStarted/Succeeded/     │
│   Failed, Heartbeat, Topology,  │
│   Pool events)                  │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Telemetry listeners            │  ← translate events into metrics/spans
│  (CommandListener, Heartbeat,   │
│   Topology, Pool)               │
└────────────┬────────────────────┘
             │
             ▼
┌─────────────────────────────────┐
│  Backend (LOG or CloudWatch)    │  ← decides where the data goes
└─────────────────────────────────┘
```

---

## Quickstart

```python
from docdb import DocumentDBConfig
from docdb.telemetry import TelemetryConfig, MetricsBackend

# Log to stdout
config = DocumentDBConfig(
    host="your-cluster.docdb.amazonaws.com",
    app_name="my-service",
    telemetry=TelemetryConfig(enabled=True),
)

# Publish to CloudWatch
config = DocumentDBConfig(
    host="your-cluster.docdb.amazonaws.com",
    app_name="my-service",
    telemetry=TelemetryConfig(
        enabled=True,
        metrics_backend=MetricsBackend.CLOUDWATCH,
        default_dimensions={"environment": "prod"},
    ),
)
```

Example LOG output:

```bash
10:09:27 docdb.telemetry | [metric] docdb.commands.total += 1
10:09:27 docdb.telemetry | [span:start] docdb.command.insert
10:09:27 docdb.telemetry | [span:end] docdb.command.insert duration=2.1ms
10:09:27 docdb.telemetry | [metric] docdb.connections.opened += 1
10:09:37 docdb.telemetry | [metric] docdb.heartbeat.duration_ms: 0.8
```

---

## Configuration

| Parameter | Default | Description |
| --- | --- | --- |
| `enabled` | `False` | Main switch. When False, all telemetry is disabled. |
| `metrics_backend` | `MetricsBackend.LOG` | Where metrics go - `LOG` (stdout) or `CLOUDWATCH` |
| `cloudwatch_namespace` | Auto-derived | CloudWatch namespace (default `DocumentDB/{cluster-id}` from host) |
| `cloudwatch_flush_interval_seconds` | `60` | How often buffered CloudWatch metrics are flushed |
| `cloudwatch_client` | `None` | Optional pre-configured boto3 CloudWatch client |
| `default_dimensions` | `{}` | Dimensions added to every metric (e.g., `{"environment": "prod"}`) |
| `enabled_listeners` | `{"command", "heartbeat", "topology", "pool"}` | Which PyMongo listeners are active |
| `emit_spans` | `True` | Whether to emit span start/end events |
| `log_level` | `DEBUG` | Log level for the LOG backend |

Auto-derived dimensions (always present):

- `service` — from `app_name`
- `cluster` — from the host endpoint

### CloudWatch prerequisites

| Requirement | Details |
| --- | --- |
| IAM | `cloudwatch:PutMetricData` permission on your app's role |
| pip | `pip install 'amazon-documentdb-python-client[cloudwatch]'` |
| Region | Via `AWS_DEFAULT_REGION`, instance metadata, or explicit client |
| CloudWatch console | Namespaces and metrics auto-create on first write |

### Bring your own boto3 client

```python
import boto3

cw_client = boto3.client("cloudwatch", region_name="us-west-2")

telemetry=TelemetryConfig(
    enabled=True,
    metrics_backend=MetricsBackend.CLOUDWATCH,
    cloudwatch_client=cw_client,
)
```

---

## Behavior

### Metrics emitted

| Metric | Source | When it fires |
| --- | --- | --- |
| `docdb.commands.total` | Telemetry listener | Every command executed |
| `docdb.commands.failed` | Telemetry listener | Command returns an error |
| `docdb.commands.duration_ms` | Telemetry listener | Command completes (success or failure) |
| `docdb.heartbeat.duration_ms` | Telemetry listener | Every heartbeat probe (~10s per node) |
| `docdb.heartbeat.failed` | Telemetry listener | Heartbeat probe fails |
| `docdb.connections.opened` | Telemetry listener | New TCP connection created |
| `docdb.connections.closed` | Telemetry listener | Connection closed (with reason dimension) |
| `docdb.topology.changed` | Telemetry listener | Server added or removed from replica set |
| `docdb.tracker.pool_reset` | Connection tracker | Host removed from topology, connections cleared |
| `docdb.retry.triggered` | Retry plugin | Each retry attempt on a transient error |
| `docdb.retry.exhausted` | Retry plugin | All retry attempts failed |

### CloudWatch dimension groups

Metrics appear under namespace `DocumentDB/{cluster-id}`:

```text
┌─────────────────────────────────────────────────────────────┐
│ command, cluster, environment, service                      │
│   docdb.commands.total                                      │
│   docdb.commands.duration_ms                                │
│   docdb.commands.failed                                     │
├─────────────────────────────────────────────────────────────┤
│ cluster, environment, service                               │
│   docdb.connections.opened                                  │
│   docdb.heartbeat.duration_ms                               │
│   docdb.heartbeat.failed                                    │
│   docdb.topology.changed                                    │
├─────────────────────────────────────────────────────────────┤
│ cluster, environment, reason, service                       │
│   docdb.connections.closed                                  │
└─────────────────────────────────────────────────────────────┘
```

### Heartbeats

PyMongo runs a background thread that pings every server in the replica set every 10 seconds. This is how the driver discovers which nodes are alive, which is primary, and which are secondaries.

```text
PyMongo background thread
    │ every 10s, sends "hello" command to each node
    │
    ├─ Node responds → ServerHeartbeatSucceededEvent
    │   └─ Plugin emits: docdb.heartbeat.duration_ms = 1.2
    │
    └─ Node doesn't respond → ServerHeartbeatFailedEvent
        └─ Plugin emits: docdb.heartbeat.failed += 1
                    docdb.heartbeat.duration_ms = 5000.0 (timeout value)
```

| Scenario | What happens | Metric pattern |
| --- | --- | --- |
| Primary failover | Old primary stops responding, new primary elected | Spike of `heartbeat.failed` for ~30s, then recovers |
| Instance reboot | One instance goes offline temporarily | `heartbeat.failed` spikes for that instance's 10s-intervals until it's back |
| Network partition | VPC routing issue between app and a replica | Sustained `heartbeat.failed` — triggers investigation |
| Instance removed | Instance leaves replica set | Brief `heartbeat.failed` until PyMongo removes it from topology |

Heartbeat failures show up in CloudWatch *before* your application queries fail. If you see `heartbeat.failed` spiking but `commands.failed` is flat, it means PyMongo is routing around the bad instance successfully. If both spike together, the cluster should be reviewed.

### Failed commands

Any command (find, insert, update, aggregate, etc.) that PyMongo reports as failed at the wire protocol level. This is *not* "zero results found", it's an actual error response from the server or a network failure during the command.

```text
Your code: db.orders.insert_one({"_id": "..."})
    │
    PyMongo sends insert command to Amazon DocumentDB
    │
    Amazon DocumentDB returns error (e.g., duplicate key)
    │
    PyMongo fires CommandFailedEvent
    │
    Plugin emits:
        docdb.commands.failed += 1       {command: "insert"}
        docdb.commands.duration_ms = 3.2 {command: "insert"}
```

| Scenario | Error type | Example |
| --- | --- | --- |
| Duplicate key | `DuplicateKeyError` | Retry logic inserts the same `_id` twice |
| Write to a secondary | `NotWritablePrimary` | App connects to reader endpoint and attempts a write |
| Command timeout | `ExecutionTimeout` | Long-running aggregation exceeds `maxTimeMS` |
| Auth failure | `AuthenticationFailed` | Credentials rotated but app not restarted |
| Exceeding Amazon DocumentDB limits | `ExceededMemoryLimit` | Aggregation pipeline uses too much RAM |
| Network error mid-command | `NetworkTimeout` | Connection drops after command sent but before response received |
| Collection doesn't exist | `NamespaceNotFound` | Code references a dropped collection |

**What it does NOT capture:**

- A `find()` returning zero documents. This is a successful command with empty results.
- Application-level validation errors (occur before the command is sent)
- Slow queries - those succeed (tracked by `duration_ms` instead)

### Failover scenario

In CloudWatch, you'd see `heartbeat.failed` spike 15 seconds before `commands.failed`, giving you early warning that the cluster is degraded before users are impacted.

```text
Timeline:
  00:00  Normal operation
         heartbeat.failed = 0, commands.failed = 0

  00:15  Primary instance starts failing
         heartbeat.failed += 1 (every 10s probe fails)
         heartbeat.duration_ms spikes to 5000ms (timeout)
         commands.failed still 0 (PyMongo hasn't tried primary yet)

  00:25  App sends a write → routed to old primary → fails
         commands.failed += 1 {command: "insert"}
         PyMongo triggers server selection, waits for new primary

  00:30  New primary elected, heartbeat succeeds
         heartbeat.failed drops to 0
         heartbeat.duration_ms returns to ~1ms
         commands resume succeeding

  00:35  Fully recovered
         commands.failed = 0, all metrics normal
```

### Batching

The CloudWatch backend buffers metric data points and flushes them in batches of 20 (CloudWatch's `put_metric_data` limit per call).

1. Each `record_metric()` call adds to an in-memory buffer
2. If the buffer hits 20 items, it flushes immediately
3. Otherwise a background thread flushes every `flush_interval_seconds`
4. On `client.close()` / `docdb.shutdown()`, remaining metrics are flushed

---

## FAQ

### When should I tune `enabled_listeners` or `emit_spans`?

The defaults work for the vast majority of services. These options exist for high-throughput production services (10,000+ QPS) where have metric data or large log volumes you're not looking at. See the tuning guide below.

### Dev → Test → Prod tuning guide

| Stage | `enabled_listeners` | `emit_spans` | Why |
| --- | --- | --- | --- |
| Dev | all (default) | `True` (default) | See everything, learn the system |
| Test | all (default) | `True` (default) | Validate metrics appear correctly in CloudWatch |
| Prod (normal) | all (default) | `False` | Metrics to CloudWatch, no log noise |
| Prod (high-throughput) | heartbeat, topology, pool | `False` | Infrastructure visibility only, minimal overhead |
| Prod (incident investigation) | all | `True` | Temporarily re-enable everything to diagnose |

### What's the overhead?

Per-query cost is a dict lookup and a deque append. The background flush thread handles CloudWatch API calls.
