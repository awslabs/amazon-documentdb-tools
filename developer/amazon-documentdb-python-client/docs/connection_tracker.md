# Connection Tracker

## Overview

The connection tracker monitors open connections per host and forcibly closes all connections to a host when it is removed from the topology (failover, scale-in, instance resize). Without it, stale connections can linger until `server_selection_timeout_ms` expires (30s default), causing delays during failover recovery.

This is always active and enforced at the library level.

---

## Quickstart

The connection tracker is built into every `DocumentDBClient` instance automatically.

```python
# Inspect connection state at any time:
client = docdb.get_client()
print(client.connections.total_connections)  # total open TCP connections
print(client.connections.hosts)              # {("host1", 27017): 3, ("host2", 27017): 2}
```

---

## Behavior

Two PyMongo monitoring listeners fire automatically in the background:

```text
PyMongo internal events (automatic, background)
    │
    ├─ ConnectionCreatedEvent("host1:27017")
    │     └─ tracker: connections["host1:27017"] += 1
    │
    ├─ ConnectionClosedEvent("host1:27017")
    │     └─ tracker: connections["host1:27017"] -= 1
    │
    └─ TopologyDescriptionChangedEvent
          prev: {host1, host2}  →  new: {host2}
              └─ tracker: host1 removed → reset_server("host1:27017")
                  └─ PyMongo closes all pooled connections to host1
```

The tracker does nothing visible when all instances are healthy. It silently maintains counters. You can inspect it via `client.connections.hosts` for dashboards, but it has no side effects during normal operation.

#### Failover

Without the tracker, stale connections wait 30s to time out. With the tracker, they're cleared the instant PyMongo's heartbeat detects the topology change (~10s boundary). Your app recovers in ~10s instead of ~40s.

```text
00:00  - host1 (primary) crashes
       -- (5 pooled connections to host1 are now stale)

00:10  - PyMongo heartbeat detects host1 is gone
       - TopologyDescriptionChangedEvent: host1 removed

       - ConnectionTracker.server_removed("host1:27017"):
         -- logs: "host1:27017 removed from topology, closing 5 connection(s)"
         -- calls reset_server → all 5 connections closed immediately
         -- emits: docdb.tracker.pool_reset = 5

       - Next query:
         -- Server selection picks host2 (new primary)
         -- Fresh connection opened
         -- Query succeeds immediately — no 30s wait
```

#### Brief network blip

The tracker does nothing during transient network issues. If hosts remain in the topology (both heartbeats succeed when the network recovers), no pool reset occurs. PyMongo handles individual dead sockets internally.

#### Extended outage (>30s)

If a host fails heartbeats long enough for PyMongo to remove it from the topology, the tracker clears its connections. When the host recovers and rejoins, PyMongo opens fresh connections on demand.

#### Session state after connection switches

When the tracker clears a pool and PyMongo opens new connections to a different host, no session state is lost. Parameters like read preference are properties of your Python objects (`Database`, `Collection`); they are sent per-operation in the wire protocol and don't live on the TCP connection. Amazon DocumentDB also maintains server-side session state across the cluster, so any active `ClientSession` continues seamlessly on the new connection.

#### Metrics

| Metric | When |
| --- | --- |
| `docdb.tracker.pool_reset` | Host removed from topology (value = number of connections cleared) |

---

## FAQ

### Does it interfere with normal connection pool management?

No. It only acts on topology changes (host removal). PyMongo's built-in pool management (idle timeout, max pool size) continues to operate independently.

### What about multi-threading?

All counter operations are protected by a `threading.Lock`. The tracker is safe for concurrent access from multiple threads sharing the same `DocumentDBClient`.
