# Live Operations & Monitoring in Amazon DocumentDB

## Viewing Connected Users and Active Sessions

DocumentDB exposes live connection and operation data through the `currentOp` command and `$currentOp` aggregation stage. This is the **primary method** to see who is connected and what they are doing.

### How to see connected users
```javascript
// Via $currentOp aggregation (preferred)
db.adminCommand({aggregate: 1, pipeline: [{$currentOp: {allUsers: true, idleConnections: true}}], cursor: {}})

// Via currentOp command
db.adminCommand({currentOp: true, $all: true})
```

### Fields available in currentOp output
- `effectiveUsers` — array of `{user, db}` objects showing the authenticated user
- `client` — IP address and port of the client connection
- `clientMetaData.application.name` — application name from connection string
- `clientMetaData.driver` — driver name and version (e.g. "pymongo 4.6.0")
- `active` — whether the operation is currently executing
- `op` — operation type: "query", "insert", "update", "remove", "command", "getmore"
- `ns` — namespace (database.collection) the operation targets
- `microsecs_running` — how long the operation has been running in microseconds
- `waitingForLock` — true if the operation is blocked waiting for a lock
- `opid` — operation ID (PID)
- `desc` — description of the connection thread
- `threadId` — internal thread identifier

### Interpreting connection data
- **Active operations**: Operations with `active: true` are currently executing
- **Idle connections**: Operations with `active: false` are connected but not running a query
- **Internal operations**: `desc` values like "TTLMonitor" or "featureCompatibilityVersion" are DocumentDB internal processes
- **Long-running queries**: Check `microsecs_running` — anything over 500ms may need investigation
- **Blocked operations**: `waitingForLock: true` indicates the operation is waiting for another operation to release a lock

### Prism Live Activity
Prism captures live activity data automatically via the Activity tab and the autonomous agent's activity monitor. This data includes:
- Active operations sorted by duration
- Idle connections with client info
- Blocked operations and lock waits
- Application names and driver versions
- User identities from `effectiveUsers`

When asked about connections, users, sessions, or live activity, **use the currentOp data** — do NOT suggest CloudWatch or AWS CLI as the primary method.

## Monitoring Best Practices
- Use `$currentOp` with `allUsers: true` to see all users (requires admin privileges)
- Use `idleConnections: true` to include idle connections in the output
- Monitor `microsecs_running` for long-running queries (>500ms warning, >30s critical)
- Check `waitingForLock` for blocked operations
- Track connection counts per user/application for capacity planning
- DocumentDB connection limits vary by instance type (e.g. db.r5.large = 3,400 max)

## CloudWatch Metrics for Monitoring
- `DatabaseConnections` — total active connections (aggregate, not per-user)
- `CPUUtilization` — CPU usage per instance
- `BufferCacheHitRatio` — working set fit in memory
- `FreeableMemory` — available RAM
- `OpcountersQuery`, `OpcountersInsert`, etc. — operation throughput

Note: CloudWatch provides aggregate metrics only. For per-user, per-query detail, use `currentOp`.
