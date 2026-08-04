# Query Plan Analysis and Troubleshooting

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/user_diagnostics.html

## Viewing a Query Plan with explain()

Use `explain` to understand how DocumentDB executes a query and whether it uses indexes.

```javascript
db.runCommand({explain: {
    aggregate: "sample-document",
    pipeline: [{$match: {x: {$eq: 1}}}],
    cursor: {batchSize: 1}
}});
```

### Key Stages in explain() Output
- **COLLSCAN**: Full collection scan — no index used. Performance can be improved with an index.
- **IXSCAN**: Index scan — query uses an index efficiently.
- **NESTED_LOOP_LOOKUP**: $lookup using nested loop algorithm.
- **SORT_LOOKUP**: $lookup using sort merge algorithm.
- **HASH_LOOKUP**: $lookup using hash algorithm.
- **SHARD_MERGE**: Elastic cluster merging results across shards.
- **PARTITION_MERGE**: Elastic cluster merging results across partitions within a shard.

### Important Notes
- DocumentDB `explain()` output DIFFERS from MongoDB — it's a purpose-built engine.
- Use `$hint` operator to force selection of a preferred index.
- For small collections, the query processor may choose COLLSCAN even with an index if performance gains are negligible.

### Forcing Index Usage
```javascript
db.collection.find().hint("indexName")
// or
db.collection.find().hint({fieldName: 1})
```

## Collection and Index Statistics

### Collection Stats
```javascript
db.collection.stats()
```
Returns:
- `count`: Document count
- `size`: Uncompressed data size
- `storageSize`: On-disk storage size
- `avgObjSize`: Average document size
- `nindexes`: Number of indexes
- `totalIndexSize`: Total index size
- `collScans`: Number of collection scans (no index)
- `idxScans`: Number of index scans
- `opCounter`: {numDocsIns, numDocsUpd, numDocsDel}
- `cacheStats`: {collBlksHit, collBlksRead, collHitRatio, idxBlksHit, idxBlksRead, idxHitRatio}
- `unusedStorageSize`: {unusedBytes, unusedPercent} — bloat indicator
- `compression`: {enable, threshold}

### Index Stats
```javascript
db.collection.aggregate([{$indexStats:{}}]).pretty()
```
Returns per index:
- `name`: Index name
- `key`: Index fields
- `size`: Index size in bytes
- `accesses.ops`: Number of operations using this index (0 = potentially unused)
- `accesses.docsRead`: Documents read via this index
- `accesses.since`: When stats collection started
- `cacheStats`: {blksHit, blksRead, blksHitRatio}

## Finding and Terminating Long Running Queries

### List Long Running or Blocked Queries
```javascript
db.adminCommand({
    aggregate: 1,
    pipeline: [
        {$currentOp: {}},
        {$match:
            {$or: [
                {secs_running: {$gt: 10}},
                {WaitState: {$exists: true}}]}},
        {$project: {_id:0, opid: 1, secs_running: 1, WaitState: 1, blockedOn: 1, command: 1}}],
    cursor: {}
});
```

### Terminate a Query
```javascript
db.adminCommand({killOp: 1, op: <opid>});
```

### Understanding WaitState Values
- **IO**: I/O bottleneck — too many concurrent queries or instance too small for dataset.
- **CollectionLock**: Blocked by another operation holding a collection lock.
- **Latch, SystemLock, BufferLock, BackgroundActivity, Other**: Internal system task contention.

### Mitigation
- For I/O bottleneck: separate queries across replicas, or scale up instance.
- For collection lock: follow the `blockedOn` chain to find the blocking query.
- For internal tasks: terminate and retry later.

## Determining Collection Bloat

```javascript
db.runCommand({collStats: 'collectionName'})
```

Key fields:
- `unusedStorageSize.unusedBytes`: Wasted space from dead/obsolete documents.
- `unusedStorageSize.unusedPercent`: Percentage of bloat.

To remove bloat: reload collections via dump/restore or migration loop-back during maintenance window.

## Listing All Running Operations
```javascript
db.adminCommand({currentOp: 1, $all: 1});
```

Operation types (`desc` field):
- `INTERNAL`: Internal system tasks
- `TTLMonitor`: TTL monitor thread
- `GARBAGE_COLLECTION`: Internal garbage collector
- `CONN`: User query
- `CURSOR`: Idle cursor waiting for getMore

## Aggregated System State View
```javascript
db.adminCommand({
    aggregate: 1,
    pipeline: [
        {$currentOp: {allUsers: true, idleConnections: true}},
        {$group: {_id: {desc: "$desc", ns: "$ns", WaitState: "$WaitState"}, count: {$sum: 1}}}],
    cursor: {}
});
```

## Identifying Missing Indexes

1. Enable the DocumentDB profiler to log slow queries.
2. Look for queries with `COLLSCAN` in `planSummary`.
3. Create indexes on the filter fields.
4. Use `{background: true}` when creating indexes on production clusters to avoid exclusive write locks.
5. Target high cardinality fields (many unique values).
6. Use `hint()` to force index usage if the optimizer chooses COLLSCAN despite an index existing.

## Open Cursors
```javascript
db.runCommand("listCursors")
```
Limit: up to 4,560 active cursors per instance (varies by instance type). Close unused cursors.
