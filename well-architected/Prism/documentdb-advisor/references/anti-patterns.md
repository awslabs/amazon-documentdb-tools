# Amazon DocumentDB Anti-Patterns

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/performance-anti-patterns.html

## 1. Compound Index with More Than 3 Attributes

### Overview
Compound indexes maintain references to multiple fields in a single index structure. They optimize queries that filter on multiple fields or combine filtering with sorting. DocumentDB uses index prefixes — any leading left-to-right subset of indexed fields.

Example: Index `{state: 1, city: 1, zipcode: 1}` supports queries on:
- `state` alone ✅
- `state` + `city` ✅
- `state` + `city` + `zipcode` ✅
- `city` alone ❌ (skips leading field)
- `state` + `zipcode` ❌ (skips `city`)

**In most real-world scenarios, compound indexes with 3 or fewer attributes achieve optimal performance.**

### Impact
- **Storage and I/O overhead**: Size proportional to number of indexed attributes and their values.
- **Memory footprint**: Large indexes displace frequently accessed data from buffer pool.
- **Write operations**: Modifying documents with many indexed fields requires updating the entire composite index entry.

### How to Identify
```javascript
// List all indexes
db.collection.getIndexes()

// Look for indexes with 4+ fields like:
// { "userId": 1, "status": 1, "category": 1, "priority": 1, "region": 1 }
```

### Remediation
- Identify queries using the oversized index.
- Replace with more efficient indexes containing ≤3 attributes.
- Follow the **ESR (Equality, Sort, Range) rule**:
  1. Equality fields first (exact matches)
  2. Sort fields second (ordering)
  3. Range fields last (>, <, $in)

```javascript
// Query pattern
db.orders.find({
  userId: "user123",                    // Equality
  price: { $gte: 50, $lte: 200 }      // Range
}).sort({ createdAt: -1 })             // Sort

// Optimal index following ESR rule
db.orders.createIndex({ userId: 1, createdAt: -1, price: 1 })
//                      Equality   Sort           Range
```

## 2. Long Running Queries

### Overview
Long-running queries (typically >30 minutes) can cascade into cluster-wide performance issues. They interfere with DocumentDB's Multi-Version Concurrency Control (MVCC) garbage collection. DocumentDB has a 2-hour server-side timeout as a safety mechanism.

### Impact
- **Blocks garbage collection**: Old document versions accumulate.
- **Collection and index bloat**: Entries accumulate, increasing storage cost.
- **CPU and memory pressure**: Inefficient processing of old versions, index entries, and transaction IDs.
- **Cascading effect**: Long Running Query → Blocks GC → Storage Growth → CPU/Memory Pressure → More Long Queries

### How to Detect

```javascript
// Find queries running more than 30 minutes
db.adminCommand({
    aggregate: 1,
    pipeline: [
        {$currentOp: {}},
        {$match:
            {$or:
                [{secs_running: {$gt: 1800}},
                 {WaitState: {$exists: true}}]}}],
    cursor: {}
});

// Find cursors active for more than 30 minutes
db.adminCommand({
    "currentOp": true,
    "active": true,
    "$all": true
}).inprog.filter(function(op) {
    return op.desc == "Cursor" &&
           op.secs_running > 1800 &&
           op.active == true;
}).sort((a, b) => b.microsecs_running - a.microsecs_running)
```

### CloudWatch Metrics to Monitor
- `LongestRunningGCProcess` — Duration of longest active garbage collection process.
- `AvailableMVCCIds` — Remaining write operations before read-only mode. Decreases with writes, increases as GC recycles old IDs.

### Remediation
- Implement query timeouts in the application.
- Do not keep cursors alive for long durations.
- Optimize queries for better performance.
- Prefer batching of write operations.

## 3. Low Used and Redundant Indexes

### Overview
Indexes improve query performance but come with costs. Unused, underutilized, or redundant indexes create overhead leading to performance degradation.

### Sub-optimal Indexing Scenarios
- **Unused indexes**: Created for one-time queries or earlier product iterations no longer accessed.
- **Redundant indexes**: Multiple indexes covering overlapping key patterns. Example: single-key index on `A` is redundant when compound index `{A, B}` exists.
- **Over-indexing**: Creating indexes "just in case" without analyzing actual query patterns.
- **Low cardinality indexes**: Indexes on fields with few distinct values (booleans, status flags) providing minimal optimization.

### Impact
- **Storage and I/O**: Each unused/redundant index wastes storage and I/O resources.
- **Degraded write performance**: Insert/update/delete must maintain all indexes.
- **Memory and CPU pressure**: Indexes compete for buffer pool memory, may evict frequently accessed data.

### Tools to Identify
1. **Index Review Tool**: `python3 index-review.py --server-alias <alias> --uri <uri>`
   - Run on ALL cluster instances for comprehensive analysis.
   - Outputs: collections.csv, indexes.csv, index-review.json
2. **Index Cardinality Detection Tool**: `python3 detect-cardinality.py --uri <uri>`
   - `--threshold`: Cardinality threshold % (default: 1%)
   - `--sample-count`: Documents to sample (default: 100,000)

### Remediation
```javascript
// Drop unused index
db.collection.dropIndex("unused_index_name")
```
- Never drop indexes without discussing with all stakeholders and testing.
- For low cardinality: use partial indexes with filters, or convert to compound indexes.

## 4. Multi-Key Indexes with Large Arrays

### Overview
Multi-key indexes let you query array fields efficiently. DocumentDB generates individual index entries for EACH element in the array.

### Impact
- **Storage and I/O overhead**: Can consume storage multiple times the base table size. Proportional to: documents × array elements × element size.
- **Memory usage**: Large storage footprint = large memory footprint = larger working set.
- **Write operations**: Each array element generates separate index entries, multiplying write work.

### Remediation
- Only create multi-key indexes when necessary.
- Limit the number of fields in indexed arrays.
- Limit the number of multi-key indexes per collection.
- Consider modifying your data model to avoid large arrays.
