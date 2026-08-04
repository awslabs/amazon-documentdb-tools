# Functional Differences: Amazon DocumentDB and MongoDB

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html

## Functional Benefits of Amazon DocumentDB

### Implicit Transactions
- All CRUD statements (findAndModify, update, insert, delete) guarantee atomicity and consistency, even for multi-document operations.
- Amazon DocumentDB 4.0+ supports explicit transactions with ACID properties for multi-statement and multi-collection operations.
- Individual operations within bulk operations (updateMany, deleteMany) are atomic, but the entire bulk operation is NOT atomic.
- For ACID guarantees on bulk operations, use explicit transactions.

## Key Functional Differences with MongoDB

### $vectorSearch
- DocumentDB does NOT support `$vectorSearch` as an independent operator.
- Instead, use `vectorSearch` inside the `$search` operator.

### Admin Databases and Collections
- DocumentDB does NOT support the `admin` or `local` database.
- Does NOT support `system.*` or `startup_log` collections.

### cursor.maxTimeMS
- In DocumentDB, `cursor.maxTimeMS` resets the counter for each `getMore` request.
- A cursor only times out when a single operation (query or individual getMore) exceeds the specified maxTimeMS.
- The sweeper that checks cursor execution time runs at 5-minute granularity.

### explain()
- DocumentDB emulates MongoDB APIs on a purpose-built database engine.
- Query plans and `explain()` output WILL differ between DocumentDB and MongoDB.
- Use the `$hint` operator to enforce selection of a preferred index.

### Index Builds
- Only ONE index build per collection at a time (foreground or background).
- Concurrent `createIndex()` or `dropIndex()` on the same collection will fail if a build is in progress.
- Default: foreground builds in DocumentDB and MongoDB 4.0. MongoDB 4.2+ ignores background option.
- TTL index starts expiring documents only AFTER the index build completes.

### MongoDB APIs, Operations, and Data Types
- DocumentDB is compatible with MongoDB 3.6, 4.0, 5.0, and 8.0 APIs.
- See supported-operators.md for the full compatibility matrix.

### mongodump and mongorestore
- DocumentDB does NOT dump or restore the admin database.
- After restoring with mongorestore, you must re-create user roles.
- Recommended: MongoDB Database Tools up to version 100.6.1.

### Result Ordering
- DocumentDB does NOT guarantee implicit result sort ordering.
- Always use explicit `sort()` to ensure ordering.
- `$sort` in aggregation: order not preserved unless `$sort` is the last stage.
- `$sort` with `$group`: only applied to `$first` and `$last` accumulators.
- DocumentDB 4.0+: `$push` respects sort order from previous `$sort` stage.

### Retryable Writes
- DocumentDB does NOT support retryable writes.
- MongoDB 4.2+ drivers enable retryable writes by default.
- Error: `{"ok":0,"errmsg":"Unrecognized field: 'txnNumber'"}`
- **Fix**: Disable via connection string: `retryWrites=false`
  ```
  mongodb://<user>:<pass>@<endpoint>:27017/?retryWrites=false
  ```

### Sparse Index
- To use a sparse index, you MUST include `$exists` clause on the indexed fields.
- Without `$exists`, DocumentDB will NOT use the sparse index.
  ```javascript
  db.inventory.count({ "stock": { $exists: true }})
  ```

### $elemMatch within $all
- DocumentDB does NOT support `$elemMatch` within `$all`.
- Workaround: use `$and` with `$elemMatch`:
  ```javascript
  db.col.find({
    $and: [
      { qty: { "$elemMatch": { part: "xyz", qty: { $lt: 11 } } } },
      { qty: { "$elemMatch": { qty: 40, size: "XL" } } }
    ]
  })
  ```

### $lookup
- Supports equality matches (left outer join) and uncorrelated subqueries.
- Does NOT support correlated subqueries.
- Three indexing algorithms for $lookup:
  - **Nested loop**: Best when foreign collection <1GB and has an index. Stage: `NESTED_LOOP_LOOKUP`
  - **Sort merge**: Best when no index on foreign field and working set doesn't fit in memory. Stage: `SORT_LOOKUP`
  - **Hash**: Best when foreign collection <1GB and working set fits in memory. Stage: `HASH_LOOKUP`
- Default: hash when `allowDiskUse:false`, sort merge when `allowDiskUse:true`.
- Use `planHint` in comment to force a specific algorithm:
  ```javascript
  db.foo.aggregate([
    { $lookup: { from: "bar", localField: "_id", foreignField: "_id", as: "joined" } }
  ], { comment: '{ "lookupStage": { "planHint": "HASH" } }' })
  ```

### $natural and Reverse Sorting
- DocumentDB supports `$natural` for forward collection scans ONLY.
- Reverse scans (`{$natural: -1}`) will produce a `MongoServerError`.
