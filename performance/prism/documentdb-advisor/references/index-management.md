# Index Management in Amazon DocumentDB

## Index Types Supported
- **Single field**: Index on one field. `db.coll.createIndex({field: 1})`
- **Compound**: Index on multiple fields. `db.coll.createIndex({a: 1, b: -1, c: 1})`. Follow ESR rule (Equality, Sort, Range).
- **Multikey**: Automatically created when indexing array fields. Each array element gets an index entry.
- **Sparse**: Only indexes documents that contain the indexed field. Use `{sparse: true}`. MUST use `$exists` in queries to use sparse index.
- **TTL**: Automatically deletes documents after a specified time. `db.coll.createIndex({createdAt: 1}, {expireAfterSeconds: 86400})`. TTL monitor runs every 60 seconds. Deletes are best-effort, not guaranteed within a timeframe.
- **Unique**: Enforces uniqueness. `db.coll.createIndex({email: 1}, {unique: true})`. Null values count as a value — only one document can have a missing field.
- **Text**: Full-text search (DocumentDB 5.0+). `db.coll.createIndex({content: "text"})`. Only one text index per collection.
- **2dsphere**: Geospatial queries. `db.coll.createIndex({location: "2dsphere"})`.
- **Hashed**: Not supported in DocumentDB.
- **Wildcard**: Not supported in DocumentDB.

## Listing Indexes
```javascript
// List all indexes on a collection
db.collection.getIndexes()

// Get index usage statistics (accesses count since last restart)
db.collection.aggregate([{$indexStats: {}}])

// Fields in $indexStats output:
// - name: index name
// - key: index key definition
// - accesses.ops: number of times index was used
// - accesses.since: timestamp when stats started tracking
```

## Creating Indexes
```javascript
// Basic index
db.coll.createIndex({field: 1})

// Background build (recommended for production)
db.coll.createIndex({field: 1}, {background: true})

// Compound index following ESR rule
db.coll.createIndex({status: 1, createdAt: -1, amount: 1})

// Unique index
db.coll.createIndex({email: 1}, {unique: true})

// TTL index (expire after 24 hours)
db.coll.createIndex({timestamp: 1}, {expireAfterSeconds: 86400})

// Sparse index
db.coll.createIndex({optionalField: 1}, {sparse: true})
```

### Index Build Behavior
- Only ONE index build per collection at a time (foreground or background).
- Foreground builds block all read/write operations on the collection.
- Background builds (`{background: true}`) allow reads/writes but take longer.
- DocumentDB 4.0+: foreground is default. Always use `{background: true}` in production.
- Index builds cannot be cancelled once started.
- Create indexes BEFORE importing large datasets for best performance.
- TTL index starts expiring documents only AFTER the index build completes.

## Dropping Indexes
```javascript
// Drop by name
db.coll.dropIndex("index_name")

// Drop by key pattern
db.coll.dropIndex({field: 1})

// Drop all non-_id indexes
db.coll.dropIndexes()
```

### Rules for Dropping
- NEVER drop the `_id` index — it cannot be recreated and is required by DocumentDB.
- Always verify index is unused via `$indexStats` (ops: 0 for extended period) before dropping.
- Test in non-production environment first.
- Dropping an index is instant but recreating it requires a full index build.
- Get stakeholder agreement before dropping indexes in production.

## Rebuilding Indexes
```javascript
// Rebuild all indexes on a collection (DocumentDB 4.0+)
db.runCommand({reIndex: "collection_name"})
```
- `reIndex` rebuilds all indexes on a collection.
- This is a blocking operation — no reads/writes during rebuild.
- Use during maintenance windows only.
- Useful for reclaiming space from index bloat after heavy deletes/updates.
- Not supported on DocumentDB 3.6.

## Index Size and Performance
- Each index adds write overhead: N indexes = N+1 writes per insert/update/delete.
- Keep indexes per collection to 5 or fewer for optimal write performance.
- Index size = number of indexed fields × number of documents × average field value size.
- Indexes must fit in memory (buffer cache) for optimal read performance.
- Monitor `BufferCacheHitRatio` in CloudWatch — below 90% may indicate indexes don't fit in RAM.
- Use `collStats` to check `totalIndexSize` per collection.

## Compound Index Best Practices
- Follow the ESR (Equality, Sort, Range) rule for field ordering.
- Maximum recommended: 3 attributes per compound index.
- Compound indexes support prefix queries (leftmost fields).
- Example: Index `{a:1, b:1, c:1}` supports queries on `{a}`, `{a,b}`, and `{a,b,c}` but NOT `{b}` or `{c}` alone.
- A compound index can replace multiple single-field indexes if query patterns align.

## Identifying Unused Indexes
```javascript
// Check index usage
db.coll.aggregate([{$indexStats: {}}])

// Look for indexes where accesses.ops = 0
// If ops is 0 for 7+ days, the index is likely unused
// Consider: the accesses counter resets on instance restart
```

## Common Anti-Patterns
- Creating too many single-field indexes instead of compound indexes.
- Indexing low-cardinality fields (e.g., boolean, status with 3 values).
- Not using `{background: true}` for production index builds.
- Keeping unused indexes — they consume storage, I/O, and slow writes.
- Compound indexes with more than 3 attributes — diminishing returns, high overhead.
- Using TTL indexes on high-throughput collections — consider rolling collections instead.
