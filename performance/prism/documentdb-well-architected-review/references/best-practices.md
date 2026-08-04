# Amazon DocumentDB Best Practices

Source: https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html

## Basic Operational Guidelines

- Deploy clusters with 2+ instances across 2+ Availability Zones. For production: 3+ instances across 3 AZs.
- Monitor memory, CPU, connections, and storage via CloudWatch. Set alerts when approaching capacity.
- Scale up instances when approaching capacity limits. Provision enough compute for unforeseen demand spikes.
- Set backup retention period to align with recovery point objective.
- Test failover to understand duration for your use case.
- Connect using the cluster endpoint in replica set mode to minimize failover impact.
- Use `secondaryPreferred` read preference to enable replica reads and free up the primary for writes.
- Design applications to be resilient to network/database errors. Use exponential backoff for transient errors.
- Enable cluster deletion protection for production. Take a final snapshot before deleting any cluster.
- Explicitly specify `--engine-version` in production scripts/CloudFormation (default may change with new releases).

## Instance Sizing

- **Critical**: Choose instance type with enough RAM to fit your working set (data + indexes) in memory.
- Amazon DocumentDB reserves **1/3 of RAM** for its own services. Only **2/3 is available for cache**.
- Monitor `BufferCacheHitRatio` via CloudWatch for each instance under load.
- `BufferCacheHitRatio` should be as high as possible (close to 100%). Reading from memory is faster and cheaper than from storage volume.
- If `BufferCacheHitRatio` is low, scale up instance size to provide more RAM.
- Continue scaling up until `BufferCacheHitRatio` no longer increases dramatically after scaling.
- Periodic dips in `BufferCacheHitRatio` (e.g., from analytic queries scanning entire collections) are acceptable if your workload tolerates temporary latency increases.
- Isolate operational and analytic workloads: direct operational queries to primary, analytic queries to replicas.
- **Test workloads in pre-production** with representative production workload before deploying.

## Working with Indexes

### Building Indexes
- Create indexes BEFORE importing large datasets.
- Use the Amazon DocumentDB Index Tool to extract indexes from MongoDB and create them in DocumentDB.
- Creating indexes first reduces overall migration time.

### Index Selectivity
- Limit index creation to fields where duplicate values are **less than 1%** of total documents.
- Example: 100,000 documents → only index fields where same value occurs ≤1,000 times.
- High cardinality (many unique values) = good index performance.
- Low cardinality (e.g., boolean, day of week) = poor performance, unlikely to be chosen by query optimizer.
- Low cardinality indexes still consume disk space and I/Os.

### Impact of Indexes on Writing
- Each index on a collection adds a write for every insert/update/delete.
- Example: 9 indexes = 10 writes per operation (1 collection + 9 indexes).
- Additional indexes increase write latency, I/Os, and storage.
- **Best practice: keep indexes per collection to 5 or fewer.**

### Identifying Missing Indexes
- Use the DocumentDB profiler to log slow queries.
- Look for queries with `COLLSCAN` in the profiler output — indicates a full collection scan.
- Create indexes on the fields used in those query filters.

### Identifying Unused Indexes
- Use `db.collection.aggregate([{$indexStats:{}}])` to check index usage.
- An `ops` value of zero (with workload running long enough) indicates an unused index.
- Regularly identify and remove unused indexes to improve performance and reduce cost.

## Security Best Practices

- Use IAM accounts to control access to DocumentDB API operations.
- Assign individual IAM accounts per person. Don't use root account.
- Grant minimum required permissions (least privilege).
- Use IAM groups for managing permissions across multiple users.
- Regularly rotate IAM credentials.
- Configure AWS Secrets Manager for automatic secret rotation.
- Use TLS for data in transit, AWS KMS for data at rest.
- Enforce least privilege with role-based access control (RBAC).

## Cost Optimization

- Create billing alerts at 50% and 75% of expected monthly bill.
- Storage replicates 6 ways across 3 AZs regardless of instance count — even single-instance clusters are highly durable.
- Use single-instance clusters for dev/test when high availability isn't required.
- Stop clusters when not in use for dev/test scenarios.
- Both TTL and change streams incur I/Os — disable if not used by your application.

## TTL and Time Series Workloads

- TTL document deletion is best-effort, not guaranteed within a specific timeframe.
- TTL deletions incur I/O costs.
- **For time-series data**: use rolling collections instead of TTL indexes.
  - Create one collection per day/week based on ingest rate.
  - Drop collections when data is no longer needed — no I/O cost for drops.
  - Faster and more cost-effective than TTL indexes, especially for collections >1TB.

## Aggregation Pipeline Queries

- Use `$match` as the first stage (or early in the pipeline) to reduce documents processed by subsequent stages.

## batchInsert and batchUpdate

- High-rate concurrent batch operations can exhaust `FreeableMemory` on the primary.
- Either reduce concurrency or increase instance size to provide more `FreeableMemory`.
