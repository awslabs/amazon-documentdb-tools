# MongoDB Ops Tool

The MongoDB Ops tool gathers collection level query/insert/update/delete counters to assist in the process of sizing. 

# Requirements
 - Python 3.7+
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the MongoDB Ops Tool
`python3 mongodb-ops.py --uri <mongodb-uri> --server-alias <server-alias> --collect`
- Produces an output file for comparison

`python3 mongodb-ops.py --compare --file1 <first-compare-file> --file2 <second-compare-file>`
- Compares the results of two executions to estimate the number of queries, inserts, updates, and deletes per second at the collection level.

## Required privileges
The built-in `clusterMonitor` role is not sufficient: it does not grant `listCollections`, and the collection step fails with "not authorized ... listCollections". Add a role with `listCollections` and `collStats` on all databases, for example:
```
db.getSiblingDB("admin").createRole({role: "mongodbOpsRead",
  privileges: [{resource: {db: "", collection: ""}, actions: ["listCollections", "collStats"]}], roles: []})
db.getSiblingDB("admin").createUser({user: "<user>", pwd: "<password>", roles: ["clusterMonitor", "mongodbOpsRead"]})
```
On a sharded cluster, users created through mongos do not exist on the shards: create this user on each shard replica set to connect to it directly.

## What the counters measure
The collection counters come from the WiredTiger cursor statistics in `collStats` (`search calls`, `insert calls`, `modify`, `remove calls`). They count storage engine calls, not documents changed by the application, so treat the results as an estimate:
- An update that writes the full document (for example `replaceOne`, or an update the storage engine does not apply as a modify) is counted as an insert call, not as an update.
- Chunk migrations (balancer or `moveChunk`) add inserts on the receiving shard and removes on the donor shard.
- The counters are per node and reset when the node restarts.

For document-level insert/update/delete counts per collection, including writes inside transactions and without chunk migrations, use [mongodb-oplog-review](../mongodb-oplog-review/).

## Notes
- `--compare` prints rates as integers per `--unit-of-measure` (default `day`). With `sec` low rates print as 0; use `hr` or `day`.
- Run on any instance in the replicaset (the larger the oplog the better)
- If sharded, run on one instance in each shard
- Each execution creates a file starting with \<server-alias> and ending with .json
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/
- Use &directConnection=true

## License
This tool is licensed under the Apache 2.0 License. 
