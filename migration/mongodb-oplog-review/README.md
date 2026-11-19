# MongoDB Oplog Review Tool

The mongodb oplog review tool connects to any instance in a MongoDB replicaset (primary or secondary), reads the oplog (all of it, or a time window), and produces a log file containing counters for insert/update/delete operations by collection.

Each oplog entry for an insert, update or delete is one changed document, so the counters are document counts (the unit a change data capture tool reads and applies), not command counts.
- Writes inside a transaction are stored in one `applyOps` oplog entry. The tool unwinds it and counts each inner operation on its own collection. The log also lists the transactions that change more than one collection.
- Chunk migration entries (balancer or `moveChunk`, `fromMigrate: true`) are not application writes and are skipped by default. Use `--include-migrations` to count them.

# Requirements
 - Python 3.7+
   - If using Snappy wire protocol compression and MongoDB, "apt install python-snappy"
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the MongoDB Oplog Review Tool
`python3 mongodb-oplog-review.py --server-alias <server-alias> --uri <mongodb-uri> --stop-when-oplog-current`
Optionally add arguments `--output-to-csv --file-name <name of outputfile>` if you want to save the output to a csv file

To read only a time window instead of the whole oplog:
- `--window-seconds 3600` reads the last hour (the window ends at the newest oplog entry when the tool starts)
- `--start-time 2026-11-19T14:00:00Z --window-seconds 3600` reads one hour from that UTC time
- `--start-time 2026-11-19T14:00:00Z` reads from that UTC time to the newest oplog entry

In window mode the tool reads the window once and stops; `--stop-when-oplog-current` is not needed. A window is the way to measure a peak period; the whole oplog gives an average over the oplog span. If the window starts before the oldest oplog entry, the tool prints a warning and starts at the oldest entry.

## Required privileges
The user needs `find` on `local.oplog.rs`. The built-in `clusterMonitor` role grants it (tested on MongoDB 7.0.21). On a sharded cluster, users created through mongos do not exist on the shards: create a shard-local user on each shard replica set to connect to it directly.

## Notes on the counts
- Rates are integer values per `--unit-of-measure` (default `day`). With `sec` or `min` low rates print as 0; use the totals or a larger unit.
- A large transaction can use several `applyOps` entries; each inner operation is still counted once. A transaction that is prepared and then aborted is counted, because its operations are in the oplog.
- `commitTransaction` / `abortTransaction` entries are counted as commands.

- Run on any instance in the replicaset (the larger the oplog the better)
- Use a different \<server-alias> for each execution
- If sharded, run on one instance in each shard (connect to the shard, not to mongos, with `directConnection=true`), and add the results of all shards
- Prefer a secondary: it has the same oplog as the primary and the read does not load the primary
- Avoid running the tool from the server itself if possible, it consume disk space for the output files
- Each execution creates a file starting with \<server-alias> and ending with .log
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- Consider adding "&compressor=snappy" to your \<mongodb-uri> if your MongoDB server supports it

## License
This tool is licensed under the Apache 2.0 License. 
