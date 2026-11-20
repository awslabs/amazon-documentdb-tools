# MongoDB Changestream Review Tool

The mongodb changestream review tool connects to a MongoDB replicaset or to mongos, reads the changestream, and produces a log file containing counters for insert/update/delete operations by collection. 

# Requirements
 - Python 3.7+
   - If using Snappy wire protocol compression and MongoDB, "apt install python-snappy"
 - PyMongo
   - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
   - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
   - MongoDB 5.1+      | pymongo 4.0+
   - DocumentDB        | pymongo 3.10 - 4.0

## Using the MongoDB Changestream Review Tool
`python3 mongodb-changestream-review.py --server-alias <server-alias> --uri <mongodb-uri> --stop-when-changestream-current --start-position 2025-04-02+12:00:00`

Optionally add arguments `--output-to-csv --file-name <name of outputfile>` if you want to save the output to a csv file.

`--start-position` is a UTC time, in the format `YYYY-MM-DD+HH:MM:SS` or ISO 8601 (`2026-11-19T14:00:00Z`). It must be inside the oplog window of the server.

To read a fixed time window:
- `--start-position 2026-11-19T14:00:00Z --window-seconds 3600` reads one hour from that UTC time and stops at the end of the window
- The rates use the window length, so the result is the rate of that period. A window is the way to measure a peak period.
- `--stop-when-changestream-current` is not needed in window mode on MongoDB
- If the window ends in the future, the tool reads until the end of the window (or until `--collect-seconds`, default 10800)

Without `--window-seconds`, the rates use the time from `--start-position` to the last change read.

- On a sharded cluster, connect to mongos: one run gives the total of all shards
- Use a different \<server-alias> for each execution
- Avoid running the tool from the server itself if possible, it consume disk space for the output files
- Each execution creates a file starting with \<server-alias> and ending with .log
- The \<mongodb-uri> options can be found at https://www.mongodb.com/docs/manual/reference/connection-string/ 
- Consider adding "&compressor=snappy" to your \<mongodb-uri> if your MongoDB server supports it

## Required privileges
The tool opens a change stream on the whole deployment. The user needs the actions `changeStream` and `find` on all databases, for example:

```
db.getSiblingDB("admin").createRole({role: "changestreamReview", privileges: [{resource: {db: "", collection: ""}, actions: ["changeStream", "find"]}], roles: []})
db.getSiblingDB("admin").createUser({user: "csreview", pwd: passwordPrompt(), roles: ["changestreamReview"]})
```

Tested on MongoDB 7.0.21 (through mongos and on each shard replica set).

## Notes on the counts
- A change stream gives one event per operation, also for the operations inside a transaction.
- Writes of the balancer (chunk migrations) are not in a change stream, so they are not counted.
- `replace` events are counted as updates.
- Rates are integer values per `--unit-of-measure` (default `day`). With `sec` or `min` low rates print as 0; use the totals or a larger unit.
- `--stop-when-changestream-current` stops at the first empty batch from the server. If the server returns an empty batch before the stream reaches the present time, the totals are low; `--window-seconds` does not have this risk on MongoDB.
- In window mode the tool reads the stream position from the MongoDB resume token. If the resume token has a different format, the tool stops at the first change after the end of the window, or as `--stop-when-changestream-current` describes.

## License
This tool is licensed under the Apache 2.0 License. 
