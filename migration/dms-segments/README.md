# Amazon Database Migration Service (DMS) Segment Analyzer

The DMS Segment Analyzer calculates `_id` boundary values that split a MongoDB or Amazon DocumentDB collection into evenly sized segments. You paste those boundaries into an AWS DMS task's table mapping so that DMS loads the collection in parallel during full load.

## Why use this tool
By default, AWS DMS uses a single thread to unload each collection during full load. For large collections that can take a long time. DMS can load a collection in parallel, but it needs to know how to split it up. It offers two ways to do this ([DMS documentation](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Source.MongoDB.html#CHAP_Source.MongoDB.ParallelLoad)):

- **Autosegmentation** (`"type": "partitions-auto"`): DMS works out the segments itself when the task starts.
- **Range segmentation** (`"type": "ranges"`): you supply the `_id` boundary of each segment.

This tool produces the boundary list that range segmentation needs. Range segmentation is a good choice when:
- Autosegmentation is slow to start or its paginated skips time out on a very large collection.
- You want to calculate boundaries once, ahead of time, and reuse them across test runs and the final migration.
- You want to control exactly how many segments are used and see how big each one is before the migration starts.

## How it works
The tool reads only the `_id` field and never modifies data. It offers three ways to calculate boundaries:

| Mode | Option | How it works | Segment sizes | Speed |
|------|--------|--------------|---------------|-------|
| Skip (default) | _none_ | Runs one server-side `skip()` per boundary along the `_id` index | Equal document counts | Slower for large collections; individual skips can time out |
| Single cursor | `--single-cursor` | Streams every `_id` to the client in order and records every Nth value | Equal document counts | Slowest; reads every `_id` up to the last boundary over the network, but avoids long-running skips |
| Time-based | `--time-based-segments` | Splits the time range between the oldest and newest ObjectId into equal intervals | Equal time ranges; document counts can be very uneven | Fastest; only two queries no matter how big the collection is |

The skip and single-cursor modes return the same boundaries. Each segment gets `count / n` documents (rounded down), and the last segment also gets the remainder.

Pick a mode:
- Start with the **default (skip)** mode.
- If skips time out, switch to **`--single-cursor`**.
- Use **`--time-based-segments`** only when the collection is too large for the other modes, its `_id` values are ObjectIds, and documents were inserted at a steady rate over a long period. An ObjectId records when it was created, so if most documents were created in a short burst (a bulk load, for example), most of them end up in one segment. In testing, a 5 million document collection loaded in about 30 minutes put 99.9% of its documents in a single segment. Always [check segment sizes](#checking-segment-sizes) before using time-based boundaries.

## Requirements
- Python 3.7+
- PyMongo
  - MongoDB 2.6 - 3.4 | pymongo 3.10 - 3.12
  - MongoDB 3.6 - 5.0 | pymongo 3.12 - 4.0
  - MongoDB 5.1+      | pymongo 4.0+
  - DocumentDB        | pymongo 3.10+
- Network access from the machine running the tool to the source database
- A database user with read access to the collection. The skip and single-cursor modes also run the `collStats` command.

Supported `_id` types:
- Skip and single-cursor modes: ObjectId, string, and 32-bit integer. The tool checks the first and last `_id` and stops if either has an unsupported type or if the two types differ.
- Time-based mode: ObjectId only.

## Installation
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/dms-segments
python3 -m pip install pymongo
```

## Usage
```
python3 dms-segments.py --uri <uri> --database <database> --collection <collection> --num-segments <n> [--single-cursor | --time-based-segments]
```

| Option | Required | Description |
|--------|----------|-------------|
| `--uri` | Yes | Connection string for the source server ([format](https://www.mongodb.com/docs/manual/reference/connection-string/)) |
| `--database` | Yes | Database containing the collection |
| `--collection` | Yes | Collection to segment |
| `--num-segments` | Yes | Number of segments you want DMS to load in parallel (2 or more). The tool prints `n - 1` boundaries; DMS adds one more segment for everything after the last boundary. |
| `--single-cursor` | No | Find boundaries by walking the `_id` index with one cursor instead of using skips |
| `--time-based-segments` | No | Find boundaries by splitting the ObjectId timestamp range evenly |

### Connection tips
- Connect to a single server rather than as a replica set. If your driver supports it, add `directConnection=true` to the URI.
- For Amazon DocumentDB, use an instance endpoint rather than the cluster endpoint. Pointing the tool at a replica instance keeps the extra reads off your primary.
- Amazon DocumentDB enables TLS by default. Download the [Amazon DocumentDB CA bundle](https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem) and add `tls=true&tlsCAFile=global-bundle.pem` to the URI.
- Put the URI in single quotes so the shell does not interpret the `&` characters.

### Examples
MongoDB, 8 segments, default skip mode:
```
python3 dms-segments.py --uri 'mongodb://user:password@mongo-host:27017/?directConnection=true' \
  --database sales --collection orders --num-segments 8
```

Amazon DocumentDB, 8 segments, time-based:
```
python3 dms-segments.py --uri 'mongodb://user:password@my-instance.xxxxxxxxxxxx.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&directConnection=true' \
  --database sales --collection orders --num-segments 8 --time-based-segments
```

## Understanding the output
Example output from the default mode with `--num-segments 4`:
```
collection socialite.followers contains 5000344 documents
finding _id values for 4 chunks, approximately 1250086 documents in each
  boundary   1 - <class 'bson.objectid.ObjectId'> 6af5d124484178e5c6246ff7 | done in approximately 0 seconds
  boundary   2 - <class 'bson.objectid.ObjectId'> 6af5d12e0e0a6392b649ad8e | done in approximately 0 seconds
  boundary   3 - <class 'bson.objectid.ObjectId'> 6af5d13866cedfe1eea033c3 | done in approximately 0 seconds

boundaries as list | "6af5d124484178e5c6246ff7","6af5d12e0e0a6392b649ad8e","6af5d13866cedfe1eea033c3"

boundaries as list for DMS | ["6af5d124484178e5c6246ff7"],["6af5d12e0e0a6392b649ad8e"],["6af5d13866cedfe1eea033c3"]

query required 0 seconds
```

- `boundaries as list` is a plain comma-separated list, for scripts or your own records.
- `boundaries as list for DMS` is formatted for the `boundaries` array of a DMS table mapping. Copy everything after the `|`.

## Using the boundaries in AWS DMS
Add a `table-settings` rule with `"parallel-load"` of type `"ranges"` to the task's table mapping. Put the copied output inside the outer brackets of `"boundaries"`:

```json
{
  "rules": [
    {
      "rule-type": "selection",
      "rule-id": "1",
      "rule-name": "include-followers",
      "object-locator": {
        "schema-name": "socialite",
        "table-name": "followers"
      },
      "rule-action": "include"
    },
    {
      "rule-type": "table-settings",
      "rule-id": "2",
      "rule-name": "segment-followers",
      "object-locator": {
        "schema-name": "socialite",
        "table-name": "followers"
      },
      "parallel-load": {
        "type": "ranges",
        "columns": ["_id"],
        "boundaries": [
          ["6af5d124484178e5c6246ff7"],["6af5d12e0e0a6392b649ad8e"],["6af5d13866cedfe1eea033c3"]
        ]
      }
    }
  ]
}
```

For a MongoDB source, `schema-name` is the database name and `table-name` is the collection name. DMS treats each boundary as an inclusive upper limit and adds a final segment for everything after the last boundary. In this example the segments are `_id <= b1`, `b1 < _id <= b2`, `b2 < _id <= b3`, and `_id > b3`, so 3 boundaries give 4 segments. Documents inserted after you run the tool still get migrated, because the first and last segments are open-ended. They can, however, make segments less even.

How many segments DMS loads at the same time is limited by the task's `MaxFullLoadSubTasks` setting (default 8). Set it to at least the number of segments you want loaded in parallel. Keep in mind that the replication instance and the source and target databases all need enough capacity for the extra concurrent load.

## Checking segment sizes
To see how many documents fall in each segment before you start the DMS task, run this in `mongosh` against the source. It uses the same range rules as DMS. Paste the `boundaries as list` output into `bounds`:

```javascript
const coll = db.getSiblingDB("socialite").getCollection("followers");
const bounds = ["6af5d124484178e5c6246ff7","6af5d12e0e0a6392b649ad8e","6af5d13866cedfe1eea033c3"]
  .map(b => ObjectId(b));   // ObjectId _id; use .map(Number) for integer _id, or remove .map(...) for string _id
let lo = null, n = 0;
for (const hi of [...bounds, null]) {
  const range = {};
  if (lo !== null) range.$gt = lo;
  if (hi !== null) range.$lte = hi;
  print(`segment ${++n}: ${coll.countDocuments(Object.keys(range).length ? { _id: range } : {})} documents`);
  lo = hi;
}
```

Each count runs a range query on `_id`. On very large collections the counts can take a while and add load to the source, so run this against a replica instance if you can.

## Limitations
- The tool segments one collection per run. Run it once for each large collection you want to load in parallel.
- Only the first and last `_id` are checked for type. A collection with mixed `_id` types in the middle is not detected.
- The `long` and `double` `_id` types are not supported by this tool, even though DMS can use them for range segmentation.
- The default and single-cursor modes stop if the collection has fewer documents than `--num-segments`. They also stop if they reach the end of the collection early, which happens when documents are deleted while the tool runs. Small collections don't benefit from segmentation anyway.
- Time-based mode can't split time more finely than one second. If the `_id` values span fewer seconds than the segments you asked for, it prints a warning and returns fewer segments. If they were all created in the same second, it stops and suggests another mode.
- Boundaries reflect the collection when the tool ran. Large deletes or out-of-order inserts afterward can make segments uneven.

## License
This tool is licensed under the Apache 2.0 License.
