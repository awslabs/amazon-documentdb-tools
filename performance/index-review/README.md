# Amazon DocumentDB Index Review Tool

The index review tool finds indexes you may not need. It catalogs every collection and index on a server, and then reports:

- **Unused indexes:** indexes that no query has used since the usage counters were last reset.
- **Redundant indexes:** indexes whose keys are the leading keys of another index on the same collection. For example, `{a: 1}` is redundant if `{a: 1, b: 1}` exists, because queries on `a` can use the larger index.

Every index slows down inserts, updates, and deletes, and uses storage and memory. Removing the ones you don't need can improve write performance and lower cost.

The tool only reads data. It doesn't change or drop anything on the server.

- **Supported sources:** Amazon DocumentDB (instance-based clusters) and MongoDB (standalone or replica set).
- **What it creates:** a JSON file with everything it collected, plus two CSV files: one row per collection and one row per index. The list of unused and redundant indexes is printed to the screen.

> **Important:** Never drop an index based only on this report. Index usage is counted per instance and resets when an instance restarts. Read [Before you drop an index](#before-you-drop-an-index) first, and check with everyone who owns the applications that use the database.

## Requirements

- Python 3.9 or later.
- The [`pymongo`](https://pypi.org/project/pymongo/) Python package (installed from `requirements.txt` below).
- A database user that can run `listDatabases`, `listCollections`, `serverStatus`, `collStats`, and the `$indexStats` aggregation stage on every database. The administrator user created with your cluster has all of these.
- Network access to the server. Amazon DocumentDB clusters are only reachable from inside their VPC, so run the tool from a machine in the same VPC (for example an Amazon EC2 instance) or through an SSH tunnel. See [Connecting to an Amazon DocumentDB cluster from outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html).

The examples below use `python3`. On Windows, use `python` instead.

## Installation

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/index-review
python3 -m pip install -r requirements.txt
```

## Quick start

### 1. Run the tool on every instance

```
python3 index-review.py --server-alias <alias> --uri "<uri>"
```

- `--uri` is the connection string for **one instance**. For Amazon DocumentDB, see [Connecting to Amazon DocumentDB](#connecting-to-amazon-documentdb).
- `--server-alias` is any short name you choose, such as `prod-1`. The tool uses it at the start of each output file name. **Use a different alias for each instance**, or later runs will overwrite the CSV files from earlier ones.

Each instance keeps its own index usage counters. An index that is never used on the primary instance might be used by every query sent to a replica instance. So run the tool once against each instance in the cluster (the primary and every replica), one after the other:

```
python3 index-review.py --server-alias prod-1 --uri "mongodb://<user>:<password>@prod-1.<cluster-suffix>.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem"
python3 index-review.py --server-alias prod-2 --uri "mongodb://<user>:<password>@prod-2.<cluster-suffix>.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem"
python3 index-review.py --server-alias prod-3 --uri "mongodb://<user>:<password>@prod-3.<cluster-suffix>.us-east-1.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem"
```

Each run creates three files in the current folder:

```
prod-1-20250101120000-index-review.json
prod-1-collections.csv
prod-1-indexes.csv
```

The JSON file name includes a timestamp in UTC, in `YYYYMMDDHHMMSS` format. The CSV file names don't, so running again with the same alias replaces them.

Each run on its own only reports on that one instance.

### 2. Combine the results from all instances

To get a report that counts index usage across the whole cluster, run the tool again with `--files` and the JSON files from step 1, separated by commas with no spaces. **List the primary instance's file first.** This step doesn't connect to any server.

```
python3 index-review.py --server-alias prod-all --files prod-1-20250101120000-index-review.json,prod-2-20250101120005-index-review.json,prod-3-20250101120010-index-review.json
```

This creates `prod-all-collections.csv` and `prod-all-indexes.csv`, and prints the combined list of unused and redundant indexes. An index is only reported as unused if it hasn't been used on any of the instances.

For a single-instance cluster or a standalone MongoDB server, step 1 is enough.

### What you'll see

The tool prints each collection as it reads it, then a list of indexes to review, grouped by database and collection:

```
connecting to server
sales.orders
sales.customers
loading first file prod-1-20250101120000-index-review.json
  database sales
    collection orders
        index status_1 | has never been used
        index customerId_1 | is redundant and covered by the following indexes : ['customerId_1_orderDate_-1']
    collection customers
        index email_1 | has never been used
```

- **`has never been used`** means the index hasn't been used *since its usage counter was last reset*, not that it has never been used at all. See [How it works](#how-it-works).
- **`is redundant and covered by the following indexes`** lists the indexes that begin with the same keys.

The same index can appear on both lists. The `_id` index is never reported, because it can't be dropped.

## Connecting to Amazon DocumentDB

Amazon DocumentDB clusters have TLS enabled by default. To connect:

1. Download the Amazon RDS certificate bundle into the `index-review` folder:
   ```
   curl -O https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
   ```
2. Find the **instance endpoint** of each instance in your cluster. In the Amazon DocumentDB console, choose your cluster to see its instances, and then choose each instance to see its endpoint. Or use the AWS CLI:
   ```
   aws docdb describe-db-instances --filters Name=db-cluster-id,Values=<cluster-name> --query "DBInstances[].[DBInstanceIdentifier,Endpoint.Address]" --output table
   ```
3. Build a URI for each instance endpoint:
   ```
   mongodb://<username>:<password>@<instance-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem
   ```
   `tls=true&tlsCAFile=global-bundle.pem` turns on TLS using the downloaded certificate bundle. Leave out both options if TLS is disabled on your cluster.

**Use instance endpoints, not the cluster endpoint.** The tool always connects directly to the single host named in the URI, ignoring any `replicaSet` or `readPreference` options. The cluster endpoint always points to the primary instance, so using it only reviews the primary. The reader endpoint picks a replica instance at random, so you can't control which instance you get.

Special characters in the username or password must be [percent-encoded](https://www.mongodb.com/docs/manual/reference/connection-string/). For example, `@` becomes `%40`.

**Always put the URI in double quotes.** Without them, your shell treats each `&` in the URI as the end of the command, and the tool receives only part of the URI.

For MongoDB, run the tool against each member of the replica set, using that member's host name and port. See the [MongoDB connection string documentation](https://www.mongodb.com/docs/manual/reference/connection-string/) for the URI format. The tool hasn't been tested on sharded MongoDB clusters or Amazon DocumentDB elastic clusters.

## How it works

When it connects to a server, the tool:

1. Reads `serverStatus` for the host name, uptime, and server-wide operation counters.
2. Lists every database and collection, and for each collection, reads its statistics with `collStats` and its index usage with the `$indexStats` aggregation stage.
3. Saves everything to the JSON file.
4. Analyzes the JSON file (or files, with `--files`) and writes the CSV files and the screen report.

The tool skips:
- the `admin`, `config`, `local`, and `system` databases
- views
- `system.profile` collections

### Unused indexes

For each index, `$indexStats` reports how many times queries have used it (`accesses.ops`) and when that counting started (`accesses.since`). An index is reported as unused if that count is 0 on every instance you reviewed.

**The counters start again from 0 whenever an instance restarts.** That includes restarts from maintenance, patching, failover, changing the instance class, and reboots to apply parameter group changes. The counters also start from 0 for an index that was just created. Right after a restart, every index looks unused.

To get a trustworthy result:
- Run the tool after the instances have been up for a long, representative period. That period should include every regular workload, such as month-end reports, weekly batch jobs, and peak traffic.
- Check the `uptime` value (in seconds) near the top of each JSON file, and the `accesses` → `since` timestamp of each index, to see how long the counters cover.

### Redundant indexes

An index is reported as redundant if another index on the same collection begins with exactly the same keys, in the same order and with the same directions:

| Index | Other index | Redundant? |
|---|---|---|
| `{a: 1}` | `{a: 1, b: 1}` | Yes |
| `{a: 1, b: -1}` | `{a: 1, b: -1, c: 1}` | Yes |
| `{a: 1}` | `{b: 1, a: 1}` | No: `a` isn't the first key |
| `{a: 1, b: 1}` | `{a: 1, b: -1, c: 1}` | No: `b` has a different direction |

The check only compares keys. **It ignores index options**, so it can report an index as redundant even when it does something the other index doesn't:

- **Unique indexes** enforce that no two documents have the same value. Dropping `{email: 1}` with `unique: true` removes that rule, even if `{email: 1, name: 1}` exists.
- **TTL indexes** (`expireAfterSeconds`) delete expired documents automatically. Dropping one stops that cleanup.
- **Sparse and partial indexes** only include some documents, so the other index may not be a full replacement. And if the *covering* index is sparse or partial, it can't serve every query the redundant one does.

Always check the options of both indexes with `db.<collection>.getIndexes()` before you drop one.

### Insert, update, and delete rates

The CSV files include estimated insert, update, and delete rates for each collection, to help you judge how much each index costs to maintain. The tool calculates them from one of these sources, in this order:

1. With `--ops-file`: a log file from the [MongoDB Oplog Review Tool](../../migration/mongodb-oplog-review/), which counts operations per collection in a MongoDB oplog. Create the log with the default `--unit-of-measure day`.
2. With `--prior-index-review-file`: the difference between this run and an earlier JSON file **from the same Amazon DocumentDB instance** (normally the primary). Leave at least a day between the two runs. A short gap gives misleading numbers when they're scaled up to a day, and if the instance restarted in between, the numbers are meaningless.
3. Otherwise, the collection's operation counts since the instance started (Amazon DocumentDB only).

On MongoDB, the per-collection rates are always 0 unless you use `--ops-file`, because MongoDB's `collStats` doesn't include operation counts.

## Command-line options

| Option | Description |
|---|---|
| `--server-alias <alias>` | (Required) Short name used at the start of each output file name. |
| `--uri <uri>` | Connection string for the instance to review. You must give either `--uri` or `--files`, but not both. |
| `--files <file1>,<file2>,...` | Analyze JSON files from earlier runs instead of connecting to a server. List the primary instance's file first, separated by commas with no spaces. |
| `--prior-index-review-file <file>` | JSON file from an earlier run on the same instance. Used to calculate collection insert, update, and delete rates over the time between the two runs. |
| `--ops-file <file>` | Log file from the MongoDB Oplog Review Tool. Used to calculate collection insert, update, and delete rates. |
| `--skip-python-version-check` | Run on Python versions older than 3.9. This is not supported. |

## Reading the results

### `<alias>-indexes.csv`

One row for each index, including the `_id` index. This is the main file to review. For example:

```
database,collection,doc-count,average-doc-size,size-GB,storageSize-GB,coll-unused-pct,num-indexes,indexSize-GB,index-unused-pct,index-name,index-accesses-total,index-accesses-secondary,redundant,covered-by,ins/day,upd/day,del/day,ins/sec,upd/sec,del/sec,index-keys
sales,orders,7089376,196.0,1.29,1.56,3.53,3,0.51,12.49,customerId_1_productId_1,0,0,No,[],0,0,0,0,0,0,customerId||1.0||productId||1.0||
```

| Column | Description |
|---|---|
| `database`, `collection` | Database and collection name. |
| `doc-count` | Number of documents in the collection. |
| `average-doc-size` | Average document size in bytes. |
| `size-GB` | Total size of the documents, in GB. |
| `storageSize-GB` | Storage the collection uses, in GB. |
| `coll-unused-pct` | Percentage of the collection's storage that is allocated but unused (Amazon DocumentDB only, `-1` on MongoDB). |
| `num-indexes` | Number of indexes on the collection, including `_id`. |
| `indexSize-GB` | Size of **this** index, in GB. |
| `index-unused-pct` | Percentage of this index's storage that is allocated but unused (Amazon DocumentDB only, `-1` on MongoDB). |
| `index-name` | Index name. |
| `index-accesses-total` | Number of times queries used the index, added up across all the files analyzed. `0` means unused. |
| `index-accesses-secondary` | The part of `index-accesses-total` that came from the second and later files given to `--files` (normally your replica instances). Always `0` for a single file. |
| `redundant` | `Yes` if the index is redundant, otherwise `No`. |
| `covered-by` | The indexes that make this one redundant, or `[]`. |
| `ins/day`, `upd/day`, `del/day` | Estimated documents inserted, updated, and deleted in the collection per day. See [Insert, update, and delete rates](#insert-update-and-delete-rates). |
| `ins/sec`, `upd/sec`, `del/sec` | The same estimates per second, rounded down to a whole number, so they show `0` for collections with less than one operation per second. |
| `index-keys` | The index keys and directions, in the form `field||direction||`. For example, `customerId||1||orderDate||-1||` is the index `{customerId: 1, orderDate: -1}`. Amazon DocumentDB shows the directions as `1.0` and `-1.0`. |

Sizes are rounded to 0.01 GB, so small collections and indexes show `0.00`.

### `<alias>-collections.csv`

Server-wide operation rates, then one row per collection. The first lines show the host name and the server's insert, update, delete, and query rates since the instance started (`alltime`). With `--prior-index-review-file`, they also show the rates for the time between the two runs (`period`). These server-wide numbers come from `serverStatus` and count commands, not documents: one `insertMany` of 1,000 documents counts as one insert.

```
hostname,prod-1.<cluster-suffix>.us-east-1.docdb.amazonaws.com:27017

,,alltime/sec,alltime/day
,inserts,0.16,13558.26
,updates,0.00,0.00
,deletes,0.00,0.00
,queries,0.00,0.00

database,collection,doc-count,average-doc-size,size-GB,storageSize-GB,coll-unused-pct,num-indexes,indexSize-GB,ins/day,upd/day,del/day,ins/sec,upd/sec,del/sec
sales,orders,7089376,196.0,1.29,1.56,3.53,3,1.30,0,0,0,0,0,0
```

Because of the lines at the top, the file won't import cleanly as a single table. In a spreadsheet, start reading at the `database` header row. The collection columns have the same meaning as in the indexes file, except that `indexSize-GB` is the total size of all of the collection's indexes.

### JSON file

The JSON file holds the raw `serverStatus`, `collStats`, and `$indexStats` output the tool collected. Keep it: you can re-run the analysis later with `--files` without connecting to the server, combine it with files from other instances, or use it as the `--prior-index-review-file` for a later run.

## Before you drop an index

1. **Make sure the usage counters cover a long enough period** on every instance. See [Unused indexes](#unused-indexes).
2. **Check the index options** with `db.<collection>.getIndexes()`. Keep unique and TTL indexes unless you're sure the rule or the cleanup isn't needed. See [Redundant indexes](#redundant-indexes).
3. **Ask the application owners.** An index may exist for a rare but important query, such as a yearly report or an emergency support task.
4. **Save the index definition** from `getIndexes()` so you can recreate it. On a large collection, rebuilding an index can take a long time and adds load to the cluster.
5. **Test the change** in a non-production environment with a realistic workload, and watch query performance after you drop the index in production.

## Troubleshooting

- **`UserWarning: You appear to be connected to a DocumentDB cluster`:** this message comes from `pymongo` and is harmless.
- **`ServerSelectionTimeoutError` after about 30 seconds:** the tool couldn't reach the server. Check that you're running from inside the cluster's VPC (or through an SSH tunnel), that the security group allows inbound traffic on port 27017 from your machine, and that the TLS options match your cluster's settings.
- **`error: the following arguments are required: --server-alias`** even though you included it: the URI isn't in double quotes, so your shell cut the command off at the first `&`.
- **`ModuleNotFoundError: No module named 'pymongo'`:** run `python3 -m pip install -r requirements.txt`.
- **Every index is reported as unused:** the instance probably restarted recently, which resets the usage counters. Check `uptime` in the JSON file.
- **A collection is missing from the output:** it's a view, a `system.profile` collection, or in one of the skipped databases (`admin`, `config`, `local`, `system`).

### Known issues

- **MongoDB: `not authorized on <db> to execute command { aggregate: "system.views" ...`:** the tool fails on any database that contains a view, because it tries to read index usage on the `system.views` collection.
- **MongoDB: `KeyError: 'count'`:** the tool fails on time series collections.
- **`KeyError` when using `--files`:** a collection in the first file is missing from one of the other files, for example because it was created between runs. Run the tool on all instances close together in time, and list the primary instance's file first.
- **`TypeError: unsupported operand type(s) for /: 'str' and 'int'` with `--ops-file`:** the option currently fails whenever the log file contains a collection that the tool reviewed.
- **Extra columns in some rows of the indexes CSV:** when an index is covered by more than one other index, the commas in the `covered-by` list split it across several columns, which shifts the columns after it. The screen report shows the full list correctly.

## License

This tool is licensed under the Apache 2.0 License.
