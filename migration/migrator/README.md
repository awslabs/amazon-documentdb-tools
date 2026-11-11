# Amazon DocumentDB Full Load and Change Data Capture (CDC) Migration Tool

This tool copies a collection from any MongoDB compatible database to another, using many processes in parallel for speed. It has two scripts:

- **`fl-multiprocess.py` (full load):** copies every document that is in the source collection when it runs, and then exits.
- **`cdc-multiprocess.py` (change data capture):** reads the source collection's change stream and applies each insert, update, replace, and delete to the target. It keeps running until you stop it, so the target stays in sync with the source until you're ready to move your applications.

Used together, they let you migrate a collection while your applications keep writing to the source. See [Migrating a collection](#migrating-a-collection).

Each run handles **one collection**. To migrate several collections, run the scripts once for each.

The scripts only copy documents. They don't:

- Create indexes on the target. Use the [index tool](../../index-tool) to copy the index definitions.
- Copy users, roles, views, or collection options.
- Apply collection or database drops and renames that happen on the source.

## Requirements

- Python 3.7 or later.
- The [`pymongo`](https://pypi.org/project/pymongo/) and [`boto3`](https://pypi.org/project/boto3/) Python packages (installed from `requirements.txt` below). Both scripts need `boto3`, even if you don't create CloudWatch metrics.
- Network access to both the source and the target. Amazon DocumentDB clusters are only reachable from inside their VPC, so run the tool from a machine in the same VPC (for example an Amazon EC2 instance) or through an SSH tunnel. See [Connecting to an Amazon DocumentDB cluster from outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html).
- A database user on the source that can read the collection, run `collStats`, and open a change stream on it, and a user on the target that can write to the target collection.
- For CDC, change streams on the source:
  - **MongoDB:** the source must be a replica set or a sharded cluster. A standalone server has no change stream.
  - **Amazon DocumentDB:** change streams are off by default. Turn them on for the collection before you start the migration, and check that the change stream log retention is long enough. See [Change streams in Amazon DocumentDB](#change-streams-in-amazon-documentdb).
- The full load needs boundaries that split the collection into parts. Calculate them with the [dms-segments tool](../dms-segments).

The examples below use `python3`. On Windows, use `python` instead.

## Installation

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/migrator
python3 -m pip install -r requirements.txt
```

## Connecting

`--source-uri` and `--target-uri` use the [MongoDB connection string format](https://www.mongodb.com/docs/manual/reference/connection-string/). Always put the URI in double quotes, because most URIs contain `&`, and without quotes your shell cuts the command off at the first `&`.

For Amazon DocumentDB, use the cluster endpoint and include these options:

```
"mongodb://<user>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&retryWrites=false"
```

- `tls=true&tlsCAFile=global-bundle.pem` is needed when TLS is turned on for the cluster, which is the default. Download `global-bundle.pem` as described in [Connecting programmatically to Amazon DocumentDB](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect_programmatically.html).
- `retryWrites=true` is supported in MongoDB and Amazon DocumentDB v8.0.2+.
- If your user name or password contains special characters such as `@`, `:`, or `/`, percent-encode them (for example, `@` becomes `%40`).

## Migrating a collection

A full load on its own isn't a consistent copy if the source is changing, because documents can change while it runs. To migrate without stopping your applications, replay the changes made during and after the full load with CDC:

1. **Make sure the source keeps changes long enough.** CDC in step 7 has to read changes made since step 4, so the source must still have them after the full load and the index builds. On Amazon DocumentDB, turn on change streams for the collection and check the change stream log retention (see [Change streams in Amazon DocumentDB](#change-streams-in-amazon-documentdb)). On MongoDB, check that the oplog covers a long enough period.
2. **Make sure the target collection is empty or doesn't exist.** The full load only inserts, and fails if a document with the same `_id` is already in the target.
3. **Calculate the boundaries** with the [dms-segments tool](../dms-segments). See [Boundaries](#boundaries).
4. **Write down the current time in UTC**, then **start the full load** right away. Subtract a few minutes from the time you wrote down, to allow for clock differences between your machine and the source. Replaying a few extra changes is safe.
5. **Wait for the full load to finish.**
6. **Create the indexes on the target.** Building indexes while documents are being loaded slows the load down, so it's faster to create them after the full load.
7. **Start CDC** with `--start-position` set to the time from step 4, in `YYYY-MM-DD+HH:MM:SS` format. CDC replays every change made since then. Changes to documents that the full load already copied are safe to replay: see [How CDC applies changes](#how-cdc-applies-changes).
8. **Watch `secs behind`** in the CDC progress output until it stays close to 0.
9. **Cut over:** stop your applications from writing to the source, wait until `secs behind` is 0 and the `tot` count stops increasing in a couple of progress reports, then stop CDC with Ctrl+C and point your applications at the target.

Don't run the full load and CDC at the same time. CDC can write a document to the target before the full load gets to it, which makes the full load fail on a duplicate `_id`.

## Full load

```
python3 fl-multiprocess.py --source-uri "<source-uri>" --target-uri "<target-uri>" --source-namespace <database>.<collection> --boundaries <boundary-list>
```

For example, to copy `sales.orders` in four parts:

```
python3 fl-multiprocess.py --source-uri "mongodb://<user>:<password>@source-host:27017/?replicaSet=rs0" --target-uri "mongodb://<user>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&retryWrites=false" --source-namespace sales.orders --boundaries 64a0f0b2c1e4a8b9d0f1e2a3,64b1a2c3d4e5f60718293a4b,64c2b3d4e5f6071829304b5c
```

### Boundaries

The boundaries are values of the boundary field (`_id` by default) that split the collection into parts. The tool starts one process for each part, so **N boundaries means N+1 processes copying in parallel**:

- The first process copies documents with values up to and including the first boundary.
- Each middle process copies documents after one boundary, up to and including the next.
- The last process copies documents after the last boundary.

Run the [dms-segments tool](../dms-segments) with `--num-segments` set to the number of processes you want. It prints a line like this:

```
boundaries as list | "64a0f0b2c1e4a8b9d0f1e2a3","64b1a2c3d4e5f60718293a4b","64c2b3d4e5f6071829304b5c"
```

Pass the values to `--boundaries` separated by commas. You can paste the list exactly as dms-segments prints it, or leave out the double quotes. Put the whole list in single quotes so your shell keeps it together, for example `--boundaries '"64a0f0b2c1e4a8b9d0f1e2a3","64b1a2c3d4e5f60718293a4b"'`.

Things to know about boundaries:

- **The boundary type must match the field.** `_id` values are usually ObjectIds, which is the default. If they're strings or integers, add `--boundary-datatype string` or `--boundary-datatype int`.
- **Every document must have the boundary field, of that same type.** Documents without the field, or with a value of a different type (for example, a string `_id` in a collection of ObjectIds), aren't copied, and the tool doesn't warn you.
- **The boundary field should be indexed.** Otherwise each process scans the whole collection. `_id` is always indexed.
- **Boundaries don't need to be exact.** Uneven parts still copy every document, but the biggest part decides how long the full load takes.

### Full load options

| Option | Default | Description |
|---|---|---|
| `--source-uri` | (required) | Connection string for the source. |
| `--target-uri` | (required) | Connection string for the target. |
| `--source-namespace` | (required) | Collection to copy, as `<database>.<collection>`. |
| `--target-namespace` | same as `--source-namespace` | Collection to copy into, as `<database>.<collection>`. |
| `--boundaries` | (required) | Comma-separated list of boundaries. See [Boundaries](#boundaries). |
| `--boundary-datatype` | `objectid` | Type of the boundaries: `objectid`, `string`, or `int`. |
| `--boundary-field-name` | `_id` | Field the boundaries apply to. |
| `--max-inserts-per-batch` | `100` | Number of documents each process inserts in one batch. |
| `--feedback-seconds` | `60` | Seconds between progress reports. |
| `--dry-run` | off | Read the documents from the source, but don't write anything to the target. |
| `--verbose` | off | Print more detail, such as when each process starts. |
| `--create-cloudwatch-metrics` | off | Publish progress to Amazon CloudWatch. Requires `--cluster-name`. See [CloudWatch metrics](#cloudwatch-metrics). |
| `--cluster-name` | none | Value of the `Cluster` dimension for the CloudWatch metrics. |
| `--skip-python-version-check` | off | Run on Python versions older than 3.7. Not tested. |

### Full load output

The first progress report is printed after `--feedback-seconds` (60 seconds by default), and then one every interval until the load finishes:

```
[2026-11-10T15:04:05.123456+00Z] elapsed 00:10:00.01 | total o/s     8,512.33 | interval o/s     9,104.50 | tot ops        5,107,400 | loading     4 | pct  51.07% | done in 00:09:35 | GB/hr  14.21
```

- `total o/s` and `interval o/s`: documents inserted per second since the start, and during the last interval.
- `tot ops`: documents inserted so far.
- `loading`: number of processes still copying.
- `pct` and `done in`: progress and estimated time left, based on the collection's document count when the load started. If documents are added during the load, `pct` can go past 100%.
- `GB/hr`: data copied per hour during the last interval, based on the BSON size of the documents.

The tool exits when every process has finished.

## Change data capture (CDC)

```
python3 cdc-multiprocess.py --source-uri "<source-uri>" --target-uri "<target-uri>" --source-namespace <database>.<collection> --start-position <start-position> --use-change-stream
```

`--use-change-stream` is required in every run. Change streams are the only CDC source the tool supports.

For example, to apply every change made to `sales.orders` since 2:00 PM UTC on November 10, 2026, using four processes:

```
python3 cdc-multiprocess.py --source-uri "mongodb://<user>:<password>@source-host:27017/?replicaSet=rs0" --target-uri "mongodb://<user>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&retryWrites=false" --source-namespace sales.orders --start-position 2026-11-10+14:00:00 --use-change-stream --threads 4
```

### Start position

`--start-position` sets where in the change stream to start. It can be:

- `NOW`: start with changes made from now on.
- A date and time in UTC, as `YYYY-MM-DD+HH:MM:SS`, for example `2026-11-10+14:00:00`: start with changes made at or after that time.
- A resume token, from the `resume token` field of the progress output: continue from where a previous run stopped.

The start position must still be in the change stream. On MongoDB, that means it must still be in the oplog. On Amazon DocumentDB, it must be within the change stream log retention period, and after change streams were turned on for the collection.

### Stopping and resuming

CDC runs until you press Ctrl+C, or for `--duration-seconds` seconds if you set it.

Each progress report shows a `resume token`. Every change up to that point has been written to the target. To continue later without missing any changes, start CDC again with `--start-position` set to the last resume token it printed. Some changes may be applied a second time, which is safe.

The resume token shows `N/A` until every process has a safe position to report. If no changes have been made to the collection since the start position, it can stay `N/A` until the first change.

Don't use a resume token from a `--dry-run` run, because no changes were written to the target during that run.

### Using more processes

By default, one process reads the change stream and applies the changes. If CDC can't keep up with the source (`secs behind` keeps growing), use `--threads` to run several processes in parallel. Each process reads the whole change stream and applies the changes for its share of the documents. All changes to the same document are applied by the same process, in order.

Each process opens its own change stream on the source, so more processes put more read load on the source.

### How CDC applies changes

- **Inserts** are inserted. If a batch fails, for example because a document already exists in the target, the whole batch is applied again with inserts turned into replaces. This is what makes it safe to replay changes the full load already copied.
- **Updates and replaces** replace the whole target document with the source document's current version (the change stream's `updateLookup` option), creating it if it doesn't exist. If the document has been deleted from the source since then, the change is skipped, and the later delete is applied.
- **Deletes** delete the document, if it exists in the target.
- **Other events**, such as collection drops and renames, are ignored.

Changes are written in batches. A batch is written when it has `--max-operations-per-batch` changes, or when its oldest change has waited `--max-seconds-between-batches` seconds, whichever comes first. So even when CDC is caught up, a change can take up to 5 seconds (by default) to reach the target.

### CDC options

| Option | Default | Description |
|---|---|---|
| `--source-uri` | (required) | Connection string for the source. |
| `--target-uri` | (required) | Connection string for the target. |
| `--source-namespace` | (required) | Collection to read changes from, as `<database>.<collection>`. |
| `--target-namespace` | same as `--source-namespace` | Collection to apply changes to, as `<database>.<collection>`. |
| `--start-position` | (required) | `NOW`, a UTC date and time as `YYYY-MM-DD+HH:MM:SS`, or a resume token. See [Start position](#start-position). |
| `--use-change-stream` | (required) | Read changes from the source's change stream. |
| `--threads` | `1` | Number of processes applying changes. See [Using more processes](#using-more-processes). |
| `--duration-seconds` | `0` | Seconds to run before exiting. `0` runs until you press Ctrl+C. |
| `--max-operations-per-batch` | `100` | Maximum number of changes each process writes in one batch. |
| `--max-seconds-between-batches` | `5` | Maximum seconds a change waits in a batch before the batch is written. |
| `--feedback-seconds` | `60` | Seconds between progress reports. |
| `--dry-run` | off | Read changes from the source, but don't write anything to the target. |
| `--verbose` | off | Print more detail, such as the time of the first change each process reads. |
| `--get-resume-token` | off | Wait for the next change to the source collection, print its resume token, and exit. Doesn't apply any changes. Starting CDC from this token starts *after* that change. `--start-position` must still be given, but is ignored. |
| `--create-cloudwatch-metrics` | off | Publish progress to Amazon CloudWatch. Requires `--cluster-name`. See [CloudWatch metrics](#cloudwatch-metrics). |
| `--cluster-name` | none | Value of the `Cluster` dimension for the CloudWatch metrics. |
| `--skip-python-version-check` | off | Run on Python versions older than 3.7. Not tested. |

### CDC output

The first progress report is printed after `--feedback-seconds` (60 seconds by default), and then one every interval:

```
[2026-11-10T15:04:05.123Z] elapsed 00:10:00.02 | total o/s     1,204 | interval o/s     1,580 | tot          722,400 |           42 secs behind | resume token = <resume-token>
```

- `total o/s` and `interval o/s`: changes applied per second since the start, and during the last interval.
- `tot`: changes applied so far. Updates that were skipped because the document was deleted aren't counted.
- `secs behind`: how far behind the source CDC is, based on the time of the most recent change each process has read. It's the average across processes, and processes that are caught up count as 0.
- `resume token`: see [Stopping and resuming](#stopping-and-resuming).

### Change streams in Amazon DocumentDB

On an Amazon DocumentDB source, turn on change streams for the collection with the `mongo` shell or `mongosh`:

```
db.adminCommand({modifyChangeStreams: 1, database: "<database>", collection: "<collection>", enable: true});
```

Changes are kept for the change stream log retention period, which is 3 hours by default and can be raised to 7 days with the `change_stream_log_retention_duration` cluster parameter. It must be longer than the time from your CDC start position to when CDC starts, which includes the whole full load. See [Using change streams with Amazon DocumentDB](https://docs.aws.amazon.com/documentdb/latest/developerguide/change_streams.html).

## CloudWatch metrics

With `--create-cloudwatch-metrics`, both scripts publish metrics to Amazon CloudWatch, in the `CustomDocDB` namespace with a `Cluster` dimension set to `--cluster-name`:

| Script | Metric | Description |
|---|---|---|
| Full load | `MigratorFLInsertsPerSecond` | Documents inserted per second during the last progress interval. |
| Full load | `MigratorFLRemainingSeconds` | Estimated seconds until the full load finishes. |
| CDC | `MigratorCDCOperationsPerSecond` | Changes applied per second during the last progress interval. |
| CDC | `MigratorCDCNumSecondsBehind` | Same as `secs behind` in the progress output. |

Metrics are published with a progress report, at most once every 30 seconds. With the default `--feedback-seconds` of 60, that's once a minute.

`--cluster-name` is only a label for the metrics, and can be any name you choose.

The tool uses the standard [boto3 credentials](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html), such as an EC2 instance profile, and needs the `cloudwatch:PutMetricData` IAM permission. The AWS Region must also be set, for example with the `AWS_DEFAULT_REGION` environment variable.

## Troubleshooting

- **`UserWarning: You appear to be connected to a DocumentDB cluster`:** this message comes from `pymongo` and is harmless.
- **Full load fails with `E11000 duplicate key error`:** the target collection already has some of the documents, for example from an earlier run. Drop or empty the target collection and run the full load again.
- **`bson.errors.InvalidId` or `ValueError` when the full load starts:** a boundary doesn't match `--boundary-datatype`.
- **Full load finishes, but the target has fewer documents than the source:** some documents don't have the boundary field, or have a value of a different type. See [Boundaries](#boundaries). Documents inserted into the source during the load may also be missing. CDC copies those.
- **`Retryable writes are not supported`:** add `retryWrites=false` to the target URI.
- **`ServerSelectionTimeoutError` after about 30 seconds:** the tool couldn't reach the server. Check that you're running from inside the cluster's VPC (or through an SSH tunnel), that the security group allows inbound traffic on port 27017 from your machine, and that the TLS options match your cluster's settings.
- **`error: the following arguments are required`** even though you included them: the URI isn't in double quotes, so your shell cut the command off at the first `&`.
- **CDC fails right away with an error about change streams:** on Amazon DocumentDB, turn on change streams for the collection. On MongoDB, connect to a replica set or sharded cluster, not a standalone server. If the error is about the start position or resume token, it's no longer in the change stream, so you need to run the full load again.
- **`--get-resume-token` doesn't print anything:** it waits for the next change to the collection. Nothing is printed until something writes to it.
- **`ModuleNotFoundError: No module named 'pymongo'` or `'boto3'`:** run `python3 -m pip install -r requirements.txt`.
- **`botocore.exceptions.NoRegionError` or `NoCredentialsError`:** set the AWS Region and credentials, or run without `--create-cloudwatch-metrics`.

## License

This tool is licensed under the Apache 2.0 License.
