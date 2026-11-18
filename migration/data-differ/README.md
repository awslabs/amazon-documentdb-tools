# Amazon DocumentDB DataDiffer Tool

The purpose of the DataDiffer tool is to facilitate the validation of data consistency by comparing two collections, making it particularly useful in migration scenarios.
This tool performs the following checks:

- Document existence check: every document in the source collection must exist in the target collection, documents missing from the target and extra documents in the target are reported.
- Index Comparison: examines the indexes of the collections and reports any differences.
- Document Comparison: each document in the collections, with the same _id, is compared. Documents are read as raw BSON, byte-identical documents are equal. Documents whose bytes differ are decoded and compared using the DeepDiff library, field order changes and BSON type/value differences that DeepDiff does not detect (for example `0.0` vs `-0.0`, a binary subtype change, or `NumberDecimal("1.0")` vs `NumberDecimal("1.00")`) are also reported.
- Count verification: the number of documents compared is verified against the exact document count of each collection.

The comparison runs in parallel using a pool of worker processes (`--workers`, defaults to the number of CPUs).

## Prerequisites:

 - Python 3.9 or later
 - Modules: pymongo (4.3.3 or later), deepdiff (8.6.1 or later), rich (10.7.0 or later, optional, it provides the live dashboard), see [requirements.txt](requirements.txt)
```
  pip3 install -r requirements.txt
```
Note: Refer to the DeepDiff [documentation](https://zepworks.com/deepdiff/current/optimizations.html) for potential optimizations you may try out specifically for your dataset.

## How to use

1. Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/data-differ/
```

2. Run the data-differ.py tool, which accepts the following arguments:

```
python3 data-differ.py --help
usage: data-differ.py [-h] [--batch-size BATCH_SIZE] [--output-file OUTPUT_FILE] [--check-target] --source-uri SOURCE_URI --target-uri TARGET_URI --source-db SOURCE_DB
                      --target-db TARGET_DB --source-coll SOURCE_COLL --target-coll TARGET_COLL [--sample-size-percent SAMPLE_SIZE_PERCENT]
                      [--sampling-timeout-ms SAMPLING_TIMEOUT_MS] [--workers WORKERS] [--chunks CHUNKS] [--no-partition]
                      [--max-console-differences MAX_CONSOLE_DIFFERENCES] [--plain-progress] [--progress-interval PROGRESS_INTERVAL]

Compare two collections and report differences.

options:
  -h, --help            show this help message and exit
  --batch-size BATCH_SIZE
                        Batch size for bulk reads (optional, default: 1000)
  --output-file OUTPUT_FILE
                        Output file path (optional, default: differences.txt)
  --check-target        optional, Check if extra documents exist in target database
  --source-uri SOURCE_URI
                        Source cluster URI (required)
  --target-uri TARGET_URI
                        Target cluster URI (required)
  --source-db SOURCE_DB
                        Source database name (required)
  --target-db TARGET_DB
                        Target database name (required)
  --source-coll SOURCE_COLL
                        Source collection name (required)
  --target-coll TARGET_COLL
                        Target collection name (required)
  --sample-size-percent SAMPLE_SIZE_PERCENT
                        optional, if set only samples a percentage of the documents
  --sampling-timeout-ms SAMPLING_TIMEOUT_MS
                        optional, override the timeout for returning a sample of documents when using the --sample-size-percent argument
  --workers WORKERS     Degree of parallelism, number of worker processes (optional, default: number of CPUs)
  --chunks CHUNKS       Number of _id range chunks in range partitioned mode (optional, default: 8 x workers)
  --no-partition        optional, never use range partitioning, always use batched _id lookups
  --max-console-differences MAX_CONSOLE_DIFFERENCES
                        Maximum number of differences printed to the console, all differences are written to the output file (optional, default: 20)
  --plain-progress      optional, print plain progress lines instead of the live dashboard
  --progress-interval PROGRESS_INTERVAL
                        Seconds between plain progress lines (optional, default: 10)
```

## Example usage:
Connect to a standalone MongoDB instance as source and to a Amazon DocumentDB cluster as target.

From the source uri, compare the collection *mysourcecollection* from database *mysource*, against the collection *mytargetcollection* from database *mytargetdb* in the target uri.

```
python3 data-differ.py \
--source-uri "mongodb://user:password@mongodb-instance-hostname:27017/admin?directConnection=true" \
--target-uri "mongodb://user:password@target.cluster.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false" \
--source-db mysourcedb \
--source-coll mysourcecollection \
--target-db mytargetdb \
--target-coll mytargetcollection
```

For more information on the connection string format, refer to the [documentation](https://www.mongodb.com/docs/manual/reference/connection-string/).

The source and target collections should not be changing while they are compared (for example, stop the application or wait for change data capture to catch up), otherwise documents that are in flight are reported as differences. When reading from replicas (`readPreference=secondaryPreferred`) replication lag can also cause differences to be reported.

## How it works

The tool chooses one of two strategies, the strategy and the reason it was chosen are displayed when the tool starts.

### Range partitioned (fastest)
Used when the minimum and maximum _id values of both collections are all ObjectId values, all date values, or all integer (int/long) values. Because the database sorts _id values by type before value, this means every _id in both collections belongs to that type (integer partitioning also correctly handles double or decimal _id values that fall between the integer minimum and maximum).

The _id keyspace from the smallest minimum to the largest maximum of the two collections is divided mathematically into `--chunks` contiguous, non-overlapping ranges and the chunks are compared in parallel by `--workers` worker processes. Within a chunk the source documents are read in _id order in batches of `--batch-size` documents, and for every batch the target is queried for exactly the same _id window. This means:

- documents missing from the target and extra documents in the target are both found in a single pass, a reverse comparison (`--check-target`) is never needed
- the document count of every chunk is compared between the source and the target
- the minimum and maximum _id of the source and target are compared
- the total number of documents compared is verified against the exact document count of each collection

ObjectId values begin with a timestamp so the keyspace is divided by time, if your data was inserted unevenly over time some chunks will contain more documents than others. The default of 8 chunks per worker keeps the workers busy, increase `--chunks` if the workers are idle near the end of the comparison.

### Batched (all other _id types)
Used for any other _id type (strings, compound _id values, mixed types, ...), when sampling, or when `--no-partition` is specified. The main process reads the source _id values in batches and the worker processes fetch the matching documents from both collections with `$in` and compare them.

In this mode documents that only exist in the target are found by a reverse comparison. Every document of the source that was found in the target has a unique _id, so when the target document count equals the number of matched documents the target cannot contain any extra documents and the reverse comparison is skipped. Otherwise the number of unmatched target documents is reported, use `--check-target` to list their _id values.

## Output
A live dashboard shows the progress, throughput, the number of documents and the amount of data read from each collection, the number of identical, different, missing, and extra documents, and what each worker is doing. When the output is not a terminal (or with `--plain-progress`) a progress line is printed every `--progress-interval` seconds instead.

All differences are appended to the output file (default `differences.txt`):

```
Difference found at doc id: 1500
{'values_changed': {"root['v']": {'new_value': 'changed', 'old_value': 1500}}}
Document missing in target, _id: 1000
Document missing in source (extra in target), _id: 1001
Chunk 4 [1125, 1500) document count differs: source 188, target 187
```

_id values are written as [Relaxed Extended JSON](https://www.mongodb.com/docs/manual/reference/mongodb-extended-json/), for example `{"$oid": "65f1c1e5fd61c1e2e1edac2c"}`. A summary is printed and written at the end of every run.

The exit code is `0` when no differences were found, `1` when differences were found, and `2` when the comparison could not be completed (an error, an interruption, or both collections are empty).

## Performance
Examples comparing collections of ~400 byte documents with ObjectId _id values between two Amazon DocumentDB clusters, from a 16 vCPU client instance:

| Documents | Strategy | Workers | Elapsed | Throughput |
|---|---|---|---|---|
| 10,000,000 | previous version of the tool | - | 127 seconds | 79,000 docs/s |
| 10,000,000 | range partitioned | 1 | 73 seconds | 138,000 docs/s |
| 10,000,000 | range partitioned | 4 | 19 seconds | 522,000 docs/s |
| 10,000,000 | range partitioned | 16 | 6 seconds | 1,612,000 docs/s |
| 10,000,000 | batched (`--no-partition`) | 16 | 13 seconds | 787,000 docs/s |

The throughput depends on the size of the source and target instances, the client instance, and the network. Every worker runs queries on the source and the target, consider the impact on production workloads and reduce `--workers` if needed.

## Sampling
For large databases it might be unfeasible to compare every document as:
* It takes a long time to compare every document.
* Reading every document from a large busy database could have a performance impact.

If you use the `--sample-size-percent` option you can pass in a percentage of
documents to sample and compare.

E.g. `--sample-size-percent 1` would sample 1% of the documents in the source
database and compare them to the target database.

Under the hood this uses the [MongoDB `$sample` operator](https://www.mongodb.com/docs/manual/reference/operator/aggregation/sample/)
You should read the documentation on how that behaves on your version of MongoDB
when the percentage to sample is >= 5% before picking a percentage to sample.

The default timeout for retriving a sample of documents is `500ms`, if this is
not long enough you can adjust it with the `--sampling-timeout-ms` argument.
For example `--sampling-timeout-ms 600` would increase the timeout to `600ms`.

When sampling, extra documents in the target are only searched for when `--check-target` is specified.

## Testing
See [test-scripts/README.md](test-scripts/README.md).
