# Amazon DocumentDB DataDiffer Tool

The purpose of the DataDiffer tool is to facilitate the validation of data consistency by comparing two collections, making it particularly useful in migration scenarios.
This tool performs the following checks:

- Document existence check: It reads documents in batches from the source collection and checks for their existence in the target collection. If there is a discrepancy, the tool attempts will identify and report the missing documents.
- Index Comparison: examines the indexes of the collections and reports any differences.
- Document Comparison: each document in the collections, with the same _id, is compared using the DeepDiff library. This process can be computationally intensive, as it involves scanning all document fields. The duration of this check depends on factors such as document complexity and the CPU resources of the machine executing the script.

## Prerequisites:

 - Python 3
 - Modules: pymongo, deepdiff, tqdm
```
  pip3 install pymongo deepdiff tqdm
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
usage: data-differ.py [-h] [--batch-size BATCH_SIZE] [--output-file OUTPUT_FILE] [--check-target] --source-uri SOURCE_URI --target-uri TARGET_URI --source-db SOURCE_DB --target-db TARGET_DB --source-coll SOURCE_COLL --target-coll TARGET_COLL [--sample-size_percent SAMPLE_SIZE_PERCENT] [--sampling-timeout-ms SAMPLING_TIMEOUT_MS]

Compare two collections and report differences.

options:
  -h, --help            show this help message and exit
  --batch-size BATCH_SIZE
                        Batch size for bulk reads (optional, default: 100)
  --output-file OUTPUT_FILE
                        Output file path (optional, default: differences.txt)
  --check-target
                        optional, Check if extra documents exist in target database
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
```

## Example usage:
Connect to a standalone MongoDB instance as source and to a Amazon DocumentDB cluster as target.

From the source uri, compare the collection *mysourcecollection* from database *mysource*, against the collection *mytargetcollection* from database *mytargetdb* in the target uri.

```
python3 data-differ.py \
--source-uri "mongodb://user:password@mongodb-instance-hostname:27017/admin?directConnection=true" \
--target-uri "mongodb://user:password@target.cluster.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false" \
--source-db mysourcedb \
--source-coll mysourcecollection \
--target-db mytargetdb \
--target-coll mytargetcollection
```

For more information on the connection string format, refer to the [documentation](https://www.mongodb.com/docs/manual/reference/connection-string/).

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
For example `--sample-timeout-ms 600` would increase the timeout to `600ms`.
