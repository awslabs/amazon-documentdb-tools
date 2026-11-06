# Amazon DocumentDB Compression Review Tool

The compression review tool estimates how much [document compression](https://docs.aws.amazon.com/documentdb/latest/developerguide/doc-compression.html) could shrink each of your collections. It samples real documents from every collection, compresses them with the algorithm you choose, and writes the results to a CSV file. Use it before you enable compression, or before you migrate to Amazon DocumentDB, to see which collections benefit most.

Amazon DocumentDB supports two compression algorithms. By default, the tool tests both:

| Tool compressor | Amazon DocumentDB algorithm | Available in |
|---|---|---|
| `lz4-fast` | LZ4 | Amazon DocumentDB 5.0 and later |
| `zstd-3-dict` | Zstandard (Zstd) with a dictionary trained on each collection | Amazon DocumentDB 8.0 and later ([dictionary-based compression](https://docs.aws.amazon.com/documentdb/latest/developerguide/dict-compression.html)) |

The tool only reads data. It doesn't change anything on the server.

- **Supported sources:** Amazon DocumentDB (instance-based clusters) and MongoDB (standalone or replica set).
- **What it reports:** compressibility, meaning the best compression you can expect. The actual compression you get in Amazon DocumentDB is usually lower because of storage fragmentation and write amplification.

## Requirements

- Python 3.7 or later. Python 3.9 or later is recommended, because current releases of the required packages need it.
- The [`pymongo`](https://pypi.org/project/pymongo/), [`lz4`](https://pypi.org/project/lz4/), and [`zstandard`](https://pypi.org/project/zstandard/) Python packages (installed from `requirements.txt` below).
- A database user that can list databases and collections, run `collStats`, and read documents in every database. The built-in `readAnyDatabase` role covers all of these.
- Network access to the server. Amazon DocumentDB clusters are only reachable from inside their VPC, so run the tool from a machine in the same VPC (for example an Amazon EC2 instance) or through an SSH tunnel. See [Connecting to an Amazon DocumentDB cluster from outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html).

The examples below use `python3`. On Windows, use `python` instead.

## Installation

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/compression-review
python3 -m pip install -r requirements.txt
```

If your source is an older MongoDB server, you may need an older `pymongo` release. These versions have been tested:

| Server | pymongo |
|---|---|
| Amazon DocumentDB | 3.10 or later |
| MongoDB 2.6 - 3.4 | 3.10 - 3.12 |
| MongoDB 3.6 - 5.0 | 3.12 - 4.0 |
| MongoDB 5.1 or later | 4.0 or later |

For example: `python3 -m pip install "pymongo>=3.10,<3.13"`.

## Quick start

```
python3 compression-review.py --uri "<uri>" --server-alias <alias>
```

- `--uri` is the connection string for your server. For Amazon DocumentDB, see [Connecting to Amazon DocumentDB](#connecting-to-amazon-documentdb).
- `--server-alias` is any short name you choose, such as `prod-cluster`. The tool uses it at the start of each output file name. Use a different alias for each server you analyze so you can tell the results apart.

The tool prints each collection as it works through it:

```
connecting to server
analyzing collection shop.orders
creating dictionary for shop.orders of 4096 bytes using 100 samples
...
```

When it finishes, you'll find one CSV file per compressor in the current folder. A default run tests two compressors, so it creates two files:

```
prod-cluster-lz4-fast-20250101120000-compression-review.csv
prod-cluster-zstd-3-dict-20250101120000-compression-review.csv
```

The file names have the form `<alias>-<compressor>-<timestamp>-compression-review.csv`. The timestamp is in UTC, in `YYYYMMDDHHMMSS` format.

## Connecting to Amazon DocumentDB

Amazon DocumentDB clusters have TLS enabled by default. To connect:

1. Download the Amazon RDS certificate bundle into the `compression-review` folder:
   ```
   curl -O https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
   ```
2. Build a URI from your **cluster endpoint** with these options:
   ```
   mongodb://<username>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false
   ```
   - `tls=true&tlsCAFile=global-bundle.pem` turns on TLS using the downloaded certificate bundle. Leave out both options if TLS is disabled on your cluster.
   - `replicaSet=rs0` lets the driver find all the instances in your cluster.
   - `readPreference=secondaryPreferred` sends the sampling reads to a replica instance, if your cluster has one, so they don't add load to the primary instance.
   - `retryWrites=false` is required because Amazon DocumentDB doesn't support retryable writes.

You can copy a ready-made connection string from the **Connectivity & security** tab of your cluster in the Amazon DocumentDB console. Special characters in the username or password must be [percent-encoded](https://www.mongodb.com/docs/manual/reference/connection-string/).

**Always put the URI in double quotes.** Without them, your shell treats each `&` in the URI as the end of the command, and the tool receives only part of the URI.

For MongoDB, you can connect to any member of the replica set. See the [MongoDB connection string documentation](https://www.mongodb.com/docs/manual/reference/connection-string/) for the URI format.

## How it works

For each collection, the tool:

1. Reads the collection's statistics with `collStats` (document count, sizes, and current compression settings).
2. For dictionary compressors (names ending in `-dict`), builds a compression dictionary (4 KB by default) from randomly sampled documents (100 by default). This mirrors Amazon DocumentDB, which trains a dictionary for each collection. Collections with fewer than 100 documents are compressed without a dictionary, because Amazon DocumentDB doesn't train one until a collection has at least 100 documents.
3. Picks 1,000 random documents (or all of them, for smaller collections), converts each one to JSON text, and compresses each document separately.
4. Compares the average original document size with the average compressed size, and writes one row to the CSV file.

The tool skips:
- the `admin`, `config`, `local`, and `system` databases
- views
- `system.profile` and `system.views` collections
- empty collections

Sampling 1,000 documents per collection gives a good estimate for most workloads while keeping the load on the server light. Increase `--sample-size` if your documents vary a lot within a collection. Larger samples take longer and read more data.

## Command-line options

| Option | Default | Description |
|---|---|---|
| `--uri <uri>` | (required) | Connection string for the server to analyze. |
| `--server-alias <alias>` | (required) | Short name used at the start of each output file name. |
| `--compressor <name>` | `lz4-fast` and `zstd-3-dict` | Test a single compressor instead of the two defaults. See [Compressors](#compressors). |
| `--sample-size <n>` | `1000` | Number of documents to sample in each collection. |
| `--dictionary-sample-size <n>` | `100` | Number of documents used to train the dictionary for `-dict` compressors. |
| `--dictionary-size <bytes>` | `4096` | Size of the dictionary for `-dict` compressors. |
| `--skip-python-version-check` | | Run on Python versions older than 3.7. This is not supported. |

### Compressors

Only `lz4-fast` and `zstd-3-dict` match what Amazon DocumentDB uses. The others are there for comparison.

| Name | Algorithm |
|---|---|
| `lz4-fast` | LZ4, fast mode. **Matches Amazon DocumentDB LZ4 compression.** |
| `lz4-high` | LZ4, high-compression mode |
| `lz4-fast-dict`, `lz4-high-dict` | LZ4 with a dictionary |
| `zstd-1`, `zstd-3`, `zstd-5` | Zstandard at level 1, 3, or 5, without a dictionary |
| `zstd-3-dict` | Zstandard level 3 with a dictionary. **Matches Amazon DocumentDB Zstd compression.** |
| `zstd-1-dict`, `zstd-5-dict` | Zstandard at level 1 or 5, with a dictionary |
| `bz2-1` | bzip2, level 1 |
| `lzma-0` | LZMA (xz), preset 0 |
| `zlib-1` | zlib, level 1 |

Example, testing Zstandard without a dictionary on 5,000 documents per collection:

```
python3 compression-review.py --uri "<uri>" --server-alias prod-cluster --compressor zstd-3 --sample-size 5000
```

## Reading the results

Each CSV file starts with the settings used for the run, then a blank line, then one row per collection:

```
compressor,docsSampled,dictDocsSampled,dictBytes
zstd-3-dict,1000,100,4096

dbName,collName,numDocs,avgDocSize,sizeGB,storageGB,existingCompRatio,compEnabled,minSample,maxSample,avgSample,minComp,maxComp,avgComp,projectedCompRatio,exceptions,compTime(ms)
shop,orders,5000,449,0.0021,0.0008,2.7856,,440,452,449,22,125,113,3.9735,0,2.6814
```

In this example, the `orders` collection has an average document size of 449 bytes, which compresses to an average of 113 bytes, so `projectedCompRatio` is 449 / 113 = 3.97.

Because of the settings at the top, the file won't import cleanly as a single table. In a spreadsheet, start reading at the `dbName` header row.

**The most important column is `projectedCompRatio`.** It's the estimated ratio of uncompressed size to compressed size. For example, `4.0` means the documents could be stored in about a quarter of the space (75% smaller). A value close to `1.0` means compression won't help that collection much.

| Column | Description |
|---|---|
| `dbName`, `collName` | Database and collection name. |
| `numDocs` | Number of documents in the collection. |
| `avgDocSize` | Average document size in bytes, as reported by the server. |
| `sizeGB` | Total uncompressed size of the documents, in GB. |
| `storageGB` | Storage the collection currently uses, in GB. |
| `existingCompRatio` | `sizeGB` divided by `storageGB`: the compression the collection already gets. On MongoDB this reflects WiredTiger storage compression, so it isn't directly comparable to Amazon DocumentDB. Small collections can show very large or very small values, because storage is allocated in blocks. |
| `compEnabled` | `Y/<threshold>` if Amazon DocumentDB compression is already turned on for the collection, where `<threshold>` is the minimum document size in bytes that gets compressed. Empty if compression is off, or the source is MongoDB. This column doesn't show which algorithm (LZ4 or Zstd) the collection uses. To check, run `db.<collection>.stats().compression` and look at the `algorithm` field. |
| `minSample`, `maxSample`, `avgSample` | Smallest, largest, and average size in bytes of the sampled documents, measured as JSON text. |
| `minComp`, `maxComp`, `avgComp` | Smallest, largest, and average size in bytes of the sampled documents after compression. |
| `projectedCompRatio` | `avgDocSize` divided by `avgComp`. The estimated compressibility of the collection. |
| `exceptions` | `1` if an error stopped sampling for this collection early (the row then covers only the documents read before the error), otherwise `0`. |
| `compTime(ms)` | Total time spent compressing the sampled documents, in milliseconds. Use it to compare the CPU cost of compressors. |

### Next steps

- Compare compressors by running the tool with each one and comparing `projectedCompRatio` for the same collection. A higher ratio saves more storage. Compare `compTime(ms)` to see the CPU cost.
- Amazon DocumentDB only compresses documents larger than the collection's compression threshold. The default threshold is 2,032 bytes for LZ4 and 128 bytes for Zstd, and you can set it anywhere from 128 to 8,000 bytes. If most of a collection's documents are smaller than the threshold (see `avgDocSize`), the collection will see little benefit unless you lower the threshold.
- To turn on compression, see [Managing collection-level document compression](https://docs.aws.amazon.com/documentdb/latest/developerguide/doc-compression.html) (LZ4) or [Managing dictionary-based compression](https://docs.aws.amazon.com/documentdb/latest/developerguide/dict-compression.html) (Zstd).
- Turning on compression for an existing collection only affects documents inserted or updated afterward. Existing documents stay uncompressed until they're rewritten.

## Troubleshooting

- **`ServerSelectionTimeoutError` after about 30 seconds:** the tool couldn't reach the server. Check that you're running from inside the cluster's VPC (or through an SSH tunnel), that the security group allows inbound traffic on port 27017 from your machine, and that the TLS options match your cluster's settings. A CSV file containing only headers may be left behind. You can delete it.
- **`error: the following arguments are required: --server-alias`** even though you included it: the URI isn't in double quotes, so your shell cut the command off at the first `&`.
- **`ModuleNotFoundError: No module named 'lz4'`** (or `zstandard`, or `pymongo`): run `python3 -m pip install -r requirements.txt`.
- **A collection is missing from the output:** it's empty, a view, or in one of the [skipped databases](#how-it-works).

## License

This tool is licensed under the Apache 2.0 License.
