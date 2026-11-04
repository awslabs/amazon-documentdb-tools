# Amazon DocumentDB Index Tool

The index tool copies **index definitions** (not data) from an existing MongoDB-compatible database to Amazon DocumentDB. It also reports any indexes or collections that Amazon DocumentDB doesn't support, so you can deal with them before you migrate.

A typical migration uses it in three steps:

1. **Export** the index definitions from your source database to a local folder.
2. **Check** the exported definitions for compatibility issues.
3. **Restore** the compatible indexes to your Amazon DocumentDB cluster.

The tool doesn't copy documents. Use it alongside a data migration tool such as `mongodump`/`mongorestore` or [AWS Database Migration Service (AWS DMS)](https://docs.aws.amazon.com/dms/latest/userguide/CHAP_Target.DocumentDB.html).

- **Supported sources:** MongoDB 2.x and later (standalone, replica set, or sharded cluster), Amazon DocumentDB, and Azure Cosmos DB.
- **Supported target:** Amazon DocumentDB (instance-based or elastic clusters).

## Requirements

- Python 3.9 or later
- [`pymongo`](https://pypi.org/project/pymongo/) (installed from `requirements.txt` below)
- Network access to the databases you export from and restore to. Amazon DocumentDB clusters are only reachable from inside their VPC, so run the tool from a machine in the same VPC (for example an Amazon EC2 instance) or through an SSH tunnel. See [Connecting to an Amazon DocumentDB cluster from outside an Amazon VPC](https://docs.aws.amazon.com/documentdb/latest/developerguide/connect-from-outside-a-vpc.html).

The examples below use `python3`. On Windows, use `python` instead.

## Installation

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/index-tool
python3 -m pip install -r requirements.txt
```

All commands below are run from the `index-tool` folder.

## Quick start

1. Create a folder and export the index definitions from the source:
   ```
   mkdir index_export
   python3 migrationtools/documentdb_index_tool.py --dump-indexes --dir index_export --uri "mongodb://<username>:<password>@<source-host>:27017/"
   ```
2. Report anything Amazon DocumentDB doesn't support:
   ```
   python3 migrationtools/documentdb_index_tool.py --show-issues --dir index_export
   ```
3. Preview the restore with `--dry-run`, then run it again without `--dry-run` to create the supported indexes on Amazon DocumentDB. See [Connecting to Amazon DocumentDB](#connecting-to-amazon-documentdb) for how to build `<documentdb-uri>`.
   ```
   python3 migrationtools/documentdb_index_tool.py --restore-indexes --skip-incompatible --dry-run --dir index_export --uri "<documentdb-uri>"
   python3 migrationtools/documentdb_index_tool.py --restore-indexes --skip-incompatible --dir index_export --uri "<documentdb-uri>"
   ```

Each step is described in detail below.

## Connecting to Amazon DocumentDB

Amazon DocumentDB clusters have TLS enabled by default. To connect:

1. Download the Amazon RDS certificate bundle into the `index-tool` folder:
   ```
   curl -O https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
   ```
2. Use the **cluster endpoint** (not an instance endpoint) and include these options in the URI:
   ```
   mongodb://<username>:<password>@<cluster-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&replicaSet=rs0&retryWrites=false
   ```
   - `tls=true&tlsCAFile=global-bundle.pem` enables TLS using the downloaded certificate bundle. Omit both if TLS is disabled on your cluster.
   - `replicaSet=rs0` lets the driver discover the cluster's primary instance.
   - `retryWrites=false` is required because Amazon DocumentDB doesn't support retryable writes.

You can copy a ready-made connection string from the **Connectivity & security** tab of your cluster in the Amazon DocumentDB console. Special characters in the username or password must be [percent-encoded](https://www.mongodb.com/docs/manual/reference/connection-string/). Put the URI in quotes so your shell doesn't interpret the `&` characters.

## Step 1: Export index definitions

```
mkdir index_export
python3 migrationtools/documentdb_index_tool.py --dump-indexes --dir index_export --uri "<source-uri>"
```

- **The `--dir` folder must already exist.** The tool exits with `--dir must specify a directory` if it doesn't.
- The tool writes one file per collection, organized by database: `index_export/<database>/<collection>.metadata.json`. Each file holds the collection's options and the definition of each of its indexes. This is the same layout `mongodump` uses.
- The `admin`, `config`, `local`, and `system` databases, `system.*` collections, and views are skipped.
- The user in the URI needs permission to list databases, collections, and indexes on every database. The built-in `readAnyDatabase` role is sufficient.
- Add `--dry-run` to print the definitions to the screen instead of writing files.

Example output:
```
2026-11-04 10:47:41,741: Connecting to instance using provided URI
2026-11-04 10:47:41,772:   .. successfully connected
2026-11-04 10:47:41,772: Retrieving indexes from server...
2026-11-04 10:47:41,819: Completed writing index metadata to local folder: index_export
```

The export files are plain JSON, so you can review them or keep them under version control.

## Step 2: Check compatibility

`--show-issues` reads the exported files and lists every index or collection that Amazon DocumentDB doesn't support. No connection is needed.

```
python3 migrationtools/documentdb_index_tool.py --show-issues --dir index_export
```

If everything is compatible, the tool prints `No incompatibilities found.` Otherwise it prints a JSON report grouped by database, collection, and index. For example:

```
{
    "shop": {
        "events": {
            "unsupported_collection_options": [
                "capped"
            ]
        },
        "places": {
            "legacy_2d": {
                "unsupported_index_types": "2d"
            },
            "loc_2dsphere": {
                "unsupported_index_types": "2dsphere"
            }
        },
        "users": {
            "$**_1": {
                "unsupported_field_names": [
                    "$**"
                ]
            },
            "userId_hashed": {
                "unsupported_index_types": "hashed"
            }
        }
    }
}
```

To see what *will* be restored instead, use `--show-compatible`. It prints the full definition of every compatible collection and index.

```
python3 migrationtools/documentdb_index_tool.py --show-compatible --dir index_export
```

### What the tool checks

| Report entry | Meaning | What gets skipped on restore |
|---|---|---|
| `unsupported_collection_options` | The collection is `capped`. | The whole collection and all of its indexes. |
| `unsupported_index_types` | The index is `2d`, `2dsphere`, `geoHaystack`, or `hashed`. | The index. |
| `unsupported_field_names` | The index is a wildcard index (`$**`). | The index. |
| `unsupported_index_options` | The index uses the `storageEngine` or `dropDuplicates` option. | The index. |
| `unsupported_index_sort_value` | A key's sort order is a number other than `1` or `-1`. | The index. |
| `exceeded_limits` | A name or index exceeds an Amazon DocumentDB limit (see below). | The index, or the whole collection if the collection name or namespace is too long. A database name that's too long is reported but not skipped, so the target rejects it during restore. |

The `exceeded_limits` checks are:
- Database name longer than 63 characters
- Collection name longer than 255 characters, or `<database>.<collection>` longer than 120 characters
- `<collection>$<index name>` longer than 255 characters, or `<database>.<collection>$<index name>` longer than 377 characters (see [Long index names](#long-index-names))
- More than 32 keys in a compound index
- Text indexes that include non-text fields (for example `{title: "text", category: 1}`)

The tool doesn't know which Amazon DocumentDB version you're restoring to, and these checks are the same for every version. Some name-length limits differ by version: for example, Amazon DocumentDB 5.0 and later allow `<database>.<collection>` up to 255 characters, while 4.0 and earlier allow collection names of only 57 characters. Compare names close to these limits against [Amazon DocumentDB quotas](https://docs.aws.amazon.com/documentdb/latest/developerguide/limits.html) for your version. Support for some index features also varies by version (see [Supported MongoDB APIs, operations, and data types](https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html)). If the target rejects an index during restore, the tool logs the reason and moves on when `--skip-incompatible` is used.

#### 2dsphere indexes

Amazon DocumentDB supports `2dsphere` indexes only on GeoJSON `Point` data, so the tool reports them as incompatible by default. If your indexed fields contain only `Point` values, add `--support-2dsphere` to `--show-issues`, `--show-compatible`, and `--restore-indexes` to treat them as compatible.

#### Long index names

Add `--shorten-index-name` to restore indexes whose names are too long instead of skipping them. The tool truncates each long name and appends five random characters to keep it unique. Your application doesn't need to change unless it refers to indexes by name, for example in `hint()` or `dropIndex()`. Pass the option to `--show-issues` too, to see the report that matches what will be restored.

## Step 3: Restore indexes to Amazon DocumentDB

Preview first with `--dry-run`. This connects to the target but creates nothing:

```
python3 migrationtools/documentdb_index_tool.py --restore-indexes --skip-incompatible --dry-run --dir index_export --uri "<documentdb-uri>"
```

```
(dry run) shop.orders: would attempt to add index: email_unique
  (dry run) index options: OrderedDict({'name': 'email_unique', 'unique': True, 'ns': 'shop.orders'})
  (dry run) index keys: [('email', 1)]
```

Then run it without `--dry-run`:

```
python3 migrationtools/documentdb_index_tool.py --restore-indexes --skip-incompatible --dir index_export --uri "<documentdb-uri>"
```

```
2026-11-04 10:48:13,302: shop.orders: added index: _id_
2026-11-04 10:48:13,313: shop.orders: added index: customerId_1_orderDate_-1
2026-11-04 10:48:13,323: shop.orders: added index: email_unique
```

How restore behaves:

- **Without `--skip-incompatible`, nothing is restored if any incompatibility exists.** The tool prints `incompatible indexes exist and --skip-incompatible not specified.` and stops. You will almost always want `--skip-incompatible`.
- **With `--skip-incompatible`**, the tool skips everything `--show-issues` reports. If the target rejects an index anyway, the tool logs `skipping index <name> because create_index failed: <reason>` and continues.
- **Collections are created as needed.** Every compatible collection is created on the target, even if it has no indexes other than `_id`. Collections skipped as incompatible (for example capped collections) are not created.
- **Re-running is safe.** Creating an index that already exists with the same definition does nothing, so you can run the restore again after fixing a problem. (The tool still prints `added index` for these.)
- The tool stops immediately if the target reaches the Amazon DocumentDB limit on the number of collections.
- The user in the URI needs permission to create collections and indexes in every database being restored, such as the cluster's admin user or the `readWriteAnyDatabase` role.

### Before or after loading data?

You can restore indexes before or after loading your data:

- **Before:** indexes are maintained as data is loaded, and unique indexes are enforced from the start.
- **After:** loading is faster, but each index has to be built over the full collection afterward, and a unique index fails to build if the loaded data contains duplicates.

## Using a mongodump backup

If you already have a `mongodump` backup, you can check it for compatibility issues without connecting to the source:

```
python3 migrationtools/documentdb_index_tool.py --show-issues --dir /path/to/dump
```

- The backup must be an uncompressed directory dump. Backups taken with `--gzip` (`.metadata.json.gz` files) or `--archive` aren't supported.
- If the backup contains views, delete their `.metadata.json` files first (they contain `"type":"view"`). Otherwise the tool exits with `Failed to load collection metadata: 'indexes'`.
- To **restore** indexes, export them with `--dump-indexes` (Step 1) rather than using `mongodump` output. Current versions of `mongodump` write numbers in a format (`{"$numberInt": "1"}`) that the restore step doesn't handle, so every index fails to create.

## Command-line reference

```
python3 migrationtools/documentdb_index_tool.py <action> --dir DIR [options]
```

Exactly one action is required:

| Action | Description | Needs `--uri` |
|---|---|---|
| `--dump-indexes` | Export index definitions from the server in `--uri` to `--dir`. | Yes (source) |
| `--show-issues` | Report compatibility issues in the definitions in `--dir`. | No |
| `--show-compatible` | Print the compatible collections and indexes in `--dir`. | No |
| `--restore-indexes` | Create the indexes from `--dir` on the server in `--uri`. | Yes (target) |

Options:

| Option | Description |
|---|---|
| `--dir DIR` | Folder to export to or read from. Required, and must already exist. Subfolders are searched for `.metadata.json` files. |
| `--uri URI` | Connection string for the source (`--dump-indexes`) or target (`--restore-indexes`). |
| `--dry-run` | Show what would be exported or restored without writing files or creating indexes. |
| `--skip-incompatible` | Restore the compatible indexes and skip the rest. Without it, restore stops if any issue is found. |
| `--support-2dsphere` | Treat `2dsphere` indexes as compatible. Only use this if the indexed fields contain only GeoJSON `Point` values. |
| `--shorten-index-name` | Shorten index names that exceed Amazon DocumentDB limits instead of skipping those indexes. |
| `--skip-id-indexes` | Don't create `_id` indexes during restore. Every collection gets one automatically, so this only changes whether the tool requests it explicitly. |
| `--debug` | Print detailed progress information. |
| `--skip-python-version-check` | Run on an older Python version than the tool supports. |

The tool exits with code `0` even when it reports compatibility issues or stops because of an error, so check its output rather than its exit code in scripts.

## Running with Docker

A `Dockerfile` is included. Mount a local folder for the export files:

```
docker build -t documentdb-index-tool .
docker run --rm -v "$(pwd)/index_export:/index_export" documentdb-index-tool --show-issues --dir /index_export
```

To connect to Amazon DocumentDB with TLS from the container, put `global-bundle.pem` in the `index-tool` folder before building the image.

## Contributing

Contributions are always welcome! See the [contributing page](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) for ways to get involved.

## License

[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)
