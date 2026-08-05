# DocumentDB-Reindex

In Amazon DocumentDB, high-write workloads cause index fragmentation (bloat) over time. Reindexing removes this bloat and reclaims storage. This tool identifies highly bloated indexes and provides the reindex commands to remove the bloat.

## Requirements

Python 3.8 or greater, Pymongo.

boto3 is only required if using `--secret-name` to retrieve credentials from AWS Secrets Manager.

## Installation

```
git clone <GIT-REPOSITORY>
cd DocumentDB-Reindex
python3 -m pip install -r requirements.txt
```

## Usage

```
--uri                              Amazon DocumentDB URI
--secret-name                      AWS Secrets Manager secret name containing MongoDB URI
--region                           AWS region for Secrets Manager. Defaults to us-east-1
--unusedCollectionSizePercent      Unused collection size percentage. Defaults to 30
--workers                          Number of workers for reindex operation. Defaults to 2
--tls-ca-file                      Path to CA file for TLS connections (e.g., global-bundle.pem)
--output-file                      Path to write the reindex commands to a file
```

## Example commands

```
python reindex-script.py --uri mongodb://localhost:27017/ --unusedCollectionSizePercent 30 --workers 4

python reindex-script.py --uri "mongodb://user:pass@docdb-cluster.amazonaws.com:27017/?replicaSet=rs0&readPreference=secondaryPreferred" --tls-ca-file global-bundle.pem --output-file reindex-commands.txt

python reindex-script.py --secret-name my-docdb-secret --region us-west-2 --unusedCollectionSizePercent 50
```

## Example Output

```
db.runCommand({ reIndex: "dummy.books", index: "_id_", workers: 4 })
db.runCommand({ reIndex: "dummy.books", index: "isbn_1_title_1_pageCount_1", workers: 4 })
db.runCommand({ reIndex: "dummy.books", index: "shortDescription_text", workers: 4 })
```
