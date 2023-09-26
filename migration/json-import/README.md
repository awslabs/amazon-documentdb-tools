# Amazon DocumentDB JSON Import Tool

The purpose of the JSON Import Tool is to load JSON formatted data from a single file into DocumentDB or MongoDB in parallel. Input file must contain one JSON document per line.

## Prerequisites:

 - Python 3
 - Modules: pymongo
```
  pip3 install pymongo
```
## How to use

1. Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/migration/json-import/
```

2. Run the json-import.py tool, which accepts the following arguments:

```
python3 json-import.py --help
usage: json-import.py [-h] --uri URI --file-name FILE_NAME --operations-per-batch OPERATIONS_PER_BATCH --workers WORKERS --database DATABASE --collection COLLECTION --log-file-name LOG_FILE_NAME
                      [--skip-python-version-check] [--lines-per-chunk LINES_PER_CHUNK] [--debug-level DEBUG_LEVEL] --mode {insert,replace,update} [--drop-collection]

Bulk/Concurrent JSON file import utility.

optional arguments:
  -h, --help            show this help message and exit
  --uri URI             URI
  --file-name FILE_NAME
                        Name of JSON file to load
  --operations-per-batch OPERATIONS_PER_BATCH
                        Number of operations per batch
  --workers WORKERS     Number of parallel workers
  --database DATABASE   Database name
  --collection COLLECTION
                        Collection name
  --log-file-name LOG_FILE_NAME
                        Log file name
  --skip-python-version-check
                        Permit execution on Python 3.6 and prior
  --lines-per-chunk LINES_PER_CHUNK
                        Number of lines each worker reserves before jumping ahead in the file to the next chunk
  --debug-level DEBUG_LEVEL
                        Debug output level.
  --mode {insert,replace,update}
                        Mode - insert, replace, or update
  --drop-collection     Drop the collection prior to loading data

```

## Example usage:
Load data (as inserts) from JSON formatted file load-me.json

```
python3 json-import.py \
  --uri "mongodb://user:password@target.cluster.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&readPreference=secondaryPreferred&retryWrites=false" \
  --file-name load-me.json
  --operations-per-batch 100
  --workers 4
  --database jsonimport
  --collection coll1
  --log-file-name json-import-log-file.log
  --lines-per-chunk 1000
  --mode insert
  --drop-collection
```

For more information on the connection string format, refer to the [documentation](https://www.mongodb.com/docs/manual/reference/connection-string/).
