# Amazon DocumentDB Full Load and Change Data Capture (CDC) Synchronization Tool
This synchronization tool enables high-speed Full Load and CDC from a MongoDB/DocumentDB source database to an Amazon DocumentDB target database.

The full load script requires "boundaries" for parallelism, you can run the [dms-segments tool](https://github.com/awslabs/amazon-documentdb-tools/tree/master/migration/dms-segments) to calculate them.

## Installation
Clone the repository.

## Requirements
* Python 3.7+
* PyMongo

## Using the tool
```
python3 cdc-multiprocess.py --source-uri <source-uri> --target-uri <target-uri> --source-namespace <database.collection> --start-position [0 or YYYY-MM-DD+HH:MM:SS in UTC] --use-[oplog|change-stream]
```

* source-uri and target-uri follow the [MongoDB Connection String URI Format](https://www.mongodb.com/docs/manual/reference/connection-string/)
* source-namespace in database.collection format (i.e. "database1.collection2")
* start-position either 0 (process entire oplog) or specific oplog position as YYYY-MM-DD+HH:MM:SS in UTC
* must pass either --use-oplog for oplog to be source (MongoDB only) or --use-change-stream to use change streams for source (MongoDB or DocumentDB)
* optionally pass 2+ for the --threads option to process the oplog with concurrent processes
* several other optional parameters as supported, execute the script with -h for a full listing
