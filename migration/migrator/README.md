# Amazon DocumentDB Change Data Capture (CDC) Synchronization Tool
This synchronization tool enables high-speed CDC from a MongoDB source database to an Amazon DocumentDB target database.

## Installation
Clone the repository.

## Requirements
* Python 3.7+
* PyMongo

## Using the tool
```
python3 cdc-multiprocess.py --source-uri <source-uri> --target-uri <target-uri> --source-namespace <database.collection> --start-position [0 or YYYY-MM-DD+HH:MM:SS in UTC]
```

* source-uri and target-uri follow the [MongoDB Connection String URI Format](https://www.mongodb.com/docs/manual/reference/connection-string/)
* source-namespace in database.collection format (i.e. "database1.collection2")
* start-position either 0 (process entire oplog) or specific oplog position as YYYY-MM-DD+HH:MM:SS in UTC
* optionally pass 2+ for the --threads option to process the oplog with concurrent processes
* several other optional parameters as supported, execute the script with -h for a full listing
