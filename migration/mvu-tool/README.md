# Amazon DocumentDB MVU CDC Migrator Tool

The purpose of mvu cdc migrator tool is to migrate the cluster wide changes from source Amazon DocumentDB Cluster to target  Amazon DocumentDB Cluster.

It enables to perform near zero downtime Major Version Upgrade(MVU) from Amazon DocumentDB 3.6  to Amazon DocumentDB 5.0.

This tool is only recommended for performing MVU from Amazon DocumentDB 3.6. If you are performing MVU from Amazon DocumentDB 4.0 to 5.0, we recommend using the AWS Database Migration Service CDC approach.

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
cd amazon-documentdb-tools/mvu-tool/
```

2. Run the mvu-cdc-migrator.py tool to capature the cluster wide change stream token and migrate the changes. It accepts the following arguments:
```
python3 mvu-cdc-migrator.py  --help
usage: mvu-cdc-migrator.py [-h] [--skip-python-version-check] --source-uri SOURCE_URI [--target-uri TARGET_URI]
                           [--source-database SOURCE_DATABASE] 
                           [--duration-seconds DURATION_SECONDS]
                           [--feedback-seconds FEEDBACK_SECONDS] [--threads THREADS]
                           [--max-seconds-between-batches MAX_SECONDS_BETWEEN_BATCHES]
                           [--max-operations-per-batch MAX_OPERATIONS_PER_BATCH]    
                           [--dry-run] --start-position START_POSITION
                           [--verbose] [--get-resume-token]

MVU CDC Migrator Tool.

options:
  -h, --help            show this help message and exit
  --skip-python-version-check
                        Permit execution on Python 3.6 and prior
  --source-uri SOURCE_URI
                        Source URI
  --target-uri TARGET_URI
                        Target URI you can skip if you run with get-resume-token
  --source-database SOURCE_DATABASE
                        Source database name if you skip it will replicate all the databases
  --duration-seconds DURATION_SECONDS
                        Number of seconds to run before exiting, 0 = run forever
  --feedback-seconds FEEDBACK_SECONDS
                        Number of seconds between feedback output
  --threads THREADS     Number of threads (parallel processing)
  --max-seconds-between-batches MAX_SECONDS_BETWEEN_BATCHES
                        Maximum number of seconds to await full batch
  --max-operations-per-batch MAX_OPERATIONS_PER_BATCH
                        Maximum number of operations to include in a single batch
  --dry-run             Read source changes only, do not apply to target
  --start-position START_POSITION
                        Starting position - 0 to get change stream resume token, or change stream resume token
  --verbose             Enable verbose logging
  --get-resume-token    Display the current change stream resume token
```
## Example usage:

* To get the cluster wide change stream token 
```
python3 mvu-cdc-migrator.py --source-uri <source-cluster-uri> -- start-position 0 --verbose --get-resume-token
```
* To Migrate the CDC changes during MVU
```
python3 migrate-cdc-cluster.py --source-uri <source-cluster-uri> -- target-uri <target-cluster-uri> --start-position <change stream token> --verbose
```
