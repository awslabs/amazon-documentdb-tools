# Amazon DocumentDB Full Load and Change Data Capture (CDC) Synchronization Tool
This synchronization tool enables high-speed Full Load and CDC from a MongoDB/DocumentDB source database to an Amazon DocumentDB target database.

The full load script requires "boundaries" for parallelism, you can run the [dms-segments tool](https://github.com/awslabs/amazon-documentdb-tools/tree/master/migration/dms-segments) to calculate them.

## Installation
Clone the repository.

## Requirements
* Python 3.7+
* PyMongo, boto3
  * IAM permission "cloudwatch:PutMetricData" is required to create CloudWatch metrics 


## Using the change data capture tool
```
python3 cdc-multiprocess.py --source-uri <source-uri> --target-uri <target-uri> --source-namespace <database.collection> --start-position [NOW, YYYY-MM-DD+HH:MM:SS in UTC, or resume token] --use-change-stream [--create-cloudwatch-metrics] [--cluster-name <cluster-name>]
```

* source-uri and target-uri follow the [MongoDB Connection String URI Format](https://www.mongodb.com/docs/manual/reference/connection-string/)
* source-namespace in database.collection format (i.e. "database1.collection2")
* start-position either NOW, specific datetime position as YYYY-MM-DD+HH:MM:SS in UTC, or resume token
* the resume token shown in the progress output is the position up to which all changes have been applied to the target, pass it as --start-position to restart without losing changes
* must pass --use-change-stream to use change streams for source (MongoDB or DocumentDB)
* optionally pass 2+ for the --threads option to process the changestream with concurrent processes for additional throughput
* several other optional parameters as supported, execute the script with -h for a full listing
* include --create-cloudwatch-metrics to create metrics for the number of CDC operations per second and the number of seconds behind current
  * CloudWatch metrics are captured in namespace "CustomDocDB" as "MigratorCDCOperationsPerSecond" and "MigratorCDCNumSecondsBehind"
* include --cluster-name <cluster-name> if capturing CloudWatch metrics via --create-cloudwatch-metrics

## Using the full load tool
```
python3 fl-multiprocess.py --source-uri <source-uri> --target-uri <target-uri> --source-namespace <database.collection> --boundaries <comma-separated-boundary-list>  [--create-cloudwatch-metrics] [--cluster-name <cluster-name>]
```

* source-uri and target-uri follow the [MongoDB Connection String URI Format](https://www.mongodb.com/docs/manual/reference/connection-string/)
* source-namespace and target-namespace in database.collection format (i.e. "database1.collection2")
* pass --boundary-datatype for string or int for _id boundaries that are not objectid type
* several other optional parameters as supported, execute the script with -h for a full listing
* include --create-cloudwatch-metrics to create metrics for the number of inserts per second and the approximate number of seconds to done
  * CloudWatch metrics are captured in namespace "CustomDocDB" as "MigratorFLInsertsPerSecond" and "MigratorFLRemainingSeconds"
* include --cluster-name <cluster-name> if capturing CloudWatch metrics via --create-cloudwatch-metrics
