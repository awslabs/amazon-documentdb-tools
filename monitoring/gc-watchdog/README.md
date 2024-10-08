# Amazon DocumentDB Garbage Collection Watchdog
This tool monitors a DocumentDB cluster for garbage collection activity. It displays the start and end of each garbage collection to a log file and can optionally create 3 CloudWatch metrics for monitoring and alerting purposes.

## Installation
Clone the repository

## Requirements
* Python 3.7+
* PyMongo, boto3

## Using the garbage collection watchdog
```
python3 gc-watchdog.py --uri <uri> --log-file-name <log-file-name> [--create-cloudwatch-metrics] [--cluster-name <cluster-name>]
```

* <uri> follows the [MongoDB Connection String URI Format](https://www.mongodb.com/docs/manual/reference/connection-string/)
* <log-file-name> is the name of the log file created by the tool 
* include --create-cloudwatch-metrics to create metrics for the number of ongoing garbage collections, maximum time of an ongoing garbage collection in seconds, and total time of all ongoing garbage collections in seconds
  * CloudWatch metrics are captured in namespace "CustomDocDB" as "GCCount", "GCTotalSeconds", and "GCMaxSeconds"
* include --cluster-name <cluster-name> if capturing CloudWatch metrics via --create-cloudwatch-metrics
* NOTE - The default frequency to check for garbage collection activity is every 5 seconds. Garbage collections requiring less than 5 seconds might not be recorded by this tool. 
