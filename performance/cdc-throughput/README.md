# CDC Throughput Tool

The cdc throughput tool measures the maximum throughput supported by a DocumentDB or MongoDB changestream. It supports testing the following scenarios:
 - with changestream updateLookup enabled or disabled
 - starting the changestream watch immediately or some time in the past
 - sourcing the changestream from the primary instance or a read-replica/secondary
 - limiting the changestream to a single collection, all collections in a single database, or all collections
 
# Requirements
 - Python 3.7+
 - PyMongo

# Using the CDC Throughput Tool
`python3 cdc-throughput.py --source-uri <mongodb-uri>`

In default mode the tool measures changestream throughput for all collections, starting now, with updateLookups disabled.

Connectivity options like directConnection, replicaSet, and readPreference are controlled via the URI provided to `--source-uri`

## Optional Parameters
```
--source-namespace               Default all collections, pass database name for database, or database.collection names for single collection
--duration-seconds               Number of seconds to run the tool before stopping 
--feedback-seconds               Number of seconds between throughput outputs
--start-position                 "NOW" (default) or date/time to start from in YYYY-MM-DD+HH24:MI:SS UTC
--update-lookup                  Perform updateLookup for update events found in the changestream
```

## License
This tool is licensed under the Apache 2.0 License. 
