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
--source-namespace       Default all collections. Pass database name for database, or database.collection names for single collection
--duration-seconds       Number of seconds to run the tool before stopping 
--feedback-seconds       Number of seconds between throughput outputs, "interval" (default 5)
--start-position         "NOW" (default) or date/time to start from in YYYY-MM-DD+HH24:MI:SS UTC
--update-lookup          Perform updateLookup for update events found in the changestream
```

# Output
```
[2026-04-30T18:07:36.167420+00Z] elapsed 00:00:10.00 | total o/s    24,964 | interval o/s    26,840 | tot   249,673 |     311 secs behind |    27.32  max fetch ms |     0.04 avg fetch ms
[2026-04-30T18:07:41.167953+00Z] elapsed 00:00:15.00 | total o/s    25,469 | interval o/s    26,479 | tot   382,084 |     238 secs behind |    74.63  max fetch ms |     0.04 avg fetch ms
[2026-04-30T18:07:46.168467+00Z] elapsed 00:00:20.00 | total o/s    25,504 | interval o/s    25,610 | tot   510,152 |     173 secs behind |    32.97  max fetch ms |     0.04 avg fetch ms
[2026-04-30T18:07:51.169005+00Z] elapsed 00:00:25.00 | total o/s    25,772 | interval o/s    26,843 | tot   644,381 |     100 secs behind |    29.96  max fetch ms |     0.04 avg fetch ms
[2026-04-30T18:07:56.169578+00Z] elapsed 00:00:30.00 | total o/s    25,939 | interval o/s    26,775 | tot   778,272 |      27 secs behind |   107.02  max fetch ms |     0.04 avg fetch ms
[2026-04-30T18:08:01.170175+00Z] elapsed 00:00:35.00 | total o/s    23,359 | interval o/s     7,881 | tot   817,683 |       1 secs behind |   143.69  max fetch ms |     0.41 avg fetch ms
[2026-04-30T18:08:06.170707+00Z] elapsed 00:00:40.00 | total o/s    20,653 | interval o/s     1,707 | tot   826,222 |       1 secs behind |   147.47  max fetch ms |     0.61 avg fetch ms

total o/s                Overall changestream operations per second
interval o/s             Interval changestream operations per second
tot                      Total changestream operations
secs behind              Number of seconds behind current date/time, "Is the changestream catching up and keeping up?"
max fetch ms             Interval maximum number of milliseconds to fetch the next changestream event
avg fetch ms             Interval average number of milliseconds to fetch the next changestream event
```

## License
This tool is licensed under the Apache 2.0 License. 
