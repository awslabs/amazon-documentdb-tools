# Amazon DocumentDB Compatibility Tool
The tool examines **MongoDB log files** or **source code** from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB. This tool produces a simple report of unsupported operators and file names with line numbers for further investigation.

## Prerequisites for MongoDB Log File Analysis

### Enable query logging in MongoDB
#### For local, on-premise, or self-managed installations:
By default, MongoDB logs the slow queries, over the 100ms threshold, to the configured log file.

1. Check current profiling status and note the `slowms` value:
```
> db.getProfilingStatus()
{
  "was": 0,
  "slowms": 100,
  "sampleRate": 1
}
```

2. Enable logging of all queries by setting `slowms` to `-1`:
```
> db.setProfilingLevel(0, -1)
```
3. **After completing the compatibility analysis**, reset to the original profiling level (using the `slowms` value from step 1):
```
> db.setProfilingLevel(0, 100)
```

#### For MongoDB Atlas:
MongoDB Atlas dynamically adjusts slow query threshold based on the execution time of operations across the cluster. The Atlas-managed slow operation threshold is enabled by default and must be disabled using the Atlas CLI, Atlas Administration API, or Atlas UI.

1. Disable the Atlas-Managed Slow Operation Threshold by going to Project Settings for the cluster project and toggling Managed Slow Operations to Off. Please refer to the documentation here: https://www.mongodb.com/docs/atlas/performance-advisor/
2. Connect to your cluster using MongoDB shell or compass and enable full logging:
   ```
   db.setProfilingLevel(0, -1)
   ```
3. [Download the logs](https://www.mongodb.com/docs/atlas/mongodb-logs/) from the Atlas console
4. **After completing the compatibility analysis**, enable the Atlas-Managed Slow Operation Threshold

If Atlas-managed slow operation threshold is not enabled, follow the same steps as local or on-premise installations above.

#### NOTE:
Query profiling can cause additional overhead, it is recommended to use a dev/test environment to capture the queries.
See the MongoDB [documentation](https://www.mongodb.com/docs/manual/reference/method/db.setProfilingLevel/) for additional information.

## Requirements
- Python 3.6 or later


## Installation
Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/compat-tool/
```

## Usage/Examples
This tool supports examining compatibility with either the 3.6, 4.0, 5.0, or Elastic Clusters 5.0 versions of Amazon DocumentDB. The script has the following arguments:
```
--version {3.6,4.0,5.0,EC5.0}               -> Check for DocumentDB version compatibility (default is 5.0)
--directory SCANDIR                         -> Directory containing files to scan for compatibility
--file SCANFILE                             -> Specific file to scan for compatibility
--excluded-extensions EXCLUDEDEXTENSIONS    -> Filename extensions to exclude from scanning, comma separated
--excluded-directories EXCLUDEDDIRECTORIES  -> Fully qualified path to directory to exclude, comma separated
--included-extensions INCLUDEDEXTENSIONS    -> Filename extensions to include in scanning, comma separated
```

#### Example 1:
Check for compatibility with Amazon DocumentDB version 5.0, files from the folder called test, excluding the ones with extension `.txt`:
```
python3 compat.py --version 5.0 --directory test --excluded-extensions txt

processing file test/mongod.log.2020-11-10T19-33-14
processing file test/mongodb.log
processing file test/sample-5-0-features.py
processing file test/sample-python-1.py
processing file test/sample-python-2.py

Processed 5 files, skipped 3 files

The following 5 unsupported operators were found
  $facet | found 2 time(s)
  $sortByCount | found 2 time(s)
  $bucket | found 1 time(s)
  $bucketAuto | found 1 time(s)
  $expr | found 1 time(s)

Unsupported operators by filename and line number
  $facet | lines = found 2 time(s)
    test/mongodb.log | lines = [80, 82]
  $sortByCount | lines = found 2 time(s)
    test/mongod.log.2020-11-10T19-33-14 | lines = [83]
    test/sample-python-2.py | lines = [29]
  $bucket | lines = found 1 time(s)
    test/mongodb.log | lines = [80]
  $bucketAuto | lines = found 1 time(s)
    test/mongodb.log | lines = [82]
  $expr | lines = found 1 time(s)
    test/mongod.log.2020-11-10T19-33-14 | lines = [107]

List of skipped files - excluded extensions
  test/not_a_log_file.txt
  test/testlog.txt
  test/testlog2.txt
```

#### Example 2:
Check a specific file and show the supported operators found:

```
python3 compat.py --file test/testlog.txt

processing file test/testlog.txt
Processed 1 files, skipped 0 files

The following 3 unsupported operators were found
  $facet | found 2 time(s)
  $bucket | found 1 time(s)
  $bucketAuto | found 1 time(s)

Unsupported operators by filename and line number
  $facet | lines = found 2 time(s)
    test/testlog.txt | lines = [6, 7]
  $bucket | lines = found 1 time(s)
    test/testlog.txt | lines = [7]
  $bucketAuto | lines = found 1 time(s)
    test/testlog.txt | lines = [6]

The following 9 supported operators were found
  - $gt | found 2 time(s)
  - $and | found 1 time(s)
  - $group | found 1 time(s)
  - $gte | found 1 time(s)
  - $in | found 1 time(s)
  - $lte | found 1 time(s)
  - $match | found 1 time(s)
  - $or | found 1 time(s)
  - $sum | found 1 time(s)
```

### Example 3:
Check for compatibility with Amazon DocumentDB, files from the folder called test, excluding the ones with extension `.txt` and excluding directories `exclude1` and `exclude2`:

```
python3 compat.py --version 5.0 --directory test --excluded-extensions txt --excluded-directories /path/to/directory/exclude1,/path/do/directory/exclude2

processing file test/mongod.log.2020-11-10T19-33-14
processing file test/mongodb.log
processing file test/sample-5-0-features.py
processing file test/sample-python-1.py
processing file test/sample-python-2.py

Processed 5 files, skipped 3 files

The following 5 unsupported operators were found:
  $facet | found 2 time(s)
  $sortByCount | found 2 time(s)
  $bucket | found 1 time(s)
  $bucketAuto | found 1 time(s)
  $expr | found 1 time(s)

Unsupported operators by filename and line number:
  $facet | lines = found 2 time(s)
    test/mongodb.log | lines = [80, 82]
  $sortByCount | lines = found 2 time(s)
    test/mongod.log.2020-11-10T19-33-14 | lines = [83]
    test/sample-python-2.py | lines = [29]
  $bucket | lines = found 1 time(s)
    test/mongodb.log | lines = [80]
  $bucketAuto | lines = found 1 time(s)
    test/mongodb.log | lines = [82]
  $expr | lines = found 1 time(s)
    test/mongod.log.2020-11-10T19-33-14 | lines = [107]

The following 34 supported operators were found:
  - $match | found 34 time(s)
  - $gt | found 15 time(s)
  - $project | found 15 time(s)
  - $lte | found 14 time(s)
  - $group | found 13 time(s)
  - $gte | found 13 time(s)
  - $sum | found 11 time(s)
  - $in | found 10 time(s)
  - $count | found 7 time(s)
  - $ne | found 7 time(s)
  - $lookup | found 6 time(s)
  - $unwind | found 4 time(s)
  - $eq | found 3 time(s)
  - $sort | found 3 time(s)
  - $nin | found 2 time(s)
  - $nor | found 2 time(s)
  - $set | found 2 time(s)
  - $skip | found 2 time(s)
  - $addToSet | found 1 time(s)
  - $and | found 1 time(s)
  - $arrayElemAt | found 1 time(s)
  - $avg | found 1 time(s)
  - $dateAdd | found 1 time(s)
  - $dateSubtract | found 1 time(s)
  - $elemMatch | found 1 time(s)
  - $first | found 1 time(s)
  - $inc | found 1 time(s)
  - $last | found 1 time(s)
  - $limit | found 1 time(s)
  - $lt | found 1 time(s)
  - $max | found 1 time(s)
  - $min | found 1 time(s)
  - $not | found 1 time(s)
  - $or | found 1 time(s)

List of skipped files - excluded extensions
  test/not_a_log_file.txt
  test/testlog.txt
  test/testlog2.txt

List of skipped directories - excluded directories
  test/exclude1
  test/exclude2
  
```
#### NOTES:
* All files scanned by this utility are opened read-only and scanned in memory. For large files, make sure you have enough available RAM or split the files accordingly.
* With the exception of operators used, there is no logging of the file contents.
* Using the `--directory` argument will scan all the files, including subdirectories which will be scanned resursively.

## Contributing
Contributions are always welcome! See the [contributing page](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) for ways to get involved.

## License
Apache 2.0