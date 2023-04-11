# Amazon DocumentDB Compatibility Tool
The tool examines MongoDB log files or source code from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB. This tool produces a simple report of unsupported operators and file names with line numbers for further investigation.

## Requirements
Python 3.6 or later

## Installation
Clone the repository and go to the tool folder:
```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/compat-tool/
```

## Usage/Examples
This tool supports examining compatibility with either the 3.6, 4.0 or 5.0 versions of Amazon DocumentDB. The script has the following arguments:
```
--version {3.6,4.0,5.0} -> Check for DocumentDB version compatibility (default is 5.0)
--directory SCANDIR     -> Directory containing files to scan for compatibility
--file SCANFILE         -> Specific file to scan for compatibility
--excluded-extensions EXCLUDEDEXTENSIONS -> Filename extensions to exclude from scanning, comma separated
--included-extensions INCLUDEDEXTENSIONS -> Filename extensions to include in scanning, comma separated
--show-supported        -> Include supported operators in the report
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
python3 compat.py --file test/testlog.txt --show-supported

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

#### NOTES:
* All files scanned by this utility are opened read-only and scanned in memory. For large files, make sure you have enough available RAM or split the files accordingly.
* With the exception of operators used, there is no logging of the file contents.
* Using the `--directory` argument will scan all the files, including subdirectories which will be scanned resursively.

### Enable query logging in MongoDB
#### For local or on-premise installations:
By default, MongoDB logs the slow queries, over the 100ms threshold, to the configured log file.
To view the current profiling status, use the `getProfilingStatus()` in MongoDB shell:

```
> db.getProfilingStatus()
{
  "was": 0,
  "slowms": 100,
  "sampleRate": 1
}
```

To enable logging of all queries, set the `slowms` parameter to `-1`:

```
> db.setProfilingLevel(0, -1)
```

To set the slow logging threshold to the prvious level:
```
> db.setProfilingLevel(0, 100)
```

#### For MongoDB Atlas:
Check the MongoDB Atlas documentation for how to [enable profiling](https://www.mongodb.com/docs/atlas/tutorial/profile-database/#access-the-query-profiler) and [download the logs](https://www.mongodb.com/docs/atlas/mongodb-logs/).

#### NOTE:
Query profiling can cause additional overhead, it is recommended to use a dev/test environment to capture the queries.
See the MongoDB [documentation](https://www.mongodb.com/docs/manual/reference/method/db.setProfilingLevel/) for additional information.
