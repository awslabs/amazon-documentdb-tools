# Amazon DocumentDB Compatibility Tool

The compatibility tool finds MongoDB query operators and aggregation stages used by your application that are not supported by Amazon DocumentDB. It reports which unsupported operators were found, how often they were used, and (for files) where they appear, along with the supported operators it found.

The tool checks **operators and aggregation stages only** (for example `$facet`, `$where`, `$setWindowFields`). It does not check commands, index types, data types, or other [functional differences](https://docs.aws.amazon.com/documentdb/latest/developerguide/functional-differences.html) between MongoDB and Amazon DocumentDB.

## Choose a scan mode

The tool has three modes. Each run uses exactly one of `--uri`, `--file`, or `--directory`.

| Mode | Option | What it examines | Best for | Requirements |
|---|---|---|---|---|
| Server counters | `--uri` | Operator usage counters from `db.serverStatus()` on a running server | The most accurate picture of what your workload actually executes | MongoDB 5.0 or later, `pymongo`, `serverStatus` permission |
| Log files | `--file` or `--directory` | MongoDB log files with query logging enabled | MongoDB versions older than 5.0, or when you can't connect to the server directly | Query logging must be enabled beforehand (see [Scan MongoDB log files](#scan-mongodb-log-files)) |
| Source code | `--file` or `--directory` | Your application's source files | Early assessment before you have a running workload to measure | None |

If your source deployment runs MongoDB 5.0 or later, start with `--uri`. Scanning source code is a useful complement, but it can miss operators generated at runtime (see [Notes and limitations](#notes-and-limitations)).

## Requirements

- Python 3.9 or later
- [`pymongo`](https://pypi.org/project/pymongo/), only for `--uri` mode:
  ```
  python3 -m pip install pymongo
  ```

The examples below use `python3`. On Windows, use `python` instead.

## Installation

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/compat-tool/
```

## Quick start

Scan a running MongoDB 5.0+ server:
```
python3 compat.py --uri "mongodb://<username>:<password>@<hostname>:27017/admin"
```

Scan a folder of log files or source code:
```
python3 compat.py --directory /path/to/files
```

By default, compatibility is checked against the latest Amazon DocumentDB version supported by the tool (`8.0.2`). Use `--version` to check against a different one (see [Supported versions](#supported-amazon-documentdb-versions)).

## Scan a running MongoDB server (`--uri`)

In MongoDB 5.0 and later, `db.serverStatus()` keeps counters of every query operator and aggregation stage the server has executed. The tool reads those counters and compares them with the selected Amazon DocumentDB version.

### Permissions

The user in the connection string needs permission to run `serverStatus`. The built-in `clusterMonitor` role includes it.

### How to run it

The counters are kept separately on each server and reset when the server restarts, so:

1. **Run the tool after the server has handled a representative workload.** The tool prints the server's uptime. If the server was restarted recently, operators your application uses infrequently (for example in month-end jobs) may not appear yet.
2. **Run the tool once against the primary.**
   ```
   python3 compat.py --uri "mongodb://<username>:<password>@<primary-hostname>:27017/admin"
   ```
3. **If your application sends reads to secondaries, run it again against one secondary.** Those reads are counted only on the secondary that served them.
   ```
   python3 compat.py --uri "mongodb://<username>:<password>@<secondary-hostname>:27017/admin"
   ```

The tool always connects directly to the **first host** in the URI. It ignores `replicaSet` and `readPreference` options and forces `directConnection=true`, so name the specific server you want to measure. The database in the URI path (`/admin` above) is used as the authentication database. Special characters in the username or password must be [percent-encoded](https://www.mongodb.com/docs/manual/reference/connection-string/).

### Example

Illustrative output:
```
python3 compat.py --uri "mongodb://<username>:<password>@<hostname>:27017/admin"

Gathering usage data for analysis
 + connecting to the server at <hostname>:27017
 + database server has been up for 41.27 days
 + database server major version is 7
 + checking compatibility using db.serverStatus()

The following 2 unsupported operators were found:
  $densify | executed 4 time(s)
  $unionWith | executed 12 time(s)

The following 5 supported operators were found:
  $eq | executed 1290 time(s)
  $group | executed 37 time(s)
  $gt | executed 655 time(s)
  $match | executed 402 time(s)
  $unwind | executed 18 time(s)
```

If an operator appears with `WARNING - operator is missing from compat tool`, the server reported an operator that the tool doesn't recognize yet. It is counted as unsupported. Please [open an issue](https://github.com/awslabs/amazon-documentdb-tools/issues) so it can be added.

## Scan MongoDB log files

By default, MongoDB logs only slow operations (over 100 ms), so most queries never appear in the log. Enable logging of all operations, let your workload run, collect the logs, and then scan them.

> **Warning:** Logging every operation adds overhead and can grow log files quickly. Use a dev/test environment that runs a representative workload whenever possible. If you must use production, keep the logging window short.

### 1. Enable logging of all operations

#### Self-managed MongoDB

Connect with `mongosh` to each server whose logs you will collect:

1. Check the current settings and note the `slowms` value:
   ```
   > db.getProfilingStatus()
   { "was": 0, "slowms": 100, "sampleRate": 1 }
   ```
2. Log all operations by setting `slowms` to `-1`:
   ```
   > db.setProfilingLevel(0, { slowms: -1 })
   ```

#### MongoDB Atlas

Atlas manages the slow-operation threshold automatically by default. Turn that off before changing it:

1. In the project settings, turn off **Managed Slow Operations**. See the [Atlas documentation](https://www.mongodb.com/docs/atlas/performance-advisor/).
2. Connect with `mongosh` or Compass and run:
   ```
   db.setProfilingLevel(0, { slowms: -1 })
   ```

If Managed Slow Operations is already off, follow the self-managed steps instead.

See the MongoDB [setProfilingLevel documentation](https://www.mongodb.com/docs/manual/reference/method/db.setProfilingLevel/) for details.

### 2. Collect the logs

Let your workload run long enough to exercise all your application's queries, then copy the log files. For Atlas, [download the logs](https://www.mongodb.com/docs/atlas/mongodb-logs/) from the Atlas console. **Decompress the logs** (Atlas downloads are `.gz`) because the tool reads only plain-text files.

### 3. Restore the original logging settings

When you're done collecting logs:
- **Self-managed:** restore the `slowms` value you noted in step 1, for example `db.setProfilingLevel(0, { slowms: 100 })`.
- **Atlas:** turn **Managed Slow Operations** back on.

### 4. Scan the logs

```
python3 compat.py --directory /path/to/logs
```

## Scan application source code

Point the tool at your source tree. Use `--included-extensions` to scan only the file types you care about, and `--excluded-directories` to skip dependency or build folders:

```
python3 compat.py --directory app --included-extensions py,js,ts --excluded-directories app/node_modules,app/dist
```

The tool searches each line for operator names such as `$lookup` or `$facet`. Kotlin/Groovy template escapes (`${'$'}lookup`) are recognized.

## Examples

These examples use the sample files in the `test/` folder.

### Scan a folder for Amazon DocumentDB 5.0 compatibility, skipping `.txt` files

```
python3 compat.py --version 5.0.1 --directory test --excluded-extensions txt

processing file test/mongod.log.2020-11-10T19-33-14
processing file test/mongodb.log
processing file test/sample-5-0-features.py
processing file test/sample-python-1.py
processing file test/sample-python-2.py

Processed 5 files, skipped 3 files

The following 4 unsupported operators were found:
  $facet | found 2 time(s)
  $sortByCount | found 2 time(s)
  $bucket | found 1 time(s)
  $bucketAuto | found 1 time(s)

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

The following 35 supported operators were found:
  $match | found 34 time(s)
  $gt | found 15 time(s)
  $project | found 15 time(s)
  ... (32 more lines)

List of skipped files - excluded extensions
  test/not_a_log_file.txt
  test/testlog.txt
  test/testlog2.txt
```

### Scan a single file for Amazon DocumentDB 8.0 compatibility

```
python3 compat.py --version 8.0 --file test/testlog.txt

processing file test/testlog.txt

Processed 1 files, skipped 0 files

The following 2 unsupported operators were found:
  $facet | found 2 time(s)
  $bucketAuto | found 1 time(s)

Unsupported operators by filename and line number:
  $facet | lines = found 2 time(s)
    test/testlog.txt | lines = [6, 7]
  $bucketAuto | lines = found 1 time(s)
    test/testlog.txt | lines = [6]

The following 10 supported operators were found:
  $gt | found 2 time(s)
  $and | found 1 time(s)
  $bucket | found 1 time(s)
  $group | found 1 time(s)
  $gte | found 1 time(s)
  $in | found 1 time(s)
  $lte | found 1 time(s)
  $match | found 1 time(s)
  $or | found 1 time(s)
  $sum | found 1 time(s)
```

## Command-line reference

| Option | Description |
|---|---|
| `--uri URI` | Connect to a MongoDB 5.0+ server and check the operators counted by `db.serverStatus()`. |
| `--file FILE` | Scan a single log or source file. |
| `--directory DIR` | Scan every file in a directory, including subdirectories and hidden files. Version control directories (`.git`, `.hg`, `.svn`) are always skipped. |
| `--version VERSION` | Amazon DocumentDB version to check against. Default: `8.0.2`. |
| `--included-extensions LIST` | Comma-separated extensions to scan, without the dot (for example `py,js`). Default: all files. Only used with `--directory`. |
| `--excluded-extensions LIST` | Comma-separated extensions to skip, without the dot (for example `txt,md`). Only used with `--directory`. |
| `--excluded-directories LIST` | Comma-separated directories to skip, including their subdirectories. Paths are relative to the current directory (not to `--directory`) or absolute, for example `--directory app --excluded-directories app/node_modules`. A warning is printed for any path that doesn't exist. Only used with `--directory`. |

`--uri`, `--file`, and `--directory` can't be combined. Exactly one is required.

### Supported Amazon DocumentDB versions

Valid values for `--version` are:

| Value | Amazon DocumentDB version |
|---|---|
| `3.6` | 3.6 |
| `4.0` | 4.0 |
| `5.0.0`, `5.0.1` | 5.0 |
| `8.0`, `8.0.1`, `8.0.2` | 8.0 |
| `EC5.0` | Elastic clusters (5.0) |

Values such as `5.0` or `8` are rejected. When a version has more than one value, use the latest one unless you need to match a specific older release.

## Understanding the results

- **Unsupported operators** must be removed or rewritten before your application can run on the selected Amazon DocumentDB version.
- **Supported operators** confirm that the scan found real queries. If none are found, the tool prints a warning: check that query logging was enabled, that the path is correct, or (for `--uri`) that you connected to the right server.
- **Line numbers** (file scans only) show where each unsupported operator appears, so you can locate the queries to change.
- **Skipped files** are files excluded by extension, or files that couldn't be read as text (for example compressed or binary files).

### Exit codes

For `--file` and `--directory` scans, the tool exits with `1` if any unsupported operators were found and `0` otherwise, so you can use it as a check in CI. `--uri` mode always exits with `0` after a successful scan.

## Next steps

If unsupported operators are found:

1. Check whether a newer Amazon DocumentDB version supports them by re-running with a different `--version`.
2. Review the [supported MongoDB APIs, operations, and data types](https://docs.aws.amazon.com/documentdb/latest/developerguide/mongo-apis.html) to find alternatives.
3. Rewrite the affected queries, then scan again.

## Notes and limitations

- **Source code and log scans are text matches.** The tool looks for operator names in the text, so:
  - **False positives** are possible when the same text appears in comments, strings, or shell/PHP/Perl variables (for example `$set` or `$in`).
  - **False negatives** are possible when operators are built at runtime or generated by an ODM/query builder (for example Mongoose `.where('age').gt(21)` or Spring Data criteria), which never write the `$` operator in your code. Use `--uri` or log scanning to catch these.
- **Server counters reflect only what has run** on that server since its last restart.
- **Files are read as UTF-8.** Bytes that aren't valid UTF-8 are replaced rather than causing the file to be skipped, so files in other encodings (for example Latin-1) are still scanned. Files containing NUL bytes are treated as binary and skipped.
- **Files are read fully into memory.** For very large log files, make sure enough RAM is available or split the files first.
- **File contents are not logged or sent anywhere.** Files are opened read-only, and only the operator names, counts, file names, and line numbers are reported.

## Contributing

Contributions are always welcome! See the [contributing page](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) for ways to get involved.

## License

Apache 2.0
