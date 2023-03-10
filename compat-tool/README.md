# Amazon DocumentDB Compatibility Tool
This compatibility tool examines log files from MongoDB or source code from MongoDB applications to determine if there are any queries which use operators that are not supported in Amazon DocumentDB. This tool produces a simple report of unsupported operators and file names with line numbers for further investigation.

## Installation
Clone the repository.

## Using the tool
This tool supports examining compatibility with either the 3.6
or 4.0 versions of Amazon DocumentDB. The format of the command is:
```
python3 compat.py --file full-path-to-file-to-scan

or

python3 compat.py --directory full-path-to-directory-to-scan
```

* By default the tool will test for Amazon DocumentDB 4.0 compatibility, 
include --version 3.6 to test for that specific version.
* "full-path-to-file-to-scan" is a single MongoDB log file or source 
code file to be scanned for compatibility.
* "full-path-to-directory-to-scan" is a directory containing MongoDB log 
files or source code files, all included files will be scanned and 
subdirectories will be scanned resursively.
* NOTE - all files scanned by this utility are opened read-only and scanned in
memory. With the exception of operators used there is no logging of the file
contents.


### Enabling query logging
To enable logging of queries to the MongoDB logs you enable the query profiler
and set the `slowms` to `-1`, which will cause all queries to be logged.
To do so, run the following query from the `mongo` shell.
```
db.setProfilingLevel(2, -1)
```

It is recommended to use a dev/test MongoDB installation to capture the queries, as
the logging of all queries can impact production workloads.
