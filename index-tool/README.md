# Amazon DocumentDB Index Tool 

The Index Tool makes it easier to migrate only indexes (not data) between a source MongoDB deployment and a Amazon DocumentDB  cluster. The Index Tool can also help you find potential compatibility issues between your source databases and Amazon DocumentDB. You can use the Index Tool to dump indexes and database metadata, or you can use the tool against an existing dump created with the mongodump tool.

Features:
 - Dump just the indexes from a running mongodb instance/replica set
 - Outputs in the same dump format that mongodump uses
 - Checks indexes, collections, and databases for compatibility with Amazon DocumentDB
 - Checks indexes for unsupported index types
 - Checks collections for unsupported options
 - Restores supported indexes (without data) to Amazon DocumentDB
 - Supports creation of 2dsphere indexes using --support-2dsphere command line option

## Installing
Clone the repository, then run the following command in the repo top-level director:
`pip3 install -r requirements.txt`

**NOTE** - This tool requires Python 3.7 or greater.

## Using the Index Tool
To dump indexes from a running MongoDB instance or replica set, run the following command:
`python3 migrationtools/documentdb_index_tool.py --dump-indexes --uri <source-server-uri> --dir <directory to dump metadata to>`

To check for compatibility issues against dumped database metadata, run the following command:
`python3 migrationtools/documentdb_index_tool.py --show-issues --uri <source-server-uri> --dir <directory that contains metadata dump>`

To restore only indexes that are compatible with Amazon DocumentDB, run the following command:
`python3 migrationtools/documentdb_index_tool.py --restore-indexes --uri <destination-server-uri> --dir <directory that contains metadata dump>`

## License

This library is licensed under the Apache 2.0 License. 
