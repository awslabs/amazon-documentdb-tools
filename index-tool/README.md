# Amazon DocumentDB Index Tool

The Index Tool facilitates the migration of indexes metadata (excluding data) between document databases deployments.

Supported source: 
 - Amazon DocumentDB (any version)
 - MongoDB (2.x and later versions) standalone, replicaset or sharded cluster
 - Azure Cosmos DB

Supported target: 
 - Amazon DocumentDB (any version)


## Features

- Export indexes metadata from a running MongoDB or Amazon DocumentDB deployment
- Checks for any unsupported indexes types or collections options with Amazon DocumentDB
- Check index and collections options compatibility against a logical backup, taken with mongodump. The backup has to be uncompressed.
- Restores supported indexes to Amazon DocumentDB (instance based or Elastic cluster)
- Output is a json file, similar to mongodump format
- Supports creation of 2dsphere indexes using the *--support-2dsphere* command line option

## Requirements
Python 3.7 or greater, Pymongo.

## Installation
Clone the repository and install the requirements:

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/index-tool
python3 -m pip install -r requirements.txt
```

## Usage/Examples
The Index Tool accepts the following arguments:

```
--debug                      Output debugging information
--dry-run                    Perform processing, but do not actually export or restore indexes
--uri URI                    URI to connect to MongoDB or Amazon DocumentDB
--dir DIR                    Specify the folder to export to or restore from (required)
--show-compatible            Output all compatible indexes with Amazon DocumentDB (no change is applied)
--show-issues                Output a report of compatibility issues found
--dump-indexes               Perform index export from the specified server
--restore-indexes            Restore indexes found in metadata to the specified server
--skip-incompatible          Skip incompatible indexes when restoring metadata
--support-2dsphere           Support 2dsphere indexes creation (collections must use GeoJSON Point type for indexing)
--skip-python-version-check  Permit execution using Python 3.6 and prior
```

### Export indexes from a MongoDB instance:
```
python3 migrationtools/documentdb_index_tool.py --dump-indexes --dir mongodb_index_export --uri 'mongodb://localhost:27017' 
```

### Export indexes from an Amazon DocumentDB cluster
```
python3 migrationtools/documentdb_index_tool.py --dump-indexes --dir docdb_index_export --uri 'mongodb://user:password@mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false' 
```

### Check compatibility with Amazon DocumentDB against exported index metadata
```
python3 migrationtools/documentdb_index_tool.py --show-issues --dir mongodb_index_export
```

### Restore compatible indexes to Amazon DocumentDB
```
python3 migrationtools/documentdb_index_tool.py --restore-indexes --skip-incompatible --dir mongodb_index_export --uri 'mongodb://user:password@mydocdb.cluster-cdtjj00yfi95.eu-west-2.docdb.amazonaws.com:27017/?tls=true&tlsCAFile=rds-combined-ca-bundle.pem&replicaSet=rs0&retryWrites=false' 
```

## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Contributing
Contributions are always welcome! See the [contributing](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) page for ways to get involved.
