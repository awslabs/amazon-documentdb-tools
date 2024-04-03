# Amazon DocumentDB Index Creator

Index Creator enables the creation of indexes while viewing the status and progress from the command line.

## Features
- Create single key and compound indexes from the command line, including multi-key indexes
- **NOTE** - does not currently support creation of partial, geospatial, text, or vector indexes
- During index creation the status of the index creation process as well as estimated time to complete the current stage is displayed

## Requirements
Python 3.7 or greater, Pymongo.

## Usage/Examples
Index Creator accepts the following arguments:

```
--uri URI                             URI to connect to Amazon DocumentDB (required)
--workers WORKERS                     Number of worker processes for heap scan stage of index creation (required)
--database DATABASE                   Database containing collection for index creation (required)
--collection COLLECTION               Collection to create index (required)
--index-name INDEX_NAME               Name of index to create (required)
--index-keys INDEX_KEYS               Comma separated list of index key(s), append :-1 after key for descending (required)
--unique                              Create unique index
--foreground                          Create index in the foreground (must provide this or --background)
--background                          Create index in the background (must provide this or --foreground)
--update-frequency-seconds SECONDS    Number of seconds between progress updates (default 15)
--log-file-name LOG_FILE_NAME         Name of file for output logging (default index-creator.log)
```

### Create a compound index with 4 workers on testdb.testcoll on fields f1 and f2
```
python3 index-creator.py --uri $DOCDB_URI --workers 4 --database testdb --collection testcoll --index-name test_idx --index-keys f1,f2 --background
```

## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)
