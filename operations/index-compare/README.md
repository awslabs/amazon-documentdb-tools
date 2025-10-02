# Amazon DocumentDB Index Compare

Index Compare detects extra, missing, and differing indexes between two DocumentDB or MongoDB clusters.

## Requirements
Python 3.7 or greater, Pymongo.

## Usage/Examples
Index Compare accepts the following arguments:

```
--source-uri URI                      URI to connect to source Amazon DocumentDB or MongoDB cluster (required)
--target-uri URI                      URI to connect to target Amazon DocumentDB or MongoDB cluster (required)
--verbose                             Verbose output
```

### Compare indexes between two clusters
```
python3 index-compare.py --source-uri $SOURCE_CLUSTER_URI --target-uri $TARGET_CLUSTER_URI
```

## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

