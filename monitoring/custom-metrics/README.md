# Custom Metrics Tool
There are Amazon DocumentDB cluster limits that are not currently exposed as Amazon CloudWatch metrics. The **custom-metrics** tool connects to an Amazon DocumentDB cluster, collects the specified metrics, and publishes them as custom CloudWatch metrics. The following metrics can be collected by the **custom-metrics** tool:

1. collection count (per cluster)
2. collection size (per collection)
3. database count (per cluster)
4. index count (per collection)
5. index size (per index)
6. user count (per cluster)

CloudWatch metrics will be published to the following dimensions in the **CustomDocDB** namespace:

1. **Cluster, Collection, Database, Index** - index size
2. **Cluster, Collection, Database** - collection size and index count
3. **Database** - collection count, database count, and user count



------------------------------------------------------------------------------------------------------------------------
## Requirements 

Python 3.x with modules: 

* boto3 - AWS SDK that allows management of AWS resources through Python
* pymongo - MongoDB driver for Python applications

```
pip install boto3
pip install pymongo
```

Download the Amazon DocumentDB Certificate Authority (CA) certificate required to authenticate to your cluster:
```
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

------------------------------------------------------------------------------------------------------------------------
## Usage

The tool accepts the following arguments:

```
python3 custom-metrics.py --help
usage: custom-metrics.py [-h] [--skip-python-version-check] --cluster_name
                         CLUSTER_NAME --uri URI --namespaces NAMESPACES
                         [--collection_count] [--database_count]
                         [--user_count] [--collection_size] [--index_count]
                         [--index_size]

optional arguments:
  -h, --help            show this help message and exit
  --skip-python-version-check
                        Permit execution on Python 3.6 and prior
  --cluster_name CLUSTER_NAME
                        Name of cluster for Amazon CloudWatch custom metric
  --uri URI             Amazon DocumentDB Connection URI
  --namespaces NAMESPACES
                        comma separated list of namespaces to monitor
  --collection_count    log cluster collection count
  --database_count      log cluster database count
  --user_count          log cluster user count
  --collection_size     log collection size
  --index_count         log collection index count
  --index_size          log collection index size
```

Examples of ```namespaces``` parameter:

1. Specific namespace: ```"<database>.<collection>"```
2. All collections in specific database: ```"<database>.*"```
3. Specific collection in any database: ```"*.<collection>"```
4. All namespaces: ```"*.*"```
5. Multiple namespaces: ```"<database>.*, *.<collection>, <database>.<collection>"```




