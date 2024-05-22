"""Python script to publish custom Amazon DocumentDB CloudWatch metrics."""
import re
import logging
import argparse
import boto3
import pymongo

boto3.set_stream_logger(name='botocore.credentials', level=logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudWatchClient = boto3.client('cloudwatch')
namespaceRegex = re.compile(r".+\..+")
DATABASE_CLIENT = None

def connect_to_docdb(app_config):
    """Connect to Amazon DocumentDB cluster in specified secret."""
    global DATABASE_CLIENT
    if DATABASE_CLIENT is None:
        try:
            DATABASE_CLIENT = pymongo.MongoClient(host=app_config['uri'], appname='customMetrics')
            print('Successfully created new DocumentDB client.')
        except pymongo.errors.ConnectionFailure as connection_failure:
            print('An error occurred while connecting to DocumentDB: %s', connection_failure)

def log_collection_size_metric(cluster_name, database_name, collection_name, collection_size):
    """Create custom metric for collection size."""
    print("cluster_name = %s, database_name = %s collection_name = %s collection_size = %s", cluster_name, database_name, collection_name, collection_size)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'CollectionSize',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    },
                    {
                        'Name': 'Database',
                        'Value': database_name
                    },
                    {
                        'Name': 'Collection',
                        'Value': collection_name
                    }
                ],
                'Value': collection_size,
                'Unit': 'Bytes',
                'StorageResolution': 60
            }
        ]
    )

def log_index_count_metric(cluster_name, database_name, collection_name, index_count):
    """Create custom metric for number of indexes in collection."""
    print("cluster_name = %s, database_name = %s, collection_name = %s, index_count = %s", cluster_name, database_name, collection_name, index_count)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'IndexCount',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    },
                    {
                        'Name': 'Database',
                        'Value': database_name
                    },
                    {
                        'Name': 'Collection',
                        'Value': collection_name
                    }
                ],
                'Value': index_count,
                'StorageResolution': 60
            }
        ]
    )

def log_index_size_metric(cluster_name, database_name, collection_name, index_name, index_size):
    """Create custom metric for index size."""
    print("cluster_name = %s, database_name = %s, collection_name = %s, index_name = %s, index_size = %s", cluster_name, database_name, collection_name, index_name, index_size)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'IndexSize',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    },
                    {
                        'Name': 'Database',
                        'Value': database_name
                    },
                    {
                        'Name': 'Collection',
                        'Value': collection_name
                    },
                    {
                        'Name': 'Index',
                        'Value': index_name
                    }
                ],
            'Value': index_size,
            'Unit': 'Bytes',
            'StorageResolution': 60
        }
    ]
)

def log_number_of_databases_metric(cluster_name, number_of_databases):
    """Create custom metric for number of databases in cluster."""
    print("cluster_name = %s, number_of_databases = %s", cluster_name, number_of_databases)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'DatabaseCount',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    }
                ],
                'Value': number_of_databases,
                'StorageResolution': 60
            }
        ]
    )

def log_number_of_collections_metric(cluster_name, collection_count):
    """Create custom metric for number of collections in cluster."""
    print("cluster_name = %s, collection_count = %s", cluster_name, collection_count)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'CollectionCount',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    }
                ],
                'Value': collection_count,
                'StorageResolution': 60
            }
        ]
    )

def log_number_of_users_metric(cluster_name, number_of_users):
    """Create custom metric for number of users in cluster."""
    print("cluster_name = %s, number_of_users = %s", cluster_name, number_of_users)
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'UserCount',
                'Dimensions': [
                    {
                        'Name': 'Cluster',
                        'Value': cluster_name
                    }
                ],
                'Value': number_of_users,
                'StorageResolution': 60
            }
        ]
    )

def monitor_namespace(database, collection, namespaces):
    """Add this namespace to the list of namespaces to monitor."""
    namespace = f"{database}.{collection}"
    if (namespace in namespaces) is False:
        namespaces.append(namespace)

    return namespaces

def log_custom_metrics(parameters):
    """Determine which custom metrics to log and then log them."""
    connect_to_docdb(parameters)
    database_names = DATABASE_CLIENT.list_database_names()

    if parameters['log_cluster_database_count'] is True:
        log_number_of_databases_metric(parameters["cluster_name"], len(database_names) if len(database_names) > 0 else 0)

    if parameters["log_cluster_user_count"] is True:
        number_of_users = 0
        if len(database_names) > 0:
            database = DATABASE_CLIENT[database_names[0]]
            number_of_users = len(database.command("usersInfo")["users"])

        log_number_of_users_metric(parameters["cluster_name"], number_of_users)

    if (parameters["log_cluster_collection_count"] is True or
        parameters["log_collection_size"] is True or
        parameters["log_collection_index_count"] is True or
        parameters["log_collection_index_size"] is True):
        collections_by_database = {}
        for database_name in database_names:
            collections_by_database[database_name] = DATABASE_CLIENT[database_name].list_collection_names()

        if parameters["log_cluster_collection_count"] is True:
            collection_count = 0
            for database_name in database_names:
                collection_count += len(collections_by_database[database_name])

            log_number_of_collections_metric(parameters["cluster_name"], collection_count)

        if (parameters["log_collection_size"] is True or
            parameters["log_collection_index_count"] is True or
            parameters["log_collection_index_size"] is True):
            # build list of namespaces to monitor
            namespaces_to_monitor = []
            for namespace in parameters["namespaces"]:
                namespace = namespace.strip()
                if namespaceRegex.match(namespace) is None:
                    logger.error("Skipping invalid namespace %s", namespace)
                else:
                    # split namespace into database and collection
                    tokens = namespace.split(".")
                    database = tokens[0]
                    collection = tokens[1]

                    if database == "*":
                        # all databases
                        for database_to_monitor in database_names:
                            if collection == "*":
                                # all collections in all databases
                                # add all namespaces returned by list_database_names() and list_collection_names()
                                for collection_to_monitor in collections_by_database[database_to_monitor]:
                                    namespaces_to_monitor = monitor_namespace(database_to_monitor, collection_to_monitor, namespaces_to_monitor)
                            else:
                                # specific collection in all databases
                                # add namespace if collection exists in the database
                                if collection in collections_by_database[database_to_monitor]:
                                    namespaces_to_monitor = monitor_namespace(database_to_monitor, collection, namespaces_to_monitor)
                    else:
                        database_to_monitor = database
                        if database_to_monitor in collections_by_database:
                            if collection == "*":
                                # all collections in a specific database
                                for collection_to_monitor in collections_by_database[database_to_monitor]:
                                    namespaces_to_monitor = monitor_namespace(database_to_monitor, collection_to_monitor, namespaces_to_monitor)
                            else:
                                # specific collection in a specific database
                                if collection in collections_by_database[database_to_monitor]:
                                    namespaces_to_monitor = monitor_namespace(database_to_monitor, collection, namespaces_to_monitor)

            for namespace in namespaces_to_monitor:
                tokens = namespace.split(".")
                database_name = tokens[0]
                collection_name = tokens[1]
                database = DATABASE_CLIENT[database_name]
                collection_statistics = database.command("collStats", collection_name)
                if parameters["log_collection_size"] is True:
                    log_collection_size_metric(parameters["cluster_name"], database_name, collection_name, collection_statistics["storageSize"])

                if parameters["log_collection_index_count"] is True:
                    log_index_count_metric(parameters["cluster_name"], database_name, collection_name, collection_statistics["nindexes"])

                if parameters["log_collection_index_size"] is True:
                    for index_name in collection_statistics["indexSizes"]:
                        log_index_size_metric(parameters["cluster_name"], database_name, collection_name, index_name, collection_statistics["indexSizes"][index_name])

def main():
    """custom_metrics script entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--cluster_name',
                        required=True,
                        type=str,
                        help='Name of cluster for Amazon CloudWatch custom metric')

    parser.add_argument('--uri',
                        required=True,
                        type=str,
                        help='Amazon DocumentDB Connection URI')

    parser.add_argument('--namespaces',
                        required=True,
                        type=str,
                        help="comma separated list of namespaces to monitor")

    parser.add_argument('--collection_count',
                        action='store_true',
                        help="log cluster collection count")

    parser.add_argument('--database_count',
                        action='store_true',
                        help="log cluster database count")

    parser.add_argument('--user_count',
                        action='store_true',
                        help="log cluster user count")

    parser.add_argument('--collection_size',
                        action='store_true',
                        help="log collection size")

    parser.add_argument('--index_count',
                        action='store_true',
                        help="log collection index count")

    parser.add_argument('--index_size',
                        action='store_true',
                        help="log collection index size")

    args = parser.parse_args()
    if (args.collection_count is False and
        args.database_count is False and
        args.user_count is False and
        args.collection_size is False and
        args.index_count is False and
        args.index_size is False):
        print('Specify at least 1 metric to monitor.')
        return

    app_config = {}
    app_config['cluster_name'] = args.cluster_name
    app_config['uri'] = args.uri
    app_config['namespaces'] = args.namespaces.split(",")
    app_config['log_cluster_collection_count'] = args.collection_count
    app_config['log_cluster_database_count'] = args.database_count
    app_config['log_cluster_user_count'] = args.user_count
    app_config['log_collection_size'] = args.collection_size
    app_config['log_collection_index_count'] = args.index_count
    app_config['log_collection_index_size'] = args.index_size

    log_custom_metrics(app_config)

if __name__ == "__main__":
    main()
