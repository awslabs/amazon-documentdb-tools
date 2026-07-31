"""Python script to publish custom Amazon DocumentDB CloudWatch metrics."""
import sys
import re
import logging
import argparse
from urllib.parse import urlsplit, urlunsplit
import boto3
import pymongo

boto3.set_stream_logger(name='botocore.credentials', level=logging.ERROR)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

cloudWatchClient = boto3.client('cloudwatch')
namespaceRegex = re.compile(r".+\..+")
DATABASE_CLIENT = None

def connect_to_docdb(app_config):
    global DATABASE_CLIENT
    if DATABASE_CLIENT is None:
        try:
            DATABASE_CLIENT = pymongo.MongoClient(host=app_config['uri'], appname='customMetrics')
            logger.info('Successfully created new DocumentDB client.')
        except pymongo.errors.ConnectionFailure as connection_failure:
            logger.error('An error occurred while connecting to Amazon DocumentDB: %s', connection_failure)

def log_collection_size_metric(cluster_name, database_name, collection_name, collection_size):
    """Create custom metric for collection size."""
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

def log_collection_scans_metric(cluster_name, database_name, collection_name, instance_id, collection_scans):
    """Create custom metric for collection scans on collection for the instance."""
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'CollectionScans',
                'Dimensions': [
                    {'Name': 'Cluster', 'Value': cluster_name},
                    {'Name': 'Database', 'Value': database_name},
                    {'Name': 'Collection', 'Value': collection_name},
                    {'Name': 'Instance', 'Value': instance_id},
                ],
                'Value': collection_scans,
                'Unit': 'Count',
                'StorageResolution': 60
            },
        ]
    )

def log_index_scans_metric(cluster_name, database_name, collection_name, instance_id, index_scans):
    """Create custom metric for index scans on collection for the instance."""
    cloudWatchClient.put_metric_data(
        Namespace='CustomDocDB',
        MetricData=[
            {
                'MetricName': 'IndexScans',
                'Dimensions': [
                    {'Name': 'Cluster', 'Value': cluster_name},
                    {'Name': 'Database', 'Value': database_name},
                    {'Name': 'Collection', 'Value': collection_name},
                    {'Name': 'Instance', 'Value': instance_id},
                ],
                'Value': index_scans,
                'Unit': 'Count',
                'StorageResolution': 60
            },
        ]
    )

def monitor_namespace(database, collection, namespaces):
    """Add this namespace to the list of namespaces to monitor."""
    namespace = f"{database}.{collection}"
    if (namespace in namespaces) is False:
        namespaces.append(namespace)

    return namespaces

def resolve_namespaces(namespace_patterns, database_names, collections_by_database):
    """Expand namespace patterns (db.coll, db.*, *.coll, *.*) into concrete namespaces."""
    namespaces_to_monitor = []
    for namespace in namespace_patterns:
        namespace = namespace.strip()
        if namespaceRegex.match(namespace) is None:
            logger.error("Skipping invalid namespace %s", namespace)
            continue
        # split namespace into database and collection
        tokens = namespace.split(".")
        database = tokens[0]
        collection = tokens[1]

        if database == "*":
            # all databases
            for database_to_monitor in database_names:
                if collection == "*":
                    # all collections in all databases
                    for collection_to_monitor in collections_by_database[database_to_monitor]:
                        namespaces_to_monitor = monitor_namespace(database_to_monitor, collection_to_monitor, namespaces_to_monitor)
                else:
                    # specific collection in all databases
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
    
    return namespaces_to_monitor

def get_cluster_hosts(client):
    """Discover every instance in the cluster via hello. Returns ['host:port', ...]."""
    hello = client.admin.command("hello")
    return hello["hosts"] if "hosts" in hello else []

def build_instance_uri(cluster_uri, host):
    """Build an instance URI based on the cluster URI"""
    # split cluster URI into its structural components (e.g. scheme, location, query, etc.)
    parts = urlsplit(cluster_uri)

    # preserve credentials (user:pass@) from the base URI, swap in the instance host:port
    credentials = f"{parts.username}:{parts.password}@" if parts.username is not None else ''
    netloc = f"{credentials}{host}"

    # preserve query parameters from the base URI
    # drop replicaSet and readPreference parameters
    # add directConnection parameter
    query_parameters = []
    for query_parameter in parts.query.split('&'):
        if query_parameter == '' or query_parameter.startswith('replicaSet=') or query_parameter.startswith('readPreference='):
            continue

        query_parameters.append(query_parameter)

    if 'directConnection=' not in parts.query:
        query_parameters.append('directConnection=true')

    query = '&'.join(query_parameters)

    # build the new URI from the parts
    return urlunsplit((parts.scheme, netloc, parts.path, query, parts.fragment))

def get_host_metrics(parameters, host):
    """Connect directly to the specified host, run collStats per namespace, and publish metrics."""
    instance_id = host.split(":")[0].split(".")[0]
    instance_uri = build_instance_uri(parameters["uri"], host)
    
    try:
        instance_client = pymongo.MongoClient(host=instance_uri, appname='customMetrics', serverSelectionTimeoutMS=10000)
    except pymongo.errors.ConnectionFailure as connection_failure:
        logger.error('An error occurred while connecting to instance %s: %s', instance_id, connection_failure)
        return

    database_names = instance_client.list_database_names()
    collections_by_database = {}
    for database_name in database_names:
        collections_by_database[database_name] = instance_client[database_name].list_collection_names()

    namespaces_to_monitor = resolve_namespaces(parameters["namespaces"], database_names, collections_by_database)

    for namespace in namespaces_to_monitor:
        tokens = namespace.split(".")
        database_name = tokens[0]
        collection_name = tokens[1]
        collection_statistics = instance_client[database_name].command("collStats", collection_name)

        if parameters["log_collection_scans"] is True and "collScans" in collection_statistics:
            log_collection_scans_metric(parameters["cluster_name"], database_name, collection_name, instance_id, collection_statistics["collScans"])

        if parameters["log_index_scans"] is True and "idxScans" in collection_statistics:
            log_index_scans_metric(parameters["cluster_name"], database_name, collection_name, instance_id, collection_statistics["idxScans"])

    instance_client.close()

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
            namespaces_to_monitor = resolve_namespaces(parameters["namespaces"], database_names, collections_by_database)
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

    if (parameters["log_collection_scans"] is True or
        parameters["log_index_scans"] is True):
        # Discover every cluster host via hello and collect per-instance scan metrics.
        hosts = get_cluster_hosts(DATABASE_CLIENT)
        if not hosts:
            logger.error("hello returned no hosts; cannot collect per-instance collection scans")
            return
        for host in hosts:
            get_host_metrics(parameters, host)

def main():
    """custom_metrics script entry point."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--skip-python-version-check',
                        required=False,
                        action='store_true',
                        help='Permit execution on Python 3.6 and prior')
    
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

    parser.add_argument('--collection_scans',
                        action='store_true',
                        help="log collection scans for each cluster instance")

    parser.add_argument('--index_scans',
                        action='store_true',
                        help="log index scans for each cluster instance")

    args = parser.parse_args()

    MIN_PYTHON = (3, 7)
    if (not args.skip_python_version_check) and (sys.version_info < MIN_PYTHON):
        sys.exit("\nPython %s.%s or later is required.\n" % MIN_PYTHON)

    if (args.collection_count is False and
        args.database_count is False and
        args.user_count is False and
        args.collection_size is False and
        args.index_count is False and
        args.index_size is False and
        args.collection_scans is False and
        args.index_scans is False):
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
    app_config['log_collection_scans'] = args.collection_scans
    app_config['log_index_scans'] = args.index_scans

    log_custom_metrics(app_config)

if __name__ == "__main__":
    main()
