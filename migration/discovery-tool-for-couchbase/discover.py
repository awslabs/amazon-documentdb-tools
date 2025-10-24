import argparse
import csv
import json
import math
import requests
import statistics
import subprocess


# helper function to send an HTTP request to a REST endpoint
def send_request(node,
                 port,
                 rest_endpoint,
                 username,
                 password,
                 check_status_code=True,
                 params={}):
    try:
        url = f'{node}:{port}{rest_endpoint}'
        response = requests.get(url, params=params, auth=(username, password))
    except Exception as e:
        raise Exception(f'send_request: error {e} when calling {url}')

    if (check_status_code == True and response.status_code != 200):
        raise Exception(
            f'send_request: error {response.status_code} when calling {url}')

    return response


# get host name/IP address of all data nodes in the cluster
# See https://docs.couchbase.com/server/current/rest-api/rest-node-get-info.html for more information
def get_data_nodes(app_config):
    response = send_request(app_config["data_node"],
                            app_config["admin_port"],
                            '/pools/nodes',
                            app_config["username"],
                            app_config["password"])
    return [
        node['hostname'].split(':')[0] for node in response.json()['nodes']
        if 'kv' in node['services']
    ]


# get host name/IP address of all index nodes in the cluster
# See https://docs.couchbase.com/server/current/rest-api/rest-node-get-info.html for more information
def get_index_nodes(app_config):
    response = send_request(app_config["data_node"],
                            app_config["admin_port"],
                            '/pools/nodes',
                            app_config["username"],
                            app_config["password"])
    return [
        node['hostname'].split(':')[0] for node in response.json()['nodes']
        if 'index' in node['services']
    ]


# get the name of all buckets in the cluster
# See https://docs.couchbase.com/server/current/rest-api/rest-buckets-summary.html for more information
def get_buckets(app_config):
    response = send_request(app_config["data_node"],
                            app_config["admin_port"],
                            '/pools/default/buckets/',
                            app_config["username"],
                            app_config["password"])
    return [bucket['name'] for bucket in response.json()]


# write the following details for all collections in the cluster to collection-stats.csv:
#   bucket name
#   bucket type
#   scope name
#   collection name
#   total size
#   total items
#   average document size
# See the following for more information:
#   https://docs.couchbase.com/server/current/rest-api/rest-bucket-intro.html
#   https://docs.couchbase.com/server/current/cli/cbstats/cbstats-collections.html
def get_collection_stats(app_config, buckets, data_nodes):
    print("\ngetting collection stats...")

    with open('collection-stats.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['bucket','bucket_type', 'scope_name', 'collection_name', 'total_size', 'total_items', 'document_size'])
        for bucket in buckets:
            # Get bucket type
            response = send_request(app_config["data_node"],
                                    app_config["admin_port"],
                                    f'/pools/default/buckets/{bucket}/',
                                    app_config["username"],
                                    app_config["password"])
            bucket_type = response.json()['bucketType']

            # Get collections
            response = send_request(app_config["data_node"],
                                    app_config["admin_port"],
                                    f'/pools/default/buckets/{bucket}/scopes/',
                                    app_config["username"], app_config["password"])
            scopes_data = response.json()['scopes']

            for scope in scopes_data:
                scope_name = scope['name']

                for collection in scope['collections']:
                    collection_name = collection['name']
                    print(f"found collection {bucket}.{scope_name}.{collection_name}")

                    collection_uid = collection['uid']
                    total_items = 0
                    total_size = 0

                    # Get stats from each data node
                    for node in data_nodes:
                        cmd = [
                            f'{app_config["tools_path"]}/cbstats',
                            node, '-u',
                            app_config["username"], '-p',
                            app_config["password"], '-b',
                            bucket,
                            'collections',
                            'id',
                            f'0x{collection_uid}'
                        ]

                        try:
                            output = subprocess.check_output(cmd, text=True)

                            # Parse size and items from output
                            for line in output.splitlines():
                                if 'data_size:' in line:
                                    size = int(line.split()[1])
                                    total_size += size
                                elif 'items:' in line:
                                    items = int(line.split()[1])
                                    total_items += items

                        except subprocess.CalledProcessError as e:
                            print(f'get_collection_stats: caught exception {e}')
                            raise e

                    # Calculate document size
                    document_size = math.ceil(total_size / total_items) if total_items > 0 else 0

                    # Write to CSV
                    writer.writerow([bucket, bucket_type, scope_name, collection_name, total_size, total_items, document_size])


# helper function to get specified KV metric (cmd_get, cmd_set, delete_hits) for specifed bucket
# See the https://docs.couchbase.com/server/current/rest-api/rest-bucket-stats.html for more information.
def get_kv_metric(metric, bucket, app_config):
    result = 0
    params = {'zoom': app_config["kv_zoom"]}

    response = send_request(app_config["data_node"],
                            app_config["admin_port"],
                            f'/pools/default/buckets/{bucket}/stats',
                            app_config["username"],
                            app_config["password"],
                            params)
    data = response.json()

    # Extract values and convert to numbers
    samples = data['op']['samples'][metric]

    # Calculate average and round up
    if samples:
        average = statistics.mean(samples)
        result = math.ceil(average)
        print(f"{metric}: {result}")

    return result


# helper function to get specified N1QL metric (n1ql_selects, n1ql_deletes, n1ql_inserts)
# See the https://docs.couchbase.com/server/current/rest-api/rest-bucket-stats.html for more information.
def get_n1ql_metric(metric, app_config):
    result = 0
    response = send_request(app_config["data_node"],
                            app_config["admin_port"],
                            f'/pools/default/stats/range/{metric}/irate?start={app_config["n1ql_start"]}&step={app_config["n1ql_step"]}',
                            app_config["username"],
                            app_config["password"])
    data = response.json()

    if data['data'] == []:
        # Metrics could not be retrieved
        raise Exception(f'get_n1ql_metric: {data["errors"][0]["error"]}')

    # Extract values and convert to numbers
    values = [
        float(val[1]) for arr in data['data'] for val in arr['values']
        if val[1] is not None
    ]

    # Calculate average and round up
    if values:
        average = statistics.mean(values)
        result = math.ceil(average)
        print(f"{metric}: {result}")

    return result


# write the following KV operation details for all buckets in the cluster to kv-stats.csv:
#   bucket name
#   gets/second
#   sets/second
#   deletes/second
def get_kv_metrics(app_config, buckets):
    print("\ngetting K/V stats...")

    # Initialize CSV file with header
    with open('kv-stats.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['bucket', 'gets', 'sets', 'deletes'])

        for bucket in buckets:
            print(f"\ngetting KV stats for last {app_config['kv_zoom']} for bucket {bucket}...")
            gets = get_kv_metric('cmd_get', bucket, app_config)
            sets = get_kv_metric('cmd_set', bucket, app_config)
            deletes = get_kv_metric('delete_hits', bucket, app_config)
            writer.writerow([bucket, gets, sets, deletes])


# write the following N1QL query details to n1ql-stats.csv:
#   selects/second
#   deletes/second
#   inserts/second
def get_n1ql_metrics(app_config):
    print(f"\ngetting N1QL stats every {app_config['n1ql_step']} ms for {app_config['n1ql_start']} ms...")

    # Initialize CSV file with header
    with open('n1ql-stats.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['selects', 'deletes', 'inserts'])

        selects = get_n1ql_metric('n1ql_selects', app_config)
        deletes = get_n1ql_metric('n1ql_deletes', app_config)
        inserts = get_n1ql_metric('n1ql_inserts', app_config)
        writer.writerow([selects, deletes, inserts])


# write index defintions to indexes-<bucket name>.txt
# primary index defintions are not included since all Amazon DocumentDB collections have a default primary index on _id
def get_index_definitions(app_config, buckets, index_node):
    print("\ngetting index definitions...")

    for bucket in buckets:
        bucket_index_definitions = []

        # Write filtered index definitions to file
        filename = f"indexes-{bucket}.txt"
        try:
            # Get final index definitions from specific node
            response = send_request(f'http://{index_node}',
                                    app_config["indexer_port"],
                                    f'/getIndexStatement',
                                    app_config["username"],
                                    app_config["password"])
            index_statements = response.json()

            # Filter and write to file
            index_count = 0
            with open(filename, 'w') as f:
                for stmt in index_statements:
                    # skip primary index defintion
                    if f'`{bucket}`' in stmt and 'CREATE PRIMARY INDEX' not in stmt:
                        index_count += 1
                        f.write(f"{stmt.split(' WITH')[0] if ' WITH' in stmt else stmt}\n")

            print(f"found {index_count} indexes in bucket {bucket}")

        except requests.RequestException as e:
            print(f"Error writing index definitions to file for bucket {bucket}: {e}")
        except IOError as e:
            print(f"Error writing to file {filename}: {e}")


# write index statistics to index-stats.csv:
#   bucket name
#   scope name
#   collection name
#   index name
#   index size
# See https://docs.couchbase.com/server/current/index-rest-stats/index.html for more information
def get_index_stats(app_config, buckets, index_node):
    print("\n")

    # Initialize CSV file with header
    with open('index-stats.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(
            ['bucket', 'scope', 'collection', 'index-name', 'index-size'])

    for bucket in buckets:
        print(f"getting index stats for bucket {bucket}")

        # Get index stats
        try:
            response = send_request(f'http://{index_node}',
                                    app_config["indexer_port"],
                                    f'/api/v1/stats/{bucket}',
                                    app_config["username"],
                                    app_config["password"], False)
            
            # 404 will be returned if there are no indexes on the specified bucket
            if (response.status_code != 404):
                # there are indexes on the specified bucket
                stats = response.json()
                with open('index-stats.csv', 'a', newline='') as f:
                    writer = csv.writer(f)

                    for key, value in stats.items():
                        parts = key.split(':')

                        if len(parts) == 2:
                            # Format: bucket:index
                            writer.writerow([
                                parts[0],  # bucket
                                '_default',  # scope
                                '_default',  # collection
                                parts[1],  # index-name
                                value['data_size']  # index-size
                            ])
                        elif len(parts) == 3:
                            # Format: bucket:scope:index
                            writer.writerow([
                                parts[0],  # bucket
                                parts[1],  # scope
                                '_default',  # collection
                                parts[2],  # index-name
                                value['data_size']  # index-size
                            ])
                        else:
                            # Format: bucket:scope:collection:index
                            writer.writerow([
                                parts[0],  # bucket
                                parts[1],  # scope
                                parts[2],  # collection
                                parts[3],  # index-name
                                value['data_size']  # index-size
                            ])

        except requests.RequestException as e:
            print(f"Error getting index stats for bucket {bucket}: {e}")
        except IOError as e:
            print(f"Error writing to CSV file: {e}")


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('--username',
                        required=True,
                        type=str,
                        help='Couchbase cluster username')

    parser.add_argument('--password',
                        required=True,
                        type=str,
                        help='Couchbase cluster password')

    parser.add_argument('--data_node',
                        required=True,
                        type=str,
                        help='Couchbase data node IP address or DNS name')

    parser.add_argument('--admin_port',
                        required=True,
                        type=str,
                        default="8091",
                        help='administration REST port')

    parser.add_argument('--kv_zoom',
                        required=True,type=str,
                        default="month",
                        help='get bucket statistics for specified interval: <minute | hour | day | week | month | year>'
    )

    parser.add_argument('--tools_path',
                        required=True,
                        type=str,
                        default="/opt/couchbase/bin",
                        help='full path to Couchbase tools')

    parser.add_argument('--index_metrics',
                        required=False,
                        type=str,
                        default="false",
                        help='gather index definitions and N1QL metrics: <true | false>')

    parser.add_argument('--indexer_port',
                        required=False,
                        type=str,
                        default="9102",
                        help='indexer service http REST port')

    parser.add_argument('--n1ql_start',
                        required=False,
                        type=str,
                        default="-60000",
                        help='number of milliseconds prior at which to start sampling'
    )

    parser.add_argument('--n1ql_step',
                        required=False,
                        type=str,
                        default="100",
                        help='sample interval over the sample period, in milliseconds')

    args = parser.parse_args()
    app_config = {}
    app_config['username'] = args.username
    app_config['password'] = args.password
    app_config['data_node'] = args.data_node
    app_config['admin_port'] = args.admin_port
    app_config['kv_zoom'] = args.kv_zoom
    app_config['tools_path'] = args.tools_path
    app_config['index_metrics'] = True if args.index_metrics == 'true' else False
    app_config['indexer_port'] = args.indexer_port
    app_config['n1ql_start'] = args.n1ql_start
    app_config['n1ql_step'] = args.n1ql_step

    # get all information about the Couchbase cluster
    try:
        data_nodes = get_data_nodes(app_config)
        print(f"found data nodes {data_nodes}")

        buckets = get_buckets(app_config)
        print(f"found buckets {buckets}")

        get_collection_stats(app_config, buckets, data_nodes)

        get_kv_metrics(app_config, buckets)

        if app_config["index_metrics"] == True:
            index_nodes = get_index_nodes(app_config)
            print(f"found index nodes {index_nodes}")

            if len(index_nodes) > 0:
                get_index_definitions(app_config, buckets, index_nodes[0])
                get_index_stats(app_config, buckets, index_nodes[0])
                get_n1ql_metrics(app_config)
            else:
                print(f"no index nodes exist in cluster, cannot gather index definitions and N1QL metrics.")

    except Exception as e:
        print(f'{e}')


if __name__ == "__main__":
    main()
