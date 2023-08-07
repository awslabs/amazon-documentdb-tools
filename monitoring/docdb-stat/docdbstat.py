# Amazon DocumentDB Stats Monitoring Tool (Version 1.0)

import argparse
import time
from pymongo import MongoClient
import pandas as pd


def connect_to_docdb(uri):
    # Check for replicaSet in uri and replace with directConnection
    if "replicaSet=rs0" in uri:
        uri = uri.replace("replicaSet=rs0", "directConnection=true")

    client = MongoClient(uri)
    db = client.admin
    return db


def get_server_stats(db):
    return db.command('serverStatus')


def get_replica_status(db):
    is_master_result = db.command('isMaster')
    if 'setName' in is_master_result:
        if is_master_result['ismaster']:
            return 'Primary'
        else:
            return 'Secondary'


def display_server_stats(stats, header_interval_counter, db, fields):
    # Extract specific metrics from the stats dictionary
    metrics = {
        'Host': stats['host'].split('.')[0],
        'Status': get_replica_status(db),
        'Connections': stats['connections']['current'],
        'Inserts': stats['opcounters']['insert'],
        'Query': stats['opcounters']['query'],
        'Updates': stats['opcounters']['update'],
        'Deletes': stats['opcounters']['delete'],
        'GetMore': stats['opcounters']['getmore'],
        'Command': stats['opcounters']['command'],
        'CursorsTotal': stats['metrics']['cursor']['open']['total'],
        'CursorsNoTimeout': stats['metrics']['cursor']['open']['noTimeout'],
        'Transactions': stats['transactions']['currentActive'],
        'Timestamp': stats['localTime']
    }

    fields = [field.lower() for field in fields]
    selected_metrics = {key: value for key, value in metrics.items() if key.lower() in fields}

    # Convert the selected metrics dictionary to a DataFrame for tabular display
    df = pd.DataFrame(selected_metrics, index=[0])

    # Convert the DataFrame to a string and remove the index from the output
    table_str = df.to_string(header=True, index=False)

    # Print the DataFrame with proper alignment and show the header based on the header_interval_counter
    if header_interval_counter == 0:
        print(table_str)
    else:
        print(table_str[table_str.index('\n') + 1:])


def main(uri, polling_interval, header_interval, fields):
    db = connect_to_docdb(uri)
    previous_server_stats = None
    iteration_counter = 0
    header_interval_counter = 0

    try:
        while True:
            server_stats = get_server_stats(db)
            if previous_server_stats is not None:
                if header_interval_counter == 0:
                    display_server_stats(server_stats, header_interval_counter, db, fields)
                else:
                    display_server_stats(server_stats, header_interval, db, fields)

                header_interval_counter = (header_interval_counter + 1) % header_interval

            previous_server_stats = server_stats.copy()
            time.sleep(polling_interval)
            iteration_counter += 1
    except KeyboardInterrupt:
        print("\nMonitoring stopped by the user.")
    finally:
        db.client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time Amazon DocumentDB server stats monitoring tool.")
    parser.add_argument("--uri", required=True, help="DocumentDB connection URI.")
    parser.add_argument("-i", "--interval", type=int, default=1, help="Polling interval in seconds (Default: 1s).")
    parser.add_argument("-hi", "--header-interval", type=int, default=10, help="Interval to display the header in iterations (Default: 10).")
    parser.add_argument("-f", "--field", type=str, default='Host,Status,Connections,Inserts,Query,Updates,Deletes,GetMore,Command,CursorsTotal,CursorsNoTimeout,Transactions,Timestamp',
                        help="Comma-separated fields to display in the output.")
    args = parser.parse_args()

    fields = [field.strip() for field in args.field.split(',')]
    main(args.uri, args.interval, args.header_interval, fields)
