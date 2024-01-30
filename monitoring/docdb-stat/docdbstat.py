# Amazon DocumentDB Stats Monitoring Tool (Version 1.0)

import argparse
import time
from pymongo import MongoClient
import pandas as pd


def connect_to_docdb(uri):
    """Connects to a DocumentDB instance.
    Check for replicaSet in uri and replace with directConnection.

    Args:
     uri: The DocumentDB connection URI.

    Returns:
     A pymongo.MongoClient object.
    """
    if "replicaSet=rs0" in uri:
        uri = uri.replace("replicaSet=rs0", "directConnection=true")

    client = MongoClient(host=uri,appname='ddbstat')
    db = client.admin
    return db


def get_server_stats(db):
    """Retrieve the serverStatus() for a DocumentDB instance.
    Args:
     db: A pymongo.MongoClient object.

    Returns:
     A dictionary containing the server stats.
    """
    return db.command('serverStatus')


def get_replica_status(db):
    """Retrieve the replica status for a DocumentDB instance.

    Args:
     db: A pymongo.MongoClient object.

    Returns:
     The replica status, string.
    """
    is_master_result = db.command('isMaster')
    if 'setName' in is_master_result:
        if is_master_result['ismaster']:
            return 'Primary'
        else:
            return 'Secondary'


def display_server_stats(previous_stats, current_stats, header_interval_counter, db, fields, polling_interval):
    """Displays db.serverStatus() for a DocumentDB instance.

    Args:
      previous_stats: The previous stats, or None if there are no previous stats.
      current_stats: The current stats.
      header_interval_counter: A counter that is used to determine when to print the header of the table.
      db: A pymongo.MongoClient object.
      fields: A list of the fields to display in the table.
      polling_interval: The polling interval in seconds.

    Returns:
      previous_stats: dict, current_stats: dict, header_interval_counter: int, db: object, fields: List[str], polling_interval: int
    """
    if previous_stats is None:
        return

    metrics = {
        'Host': current_stats['host'].split('.')[0],
        'Status': get_replica_status(db),
        'Connections': current_stats['connections']['current'],
        'Inserts': (current_stats['opcounters']['insert'] - previous_stats['opcounters']['insert']) / polling_interval,
        'Query': (current_stats['opcounters']['query'] - previous_stats['opcounters']['query']) / polling_interval,
        'Updates': (current_stats['opcounters']['update'] - previous_stats['opcounters']['update']) / polling_interval,
        'Deletes': (current_stats['opcounters']['delete'] - previous_stats['opcounters']['delete']) / polling_interval,
        'GetMore': (current_stats['opcounters']['getmore'] - previous_stats['opcounters']['getmore']) / polling_interval,
        'Command': (current_stats['opcounters']['command'] - previous_stats['opcounters']['command']) / polling_interval,
        'CursorsTotal': current_stats['metrics']['cursor']['open']['total'],
        'CursorsNoTimeout': current_stats['metrics']['cursor']['open']['noTimeout'],
        'Transactions': current_stats['transactions']['currentActive'],
        'Timestamp': current_stats['localTime']
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
                display_server_stats(previous_server_stats, server_stats, header_interval_counter, db, fields, polling_interval)
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
