# Real-time Amazon DocumentDB server stats monitoring tool. 

The **docdbstat** tool connect to a compute instance and retrives real time metrics by polling `db.serverStatus()` at a configurable interval (defaults to 1 sec).


## Requirements

- Python 3.x with modules:
  - Pymongo
  - Pandas
```
pip3 install pymongo pandas
```
- TLS CA file in the same path of the script.
Or specify the CA file using the `--tls-ca-file` argument
```
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

## Usage
The tools accepts the following arguments:

```
# python3 docdbstat.py --help
usage: docdbtop902.py [-h] --uri URI [--interval INTERVAL]
                      [--header-interval HEADER_INTERVAL] [--field FIELD]
                      [--notls] [--tls-ca-file TLS_CA_FILE]

Real-time Amazon DocumentDB server stats monitoring tool.

optional arguments:
  -h, --help            show this help message and exit
  --uri URI             DocumentDB connection URI.
  --interval            Polling interval in seconds (Default: 1s).
  --header-interval     Interval to display the header in iterations (Default: 10).
  --field FIELD         Comma-separated fields to display in the output. Supported: Host,Status,Connections,Inserts,Query,Updates,Deletes,GetMore,Command,CursorsTotal,CursorsNoTimeout,Transactions,Timestamp
  --notls               Disable the TLS option.
  --tls-ca-file         Path to the TLS CA file.
```

## Example

Get stats every 5 seconds:

```
python3 docdbstat.py --uri "mongodb://<user>:<pass>@docdb-endpoint:27017" --interval 5
```

Get specific stats, for example to ouput just write operations:

```
python3 docdbstat.py --uri "mongodb://<user>:<pass>@docdb-endpoint:27017" --field inserts,updates,deletes
```
