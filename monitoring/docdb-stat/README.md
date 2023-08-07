# Real-time Amazon DocumentDB server stats monitoring tool. 

The **docdbstat** tool connects to a compute instance and continuously fetches real-time metrics by polling `db.serverStatus()` at a configurable interval (defaults to 1 sec).


## Requirements

- Python 3.x with modules:
  - Pymongo
  - Pandas
```
pip3 install pymongo pandas
```

- Download the Amazon DocumentDB Certificate Authority (CA) certificate required to authenticate to your instance
```
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

## Usage
The tools accepts the following arguments:

```
# python3 docdbstat.py --help
usage: docdbstat.py [-h] --uri URI [-i INTERVAL] [-hi HEADER_INTERVAL] [-f FIELD]

Real-time Amazon DocumentDB server stats monitoring tool.

options:
  -h, --help            show this help message and exit
  --uri URI             DocumentDB connection URI.
  -i INTERVAL, --interval INTERVAL
                        Polling interval in seconds (Default: 1s).
  -hi HEADER_INTERVAL, --header-interval HEADER_INTERVAL
                        Interval to display the header in iterations (Default: 10).
  -f FIELD, --field FIELD
                        Comma-separated fields to display in the output.
```

## Example

Get stats every 5 seconds:

```
python3 docdbstat.py --uri "mongodb://<user>:<pass>@<docdb-instance-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&retryWrites=false" -i 5
```

Get specific stats, for example to ouput just write operations:

```
python3 docdbstat.py --uri "mongodb://<user>:<pass>@<docdb-instance-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&retryWrites=false" -f inserts,updates,deletes
```
