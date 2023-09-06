# Real-time Amazon DocumentDB collection level monitoring tool. 

The **documentdb-top** tool connects to a DocumentDB instance and continuously fetches real-time collection level metrics by polling `db.<collection>.stats()` at a configurable interval (defaults 60 seconds).


## Requirements

- Python 3.x with modules:
  - Pymongo
```
pip3 install pymongo
```

- Download the Amazon DocumentDB Certificate Authority (CA) certificate required to authenticate to your instance
```
wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
```

## Usage
The tools accepts the following arguments:

```
# python3 documentdb-top.py --help

usage: documentdb-top.py [-h] --uri URI --database DATABASE [--update-frequency-seconds UPDATE_FREQUENCY_SECONDS] [--must-crud] --log-file-name LOG_FILE_NAME [--skip-python-version-check] [--show-per-second]

DocumentDB Top

optional arguments:
  -h, --help                                             show this help message and exit
  --uri URI                                              URI
  --database DATABASE                                    Database name
  --update-frequency-seconds UPDATE_FREQUENCY_SECONDS    Number of seconds before update
  --must-crud                                            Only display when insert/update/delete occurred
  --log-file-name LOG_FILE_NAME                          Log file name
  --skip-python-version-check                            Permit execution on Python 3.6 and prior
  --show-per-second                                      Show operations as "per second"
```

## Example

Get collection stats every 15 seconds, only if insert/update/delete has occurred:

```
python3 documentdb-top.py --uri "mongodb://<user>:<pass>@<docdb-instance-endpoint>:27017/?tls=true&tlsCAFile=global-bundle.pem&retryWrites=false&directConnection=true" --database db1 --update-frequency-seconds 15 --log-file-name my-log-file.log --must-crud
```

