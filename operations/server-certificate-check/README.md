# Amazon DocumentDB Server Certificate Check
The server certificate check returns a list of all instances in a region including the expiration of the servers certificate and maintenance window.

## Features
- Output may be filtered using case-insensitive matching on cluster name and/or instance name.

## Requirements
- Python 3.7 or greater, boto3, urllib3
- IAM privileges in https://github.com/awslabs/amazon-documentdb-tools/blob/master/operations/server-certificate-check/iam-policy.json

## Installation
Clone the repository and install the requirements:

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/server-certificate-check
python3 -m pip install -r requirements.txt
```

## Usage/Examples
The utility accepts the following arguments:

```
--region                     AWS region for scan
--log-file-name              Name of log file to capture all output
--cluster-filter             [optional] Case-insensitive string to use for filtering clusters to include in output
--instance-filter            [optional] Case-insensitive string to use for filtering instances to include in output 

```

### Report all Amazon DocumentDB instances in us-east-1
```
python3 server-certificate-check.py --log-file-name certs.log --region us-east-1
```

### Report all Amazon DocumentDB instances in us-east-1 containing "ddb5" in instance name
```
python3 server-certificate-check.py --log-file-name certs.log --region us-east-1 --instance-filter ddb5
```

## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Contributing
Contributions are always welcome! See the [contributing](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) page for ways to get involved.
