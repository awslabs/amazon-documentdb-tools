# Amazon DocumentDB Deployment Scanner
The deployment scanner reviews DocumentDB clusters for possible cost optimization and utilization.

## Features
- Estimate the monthly cost for each cluster in a region in both standard storage and IO optimized storage configurations

## Requirements
Python 3.7 or greater, boto3, urllib3
IAM privileges in https://github.com/awslabs/amazon-documentdb-tools/blob/master/performance/deployment-scanner/iam-policy.json

## Installation
Clone the repository and install the requirements:

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/deployment-scanner
python3 -m pip install -r requirements.txt
```

## Usage/Examples
The deployment scanner accepts the following arguments:

```
--region                     AWS region for scan
--log-file-name              Name of file write CSV data to
--start-date                 [optional] Starting date in YYYYMMDD for historical review of cluster resource usage
--end-date                   [optional] Ending date in YYYYMMDD for historical review of cluster resource usage

If --start-date and --end-date are not provided, the last 30 days are used for historical cluster resource usage.
```

### Review Amazon DocumentDB clusters in us-east-1 for November 2023:
```
python3 deployment-scanner.py --log-file-name nov-23-us-east-1 --start-date 20231101 --end-date 20231130
```


## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Contributing
Contributions are always welcome! See the [contributing](https://github.com/awslabs/amazon-documentdb-tools/blob/master/CONTRIBUTING.md) page for ways to get involved.
