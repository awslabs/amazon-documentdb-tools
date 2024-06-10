# Amazon DocumentDB Metric Collector Tool

The metric collector tool provides a csv output consolidating metrics for all DocumentDB clusters within a defined region. In addition to metadata such as cluster name, engine version, multi-AZ configuration, TLS status, and instance types, the script captures the Min, Max, Mean, p99, and Std values for a chosen time period. These can be compared against [Best Practices for Amazon DocumentDB](https://docs.aws.amazon.com/documentdb/latest/developerguide/best_practices.html) to ensure your cluster and instances are correctly sized for performance, resiliency, and cost.

## Requirements
 - Python 3.9+
 - boto3 1.24.49+
 - pandas 2.2.1+

```
pip3 install boto3, pandas
```

- This script reads DocumentDB instance and cluster metrics from [Amazon CloudWatch](https://aws.amazon.com/cloudwatch/), as well as DocumentDB cluster details including parameter group information. The required IAM permissions can be found in `IAM-policy.json`.

## Usage parameters
Usage:
    
```
metric-collector.py --region <aws-region-name> \\
    --log-file-name <output-file-name> \\
    --start-date <YYYYMMDD> \\
    --end-date <YYYYMMDD>
```

Script Parameters:

 - region: str
    AWS Region
 - start-date: str
    Start date for CloudWatch logs, format=YYYYMMDD
 - end-date: str
    End date for CloudWatch logs, format=YYYYMMDD
 - log-file-name: str
    Log file for CSV output
 - log-level: str
    Log level for logging, default=INFO

## License
This tool is licensed under the Apache 2.0 License. 
