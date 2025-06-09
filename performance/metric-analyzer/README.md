# Amazon DocumentDB Metric Analyzer

This tool analyzes the output of the [Amazon DocumentDB Metric Collector Tool](https://github.com/awslabs/amazon-documentdb-tools/tree/master/performance/metric-collector) to provide recommendations for optimizing performance, cost, and availability.

## Features

- Analyzes CPU utilization, cache hit ratios, connection limits, and more
- Provides specific recommendations based on best practices
- Includes detailed context for each recommendation type
- Generates CSV output for easy review
- Creates interactive HTML reports with recommendation context details

## Usage

```bash
python metric-analyzer.py --metrics-file <input-file-name> \
    --region <aws-region-name> \
    --output <output-file-name> \
    --log-level <log-level> \
    [--no-html]
```

### Parameters

- `--metrics-file`: Path to the metrics CSV file to analyze (required)
- `--region`: AWS Region (default: us-east-1)
- `--output`: Base name for output files (default: metric-analyzer)
- `--log-level`: Log level for logging (choices: DEBUG, INFO, WARNING, ERROR, CRITICAL, default: WARNING)
- `--no-html`: Disable HTML output generation (HTML output is enabled by default)

## Recommendation Context

Each recommendation includes a link to a context file in the `context/` directory that provides additional information about:

- Considerations before implementing the recommendation
- Potential impacts (positive and negative)
- Alternative approaches
- Implementation guidance

These context files supplement the AWS documentation references and provide more nuanced guidance for decision-making.

## Output Format

### CSV Output

The tool generates a CSV file with the following columns:

- ClusterName: Name of the DocumentDB cluster
- InstanceName: Name of the instance (if applicable)
- InstanceType: Instance type (e.g., db.r6g.large)
- InstanceRole: PRIMARY or SECONDARY
- Category: Instance or Cluster level recommendation
- ModifyInstance: Action to take (INCREASE, DECREASE, UPGRADE)
- Finding: Specific finding with metrics
- Recommendation: Recommended action
- Reference: Link to AWS documentation

### HTML Output

The tool also generates an interactive HTML report that includes:

- All information from the CSV output
- Interactive "View Context" buttons that display detailed guidance for each recommendation
- Responsive design for better readability

## Requirements

- Python 3.6+
- boto3>=1.26.0
- pandas>=1.3.0
- markdown>=3.3.0
