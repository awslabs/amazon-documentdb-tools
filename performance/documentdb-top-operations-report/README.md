# Amazon DocumentDB Top Operations Report
A tool for analyzing DocumentDB profiler logs to identify top operations and performance bottlenecks. Analyze DocumentDB profiler logs for slowest operations, run as AWS Lambda for automated reports or command-line for ad-hoc analysis, email reports via Amazon SES or CSV file output, support for multiple DocumentDB clusters, and configurable time windows for analysis.

## Requirements
- Python 3.8+
- AWS credentials configured for CloudWatch Logs access
- IAM privileges for DocumentDB profiler log access


## Installation
Clone the repository and install the requirements:

```
git clone https://github.com/awslabs/amazon-documentdb-tools.git
cd amazon-documentdb-tools/performance/documentdb-top-operations-report
python3 -m pip install -r requirements.txt
```

## Usage/Examples
The profiler analyzer accepts the following arguments:

```
--log-groups                 Comma-separated list of DocumentDB profiler log groups
--top-ops-count              [optional] Number of top operations to return (default: all)
--output-dir                 [optional] Output directory for CSV files (default: current directory)
--start-time                 [optional] Report start time in format "YYYY-MM-DD HH:MM:SS"
--end-time                   [optional] Report end time in format "YYYY-MM-DD HH:MM:SS"

If --start-time and --end-time are not provided, the last 24 hours are used.
```


### Analyze DocumentDB clusters for the last 24 hours:
```
python3 src/docdb_profiler_analyzer.py --log-groups "/aws/docdb/cluster1/profiler,/aws/docdb/cluster2/profiler"
```

### Custom time range with specific output directory:
```
python3 src/docdb_profiler_analyzer.py --log-groups "/aws/docdb/cluster1/profiler" --start-time "2024-01-01 00:00:00" --end-time "2024-01-02 00:00:00" --top-ops-count 20 --output-dir "./reports"
```
**_NOTE:_** 
This tool uses the CloudWatch Logs large query functionality to handle result sets larger than 10,000 entries. The implementation is based on the AWS SDK examples for [CloudWatch Logs Large Query](https://github.com/awsdocs/aws-doc-sdk-examples/tree/main/scenarios/features/cloudwatch_logs_large_query), which uses recursive querying with threading to efficiently retrieve large volumes of log data from CloudWatch Logs.

## AWS Lambda Deployment
For automated reporting, deploy as an AWS Lambda function with scheduled execution.

### Prerequisites
- Amazon SES configured with verified sender and recipient email addresses
- SAM CLI installed
- Docker installed

### Deploy with SAM CLI
```
sam build --use-container
sam deploy --guided
```

The first command will build the source of your application. The second command will package and deploy your application to AWS, with a series of prompts:

- **Stack Name**: The name of the stack to deploy to CloudFormation. This should be unique to your account and region.
- **AWS Region**: The AWS region you want to deploy your app to.
- **Parameter LambdaTriggerSchedule**: Provide cron schedule to trigger Lambda in UTC. Example: cron(0 13 * * ? *)
- **Parameter DocDBProfilerLogGrpsName**: Comma separated list of CloudWatch Profile log group names for DocumentDB
- **Parameter TopOpsCount**: Number of top operations to be reported. If left empty then it will report all operations
- **Parameter ReportStartTime**: Start time for adhoc report in UTC. Example: 2024-02-11 00:00:00 . If left empty then it will run the report for the last one day.
- **Parameter ReportEndTime**: End time for adhoc report in UTC. Example: 2024-02-11 00:00:00 . If left empty then it will run the report for the last one day.
- **Parameter SenderEmail**: Sender email address for the report
- **Parameter RecipientEmailList**: Comma separated list of recipient email addresses for the report
- **Confirm changes before deploy**: If set to yes, any change sets will be shown to you before execution for manual review. If set to no, the AWS SAM CLI will automatically deploy application changes.
- **Allow SAM CLI IAM role creation**: Many AWS SAM templates, including this example, create AWS IAM roles required for the AWS Lambda function(s) included to access AWS services. By default, these are scoped down to minimum required permissions. To deploy an AWS CloudFormation stack which creates or modifies IAM roles, the `CAPABILITY_IAM` value for `capabilities` must be provided. If permission isn't provided through this prompt, to deploy this example you must explicitly pass `--capabilities CAPABILITY_IAM` to the `sam deploy` command.
- **Save arguments to samconfig.toml**: If set to yes, your choices will be saved to a configuration file inside the project, so that in the future you can just re-run `sam deploy` without parameters to deploy changes to your application.

### Use the SAM CLI to build and test locally

Build your application with the `sam build --use-container` command.

```
sam build --use-container
```

The SAM CLI installs dependencies defined in `src/requirements.txt`, creates a deployment package, and saves it in the `.aws-sam/build` folder.

Test a single function by invoking it directly with a test event. An event is a JSON document that represents the input that the function receives from the event source. Test events are included in the `events` folder in this project.

Set environment variables for testing:
```
export DOCDB_LOG_GROUP_NAME="/aws/docdb/your-cluster/profiler"
export TOP_OPS_COUNT="10"
export SENDER_EMAIL="test@example.com"
export RECIPIENT_EMAIL_LIST="recipient@example.com"
export REPORT_START_TIME="2024-01-01 00:00:00"
export REPORT_END_TIME="2024-01-02 00:00:00"
```

Run functions locally and invoke them with the `sam local invoke` command.

```
sam local invoke -e events/manual_invoke.json | jq '.'
```

### Deploying with CloudFormation Template
If SAM CLI is not available then you can deploy with cloudformation template `src/cfn_template.yaml`

1. Create a zip file for AWS Lambda function:
```
(cd src && zip -r ./function.zip . -x "__pycache__/*" "__init__.py")
```

2. Create an Amazon S3 bucket named 'lambda-code-bucket-SourceAWSAccountID' in the source AWS account.

3. Upload the 'src/function.zip' file to the Amazon S3 bucket named 'lambda-code-bucket-SourceAWSAccountID' created in step 2.

4. Deploy the CloudFormation template `src/cfn_template.yaml` in the AWS account to create required resources such Amazon IAM roles, Amazon EventBridge Scheduler and AWS Lambda function.

## Cleanup

### For SAM Deployment
To delete the sample application that you created, use the AWS CLI. Assuming you used your project name for the stack name, you can run the following:

```
sam delete --stack-name "docdb-profiler-top-ops"
```

### For CloudFormation Deployment
If you deployed using the CloudFormation template directly, you can delete the stack using:

```
# Using AWS CLI
aws cloudformation delete-stack --stack-name "Stack Name"

# Check deletion status
aws cloudformation describe-stacks --stack-name "Stack Name"
```

Alternatively, you can delete the stack through the AWS Management Console:
1. Go to the CloudFormation service in the AWS Console
2. Select your stack (e.g., "docdb-profiler-top-ops")
3. Click "Delete" and confirm the deletion

**Note**: Make sure to also clean up any S3 bucket you created for storing the Lambda code if you used the CloudFormation deployment method.

## License
[Apache 2.0](http://www.apache.org/licenses/LICENSE-2.0)

## Contributing
Contributions are always welcome! See the [contributing](CONTRIBUTING.md) page for ways to get involved.
