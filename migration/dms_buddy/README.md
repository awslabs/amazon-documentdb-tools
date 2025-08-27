# DMS Buddy

A tool for analyzing MongoDB collections and generating AWS Database Migration Service (DMS) configuration recommendations.

## Overview

DMS Buddy analyzes your MongoDB collections and provides optimized configuration recommendations for AWS DMS migrations to Amazon DocumentDB. It helps you determine:

- Appropriate DMS instance type based on data transfer requirements
- Required storage size based on collection size and change rate
- Optimal number of partitions for parallel full load
- Number of threads needed for CDC phase
- Other critical DMS configuration parameters

The tool also generates a parameter file that can be directly used with the included CloudFormation template to deploy the AWS DMS resources.

## Requirements

- Python 3.6+
- pymongo
- humanize
- AWS CLI (for deploying the CloudFormation template)
- AWS VPC with at least two subnets in different Availability Zones for DMS deployment
- SSL certificate imported into AWS DMS for DocumentDB connections (required for target endpoint)

## SSL Certificate Setup for DocumentDB (Pre-requisite)

When migrating to Amazon DocumentDB, SSL/TLS encryption is required. You need to import the DocumentDB certificate into AWS DMS before running DMS Buddy:

1. **Download the DocumentDB Certificate**:
   ```bash
   wget https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem
   ```

2. **Import the Certificate into DMS**:
   ```bash
   aws dms import-certificate \
     --certificate-identifier docdb-ca-cert \
     --certificate-pem file://global-bundle.pem
   ```

3. **Get the Certificate ARN**:
   ```bash
   aws dms describe-certificates --filters Name=certificate-id,Values=global-bundle --query "Certificates[0].CertificateArn"
   ```

4. **Use the Certificate ARN** in your configuration file or command line parameter.

## Installation

1. Clone or download this repository
2. Install the required Python packages:

```bash
pip install -r requirements.txt
```

## Usage

### Basic Usage

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --collection-name-for-parallel-load mycollection
```

**Note**: When you specify a single collection using `--collection-name-for-parallel-load`, the tool will analyze only that collection and the migration summary will show information specific to that collection. If no collection is specified, the tool will analyze all collections in the database with 10,000+ documents.

### Command Line Options

| Option | Description |
|--------|-------------|
| `--source-uri` | MongoDB connection URI (required) |
| `--source-database` | Source database name to analyze (required) |
| `--collection-name-for-parallel-load` | Collection name to analyze and use for parallel load (optional - if not provided, analyzes all collections with 10K+ documents) |
| `--migration-type` | Migration type: full-load, cdc, or full-load-and-cdc (default: full-load-and-cdc) |
| `--monitor-time` | Monitoring time in minutes (default: 10) |
| `--vpc-id` | VPC ID for DMS replication instance |
| `--subnet-ids` | Subnet IDs for DMS replication instance (comma-separated) |
| `--multi-az` | Whether to use Multi-AZ for DMS replication instance (true/false) |
| `--source-host` | Source database host |
| `--source-port` | Source database port (default: 27017) |
| `--source-username` | Source database username |
| `--source-password` | Source database password |
| `--target-host` | Target database host |
| `--target-port` | Target database port (default: 27017) |
| `--target-database` | Target database name |
| `--target-username` | Target database username |
| `--target-password` | Target database password |
| `--target-certificate-arn` | Target database SSL certificate ARN for DocumentDB connections |

### Examples

#### Single Collection Analysis

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --collection-name-for-parallel-load mycollection --migration-type full-load
```

#### All Collections Analysis

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --migration-type full-load
```

#### CDC Migration (Includes Monitoring Period)

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --collection-name-for-parallel-load mycollection --migration-type cdc
```

#### Custom Monitoring Time

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --collection-name-for-parallel-load mycollection --monitor-time 5
```

#### With Additional Parameters

```bash
python dms_buddy.py --source-uri mongodb://localhost:27017 --source-database mydb --collection-name-for-parallel-load mycollection --vpc-id vpc-12345 --subnet-ids subnet-a,subnet-b --multi-az true
```

## Configuration File

Instead of specifying all parameters on the command line, you can create a configuration file named `dms_buddy.cfg` in the same directory:

```ini
[DMS]
VpcId = vpc-02095d845d94b21b4
SubnetIds = subnet-xxxxx,subnet-yyyyy
MultiAZ = false
SourceDBHost = your-mongodb-host
SourceDBPort = 27017
SourceDatabase = your-database
SourceUsername = your-username
SourcePassword = your-password
TargetHost = your-docdb-cluster-endpoint
TargetPort = 27017
TargetDatabase = your-database
TargetUsername = your-username
TargetPassword = your-password
TargetCertificateArn = arn:aws:dms:us-east-1:123456789012:cert:your-cert-id
MigrationType = full-load-and-cdc
CollectionNameForParallelLoad = collectionname
```

### Configuration Parameters:

- **VpcId**: VPC ID for DMS replication instance
- **SubnetIds**: Subnet IDs for DMS replication instance (comma-separated)
- **MultiAZ**: Whether to use Multi-AZ for DMS replication instance (true/false)
- **SourceDBHost**: Source database host
- **SourceDBPort**: Source database port (default: 27017)
- **SourceDatabase**: Source database name to analyze
- **SourceUsername**: Source database username
- **SourcePassword**: Source database password
- **TargetHost**: Target database host
- **TargetPort**: Target database port (default: 27017)
- **TargetDatabase**: Target database name
- **TargetUsername**: Target database username
- **TargetPassword**: Target database password
- **TargetCertificateArn**: Target database SSL certificate ARN for DocumentDB connections
- **MigrationType**: Migration type: full-load, cdc, or full-load-and-cdc (default: full-load-and-cdc)
- **CollectionNameForParallelLoad**: Collection name to analyze and use for parallel load (leave empty string to analyze all collections with 10K+ documents)

**Note**: To analyze all collections in the database, set `CollectionNameForParallelLoad` to an empty string.

Command line arguments take precedence over configuration file values.

## How It Works

1. **Collection Analysis**: The tool connects to your MongoDB instance and retrieves statistics about the specified collection.

2. **Operations Monitoring**: For CDC migrations, it monitors database operations for a specified period (default: 10 minutes) to determine the rate of change.

3. **Recommendations Calculation**:
   - **Instance Type**: Based on bandwidth requirements calculated from document size and partitions
   - **Storage Size**: Based on collection size and daily change rate, rounded to nearest 100GB
   - **Partitions**: Based on document count, optimized for parallel processing
   - **Parallel Apply Threads**: Based on operations per second for CDC

4. **Parameter Generation**: Creates a `parameter.json` file with all the calculated and provided parameters.

## Deploying with CloudFormation

After running DMS Buddy to generate the `parameter.json` file, you can deploy the AWS DMS resources using the included CloudFormation template:

```bash
# Deploy the CloudFormation stack
aws cloudformation create-stack \
  --stack-name mongodb-to-docdb-migration \
  --template-body file://dms_buddy.cfn \
  --parameters file://parameter.json \
  --capabilities CAPABILITY_IAM

# Check the stack creation status
aws cloudformation describe-stacks --stack-name mongodb-to-docdb-migration

# Update security groups to allow DMS access
# Get the DMS security group ID from the stack outputs
DMS_SG_ID=$(aws cloudformation describe-stacks --stack-name mongodb-to-docdb-migration --query "Stacks[0].Outputs[?OutputKey=='DMSSecurityGroupId'].OutputValue" --output text)

# Update source MongoDB cluster security group to allow inbound TCP access from DMS
aws ec2 authorize-security-group-ingress \
  --group-id <source-mongodb-security-group-id> \
  --protocol tcp \
  --port 27017 \
  --source-group $DMS_SG_ID

# Update target DocumentDB cluster security group to allow inbound TCP access from DMS
aws ec2 authorize-security-group-ingress \
  --group-id <target-docdb-security-group-id> \
  --protocol tcp \
  --port 27017 \
  --source-group $DMS_SG_ID

# Once the stack is created and security groups are updated, you can start the DMS task
aws dms start-replication-task \
  --replication-task-arn $(aws cloudformation describe-stacks --stack-name mongodb-to-docdb-migration --query "Stacks[0].Outputs[?OutputKey=='ReplicationTaskARN'].OutputValue" --output text) \
  --start-replication-task-type start-replication
```


## Troubleshooting

### Connection Issues

If you encounter connection issues:
- Verify that the MongoDB URI is correct
- Ensure that the MongoDB server is running and accessible
- Check that the specified database and collection exist
- Verify that the user has appropriate permissions

### CloudFormation Deployment Issues

If you encounter issues deploying the CloudFormation template:
- Check the stack events for detailed error messages:
  ```bash
  aws cloudformation describe-stack-events --stack-name mongodb-to-docdb-migration
  ```
- Verify that all required parameters are present in the parameter.json file
- Ensure that the VPC and subnet IDs are valid
- **Important**: Make sure you provide at least two subnet IDs in different Availability Zones
- Check that you have the necessary permissions to create the resources


## License

This project is licensed under the MIT License - see the LICENSE file for details.
