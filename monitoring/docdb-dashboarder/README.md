# DocumentDB Dashboarder Tool
DocumentDB Dashboarder creates a CloudWatch monitoring dashboard for a DocumentDB cluster. Monitor your workload
and easily identify problems when dealing with slow  performance and high cost consumption.

------------------------------------------------------------------------------------------------------------------------
## Package Requirements 

**boto3** - AWS SDK that allows management of aws resources through python

**awscli** - Command line tools that allow access public APIs to manage AWS services

**argparse** - Python library that allows for the use of command line arguments

------------------------------------------------------------------------------------------------------------------------
## Installing Packages

1. In your terminal, install the boto3, awscli, and argparse in your terminal
```
pip install boto3
pip install awscli
pip install argparse
```
------------------------------------------------------------------------------------------------------------------------
## IAM User Creation and Setup

**Note: If you already have an existing IAM user for DocDB, associate the roles in step 4 and can move on to the next
section "Configure your AWS Credentials"**


1. Open IAM Service in your AWS Management Console


2. Select the "Users" tab using the toolbar on the left side of your screen


3. Create a new user and under "Select AWS Access Type" choose "Access Key - Programmatic Access" and click next. Be sure to save this access key for later on.


4. Associate the following permissions for your IAM User - CloudWatchFullAccess, AmazonDocDBReadOnlyAccess


5. Complete the user creation and save the csv file with your access key and secret access key in a safe place


_Congratulations you have successfully set up your IAM User to interact with CloudWatch and DocumentDB!_

------------------------------------------------------------------------------------------------------------------------
## Configure Your AWS Credentials

1. In your terminal use the following command: 
```
aws configure
```
2. You will be prompted to fill out four categories:

**Note: Access key can be found in IAM -> Users -> User Name -> Security Credentials -> Access Keys**
```
AWS Access Key: <IAM User access key> 
AWS Secret Access Key: <IAM Secret Access Key> 
Default region: <Region of your DocumentDB Cluster>
Default output format: <json>
```
3. To view your credentials, use the following command in your terminal: 
```
cat ~/.aws/credentials
```
4. To test your aws credentials by returning your account information use the following command in your terminal:
```
aws sts get-caller-identity | tee 
```

_Congratulations you have successfully configured your AWS Credentials!_

------------------------------------------------------------------------------------------------------------------------
## How to Run

Note: If you want to add additional instances to your cluster in the future, you must run the script again.

### To Run in IDE
In your IDE click edit your configurations and set your parameters as follows, then run in your IDE: 
```
--name <your dashboard name> --region <your region> --clusterID <DocDB clusterID>
```
### To Run in Terminal
Open your terminal and run the following commands: 
```
cd <location path of python-script>
```
```
python create-docdb-dashboard.py --name <your dashboard name> --region <your region> --clusterID <DocDB clusterID>
```
