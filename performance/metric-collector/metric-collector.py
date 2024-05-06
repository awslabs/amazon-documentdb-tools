"""
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License").
You may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Usage:
    docdb_metric_collector.py --region <aws-region-name> \\
        --log-file-name <output-file-name> \\
        --start-date <YYYYMMDD> \\
        --end-date <YYYYMMDD>

Script Parameters
-----------------
--region: str
    AWS Region
--start-date: str
    Start date for CloudWatch logs, format=YYYYMMDD
--end-date: str
    End date for CloudWatch logs, format=YYYYMMDD
--log-file-name: str
    Log file for CSV output
--log-level: str
    Log level for logging, default=INFO
"""

import sys
import os
from datetime import timedelta, datetime
import logging
import pandas 
import argparse
import boto3

logger = logging.getLogger(__name__)

# Check for minimum Python version for script execution
MIN_PYTHON = (3, 9)
if sys.version_info < MIN_PYTHON:
    sys.exit("Python %s.%s or later is required.\n" % MIN_PYTHON)


# List of Amazon DocumentDB instances metrics that are collected.
INSTANCE_METRICS = ['CPUUtilization', 
                    'DatabaseConnections', 
                    'BufferCacheHitRatio', 
                    'IndexBufferCacheHitRatio', 
                    'ReadThroughput',
                    'VolumeReadIOPs',
                    'OpcountersQuery',
                    'OpcountersCommand',
                    'OpcountersGetmore',
                    'DocumentsReturned']

# List of Amazon DocumentDB instance metrics that are collected only from the primary instance.
PRIMARY_INSTANCE_METRICS = ['WriteIOPS', 
                            'WriteThroughput',
                            'OpcountersDelete',
                            'OpcountersInsert',
                            'OpcountersUpdate',
                            'DocumentsDeleted',
                            'DocumentsInserted',
                            'DocumentsUpdated',
                            'TTLDeletedDocuments']

#List of Amazon DocumentDB cluster metrics that are collected.
CLUSTER_METRICS = ['IndexBufferCacheHitRatio',
                   'DatabaseConnectionsMax',
                   'VolumeBytesUsed']

def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])

def printLog(thisMessage,appConfig):
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))

# This function retrieves the metadata details of Amazon DocumentDB clusters and instances in the specified AWS region.
# The function does not connect to the actual DocumentDB clusters or access any data stored in them.
# It collects metadata such as cluster name, engine version, multi-AZ configuration,
# TLS status, profiler status, audit log status, instance names, primary instance, availability zones, and instance types.
#
# Parameters:
#   appConfig (dict): A dictionary containing the AWS region and other configuration settings.
#
# Returns:
#   None
def getClusterDetails(appConfig):
    docdb_client = boto3.client('docdb',region_name=appConfig['region'])
    parameterList = ["enabled","ddl","dml_read","dml_write","all"]
    statusTLS = 'disabled'
    statusProfiler = 'disabled'
    statusAudit = 'disabled'        
    clusterDL = []
    instanceDL = []

    response = docdb_client.describe_db_clusters(Filters=[{'Name': 'engine','Values': ['docdb']}])
    # Validate clusters exist in region
    if not response['DBClusters']:
        print("No DocumentDB clusters found in this region. Please try a different region.")
        print("Exiting")
        exit()
    else:
        for thisCluster in response['DBClusters']:
            clusterName = thisCluster['DBClusterIdentifier']
            engineVersion = thisCluster['EngineVersion']
            isMultiAZ = thisCluster['MultiAZ']
            responsePG = docdb_client.describe_db_cluster_parameters(DBClusterParameterGroupName=thisCluster['DBClusterParameterGroup'])
            for thisParameter in responsePG['Parameters']:
                if thisParameter['ParameterName'] in 'tls':
                    statusTLS = thisParameter['ParameterValue']
                if thisParameter['ParameterName'] in 'profiler':
                    statusProfiler = thisParameter['ParameterValue']
                if thisParameter['ParameterName'] in 'audit_logs':
                    if thisParameter['ParameterValue'] in parameterList:
                        statusAudit = 'enabled'
            clusterData = {'ClusterName': clusterName, 'EngineVersion': engineVersion, 'MultiAZ': isMultiAZ, 'TLS': statusTLS, 'Profiler': statusProfiler, 'AuditLogs': statusAudit}
            clusterDL.append(clusterData)

            for thisInstance in thisCluster['DBClusterMembers']:
                instanceIdentifier = thisInstance['DBInstanceIdentifier']
                isPrimary = thisInstance['IsClusterWriter']
                responseInstance = docdb_client.describe_db_instances(DBInstanceIdentifier=thisInstance['DBInstanceIdentifier'])
                for instanceName in responseInstance['DBInstances']:
                    availabilityZone = instanceName['AvailabilityZone']
                    instanceType = instanceName['DBInstanceClass']
                    clusterIdentifier = instanceName['DBClusterIdentifier']
                    instanceData = {'ClusterName': clusterIdentifier,'InstanceName': instanceIdentifier, 'Primary': isPrimary, 'AvailabilityZone': availabilityZone, 'InstanceType': instanceType}
                    instanceDL.append(instanceData)

        appConfig['clusterDF'] = pandas.DataFrame(clusterDL)
        appConfig['instanceDF'] = pandas.DataFrame(instanceDL)
        return pandas.merge(appConfig['clusterDF'],appConfig['instanceDF'],on='ClusterName')

# This function retrieves the metrics for Amazon DocumentDB clusters and instances in the specified AWS region.
# 
# Parameters:
#   appConfig (dict): A dictionary containing the AWS region and other configuration settings.
#
# Returns:
#   Pandas DataFrame: A DataFrame containing the metrics for each cluster
def getClusterMetrics(appConfig):
    metricList = []

    for cluster in appConfig['clusterDF'].dropna().itertuples():
        print("Getting metrics for cluster: {} EngineVersion: {}".format(cluster.ClusterName, cluster.EngineVersion))
        for metric in CLUSTER_METRICS:
            getMetricData(appConfig, metricList, cluster.ClusterName, '---', metric, 'DBClusterIdentifier')
    
    return pandas.DataFrame(metricList)

# This function retrieves the metrics for Amazon DocumentDB instances in the specified AWS region.
#
# Parameters:
#   appConfig (dict): A dictionary containing the AWS region and other configuration settings.
#
# Returns:
#   Pandas DataFrame: A DataFrame containing the metrics for each instance
def getInstanceMetrics(appConfig):
    metricList = []
    for instance in appConfig['instanceDF'].dropna().itertuples():
        print("Getting metrics for cluster: {} instance: {}".format(instance.ClusterName, instance.InstanceName))
        if instance.Primary:
            for metric in PRIMARY_INSTANCE_METRICS:
                getMetricData(appConfig, metricList, instance.ClusterName, instance.InstanceName, metric)
        for metric in INSTANCE_METRICS:
            getMetricData(appConfig, metricList, instance.ClusterName, instance.InstanceName, metric)

    return pandas.DataFrame(metricList)

# This function retrieves the metrics for Amazon DocumentDB instances in the specified AWS region.
# 
# Parameters:
#   appConfig (dict): A dictionary containing the AWS region and other configuration settings.
#   metricsList (list): A list to store the retrieved metrics
#   clusterName (str): The name of the cluster
#   instanceName (str): The name of the instance
#   metricName (str): The name of the metric
#   metricType (str): The type of the metric (DBClusterIdentifier or DBInstanceIdentifier)
#
# Returns:
#   None
def getMetricData(appConfig, metricsList, clusterName, instanceName, metricName, metricType='DBInstanceIdentifier'):
    cloudwatch_client = boto3.client('cloudwatch',region_name=appConfig['region'])
    metricSeries = []
    nextToken = ''

    while True:
        dimensions = []
        if metricType == 'DBInstanceIdentifier':
            dimensions.append({'Name': metricType, 'Value': instanceName})
        else:
            dimensions.append({'Name': metricType, 'Value': clusterName})
        kwargs = {}
        kwargs['StartTime'] = appConfig['startTime']
        kwargs['EndTime'] = appConfig['endTime']
        
        kwargs['MetricDataQueries'] = [{
            "Id": "m1",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/DocDB",
                    "MetricName": metricName,
                    "Dimensions": dimensions
                    },
                "Period": appConfig['period'],
                "Stat": "Average"
            }
        }]

        if nextToken:
            kwargs['NextToken'] = nextToken

        response = cloudwatch_client.get_metric_data(**kwargs)
        
        if response['MetricDataResults'][0]['StatusCode'] == 'Complete':
            metricSeries.append(pandas.Series(response['MetricDataResults'][0]['Values']))
            break
        # get_metric_data response 'PartialData' requires client to call the function again with the 
        # NextToken value from the response. This enables CloudWatch to return back the subsequent set of data.
        elif response['MetricDataResults'][0]['StatusCode'] == 'PartialData':
            metricSeries.append(pandas.Series(response['MetricDataResults'][0]['Values']))
            nextToken = response['NextToken']
            continue
        elif response['MetricDataResults'][0]['StatusCode'] == 'InternalError':
            print("InternalError, exiting")
            sys.exit(1)
        elif response['MetricDataResults'][0]['StatusCode'] == 'Forbidden':
            print("Forbidden, exiting")
            sys.exit(1)
        else:
            print("Unknown StatusCode, exiting")
            sys.exit(1)
    
    metricValues = pandas.concat(metricSeries)

    metricDataRec = {'ClusterName': clusterName, 
                     'InstanceName': instanceName, 
                     'MetricName': metricName, 
                     'Min': metricValues.min(), 
                     'Max': metricValues.max(), 
                     'Mean': metricValues.mean(), 
                     'P99': metricValues.quantile(0.99), 
                     'Std': metricValues.std()}
    metricsList.append(metricDataRec)

# This is the main function that is called when the script is run. It parses command-line arguments,
# setup log file, and calls the other functions for collecting various metics.
#
# Parameters:
#   None
#
# Returns:
#   None
def main():
    parser = argparse.ArgumentParser(description='DocumentDB Metrics Collector')

    parser.add_argument('--region',required=True,type=str,help='AWS Region')
    parser.add_argument('--start-date',required=False,type=str,help='Start date for CloudWatch logs, format=YYYYMMDD')
    parser.add_argument('--end-date',required=False,type=str,help='End date for CloudWatch logs, format=YYYYMMDD')
    parser.add_argument('--log-file-name',required=True,type=str,help='Log file for CSV output')
    parser.add_argument('--log-level', required=False, type=str, default='INFO', help='Log level DEBUG or INFO default=INFO')
                        
    args = parser.parse_args()

    if (args.start_date is not None and args.end_date is None):
        print("Must provide --end-date when providing --start-date, exiting.")
        sys.exit(1)

    elif (args.start_date is None and args.end_date is not None):
        print("Must provide --start-date when providing --end-date, exiting.")
        sys.exit(1)

    if (args.start_date is None) and (args.end_date is None):
        # use last 14 days
        startWhen = datetime.now() - timedelta(days=14)
        endWhen = datetime.now()
    else:
        # use provided start/end dates
        startWhen = datetime.strptime(args.start_date, '%Y%m%d')
        endWhen = datetime.strptime(args.end_date, '%Y%m%d')
    
    if startWhen < datetime.now() - timedelta(days=62):
        period = 3600
    elif startWhen < datetime.now() - timedelta(days=14):
        period = 300
    else:
        period = 60    

    appConfig = {}
    appConfig['region'] = args.region
    appConfig['logFileName'] = args.log_file_name+'.csv'
    appConfig['startTime'] = startWhen
    appConfig['endTime'] = endWhen
    appConfig['period'] = period

    clusterDetailsDF = getClusterDetails(appConfig)
    clusterMetricsDF = getClusterMetrics(appConfig)
    instanceMetricsDF = getInstanceMetrics(appConfig)

    allMetricsDF = pandas.merge(clusterDetailsDF,instanceMetricsDF,on=['ClusterName','InstanceName'])
    allMetricsDF = pandas.concat([allMetricsDF, clusterMetricsDF], axis=0, ignore_index=False)
    floatCol = ['Min','Max','Mean','P99','Std']
    allMetricsDF[floatCol] = allMetricsDF[floatCol].astype(float).map('{:,.2f}'.format)
    allMetricsDF.sort_values(['ClusterName','InstanceName','MetricName']).to_csv(appConfig['logFileName'], encoding='utf-8', index=False, mode='a')

    print("")
    print("Created {} with CSV data".format(appConfig['logFileName']))
    print("Region: {}".format(appConfig['region']))
    print("Log start time: ", startWhen)
    print("Log end time: ", endWhen)
    print("")

if __name__ == "__main__":
   main()