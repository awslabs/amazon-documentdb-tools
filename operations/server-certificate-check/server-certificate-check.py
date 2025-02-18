#!/usr/bin/env python3
 
import boto3
import datetime
import argparse
import requests
import json
import sys
import os


def deleteLog(appConfig):
    if os.path.exists(appConfig['logFileName']):
        os.remove(appConfig['logFileName'])


def printLog(thisMessage,appConfig):
    print("{}".format(thisMessage))
    with open(appConfig['logFileName'], 'a') as fp:
        fp.write("{}\n".format(thisMessage))


def scan_clusters(appConfig):
    client = boto3.client('docdb',region_name=appConfig['region'])
    
    response = client.describe_db_clusters(Filters=[{'Name': 'engine','Values': ['docdb']}])

    printLog("{:<30} | {:<30} | {:<25} | {:<20}".format("cluster-name","instance-name","server-cert-expire","server-maint-window"),appConfig)
    
    for thisCluster in response['DBClusters']:
        thisClusterName = thisCluster['DBClusterIdentifier']
        if appConfig['clusterFilter'] is None or appConfig['clusterFilter'].upper() in thisClusterName.upper():
            for thisInstance in thisCluster['DBClusterMembers']:
                thisInstanceName = thisInstance['DBInstanceIdentifier']
                if appConfig['instanceFilter'] is None or appConfig['instanceFilter'].upper() in thisInstanceName.upper():
                    responseInstance = client.describe_db_instances(DBInstanceIdentifier=thisInstanceName)
                    validTill = responseInstance['DBInstances'][0]['CertificateDetails']['ValidTill']
                    preferredMaintenanceWindow = responseInstance['DBInstances'][0]['PreferredMaintenanceWindow']
                    printLog("{:<30} | {:<30} | {} | {:<20}".format(thisClusterName,thisInstanceName,validTill,preferredMaintenanceWindow),appConfig)
    
    client.close()
    

def main():
    parser = argparse.ArgumentParser(description='DocumentDB Server Certificate Checker')

    parser.add_argument('--region',required=True,type=str,help='AWS Region')
    parser.add_argument('--cluster-filter',required=False,type=str,help='Cluster name filter (substring match)')
    parser.add_argument('--instance-filter',required=False,type=str,help='Instance name filter (substring match)')
    parser.add_argument('--log-file-name',required=True,type=str,help='Log file name')
                        
    args = parser.parse_args()
   
    appConfig = {}
    appConfig['region'] = args.region
    appConfig['logFileName'] = args.log_file_name
    appConfig['clusterFilter'] = args.cluster_filter
    appConfig['instanceFilter'] = args.instance_filter

    deleteLog(appConfig)
    scan_clusters(appConfig)
    
    print("")
    print("Created {} with results".format(appConfig['logFileName']))
    print("")


if __name__ == "__main__":
    main()
