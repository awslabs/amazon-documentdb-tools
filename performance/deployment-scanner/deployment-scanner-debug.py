#!/usr/bin/env python3
 
import boto3
import datetime
import argparse
import requests
import json
import sys
import os


def get_docdb_instance_based_clusters(appConfig):
    client = boto3.client('docdb',region_name=appConfig['region'])
    
    response = client.describe_db_clusters(Filters=[{'Name': 'engine','Values': ['docdb']}])
    
    for thisCluster in response['DBClusters']:
        if thisCluster['DBClusterIdentifier'] == appConfig['clusterName']:
            if 'StorageType' in thisCluster:
                print("StorageType is {}".format(thisCluster['StorageType']))
            else:
                print("StorageType not present in describe_db_clusters() output")
    
    client.close()
    
    
def main():
    parser = argparse.ArgumentParser(description='DocumentDB Deployment Scanner')

    parser.add_argument('--region',required=True,type=str,help='AWS Region')
    parser.add_argument('--cluster-name',required=True,type=str,help='name of the cluster')
                        
    args = parser.parse_args()
   
    appConfig = {}
    appConfig['region'] = args.region
    appConfig['clusterName'] = args.cluster_name

    clusterList = get_docdb_instance_based_clusters(appConfig)
    

if __name__ == "__main__":
    main()
