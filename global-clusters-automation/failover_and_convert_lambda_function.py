import json
import os
import time

import boto3
from botocore.exceptions import ClientError

from failover_and_convert_to_global import get_global_cluster_members, prepare_to_convert

"""
This function is dependent on failoverToSecondary and convertRegionalClusterToGlobal lambda functions. 
The ARN for these lambda functions will be retrieved from the environment variables FAILOVER_FUNCTION and CONVERT_TO_GLOBAL_FUNCTION.
The Cloud Formation template will set the environment variables. 
"""

# Define the client to interact with AWS Lambda
session = boto3.Session()
client = session.client('lambda')
dynamodb = session.resource('dynamodb')


def get_current_state(global_cluster_id, target_cluster_arn):
    try:
        dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']
        table = dynamodb.Table(dynamodb_table_name)
        response = table.get_item(
            Key={'global_cluster_id': global_cluster_id, 'target_cluster_arn': target_cluster_arn})
        if "Item" in response:
            current_state = response['Item']['current_state']
        else:
            current_state = ""

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    return current_state


def lambda_handler(event, context):
    try:
        # For BCP scenario, we always delete the old global cluster and create a new one with new primary as
        # indicated by the input Before initiating failover, ensure that the function is able to create requests for
        # converting back to global
        event['is_delete_global_cluster'] = True

        print('Started process to failover and convert standalone cluster to global cluster')

        # Before initiating failover, ensure that the function is able to create requests for converting back to global
        validate_input(event)

        print('Getting global cluster members for global cluster ', event['global_cluster_id'])
        global_cluster_members = get_global_cluster_members(global_cluster_id=event['global_cluster_id'])
        print('Begin process to create request to convert regional cluster to global cluster ')
        convert_to_global_request = prepare_to_convert(global_cluster_members,
                                                       global_cluster_id=event['global_cluster_id'],
                                                       secondary_cluster_arn=event['secondary_cluster_arn'])
        print('Created request to convert back to global cluster.')
        print('Starting process to failover')
        failover_function = os.environ['FAILOVER_FUNCTION']
        response_from_lambda1 = client.invoke(
            FunctionName=failover_function,
            InvocationType='RequestResponse',
            Payload=json.dumps(event)
        )

        response_from_failover_to_secondary = json.load(response_from_lambda1['Payload'])

        print('Failover process completed with response ', response_from_failover_to_secondary)

        if response_from_failover_to_secondary['statusCode'] == 200:
            """
            Check status in DynamoDB table to verify if the failover process completed. Lambda can be run multiple times 
            and duplicate requests process faster than original. The below check is to ensure that the convert to 
            regional cluster is called only after failover is completed. The initial value for current state is set in 
            DynamoDB as FAILOVER_PROCESS_STARTED
            """
            failover_process_current_state = "FAILOVER_PROCESS_STARTED"
            while failover_process_current_state != "FAILOVER_PROCESS_COMPLETED":
                print('Waiting for failover process to complete...')
                failover_process_current_state = get_current_state(global_cluster_id=event['global_cluster_id'],
                                                                   target_cluster_arn=event['secondary_cluster_arn']
                                                                   )
                if failover_process_current_state == 'FAILOVER_PROCESS_ERRORED':
                    print('ERROR OCCURRED during Failover process.')
                    raise RuntimeError

                time.sleep(5)

            print('Starting process to convert to global cluster')
            convert_to_global_function = os.environ['CONVERT_TO_GLOBAL_FUNCTION']
            response_from_lambda2 = client.invoke(
                FunctionName=convert_to_global_function,
                InvocationType='RequestResponse',
                Payload=json.dumps(convert_to_global_request)
            )
            response_from_convert_to_global = json.load(response_from_lambda2['Payload'])

            print('Convert to global cluster process completed with response ', response_from_convert_to_global)

            if response_from_convert_to_global['statusCode'] == 200:

                while failover_process_current_state != "CONVERT_TO_GLOBAL_PROCESS_COMPLETED":
                    print('Waiting for convert to global process to complete...')
                    failover_process_current_state = get_current_state(global_cluster_id=event['global_cluster_id'],
                                                                       target_cluster_arn=event[
                                                                           'secondary_cluster_arn'])

                    if failover_process_current_state == 'CONVERT_TO_GLOBAL_PROCESS_ERRORED':
                        print('ERROR OCCURRED during Convert to Global process.')
                        raise RuntimeError

                    time.sleep(2)

                print("SUCCESS: Completed failover and conversion to global cluster ")

            else:

                print('ERROR OCCURRED. Response from convert_to_global lambda function is ',
                      response_from_convert_to_global)
                raise RuntimeError

        else:

            print('ERROR OCCURRED. Response from failover_to_secondary lambda function is ',
                  response_from_failover_to_secondary)
            raise RuntimeError

    except RuntimeError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully failover and converted to global cluster')
    }


def validate_input(event):
    try:
        if not event['secondary_cluster_arn']:
            print('Secondary cluster ARN', event['secondary_cluster_arn'],
                  'is invalid. Please provide a valid secondary cluster ARN ')
            raise RuntimeError

        if not event['global_cluster_id']:
            print('Global Cluster Identifier ', event['global_cluster_id'], 'is invalid. Please provide a valid global '
                                                                            'cluster id')
            raise RuntimeError

        if not event['hosted_zone_id']:
            print('Hosted zone id ', event['hosted_zone_id'], 'is invalid. Please provide a valid hosted zone id')
            raise RuntimeError

        if not event['primary_cluster_cname']:
            print('Primary Cluster CNAME ', event['primary_cluster_cname'], 'is invalid. Please provide a valid CNAME '
                                                                            'to manage endpoint')
            raise RuntimeError
    except KeyError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise KeyError
