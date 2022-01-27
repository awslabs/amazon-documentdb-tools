import json
import os
import time

import boto3
from botocore.exceptions import ClientError

import add_secondarycluster

session = boto3.Session()
dynamodb = session.resource('dynamodb')
dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']


def update_item(global_cluster_id, primary_cluster_arn, secondary_clusters, current_state):
    table = dynamodb.Table(dynamodb_table_name)

    response = table.update_item(
        Key={
            'global_cluster_id': global_cluster_id,
            'target_cluster_arn': primary_cluster_arn
        },
        UpdateExpression="set current_state=:s, secondary_clusters=:sc",
        ExpressionAttributeValues={
            ':s': current_state,
            ':sc': secondary_clusters
        },
        ReturnValues="UPDATED_NEW"
    )
    return response


def put_item(global_cluster_id, primary_cluster_arn, secondary_clusters, current_state):
    table = dynamodb.Table(dynamodb_table_name)
    response = table.put_item(
        Item={
            'global_cluster_id': global_cluster_id,
            'target_cluster_arn': primary_cluster_arn,
            'secondary_clusters': secondary_clusters,
            'current_state': current_state
        }
    )
    return response


def get_item(global_cluster_id, primary_cluster_arn):
    try:
        table = dynamodb.Table(dynamodb_table_name)
        response = table.get_item(
            Key={'global_cluster_id': global_cluster_id, 'target_cluster_arn': primary_cluster_arn})
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    else:
        return response


def is_request_processed(event):
    try:
        record_value = get_item(event['global_cluster_id'], event['primary_cluster_arn'])
        secondary_clusters = event['secondary_clusters']
        is_processed = False
        if "Item" in record_value and "secondary_clusters" in record_value['Item']:
            for each_item in record_value['Item']['secondary_clusters']:
                region_ddb = each_item['region']
                secondary_cluster_id = each_item['secondary_cluster_id']
                for clusters in secondary_clusters:
                    if clusters['region'] == region_ddb and clusters['secondary_cluster_id'] == secondary_cluster_id:
                        is_processed = True
                        break
        else:
            is_processed = False

        return is_processed

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError


def lambda_handler(event, context):
    try:
        start_time = time.time()
        print('Started process to convert standalone cluster to global cluster')
        validate_input(event)
        print('User Input validation completed. Validating if the provided input was processed earlier to ensure '
              'idempotency')
        # Idempotency check to avoid duplicate processing
        if not is_request_processed(event):
            print('Idempotency check passed. The provided input will be recorded in DynamoDB table')
            # When triggered as a standalone lambda function, there will be no item in the DDB store. When triggered
            # via convert to local, there will an item inserted in the failover lambda function. To this record add
            # secondary clusters
            if "Item" in get_item(event['global_cluster_id'], event['primary_cluster_arn']):
                update_item(global_cluster_id=event['global_cluster_id'],
                            primary_cluster_arn=event['primary_cluster_arn'],
                            secondary_clusters=event['secondary_clusters'],
                            current_state='CONVERT_TO_GLOBAL_PROCESS_STARTED')
            else:
                put_item(global_cluster_id=event['global_cluster_id'], primary_cluster_arn=event['primary_cluster_arn'],
                         secondary_clusters=event['secondary_clusters'],
                         current_state='CONVERT_TO_GLOBAL_PROCESS_STARTED')

            print('User Input validation complete. Primary cluster ', event['primary_cluster_arn'],
                  ' will be converted to global cluster ', event['global_cluster_id'],
                  ' and the provided secondary clusters will be added to this global cluster')

            add_secondarycluster.convert_regional_to_global(primary_cluster_arn=event['primary_cluster_arn'],
                                                            global_cluster_id=event['global_cluster_id'],
                                                            secondary_clusters=event['secondary_clusters'])

            end_time = time.time()

            print('Completed process to convert standalone primary to global cluster in ',
                  end_time - start_time,
                  ' seconds. Recording state CONVERT_TO_GLOBAL_PROCESS_COMPLETED in DynamoDB table')

            update_item(global_cluster_id=event['global_cluster_id'], primary_cluster_arn=event['primary_cluster_arn'],
                        secondary_clusters=event['secondary_clusters'],
                        current_state='CONVERT_TO_GLOBAL_PROCESS_COMPLETED')
        else:
            print('Duplicate request received for the provided input. '
                  'The process will not perform conversion to global cluster. Returning status code 200...')
            return {
                'statusCode': 200,
                'body': json.dumps('Duplicate request received for the provided input. Skipping processing...')
            }

    except RuntimeError as e:
        update_item(global_cluster_id=event['global_cluster_id'], primary_cluster_arn=event['primary_cluster_arn'],
                    secondary_clusters=event['secondary_clusters'], current_state='CONVERT_TO_GLOBAL_PROCESS_ERRORED')

        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully converted to global cluster')
    }


def validate_input(event):
    try:
        if not event['primary_cluster_arn']:
            print('Primary Cluster ARN ', event['primary_cluster_arn'],
                  'is invalid. Please provide a valid ARN for the primary '
                  'cluster ')
            raise RuntimeError
        if not event['global_cluster_id']:
            print('Global Cluster Identifier ', event['global_cluster_id'], 'is invalid. Please provide a valid global '
                                                                            'cluster id')
            raise RuntimeError

        if not isinstance(event['secondary_clusters'], list):
            print('Secondary Clusters must be defined as an array. Please check input')
            raise RuntimeError

        if len(event['secondary_clusters']) == 0:
            print('No Secondary Clusters found in input. Please provide valid values for secondary cluster ')
            raise RuntimeError

    except KeyError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise KeyError
