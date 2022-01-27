import json
import os
import time

import boto3
from botocore.exceptions import ClientError

import failover_and_delete_global_cluster

session = boto3.Session()
dynamodb = session.resource('dynamodb')
dynamodb_table_name = os.environ['DYNAMODB_TABLE_NAME']


def update_item(global_cluster_id, secondary_cluster_arn, current_state):
    table = dynamodb.Table(dynamodb_table_name)

    response = table.update_item(
        Key={
            'global_cluster_id': global_cluster_id,
            'target_cluster_arn': secondary_cluster_arn
        },
        UpdateExpression="set current_state=:s",
        ExpressionAttributeValues={
            ':s': current_state
        },
        ReturnValues="UPDATED_NEW"
    )
    return response


def put_item(global_cluster_id, secondary_cluster_arn, primary_cluster_cname, hosted_zone_id, current_state):
    table = dynamodb.Table(dynamodb_table_name)
    response = table.put_item(
        Item={
            'global_cluster_id': global_cluster_id,
            'target_cluster_arn': secondary_cluster_arn,
            'primary_cluster_cname': primary_cluster_cname,
            'hosted_zone_id': hosted_zone_id,
            'current_state': current_state
        }
    )
    return response


def get_item(global_cluster_id, secondary_cluster_arn):
    try:
        table = dynamodb.Table(dynamodb_table_name)
        response = table.get_item(
            Key={'global_cluster_id': global_cluster_id, 'target_cluster_arn': secondary_cluster_arn})
    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    else:
        return response


def is_request_processed(event):
    try:
        record_value = get_item(event['global_cluster_id'], event['secondary_cluster_arn'])
        print('Record Value from DynamoDB table is ', record_value)
        return "Item" in record_value

    except ClientError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError


def lambda_handler(event, context):
    try:
        start_time = time.time()
        print('Started process to failover secondary cluster to standalone primary')
        validate_input(event)

        print('User Input validation completed. Validating if the provided input was processed earlier to ensure '
              'idempotency')

        # Idempotency check to avoid duplicate processing
        if not is_request_processed(event):
            print('Idempotency check passed. The provided input will be recorded in DynamoDB table')

            put_item(global_cluster_id=event['global_cluster_id'],
                     secondary_cluster_arn=event['secondary_cluster_arn'],
                     primary_cluster_cname=event['primary_cluster_cname'],
                     hosted_zone_id=event['hosted_zone_id'],
                     current_state="FAILOVER_PROCESS_STARTED"
                     )
            print('User Input validation complete. Secondary cluster ', event['secondary_cluster_arn'],
                  ' will be removed from global cluster ', event['global_cluster_id'], ' and the cname ',
                  event['primary_cluster_cname'], ' will be updated with promoted primary cluster endpoint')

            failover_and_delete_global_cluster.failover(global_cluster_id=event['global_cluster_id'],
                                                        secondary_cluster_arn=event['secondary_cluster_arn'],
                                                        primary_cluster_cname=event['primary_cluster_cname'],
                                                        hosted_zone_id=event['hosted_zone_id'],
                                                        is_delete_global_cluster=event['is_delete_global_cluster'])
            end_time = time.time()

            print('Completed process to failover secondary cluster to standalone primary in ',
                  end_time - start_time,
                  ' seconds. Recording state FAILOVER_PROCESS_COMPLETED in DynamoDB table')

            update_item(global_cluster_id=event['global_cluster_id'],
                        secondary_cluster_arn=event['secondary_cluster_arn'],
                        current_state="FAILOVER_PROCESS_COMPLETED")

        else:
            print('Duplicate request received for the provided input. The process will not perform failover. '
                  'Returning status code 200...')
            return {
                'statusCode': 200,
                'body': json.dumps('Duplicate request received for the provided input. Skipping processing...')
            }

    except RuntimeError as e:
        update_item(global_cluster_id=event['global_cluster_id'],
                    secondary_cluster_arn=event['secondary_cluster_arn'],
                    current_state="FAILOVER_PROCESS_ERRORED")
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise RuntimeError

    return {
        'statusCode': 200,
        'body': json.dumps('Successfully promoted cluster')
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
            print('Primary Cluster CNAME ', event['primary_cluster_cname'],
                  'is invalid. Please provide a valid CNAME to manage endpoint')
            raise RuntimeError

        if not isinstance(event['is_delete_global_cluster'], bool):
            print('Boolean value provided is_delete_global_cluster ', event['is_delete_global_cluster'],
                  'is invalid. Please provide a valid boolean for is_delete_global_cluster')
            raise RuntimeError


    except KeyError as e:
        print('ERROR OCCURRED WHILE PROCESSING: ', e)
        print('PROCESSING WILL STOP')
        raise KeyError
