import logging
import os
import json
import traceback
from datetime import datetime
import copy
import boto3
from boto3.dynamodb.conditions import Key, Attr
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def send_sqs_message(queue_name, payload, delay=0):
    data = json.dumps(payload)
    logger.info("Starting to send SQS requests to queue: %s. Payload: %s", queue_name, data)
    sqs_client = boto3.client('sqs')
    queue = sqs_client.get_queue_url(QueueName=queue_name)
    response = sqs_client.send_message(
        QueueUrl= queue['QueueUrl'], MessageBody=data, DelaySeconds=delay)
    logger.info("Successfully completed sending SQS requests to queue: %s. Response: %s",
        queue_name, response)

def update_tracker_value(cluster_name, key_name, value):
    client = boto3.resource('dynamodb')
    logger.info("Setting the tracker key: %s with value: %s on cluster_name: %s", key_name, value, cluster_name)
    key = "{}::{}".format(cluster_name, key_name)
    result = client.Table("tracker").put_item(Item=
        {"key": key, "key_name": key_name, "value": value, "cluster_name": cluster_name})
    logger.info("Successfully completed setting the tracker key: %s with value: %s on cluster_name: %s", key_name, value, cluster_name)
    return result

def get_all_namespaces(cluster_name):
    logger.info("Getting all the namespaces from the DynamoDB Table.")
    client = boto3.resource('dynamodb')
    response = client.Table("namespaces").query(
        KeyConditionExpression=Key("cluster_name").eq(cluster_name)
    )
    items = []
    for item in response["Items"]:
        items.append(item)
    while 'LastEvaluatedKey' in response:
        logger.info("Paging until we got all namespaces from the DynamoDB Table.")
        response = client.Table("namespaces").query(
            KeyConditionExpression=Key("cluster_name").eq(cluster_name),
            ExclusiveStartKey=response['LastEvaluatedKey'])
        for item in response["Items"]:
            items.append(item)
    
    logger.info("Successfully found all the namespaces. %s.", json.dumps(items))
    return items

def start_event_writer(cluster_name):
    logger.info("Updating the cluster: %s to start the event_writer.", cluster_name)
    update_tracker_value(cluster_name, "event_writer", "start")
    # for each namespace send read batch request queue
    namespaces = get_all_namespaces(cluster_name)
    for item in namespaces:
        # send sqs message
        payload = { "cluster_name": cluster_name, "namespace": item["namespace"] }
        message = send_sqs_message("read-batch-request-queue", payload)
    logger.info("Successfully completed starting the event writer on cluster: %s.", cluster_name)
    send_sqs_message("gap-watch-request-queue", { "cluster_name": cluster_name})

def stop_event_writer(cluster_name):
    logger.info("Updating the cluster: %s to stop the event_writer.", cluster_name)
    update_tracker_value(cluster_name, "event_writer", "stop")
    logger.info("Successfully updated the tracker with event_writer set to stop.")

def process_request(payload):
    cluster_name = payload["cluster_name"]
    component = payload["component"]
    operation = payload["operation"]
    if component == "event_writer":
        if operation == "start":
            start_event_writer(cluster_name)
            return {
                "cluster_name": cluster_name,
                "component": component,
                "status": "started"
            }
        else:
            stop_event_writer(cluster_name)
            return {
                "cluster_name": cluster_name,
                "component": component,
                "status": "stopped"
            }
    else:
        return "Unsupported component detected. Request: {}".format(json.dumps(payload))

def lambda_handler(event, context):
    logger.info("Lambda: app-request-reader was invoked with event: %s.", event)
    try:
        response = {}
        payload = {}
        first_one = True
        for record in event['Records']:
            payload = record["body"]
            request = json.loads(payload)
            logger.info("Completed loading payload information from message: %s.", json.dumps(request))
            if first_one:
                response = process_request(request)
                first_one = False
            else:
                # process only one. send the payload back to the queue again
                logger.info("Found more than one message in the request. Resending it back to queue. Payload: %s.", json.dumps(request))
                send_sqs_message("app-request-queue", request)
        data = {
            'statusCode': 200,
            'input': payload,
            'output': {
                "operation": "app-request-reader",
                "data": response
            }
        }
        logging.info("Successfully completed processing app-request-reader sqs messages. Result: %s.", json.dumps(data))
        return data   
    except Exception as e:
        stack_trace = traceback.format_stack()
        data = {
            "status": "error",
            "input": payload,
            "output": {
                "operation": "app-request-reader",
                "error": str(e),
                "stack_trace": stack_trace
            }
        }
        logging.error("Failed while processing app-requets-reader messages. Result: %s.", json.dumps(data))
        raise