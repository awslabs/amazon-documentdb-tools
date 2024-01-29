import logging
import os
import traceback
import copy
import boto3
import tempfile
import os
from bson.json_util import loads
from boto3.dynamodb.conditions import Key, Attr
from pymongo import ReplaceOne, MongoClient
from pymongo.errors import BulkWriteError
from random import randint
from datetime import datetime
from decimal import Decimal
import json

# the message is sent to queue
MESSAGE_DELAY_SECONDS = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class JSONFriendlyEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        if isinstance(o, datetime):
            return o.isoformat()
        return super(JSONFriendlyEncoder, self).default(o)

def get_tracker_value(cluster_name, key_name):
    client = boto3.resource('dynamodb')
    logger.info("Getting the value of the tracker cluster_name: %s, key: %s.", cluster_name, key_name)
    key = "{}::{}".format(cluster_name, key_name)
    response = client.Table("tracker").get_item(Key={"key": key})
    logger.info(response)
    value = response["Item"]["value"]
    logger.info("Successfully fetched the value of the tracker cluster_name: %s, key: %s.", cluster_name, json.dumps(value))
    return value

def send_sqs_message(queue_name, payload, delay=0):
    data = json.dumps(payload)
    logger.info("Starting to send SQS requests to queue: %s. Payload: %s", queue_name, data)
    sqs_client = boto3.client('sqs')
    queue = sqs_client.get_queue_url(QueueName=queue_name)
    response = sqs_client.send_message(
        QueueUrl= queue['QueueUrl'], MessageBody=data, DelaySeconds=delay)
    logger.info("Successfully completed sending SQS requests to queue: %s. Response: %s",
        queue_name, response)

def get_timestamp_gap_data(cluster_name):
    client = boto3.resource('dynamodb')
    logger.info("Fetching timestamp gap data for cluster_name: %s.", cluster_name)
    response = client.Table("time_gap").query(
        KeyConditionExpression=Key("cluster_name").eq(cluster_name)
    )
    max_gap = 0
    max_gap_timestamp = datetime(2020,1,1,0,0,0).isoformat()
    timestamp = datetime.utcnow().isoformat()
    items = []
    for item in response["Items"]:
        if item["time_gap_in_seconds"] > max_gap:
            max_gap = item["time_gap_in_seconds"]
            max_gap_timestamp = item["created_timestamp"]
        logger.info("Found timestamp gap data for cluster_name: %s. Item: %s.", cluster_name, json.dumps(item, cls=JSONFriendlyEncoder))
        items.append({
            "cluster_name": item["cluster_name"],
            "namespace": item["namespace"],
            "batch_id": item["batch_id"],
            "created_timestamp": item["created_timestamp"],
            "processed_timestamp": item["processed_timestamp"],
            "time_gap_in_seconds": item["time_gap_in_seconds"]
        })
    items = sorted(items, key = lambda i: i['time_gap_in_seconds'], reverse=True)
    result = {
        "cluster_name": cluster_name,
        "current_time": timestamp,
        "gap_in_seconds": max_gap,
        "details": json.dumps(items, cls=JSONFriendlyEncoder)
    }
    logger.info("Successfully completed computing the time gap for cluster name: %s. Result: %s", cluster_name, json.dumps(result, cls=JSONFriendlyEncoder))
    return result

def save_time_gap_data(cluster_name, data):
    client = boto3.resource('dynamodb')
    logger.info("Saving timestamp gap data for cluster_name: %s.", cluster_name)
    result = client.Table("migration_status").put_item(Item=data)
    logger.info("Successfully completed saving the time gap for cluster name: %s. Result: %s", cluster_name, json.dumps(result, cls=JSONFriendlyEncoder))
    return result

def process_request(payload):
    # check the status of the tracker.event_writer
    cluster_name = payload["cluster_name"]
    status = get_tracker_value(cluster_name, "event_writer")
    if status == "stop":
        return {
            "cluster_name": cluster_name,
            "message": "Detected the event_writer status as stop. Ignoring request for cluster."
        }
    data = {}
    # get the recently processed batch ids
    result = get_timestamp_gap_data(cluster_name)
    save_time_gap_data(cluster_name, result)
    # resend the SQS message after a delay
    send_sqs_message("gap-watch-request-queue", payload, MESSAGE_DELAY_SECONDS)
    return {
        "cluster_name": cluster_name,
        "message": "Successfully computed the timestamp difference for cluster_name: {}.".format(cluster_name),
        "delta": result
    }

def lambda_handler(event, context):
    logger.info("Lambda: gap-watch-request-reader was invoked with event: %s.", event)
    try:
        data = {}
        payload = {}
        first_one = True
        for record in event['Records']:
            body = record["body"]
            payload = json.loads(body)
            logger.info("Completed loading payload information from message: %s.", json.dumps(payload))
            if first_one:
                data = process_request(payload)
                first_one = False
            else:
                # process only one. send the payload back to the queue again
                # TODO: send only unique items
                logger.info("Found more than one message in the request. Resending it back to queue. Payload: %s.", json.dumps(payload))
                send_sqs_message("gap-watch-request-queue", payload, randint(0,5))
        logging.info("Successfully completed processing gap-watch-request-reader sqs messages. Result: %s.", json.dumps(data, cls=JSONFriendlyEncoder))
        return data  
    except Exception as e:
        stack_trace = traceback.format_stack()
        data = {
            "status": "error",
            "input": payload,
            "output": {
                "operation": "gap-watch-request-reader",
                "error": str(e),
                "stack_trace": stack_trace
            }
        }
        logging.error("Failed while processing gap-watch-request-reader messages. Result: %s.", json.dumps(data, cls=JSONFriendlyEncoder), exc_info=True)
        raise