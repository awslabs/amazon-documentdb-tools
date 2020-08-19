import logging
import os
import json
import traceback
from datetime import datetime
import copy
import boto3
import tempfile
import dateutil.parser
import copy 
from random import randint
from bson.json_util import loads, dumps
from decimal import Decimal
from boto3.dynamodb.conditions import Key, Attr
from pymongo import ReplaceOne, MongoClient
from pymongo.errors import BulkWriteError

# the message is sent to queue
MESSAGE_DELAY_SECONDS = 10

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def get_tracker_value(cluster_name, key_name):
    client = boto3.resource('dynamodb')
    logger.info("Getting the value of the tracker key: %s on cluster_name: %s.", key_name, cluster_name)
    key = "{}::{}".format(cluster_name, key_name)
    response = client.Table("tracker").get_item(Key={"key": key})
    logger.info(response)
    value = response["Item"]["value"]
    logger.info("Successfully fetched the value of the tracker key: %s on cluster_name: %s. Value: %s", key_name, cluster_name, json.dumps(value))
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

def get_unprocessed_batch_id(cluster_name, namespace):
    client = boto3.resource('dynamodb')
    watcher_id = "{}::{}".format(cluster_name, namespace)
    logger.info("Fetching an unprocessed batch for watcher_id: %s.", watcher_id)
    response = client.Table("change_events").query(
        KeyConditionExpression=Key("watcher_id").eq(watcher_id) & Key("batch_status").begins_with("false"),
        Limit=1
    )
    for item in response["Items"]:
        item["batch_id"] = float(item["batch_id"])
        item["document_count"] = float(item["document_count"])
        logger.info("Found an unprocessed batch for watcher_id: %s. Item: %s", watcher_id, item)
        return item
    return None

def download_s3(bucket_name, key_name):
    temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
    s3 = boto3.client('s3')
    logger.info("Starting to download s3 file - bucket_name: %s, key_name: %s to local file: %s", 
        bucket_name, key_name, temp_file.name)
    s3.download_file(Bucket=bucket_name, Key=key_name,Filename=temp_file.name)
    logger.info("Successfully downloaded s3 contents into file: %s", temp_file.name)
    return temp_file.name

def get_data_from_s3(s3_file):
    bucket_name = os.environ['S3_BUCKET_NAME']
    file_path = download_s3(bucket_name, s3_file)
    items = []
    logger.info("Reading the S3 file and converting line by line to json format.")
    with open(file_path, 'r') as f:
        for line in f: 
            item = loads(line)
            items.append(item)
    logger.info("Completed converting the lines into the json format.")
    os.unlink(file_path)
    logger.info("Completed deleting temp file: %s", file_path)
    return items

def update_batch_as_processed(cluster_name, namespace, batch_id):
    timestamp = datetime.utcnow().isoformat()
    client = boto3.resource("dynamodb")
    watcher_id = "{}::{}".format(cluster_name, namespace)
    batch_status = "{}::{:06.0f}".format("false", batch_id)
    logger.info("About to update change_events for watcher_id: %s and batch_status %s as processed.", watcher_id, batch_status)
    response = client.Table("change_events").get_item(
        Key={
            "watcher_id": watcher_id,
            "batch_status": batch_status,
            })
    if not "Item" in response:
        logger.warn("Did not find change_events for watcher_id: %s and batch_status %s as processed.", watcher_id, batch_status)
        return None
    copy_item = copy.deepcopy(response["Item"])
    copy_item["watcher_id"] = watcher_id
    copy_item["batch_status"] = "{}::{:06.0f}".format("true", batch_id)
    copy_item["processed_timestamp"] = timestamp
    copy_item["is_processed"] = True
    # DynamoDB doesn't support update on sort key. So delete and insert back with new values
    result = client.Table("change_events").delete_item(Key={
            "watcher_id": watcher_id,
            "batch_status": batch_status,
            })
    logger.info("delete_item result: %s.", json.dumps(result))
    # TODO: always returns 200 even if item doesn;t exists
    # TODO: Do delete and put in transaction.
    if result["ResponseMetadata"]["HTTPStatusCode"] == 200:
        data = client.Table("change_events").put_item(Item = copy_item)
    logger.info("Successfully completed updating change_events for watcher_id: %s and batch_status %s as processed.", watcher_id, batch_status)
    return copy_item

def get_secret_value(key):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=key)
    return response["SecretString"]

def get_cluster_connection_string(cluster_name):
    client = boto3.resource('dynamodb')
    logger.info("Getting the connection string of the cluster_name: %s.", cluster_name)
    connection_string = get_secret_value("migrator-app/{}".format(cluster_name))
    logger.info("Successfully fetched the connection string of the cluster_name: %s. Connection string: %s", cluster_name, connection_string)
    return connection_string

def bulk_write_data_to_document_db(cluster_name, namespace, data):
    names = namespace.split(".")
    database_name = names[0]
    collection_name = ".".join(names[1::])
    retry_count = 0
    while retry_count < 2:
        try:
            return bulk_write_data_to_document_db_internal(cluster_name, namespace, database_name, collection_name, data)            
        except Exception as ex:
            retry_count = retry_count + 1
            if retry_count == 2:
                logger.fatal("Bulk write operation failed twice for cluster_name: %s, namespace: %s.", cluster_name, namespace, exc_info=True)
                raise
            
def bulk_write_data_to_document_db_internal(cluster_name, namespace, database_name, collection_name, data):
    logger.info("About to apply %d bulk operations on the namespace: %s ", len(data), namespace)
    connection_string = get_cluster_connection_string(cluster_name)
    bulk_ops = []
    for item in data:
        op = ReplaceOne({"_id": item["_id"]}, item, upsert=True)
        bulk_ops.append(op)
    logger.info("Completed creating the %d replace_one bulk operations for namespace: %s ", len(data), namespace)
    try:
        with MongoClient(connection_string) as client:
            collection = client.get_database(database_name).get_collection(collection_name)
            result = collection.bulk_write(bulk_ops)
            logger.info("Successfully wrote %d documents to namespace %s on Document DB.", len(data), namespace)
        return True
    except BulkWriteError as bwe:
        if 'writeErrors' in bwe:
            error_count = len(bwe['writeErrors'])
            dupe_count = len(filter(lambda we: "E11000 duplicate key error" in we["errmsg"], bwe['writeErrors']))
            if error_count == dupe_count:
                logger.info("Ignoring the duplicate key errors while writing on cluster: %s, namespace: %s", cluster_name, namespace)
                return True
        # TODO: have a retry logic BulkWriteError: batch op errors occurred
        logger.exception("Exception while doing bulk operations. %s", bwe.details, exc_info=True)
        raise

def update_timestamp_delta(cluster_name, namespace, batch):
    if batch is None:
        return
    logger.info("About to update time_gap for cluster: %s with namespace %s. Batch: %s", cluster_name, namespace, json.dumps(batch, default=decimal_default))
    # logger.info(batch)
    created_timestamp = dateutil.parser.parse(batch["created_timestamp"])
    processed_timestamp = dateutil.parser.parse(batch["processed_timestamp"])
    time_gap_seconds = Decimal(round((processed_timestamp - created_timestamp).total_seconds(), 0))
    logger.info("Computed the timegap for the cluster_name: %s, namespace: %s as gap: %f", cluster_name, namespace, time_gap_seconds)
    client = boto3.resource("dynamodb")
    result = client.Table("time_gap").update_item(
        Key={
            "cluster_name": cluster_name,
            "namespace": namespace},
        UpdateExpression="SET batch_id= :b, created_timestamp = :c, processed_timestamp = :p, time_gap_in_seconds = :g",
        ExpressionAttributeValues={
            ":b": batch["batch_id"],
            ":c": batch["created_timestamp"],
            ":p": batch["processed_timestamp"],
            ":g": time_gap_seconds},
        ReturnValues="UPDATED_NEW")
    logger.info("Successfully completed updating time_gap for  cluster_name: %s, namespace: %s. Result: %s", 
        cluster_name, namespace, json.dumps(result["Attributes"], default=decimal_default))
    logger.info(result["Attributes"])
    return result

def process_request(request):
    namespace = request["namespace"]
    cluster_name = request["cluster_name"]
    payload = { "cluster_name": cluster_name, "namespace": namespace }
    # check the status of the tracker.event_writer
    status = get_tracker_value(cluster_name, "event_writer")
    if status == "stop":
        return {
            "cluster_name": cluster_name, 
            "namespace": namespace,
            "message": "Detected the event writer status as stop. Ignoring request for namespace"
        }
    data = {}
    item = get_unprocessed_batch_id(cluster_name, namespace)
    if item is None:
        logger.info("All batches for cluster_name: %s. namespace: %s are processed. Waiting for others namespaces to catch up", cluster_name, namespace)
        # processed em all. send delayed sqs request again
        send_sqs_message("read-batch-request-queue", payload, MESSAGE_DELAY_SECONDS)
        return {
            "cluster_name": cluster_name, 
            "namespace": namespace,
            "message": "Didn't find unprocessed batch. Retry after {} seconds".format(MESSAGE_DELAY_SECONDS)
        }
    data = []
    if item["document_count"] == float(0):
        logger.info("Zero documents were found in current batch: %f for cluster_name: %s. namespace: %s are processed. Waiting for others namespaces to catch up", item["batch_id"], cluster_name, namespace)
        mark_processed_send_sqs(cluster_name, namespace, item, payload)
    else:
        data = get_data_from_s3(item["s3_link"])
        # bulk write to document db
        write_result = bulk_write_data_to_document_db(cluster_name, namespace, data)
        if write_result:
            mark_processed_send_sqs(cluster_name, namespace, item, payload)
        else:
            logger.fatal("Bulk write operation failed for some reason. Needs more analysis")
    return {
        "cluster_name": cluster_name, 
        "namespace": namespace,
        "message": "Successfully imported batch {} containing {} items into namespace: {}.".format(item["batch_id"], len(data), namespace)
    }

def mark_processed_send_sqs(cluster_name, namespace, item, payload):
    # update the document as processed
    batch = update_batch_as_processed(cluster_name, namespace, item["batch_id"])
    # update the tracker entry with timestamp delta
    update_timestamp_delta(cluster_name, namespace, batch)
    # post one more 
    send_sqs_message("read-batch-request-queue", payload)


def lambda_handler(event, context):
    logger.info("Lambda: batch-request-reader was invoked with event: %s.", event)
    try:
        data = {}
        payload = {}
        first_one = True
        for record in event['Records']:
            payload = record["body"]
            request = json.loads(payload)
            # TODO: in some weird case scenarios I am receiving a string even after loading
            if type(request) == str:
                request = json.loads(request)
            logger.info("Completed loading payload information from message: %s.", json.dumps(request))
            if first_one:
                data = process_request(request)
                first_one = False
            else:
                # process only one. send the payload back to the queue again
                logger.info("Found more than one message in the request. Resending it back to queue. Request: %s.", json.dumps(request))
                send_sqs_message("read-batch-request-queue", request, randint(0,5))
        logging.info("Successfully completed processing batch-request-reader sqs messages. Result: %s.", json.dumps(data))
        return data  
    except Exception as e:
        stack_trace = traceback.format_stack()
        data = {
            "status": "error",
            "input": payload,
            "output": {
                "operation": "batch-request-reader",
                "error": str(e),
                "stack_trace": stack_trace
            }
        }
        logging.error("Failed while processing batch-request-reader messages. Result: %s.", json.dumps(data))
        raise