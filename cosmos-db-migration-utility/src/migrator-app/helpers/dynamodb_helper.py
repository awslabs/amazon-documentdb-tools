from decimal import Decimal
import boto3
from bson.json_util import dumps
import json
from common.Singleton import Singleton
from common.logger import get_logger
from common.json_encoder import JSONFriendlyEncoder

logger = get_logger(__name__)


class DynamodbHelper(metaclass=Singleton):
    def __init__(self, cluster_name):
        self.__client = boto3.resource('dynamodb')
        self.__cluster_name = cluster_name

    def save_namespaces(self, database_collections):
        for database_name in database_collections:
            logger.info("database: %s, collections: %s", database_name, dumps(database_collections[database_name]))
            for collection_name in database_collections[database_name]:
                self.__client.Table("namespaces").put_item(Item={
                    "cluster_name": self.__cluster_name,
                    "namespace": "{}.{}".format(database_name, collection_name),
                    "database_name": database_name,
                    "collection_name": collection_name})

    def save_change_event(self, data):
        watcher_id = "{}::{}".format(data["cluster_name"], data["namespace"])
        batch_status = "{}::{:06.0f}".format(str(data["is_processed"]).lower(), data["batch_id"])
        logger.info("About to save change event. watcher_id: %s and batch_status: %s", watcher_id, batch_status)
        change_event = {
            "watcher_id": watcher_id,
            "batch_status": batch_status,
            "cluster_name": data["cluster_name"],
            "namespace": data["namespace"],
            "batch_id": Decimal(data["batch_id"]),
            "s3_link": data["s3_link"],
            "created_timestamp": data["created_timestamp"],
            "document_count": Decimal(data["document_count"]),
            "is_processed": data["is_processed"],
            "resume_token": data["resume_token"],
            "processed_timestamp": data["processed_timestamp"]}
        result = self.__client.Table("change_events").put_item(Item=change_event)
        logger.info("Successfully saved the change event. watcher_id: %s and batch_status: %s. change_event: %s", 
            watcher_id, batch_status, json.dumps(change_event, cls=JSONFriendlyEncoder))
        return result

    def get_watcher(self, namespace):
        watcher_id = "{}::{}".format(self.__cluster_name, namespace)
        logger.info("Getting the watcher item by id: %s", watcher_id)
        response = self.__client.Table("watchers").get_item(
            Key={"watcher_id": watcher_id})
        watcher = None
        if "Item" in response:
            watcher = response['Item']
            watcher["batch_id"] = float(watcher["batch_id"])
            watcher["total_count"] = float(watcher["total_count"])
        logger.info("Successfully found the watcher item for id: %s. Item: %s", watcher_id, dumps(watcher))
        return watcher

    def save_watcher(self, data):
        result = self.__client.Table("watchers").update_item(
            Key={"watcher_id": data["watcher_id"]}, # <--- changed
            UpdateExpression="SET cluster_name = :cn, namespace = :n, resume_token = :t, validation_document = :v, batch_id = :b, #total_count = if_not_exists(#total_count, :initial) + :dc, created_timestamp = :ts",
            ExpressionAttributeNames={'#total_count': 'total_count'},
            ExpressionAttributeValues={
                ":cn": data["cluster_name"],
                ":n": data["namespace"],
                ":t": data["resume_token"], # <--- changed
                ":v": data["validation_document"],
                ":b": Decimal(data["batch_id"]),
                ":dc": Decimal(data["document_count"]),
                ":initial": 0,
                ":ts": data["created_timestamp"] # <--- changed
            },
            ReturnValues="UPDATED_NEW")
        return result