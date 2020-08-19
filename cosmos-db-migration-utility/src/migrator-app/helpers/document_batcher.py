import random
import threading
import time
from datetime import datetime
import json
import os
from bson.json_util import dumps
from common.json_encoder import JSONFriendlyEncoder
from common.logger import get_logger
from common.timer import RepeatedTimer
from helpers.file_helper import FileHelper
from helpers.s3_helper import S3Helper

logger = get_logger(__name__)


class DocumentBatcher:
    def __init__(self, cluster_name, namespace, database_name, collection_name, dynamo_helper):
        self.__cluster_name = cluster_name
        self.__namespace = namespace
        self.__database_name = database_name
        self.__collection_name = collection_name
        self.__current_change = None
        self.__previous_change = None
        self.__resume_token = None
        self.__batch_id = 0
        self.__batch = []
        self.__timer = None
        self.__event = threading.Event()
        self.__fh = FileHelper()
        self.__dh = dynamo_helper

    def initialize(self, token):
        if token is not None:
            logger.info("Initializing the document batcher with token: %s", json.dumps(token, cls=JSONFriendlyEncoder))
            self.__batch_id = token["batch_id"] + 1 # use the next batch id
            self.__previous_change = json.loads(token["validation_document"])
            self.__resume_token = json.loads(token["resume_token"])
        self.__timer = RepeatedTimer(10, self.__on_time_elapsed)
        self.__timer.start()
        self.__event.set()

    def on_change_event(self, cluster_name, database_name, collection_name, change):
        # full_document = change["fullDocument"]
        # TODO: What are you doing with the clustrer_name and other input parameters
        self.__event.wait()
        self.__previous_change = self.__current_change
        self.__current_change = change
        self.__batch.append(change)

    def __on_time_elapsed(self):
        self.__event.clear()
        # TODO: control passed wait in on_change_event, but not appended yet.
        # poor man's hack to handle above scenario. sleep for upto 0.1 second
        time.sleep(random.uniform(0.01, 0.1))
        # TODO: Allow saving empty batch even to help track the heartbeats
        s3_key_name = "null"
        if len(self.__batch) > 0:
            s3_key_name = "{}/{}/{}/{}-batch-{:06.0f}.json".format(
                self.__cluster_name, self.__database_name, 
                self.__collection_name, self.__namespace, self.__batch_id)
            self.__write_to_s3(s3_key_name)
        self.__update_dynamodb(s3_key_name)
        self.__batch_id = self.__batch_id + 1
        self.__batch[:] = []
        self.__event.set()

    def __write_to_s3(self, s3_key_name):
        # TODO: handle any failures
        file_path = self.__create_local_batch_file()
        self.__upload_to_s3(file_path, s3_key_name)
        self.__fh.delete_file(file_path)

    def __update_dynamodb(self, s3_key_name):
        # TODO: handle any failures
        # TODO: do it in transactions
        # update watchers with namespace and current batch id, last token etc
        # insert change_events with namespace 
        timestamp = datetime.utcnow().isoformat()
        watcher_item = self.__get_watcher_item(timestamp)
        change_event_item = self.__get_change_event_item(s3_key_name, timestamp)
        self.__dh.save_watcher(watcher_item)
        self.__dh.save_change_event(change_event_item)

    def __get_watcher_item(self, timestamp):
        token = None
        if self.__previous_change is not None:
            token = self.__previous_change["_id"]
        else:
            token = self.__resume_token
        item = {
            "watcher_id": "{}::{}".format(self.__cluster_name, self.__namespace),
            "cluster_name": self.__cluster_name,
            "namespace": self.__namespace,
            "resume_token": dumps(token),
            "validation_document": dumps(self.__current_change),
            "batch_id": self.__batch_id,
            "document_count": len(self.__batch),
            "created_timestamp": timestamp}
        return item

    def __get_change_event_item(self, s3_link, timestamp):
        token = None
        if self.__previous_change is not None:
            # TODO: possibly ["_id"] even on resume token
            token = self.__previous_change["_id"]
        else:
            token = self.__resume_token
        item = {
            "watcher_id": "{}::{}".format(self.__cluster_name, self.__namespace),
            "batch_status": "{}::{:06.0f}".format("false", self.__batch_id),
            "cluster_name": self.__cluster_name,
            "namespace": self.__namespace,
            "batch_id": self.__batch_id,
            "s3_link": s3_link,
            "created_timestamp": timestamp,
            "document_count": len(self.__batch),
            "is_processed": False,
            "resume_token": dumps(token),
            "processed_timestamp": "9999-12-31T00:00:00.000000"}
        return item

    def __create_local_batch_file(self):
        lines = []
        for item in self.__batch:
            lines.append("{}\n".format(dumps(item["fullDocument"])))
        temp_file = self.__fh.create_file()
        with open(temp_file.name, 'w') as stream:
            stream.writelines(lines)
        return temp_file.name

    def __upload_to_s3(self, file_path, key_name):
        s3h = S3Helper()
        bucket_name = os.environ['S3_CHANGE_FEED_BUCKET_NAME']
        s3h.upload(file_path, bucket_name, key_name)

    def close(self):
        logger.info("Cleaning up the Document Batcher for namespace: %s", self.__namespace)
        if self.__timer is not None:
            self.__timer.stop()
            # wait until writing to s3/dynamo is done
            self.__event.wait()
