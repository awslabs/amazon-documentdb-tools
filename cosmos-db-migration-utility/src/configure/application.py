import boto3
import json
from time import sleep
from common.logger import get_logger
from boto3.dynamodb.conditions import Key
from json_encoder import JSONFriendlyEncoder

logger = get_logger(__name__)

class Application:

    def __init__(self, cluster_name):
        self.__cluster_name = cluster_name

    def __update_secret_value(self, key, value):
        client = boto3.client('secretsmanager')
        try:
            response = client.create_secret(
                    Name=key,
                    SecretString=value
                )
            return response
        except Exception as e:
            if "ResourceExistsException" in str(e):
                response = client.update_secret(
                    SecretId=key,
                    SecretString=value
                )
                return response
            else:
                raise

    def set_connection_string(self, connection_string):
        logger.info("Setting the connection string for the cluster_name: %s.", self.__cluster_name)
        self.__update_secret_value("migrator-app/{}".format(self.__cluster_name), connection_string)
        logger.info("Successfully completed setting the connection string for the cluster_name: %s. Connection string: %s", self.__cluster_name, connection_string)
        self.set_event_writer("stop")

    def set_event_writer(self, status):
        logger.info("Setting the event writer status as %s", status)
        payload = {"cluster_name": self.__cluster_name, "component":"event_writer", "operation": status}
        self.__send_message("app-request-queue", payload)
        # TODO: What aobut the gap-watcher. It should be in configure
        # not in the migrator app
        logger.info("Successfully completed setting the event writer status as %s", status)

    def __send_message(self, queue_name, payload):
        data = json.dumps(payload)
        logger.info("Starting to send SQS requests to queue: %s. Payload: %s", queue_name, data)
        sqs_client = boto3.client('sqs')
        queue = sqs_client.get_queue_url(QueueName=queue_name)
        response = sqs_client.send_message(
            QueueUrl= queue['QueueUrl'], MessageBody=data)
        logger.info("Successfully completed sending SQS requests to queue: %s. Response: %s",
            queue_name, response)

    def print_status(self):
        client = boto3.resource('dynamodb')
        response = client.Table("migration_status").get_item(Key={
            "cluster_name": self.__cluster_name })
        logger.debug("Successfully completed getting the migration_status for the cluster_name: %s.", self.__cluster_name)
        if "Item" in response:
            response["Item"]["details"] = json.loads(response["Item"]["details"])
            logger.info("Status: %s", json.dumps(response["Item"], cls=JSONFriendlyEncoder, indent=1))
        else:
            logger.info("Status: Not available yet. Did you start the migration?")
    
    def watch_status(self):
        while True:
            self.print_status()
            sleep(5)