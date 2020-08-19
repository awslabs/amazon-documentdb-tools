import sys
import bson
import bson.json_util
from bson.json_util import dumps
import pymongo
from common.logger import get_logger
from common.json_encoder import JSONFriendlyEncoder
logger = get_logger(__name__)


# noinspection PyBroadException
class CollectionMigrator:
    def __init__(self, client, cluster_name, database_name, collection_name):
        self.__client = client
        self.__cluster_name = cluster_name
        self.__database_name = database_name
        self.__collection_name = collection_name
        self.__namespace = "{}.{}".format(self.__database_name, self.__collection_name)
        self.__pipeline = [
            {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}},
            {"$project": {"_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1}}
            # project cannot do "operationType": 1,
        ]
        self.__cursor = None

    def peek(self):
        logger.info("Peeking into the namespace: %s", self.__namespace)
        data = {
            "namespace": self.__namespace,
            "token": "",
            "validation_document": ""
        }
        try:
            db = self.__client.get_database(self.__database_name)
            # get a change stream object
            self.__cursor = db.get_collection(self.__collection_name
                ).watch(pipeline=self.__pipeline, full_document="updateLookup")

            logger.info("Watching for two changes on the namespace: %s", self.__namespace)
            # Prints all documents in the collection
            with self.__cursor as cursor:
                for change in cursor:
                    if data["token"] == "":
                        logger.info("Found the first change on namespace: %s, change: %s", 
                            self.__namespace, dumps(change))
                        data["token"] = dumps(change["_id"])
                    else:
                        logger.info("Found the second change on namespace: %s, change: %s", 
                            self.__namespace, dumps(change))
                        data["validation_document"] = dumps(change)
                        break
        except Exception as e:
            logger.exception(e)
        logger.info("Successfully captured the peek information on namespace: %s. Data: %s", 
            self.__namespace, dumps(data))
        return data

    def validate(self, peek_info):
        logger.info("Validating the peek_info from token: %s", dumps(peek_info))
        resume_token = bson.json_util.loads(peek_info["resume_token"])
        logger.debug("resume_token loaded from peek_info: %s", dumps(resume_token))
        is_valid = False
        try:
            db = self.__client.get_database(self.__database_name)
            # get a change stream object
            self.__cursor = db.get_collection(self.__collection_name).watch(
                    pipeline=self.__pipeline, full_document="updateLookup", resume_after=resume_token)

            logger.info("Watching for a change on the namespace: %s after the resume token", self.__namespace)
            with self.__cursor as cursor:
                for change in cursor:
                    logger.info("Fetched a change from the change stream: %s. Change: %s", self.__namespace,
                                dumps(change))
                    is_valid = bson.json_util.loads(bson.json_util.dumps(change)) == bson.json_util.loads(
                        peek_info["validation_document"])
                    logger.info("Validation of the obtained change event: %s", is_valid)
                    break
        except Exception as e:
            logger.exception(e)
        logger.info("Successfully captured the peek information on namespace: %s. Data: %s", self.__namespace,
                    dumps(peek_info))
        return is_valid

    def watch(self, token_data, notify_callback):
        # token_data from database may have resume_token
        # try resume from that. if not use big_bang_token
        # if not use resume_token = None
        big_bang_token = {'_data': b'[{"token":"\\"0\\"","range":{"min":"","max":"FF"}}]'}
        resume_token = big_bang_token
        if token_data is not None and "resume_token" in token_data:
            resume_token = bson.json_util.loads(token_data['resume_token'])
            if type(resume_token) == str:
                resume_token = bson.json_util.loads(token_data['resume_token'])
            logger.info("Resuming watch on cluster_name: %s, namespace: %s using resume_token: %s",
                self.__cluster_name, self.__namespace, dumps(resume_token))
        # from the given token/earliest available token/None
        retry_count = 0
        while retry_count < 3:
            try:
                self.__watch(resume_token, notify_callback)
                break
            except pymongo.errors.OperationFailure as of:
                if "Change feed token format is invalid" in of.details["errmsg"] or "Bad resume token: _data of missing" in of.details["errmsg"]:
                    logger.info("Detected invalid change feed token. Retrying with no resume_token. Error Message: %s", of.details["errmsg"])
                elif "operation was interrupted" in of.details["errmsg"]:
                    logger.info("Detected CTRL-C. Closing watch on cluster: %s. namespace: %s.", self.__cluster_name, self.__namespace)
                    break
                else:
                    logger.info("Watch operation failed on cluster: %s. namespace: %s.", self.__cluster_name, self.__namespace, exc_info=True)
                retry_count = retry_count + 1
            except Exception as ex:
                logger.info("Unexpected exception while watching on namespace")
                logger.exception(ex, exc_info=True)
            finally:
                if resume_token == big_bang_token:
                    resume_token = None
                elif resume_token is None:
                    break
                else:
                    resume_token = big_bang_token
    
    def __watch(self, resume_token, notify_callback):
        logger.info("Inititated change stream on the db: %s, collection: %s. Resume Token: %s", self.__database_name,
                    self.__collection_name, dumps(resume_token))
        db = self.__client.get_database(self.__database_name)
        # create change stream pipeline object
        pipeline = [
            {"$match": {"operationType": {"$in": ["insert", "update", "replace"]}}},
            {"$project": {"_id": 1, "fullDocument": 1, "ns": 1, "documentKey": 1}}
        ]
        # get a change stream object
        self.__cursor = db.get_collection(self.__collection_name).watch(
            pipeline=pipeline,
            full_document="updateLookup",
            resume_after=resume_token)
        logger.info("Watching for the changes on the cluster: %s, db: %s, collection: %s", 
            self.__cluster_name, self.__database_name, self.__collection_name)
        # Prints all documents in the collection
        for change in self.__cursor:
            # notify the change here
            logger.info("Detected a change in document. %s", dumps(change))
            notify_callback(self.__cluster_name, self.__database_name, self.__collection_name, change)

    def __close_cursor(self, cursor):
        if cursor is not None:
            try:
                cursor.close()
                logger.info("Closed the change stream cursor for namespace: %s", self.__namespace)
            except Exception as e:
                logger.error("Error closing the change stream cursor for namespace: %s", self.__namespace, exc_info=True)
                pass

    def close(self):
        self.__close_cursor(self.__cursor)
