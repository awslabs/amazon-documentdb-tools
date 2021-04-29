import json
import threading

from bson.json_util import dumps
from pymongo import MongoClient

from common.logger import get_logger
from .CollectionMigrator import CollectionMigrator
from .DatabaseMigrator import DatabaseMigrator
from .TokenTracker import TokenTracker

logger = get_logger(__name__)


class ClusterMigrator:
    def __init__(self, cluster_name, connection_string):
        self.__connection_string = connection_string
        self.__cluster_name = cluster_name
        self.__callback = None
        self.__client = MongoClient(self.__connection_string)
        self.__database_migrators = []
        self.__skip_databases = ["admin", "local", "config"]
        self.__tracker = TokenTracker()
        self.__timer_threads = None
        logger.info("Initializing the cluster migrator with connection string: %s", self.__connection_string)

    def get_namespaces(self):
        db_collections = {}
        database_names = self.__client.list_database_names()
        for db_name in database_names:
            if db_name not in self.__skip_databases:
                db = self.__client.get_database(db_name)
                collection_names = db.collection_names(include_system_collections=False)
                db_collections[db_name] = collection_names
            else:
                logger.info("Skipping the database: %s while fetching get_namespaces", db_name)
        return db_collections

    def peek(self, namepace):
        names = namepace.split(".")
        database_name = names[0]
        collection_name = ".".join(names[1::])
        collection = CollectionMigrator(self.__client, database_name, collection_name)
        return collection.peek()

    def validate(self, tokens):
        logger.info("Validating the tokens: %s", dumps(tokens))
        for namespace in tokens:
            logger.info("Validating the tokens: %s => %s", namespace, dumps(tokens[namespace]))
            token = tokens[namespace]
            names = namespace.split(".")
            database_name = names[0]
            collection_name = ".".join(names[1::])
            collection = CollectionMigrator(self.__client, database_name, collection_name)
            is_valid = collection.validate(token)
            if not is_valid:
                logger.error("Validation of change stream resume token failed on collection: %s.", namespace)
                return False
        return True

    def watch(self, tokens, notify_callback):
        try:
            self.__callback = notify_callback
            logger.info("Fetching databases from the cluster: %s", self.__connection_string)
            database_names = self.__client.list_database_names()
            logger.info("Found the databases %s", json.dumps(database_names))
            watch_threads = []
            for database_name in database_names:
                if database_name not in self.__skip_databases:
                    database_migrator = DatabaseMigrator(self.__client, self.__cluster_name, database_name)
                    t = threading.Thread(target=database_migrator.watch, args=(tokens, notify_callback,))
                    t.start()
                    watch_threads.append(t)
                    self.__database_migrators.append(database_migrator)
                else:
                    logger.info("Skipping the database: %s for watching", database_name)

            # wait for threads to join
            for watch_thread in watch_threads:
                watch_thread.join()
        except Exception as e:
            logger.exception(e)

    def __invoke_callback(self, database_name, collection_name, change):
        namespace = "{}.{}".format(database_name, collection_name)
        # self.__tracker.update_token(namespace, change)
        self.__callback(database_name, collection_name, change)

    def close(self):
        logger.info("Cleaning up the database migrators in the cluster.")
        for migrator in self.__database_migrators:
            migrator.close()
