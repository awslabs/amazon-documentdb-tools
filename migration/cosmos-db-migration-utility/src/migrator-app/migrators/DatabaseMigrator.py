from pymongo import MongoClient
import json
from common.logger import get_logger
from .CollectionMigrator import CollectionMigrator
import threading

logger = get_logger(__name__)


class DatabaseMigrator:
    def __init__(self, client, cluster_name, database_name):
        self.__client = client
        self.__cluster_name = cluster_name
        self.__database_name = database_name
        self.__collection_migrators = []
        logger.info("Initializing database migrator for database: [%s]", self.__database_name)

    def watch(self, tokens, notify_callback):
        try:
            db = self.__client.get_database(self.__database_name)
            logger.info("Fetching collections from Database: %s", self.__database_name)
            collection_names = db.collection_names(include_system_collections=False)
            logger.info("Found collections in database: %s; Collections: %s", self.__database_name,
                        json.dumps(collection_names))
            watch_threads = []
            for collection_name in collection_names:
                namespace = "{}.{}".format(self.__database_name, collection_name)
                token = {}
                if namespace in tokens:
                    token = tokens[namespace]
                collection_migrator = CollectionMigrator(self.__client, self.__cluster_name, self.__database_name, collection_name)
                t = threading.Thread(target=collection_migrator.watch, args=(token, notify_callback,))
                watch_threads.append(t)
                t.start()
                self.__collection_migrators.append(collection_migrator)
                logger.info("Found the collection with namespace %s.%s", self.__database_name, collection_name)
            # wait for threads to join
            for watch_thread in watch_threads:
                watch_thread.join()
        except Exception as e:
            logger.exception(e)
        finally:
            if self.__client is not None:
                self.__client.close()
                logger.info("Gracefully closing the connection")

    def close(self):
        logger.info("Cleaning up the database migrators in the cluster.")
        for migrator in self.__collection_migrators:
            migrator.close()
