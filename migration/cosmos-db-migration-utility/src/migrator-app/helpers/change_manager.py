import threading

from common.logger import get_logger
from .document_batcher import DocumentBatcher

logger = get_logger(__name__)


class ChangeManager:
    def __init__(self, cluster_name, dynamodb_helper, tokens):
        self.__managers = {}
        self.__cluster_name = cluster_name
        self.__tokens = tokens
        self.__lock = threading.Lock()
        self.__dynamodb_helper = dynamodb_helper

    def get_manager(self, cluster_name, database_name, collection_name):
        namespace = "{}.{}".format(database_name, collection_name)
        if namespace in self.__managers:
            return self.__managers[namespace]
        else:
            try:
                self.__lock.acquire()
                if namespace not in self.__managers:
                    manager = DocumentBatcher(self.__cluster_name, namespace, database_name, collection_name, self.__dynamodb_helper)
                    token = None
                    if namespace in self.__tokens:
                        token = self.__tokens[namespace]
                    manager.initialize(token)
                    self.__managers[namespace] = manager
                return self.__managers[namespace]
            finally:
                self.__lock.release()

    def on_change_event(self, cluster_name, database_name, collection_name, change):
        manager = self.get_manager(cluster_name, database_name, collection_name)
        # invoke the change even on the specific manager
        manager.on_change_event(cluster_name, database_name, collection_name, change)

    def close(self):
        logger.info("Cleaning up the Change Manager")
        for namespace in self.__managers:
            manager = self.__managers[namespace]
            try:
                manager.close()
            except:
                pass
