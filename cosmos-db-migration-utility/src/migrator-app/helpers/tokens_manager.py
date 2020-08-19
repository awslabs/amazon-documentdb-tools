import json
import os

import yaml
from bson.json_util import dumps

from common.logger import get_logger

logger = get_logger(__name__)


# noinspection PyBroadException
class TokensManager:
    def __init__(self, dynamo_helper):
        self.__data = {}
        self.__file_path = "{}/tokens.yaml".format(os.getcwd())
        self.__dh = dynamo_helper

    def load(self, namespaces):
        # fetch data from data store
        return self.__load_from_db(namespaces)
        # return self.__load_from_file()

    def __load_from_db(self, namespaces):
        # for each namespace, read watcher entry
        tokens = {}
        for database_name in namespaces:
            collections = namespaces[database_name]
            for collection_name in collections:
                namespace = "{}.{}".format(database_name, collection_name)
                token = self.__dh.get_watcher(namespace)
                if token is not None:
                    tokens[namespace] = token
        logger.info("Successfully loaded tokens from database: %s", dumps(tokens))
        return tokens

    def __load_from_file(self):
        # read the data from file/remote cache
        try:
            logger.info("Opening tokens file located at %s", self.__file_path)
            with open(self.__file_path, 'r') as stream:
                try:
                    self.__data = yaml.safe_load(stream)
                except yaml.YAMLError as e:
                    logger.fatal("Error occured while reading file as YAML", exc_info=True)
                    exit(1)
        except Exception as ex:
            logger.fatal("Error opening tokens file: %s", self.__file_path, exc_info=True)
            exit(1)
        logger.info("Successfully loaded tokens.yaml. Contents: %s", json.dumps(self.__data))
        return self.__data

    def save(self, peek_info):
        # save the data into a local file/remote cache
        try:
            logger.info("Writing peek_info to tokens file located at %s. Contents: %s", self.__file_path,
                        dumps(peek_info))
            with open(self.__file_path, 'w') as stream:
                try:
                    self.__data = yaml.dump(peek_info, stream)
                except yaml.YAMLError as e:
                    logger.fatal("Error occured while writing contents as YAML. Content: %s", dumps(peek_info),
                                 exc_info=True)
                    exit(1)
        except Exception as ex:
            logger.fatal("Error opening tokens file: %s", self.__file_path, exc_info=True)
            exit(1)
        logger.info("Successfully saved tokens.yaml with contents: %s", json.dumps(self.__data))
        return self.__data

    def get_token(self):
        return self.__data
