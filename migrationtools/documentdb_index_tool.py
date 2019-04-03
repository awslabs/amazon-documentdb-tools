#!/bin/env python
"""
  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  
  Licensed under the Apache License, Version 2.0 (the "License").
  You may not use this file except in compliance with the License.
  A copy of the License is located at
  
      http://www.apache.org/licenses/LICENSE-2.0
  
  or in the "license" file accompanying this file. This file is distributed 
  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
  express or implied. See the License for the specific language governing 
  permissions and limitations under the License.
"""

import argparse
import errno
import json
import logging
import os
import sys

from bson.json_util import dumps
from pymongo import MongoClient
from pymongo.errors import (ConnectionFailure, OperationFailure,
                            ServerSelectionTimeoutError)


class AutovivifyDict(dict):
    """N depth defaultdict."""

    def __getitem__(self, item):
        try:
            return dict.__getitem__(self, item)
        except KeyError:
            value = self[item] = type(self)()
            return value


class DocumentDbLimits(object):
    """
    DocumentDB limits
    """

    def __init__(self):
        pass

    COLLECTION_QUALIFIED_INDEX_NAME_MAX_LENGTH = 63
    COLLECTION_NAME_MAX_LENGTH = 57
    COMPOUND_INDEX_MAX_KEYS = 32
    DATABASE_NAME_MAX_LENGTH = 63
    FULLY_QUALIFIED_INDEX_NAME_MAX_LENGTH = 127
    INDEX_KEY_MAX_LENGTH = 2048
    INDEX_NAME_MAX_LENGTH = 63
    NAMESPACE_MAX_LENGTH = 120


class DocumentDbUnsupportedFeatures(object):
    """
    List of unsupported features in DocumentDB
    """

    def __init__(self):
        pass

    UNSUPPORTED_INDEX_TYPES = [
        'text', '2d', '2dsphere', 'geoHaystack', 'hashed'
    ]
    UNSUPPORTED_INDEX_OPTIONS = ['partialFilterExpression', 'storageEngine', \
                                'collation', 'dropDuplicates']
    UNSUPPORTED_COLLECTION_OPTIONS = ['capped']


class IndexToolConstants(object):
    """
    constants used in this tool
    """

    def __init__(self):
        pass

    DATABASES_TO_SKIP = ['admin', 'local', 'system']
    METADATA_FILE_SUFFIX_PATTERN = 'metadata.json'
    CONNECT_TIMEOUT = 5000
    EXCEEDED_LIMITS = 'exceeded_limits'
    FILE_PATH = 'filepath'
    ID = '_id_'
    INDEXES = 'indexes'
    INDEX_DEFINITION = 'definition'
    INDEX_NAME = 'name'
    INDEX_VERSION = 'v'
    INDEX_KEY = 'key'
    INDEX_NAMESPACE = 'ns'
    NAMESPACE = 'ns'
    OPTIONS = 'options'
    UNSUPPORTED_INDEX_OPTIONS_KEY = 'unsupported_index_options'
    UNSUPPORTED_COLLECTION_OPTIONS_KEY = 'unsupported_collection_options'
    UNSUPPORTED_INDEX_TYPES_KEY = 'unsupported_index_types'


class DocumentDbIndexTool(IndexToolConstants):
    """
    Traverses a mongodump directory structure performs discovery and index restore functions.
    """

    def __init__(self, args):
        super(DocumentDbIndexTool, self).__init__()
        self.args = args

        log_level = logging.INFO

        if self.args.debug is True:
            log_level = logging.DEBUG

        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)

        root_handler = logging.StreamHandler(sys.stdout)
        root_handler.setLevel(log_level)
        formatter = logging.Formatter('%(asctime)s: %(message)s')
        root_handler.setFormatter(formatter)
        root_logger.addHandler(root_handler)

    def _mkdir_p(self, filepath):
        try:
            os.makedirs(filepath)
        except OSError as ose:
            if ose.errno == errno.EEXIST and os.path.isdir(filepath):
                pass
            else:
                raise

    def _get_db_connection(self, host, port, tls, tls_ca_file, username,
                           password, auth_db):
        """Connect to instance, returning a connection"""
        logging.debug("Connecting to instance at %s:%s", host, port)

        mongodb_client = MongoClient(
            host=host,
            port=port,
            ssl=tls,
            ssl_ca_certs=tls_ca_file,
            connect=True,
            connectTimeoutMS=DocumentDbIndexTool.CONNECT_TIMEOUT,
            serverSelectionTimeoutMS=DocumentDbIndexTool.CONNECT_TIMEOUT)

        # force the client to actually connect
        mongodb_client.admin.command('ismaster')

        if password is not None:
            # deprecated in 3.6, but allows us to handle both auth types gracefully
            mongodb_client[auth_db].authenticate(name=username,
                                                 password=password)
            logging.info("Successfully authenticated to database: %s", auth_db)

        logging.info("Successfully connected to instance %s:%s", host, port)

        return mongodb_client

    def _get_compatible_metadata(self, metadata, compatibility_issues):
        compatible_metadata = metadata.copy()
        for db_name in compatibility_issues:
            if self.EXCEEDED_LIMITS in compatibility_issues[db_name]:
                del compatible_metadata[db_name]
                continue

            for collection_name in compatibility_issues[db_name]:
                if self.UNSUPPORTED_COLLECTION_OPTIONS_KEY in compatibility_issues[
                        db_name][collection_name]:
                    del compatible_metadata[db_name][collection_name]
                    continue
                if self.EXCEEDED_LIMITS in compatibility_issues[db_name][
                        collection_name]:
                    del compatible_metadata[db_name][collection_name]
                    continue
                for index_name in compatibility_issues[db_name][
                        collection_name]:
                    del compatible_metadata[db_name][collection_name][
                        self.INDEXES][index_name]

        return metadata

    def _get_metadata_from_file(self, filepath):
        """
        Given a path to a metadata file, return the JSON data structure parsed
        from the contents, formatted.
        """
        with open(filepath, 'rt') as metadata_file:
            logging.debug("Getting metadata from file: %s", filepath)

            file_metadata = json.load(metadata_file)
            collection_metadata = AutovivifyDict()
            indexes = file_metadata.get(self.INDEXES, None)

            # every collection should have at least the _id_ index. If no indexes are listed, the
            # metadata document is malformed and we should error out
            if indexes is None:
                raise Exception(
                    "Malformed metadata document {} has no indexes.".format(
                        filepath))

            first_index = indexes[0]
            first_index_namespace = first_index[self.NAMESPACE]
            (db_name, collection_name) = first_index_namespace.split('.', 1)

            collection_metadata[self.FILE_PATH] = filepath

            for index in indexes:
                index_name = index.pop(self.INDEX_NAME)
                collection_metadata[self.INDEXES][index_name] = index

                if file_metadata[self.OPTIONS]:
                    collection_metadata[self.OPTIONS] = file_metadata[
                        self.OPTIONS]

            return db_name, collection_name, collection_metadata

    def _find_metadata_files(self, start_dir):
        """Recurse through subdirectories looking for metadata files"""
        metadata_files = []

        for (dirpath, dirnames, files) in os.walk(start_dir):
            for filename in files:
                if filename.endswith(self.METADATA_FILE_SUFFIX_PATTERN):
                    metadata_files.append(os.path.join(dirpath, filename))

        return metadata_files

    def _dump_indexes_from_server(self, connection, output_dir, dry_run=False):
        """
        Discover all indexes in a mongodb server and dump them
        to files using the mongodump format
        """

        logging.info("Retrieving indexes from server...")
        try:
            database_info = connection.admin.command({'listDatabases': 1})

            for database_doc in database_info['databases']:
                database_name = database_doc['name']
                logging.debug("Database: %s", database_name)

                if database_name in self.DATABASES_TO_SKIP:
                    continue

                database_path = os.path.join(output_dir, database_name)

                if dry_run is not True:
                    self._mkdir_p(database_path)

                # Write out each collection's stats in this database
                for collection_name in connection[
                        database_name].list_collection_names():
                    logging.debug("Collection: %s", collection_name)
                    collection_metadata = {}
                    collection_indexes = connection[database_name][
                        collection_name].list_indexes()
                    collection_metadata[self.INDEXES] = [
                        index for index in collection_indexes
                    ]
                    collection_metadata[self.OPTIONS] = connection[
                        database_name][collection_name].options()

                    collection_metadata_filename = "{}.{}".format(
                        collection_name, self.METADATA_FILE_SUFFIX_PATTERN)
                    collection_metadata_filepath = os.path.join(
                        database_path, collection_metadata_filename)

                    if dry_run is True:
                        logging.info("\n%s.%s\n%s",
                                     database_name, collection_name,
                                     dumps(collection_metadata))

                    else:
                        logging.debug(
                            "Writing collection metadata for collection: %s",
                            collection_name)
                        with open(collection_metadata_filepath,
                                  'wt') as collection_metadata_file:
                            collection_metadata_file.write(
                                dumps(collection_metadata,
                                      separators=(',', ':')))

            logging.info(
                "Completed writing index metadata to local folder: %s",
                output_dir)

        except Exception:
            logging.exception("Failed to dump indexes from server")
            sys.exit()

    def get_metadata(self, start_path):
        """
        Recursively search the supplied start_path, discovering all JSON metadata files and adding the
        information to our metadata data structure.
        """
        try:
            logging.debug(
                "Beginning recursive discovery of metadata files, starting at %s",
                start_path)
            metadata_files = self._find_metadata_files(start_path)

            if metadata_files == []:
                logging.error("No metadata files found beneath directory: %s",
                              start_path)
                sys.exit()

            logging.debug("Metadata files found: {}%s", metadata_files)

            metadata = AutovivifyDict()

            for filepath in metadata_files:
                (db_name, collection_name,
                 collection_metadata) = self._get_metadata_from_file(filepath)
                metadata[db_name][collection_name] = collection_metadata

            return metadata

        except Exception:
            logging.exception("Failed to discover dump indexes")
            sys.exit()

    def find_compatibility_issues(self, metadata):
        """Check db, collection and index data in metadata files for compatibility with DocumentDB"""
        compatibility_issues = AutovivifyDict()

        for db_name in metadata:
            db_metadata = metadata[db_name]

            if len(db_name) > DocumentDbLimits.DATABASE_NAME_MAX_LENGTH:
                message = 'Database name greater than {} characters'.format(
                    DocumentDbLimits.DATABASE_NAME_MAX_LENGTH)
                compatibility_issues[db_name][
                    self.EXCEEDED_LIMITS][message] = db_name

            for collection_name in metadata[db_name]:
                collection_metadata = db_metadata[collection_name]

                if len(collection_name
                       ) > DocumentDbLimits.COLLECTION_NAME_MAX_LENGTH:
                    message = 'Collection name greater than {} characters'.format(
                        DocumentDbLimits.COLLECTION_NAME_MAX_LENGTH)
                    compatibility_issues[db_name][collection_name][
                        self.EXCEEDED_LIMITS][message] = collection_name

                collection_namespace = '{}.{}'.format(db_name, collection_name)
                # <db>.<collection>
                if len(collection_namespace
                       ) > DocumentDbLimits.NAMESPACE_MAX_LENGTH:
                    message = 'Namespace greater than {} characters'.format(
                        DocumentDbLimits.NAMESPACE_MAX_LENGTH)
                    compatibility_issues[db_name][collection_name][
                        self.EXCEEDED_LIMITS][message] = collection_namespace

                if self.OPTIONS in collection_metadata:
                    for option_key in collection_metadata[self.OPTIONS]:
                        if option_key in DocumentDbUnsupportedFeatures.UNSUPPORTED_COLLECTION_OPTIONS:
                            if self.UNSUPPORTED_COLLECTION_OPTIONS_KEY not in compatibility_issues[
                                    db_name][collection_name]:
                                compatibility_issues[db_name][collection_name][
                                    self.
                                    UNSUPPORTED_COLLECTION_OPTIONS_KEY] = []

                            compatibility_issues[db_name][collection_name][
                                self.
                                UNSUPPORTED_COLLECTION_OPTIONS_KEY].append(
                                    option_key)

                for index_name in collection_metadata[self.INDEXES]:
                    index = collection_metadata[self.INDEXES][index_name]

                    # <collection>$<index>
                    collection_qualified_index_name = '{}${}'.format(
                        collection_name, index_name)
                    if len(
                            collection_qualified_index_name
                    ) > DocumentDbLimits.COLLECTION_QUALIFIED_INDEX_NAME_MAX_LENGTH:
                        message = '<collection>$<index> greater than {} characters'.format(
                            DocumentDbLimits.
                            COLLECTION_QUALIFIED_INDEX_NAME_MAX_LENGTH)
                        compatibility_issues[db_name][collection_name][
                            index_name][self.EXCEEDED_LIMITS][
                                message] = collection_qualified_index_name

                    # <db>.<collection>$<index>
                    fully_qualified_index_name = '{}${}'.format(
                        collection_namespace, index_name)
                    if len(
                            fully_qualified_index_name
                    ) > DocumentDbLimits.FULLY_QUALIFIED_INDEX_NAME_MAX_LENGTH:
                        message = '<db>.<collection>$<index> greater than {} characters'.format(
                            DocumentDbLimits.
                            COLLECTION_QUALIFIED_INDEX_NAME_MAX_LENGTH)
                        compatibility_issues[db_name][collection_name][
                            index_name][self.EXCEEDED_LIMITS][
                                message] = fully_qualified_index_name

                    # Check for indexes with too many keys
                    if len(index) > DocumentDbLimits.COMPOUND_INDEX_MAX_KEYS:
                        message = 'Index contains more than {} keys'.format(
                            DocumentDbLimits.COMPOUND_INDEX_MAX_KEYS)
                        compatibility_issues[db_name][collection_name][
                            index_name][self.EXCEEDED_LIMITS][message] = len(
                                index)

                    for key_name in index:
                        # Check for index key names that are too long
                        if len(key_name
                               ) > DocumentDbLimits.INDEX_KEY_MAX_LENGTH:
                            message = 'Key name greater than {} characters'.format(
                                DocumentDbLimits.INDEX_KEY_MAX_LENGTH)
                            compatibility_issues[db_name][collection_name][
                                index_name][
                                    self.EXCEEDED_LIMITS][message] = key_name

                        # Check for unsupported index options like collation
                        if key_name in DocumentDbUnsupportedFeatures.UNSUPPORTED_INDEX_OPTIONS:
                            if self.UNSUPPORTED_INDEX_OPTIONS_KEY not in compatibility_issues[
                                    db_name][collection_name][index_name]:
                                compatibility_issues[db_name][collection_name][
                                    index_name][
                                        self.
                                        UNSUPPORTED_INDEX_OPTIONS_KEY] = []

                            compatibility_issues[db_name][collection_name][
                                index_name][
                                    self.UNSUPPORTED_INDEX_OPTIONS_KEY].append(
                                        key_name)

                        # Check for unsupported index types like text
                        if key_name == self.INDEX_KEY:
                            for index_key_name in index[key_name]:
                                key_value = index[key_name][index_key_name]

                                if key_value in DocumentDbUnsupportedFeatures.UNSUPPORTED_INDEX_TYPES:
                                    compatibility_issues[db_name][
                                        collection_name][index_name][
                                            self.
                                            UNSUPPORTED_INDEX_TYPES_KEY] = key_value

        return compatibility_issues

    def _restore_indexes(self, connection, metadata):
        """Restore compatible indexes to a DocumentDB instance"""
        for db_name in metadata:
            for collection_name in metadata[db_name]:
                for index_name in metadata[db_name][collection_name][
                        self.INDEXES]:
                    # convert the keys dict to a list of tuples as pymongo requires
                    index_keys = metadata[db_name][collection_name][
                        self.INDEXES][index_name][self.INDEX_KEY]
                    keys_to_create = []
                    index_options = {}

                    for key in index_keys:
                        index_direction = index_keys[key]

                        if type(index_direction) is float:
                            index_direction = int(index_direction)

                        keys_to_create.append((key, index_direction))

                    for k in metadata[db_name][collection_name][
                            self.INDEXES][index_name]:
                        if k != self.INDEX_KEY:
                            # this key is an additional index option
                            index_options[k] = metadata[db_name][
                                collection_name][self.INDEXES][index_name][k]

                    if self.args.dry_run is True:
                        logging.info(
                            "(dry run) %s.%s: would attempt to add index: %s",
                            db_name, collection_name, index_name)

                    else:
                        logging.debug("Adding index %s -> %s", keys_to_create,
                                      index_options)
                        database = connection[db_name]
                        collection = database[collection_name]
                        collection.create_index(keys_to_create,
                                                **index_options)
                        logging.info("%s.%s: added index: %s", db_name,
                                     collection_name, index_name)

    def run(self):
        """Entry point
        """
        metadata = None
        compatibility_issues = None
        connection = None

        # get a connection to our source mongodb or destination DocumentDb
        if self.args.dump_indexes is True or self.args.restore_indexes is True:
            try:
                connection = self._get_db_connection(
                    host=self.args.host,
                    port=self.args.port,
                    tls=self.args.tls,
                    tls_ca_file=self.args.tls_ca_file,
                    username=self.args.username,
                    password=self.args.password,
                    auth_db=self.args.auth_db)
            except (ConnectionFailure, ServerSelectionTimeoutError,
                    OperationFailure) as cex:
                logging.error("Connection to instance %s:%s failed: %s",
                              self.args.host, self.args.port, cex)
                sys.exit()

        # dump indexes from a MongoDB server
        if self.args.dump_indexes is True:
            self._dump_indexes_from_server(connection, self.args.dir,
                                           self.args.dry_run)
            sys.exit()

        # all non-dump operations require valid source metadata
        try:
            metadata = self.get_metadata(self.args.dir)
            compatibility_issues = self.find_compatibility_issues(metadata)
        except Exception as ex:
            logging.error("Failed to load collection metadata: %s", ex)
            sys.exit()

        # Apply indexes to a DocumentDB instance
        if self.args.restore_indexes is True:
            metadata_to_restore = metadata

            if self.args.skip_incompatible is not True:
                if compatibility_issues:
                    logging.error(
                        "incompatible indexes exist and --skip-incompatible not specified."
                    )
                    sys.exit()
            else:
                metadata_to_restore = self._get_compatible_metadata(
                    metadata, compatibility_issues)

            self._restore_indexes(connection, metadata_to_restore)
            sys.exit()

        # find and print a summary or detail or compatibility issues
        if self.args.show_issues is True:
            if not compatibility_issues:
                logging.info("No incompatibilities found.")
            else:
                logging.info(
                    json.dumps(compatibility_issues,
                               sort_keys=True,
                               indent=4,
                               separators=(',', ': ')))
            sys.exit()

        # print all compatible (restorable) collections and indexes
        if self.args.show_compatible is True:
            compatible_metadata = self._get_compatible_metadata(
                metadata, compatibility_issues)
            logging.info(
                json.dumps(compatible_metadata,
                           sort_keys=True,
                           indent=4,
                           separators=(',', ': ')))


def main():
    """
    parse command line arguments and
    """
    parser = argparse.ArgumentParser(
        description='Dump and restore indexes from MongoDB to DocumentDB.')

    parser.add_argument('--debug',
                        required=False,
                        action='store_true',
                        help='output debugging information')

    parser.add_argument(
        '--dry-run',
        required=False,
        action='store_true',
        help='Perform processing, but do not actually restore indexes')

    parser.add_argument('--dir',
                        required=False,
                        type=str,
                        default='.',
                        help='dump to or restore from DIR (default: .)')

    parser.add_argument('--show-compatible',
                        required=False,
                        action='store_true',
                        dest='show_compatible',
                        help='output all compatible indexes (without change)')

    parser.add_argument(
        '--show-issues',
        required=False,
        action='store_true',
        dest='show_issues',
        help='output a detailed structure of compatibility issues')

    parser.add_argument('--dump-indexes',
                        required=False,
                        action='store_true',
                        help='Dump indexes from the specified host/port')

    parser.add_argument(
        '--restore-indexes',
        required=False,
        action='store_true',
        help='Restore indexes found in metadata to the specified host/port')

    parser.add_argument(
        '--skip-incompatible',
        required=False,
        action='store_true',
        help='Skip incompatible indexes while dumping or restoring')

    parser.add_argument('--host',
                        required=False,
                        type=str,
                        default='localhost',
                        help='connect to host HOST (default: localhost)')

    parser.add_argument('--port',
                        required=False,
                        type=int,
                        default=27017,
                        help='connect to port PORT (default: 27017)')

    parser.add_argument('--username',
                        required=False,
                        type=str,
                        help='authenticate with username USERNAME')

    parser.add_argument('--password',
                        required=False,
                        type=str,
                        help='authenticate with password PASSWORD')

    parser.add_argument(
        '--auth-db',
        required=False,
        type=str,
        dest='auth_db',
        help='authenticate using database AUTH_DB (default: admin)')

    parser.add_argument('--tls',
                        required=False,
                        action='store_true',
                        help='connect using TLS')

    parser.add_argument('--tls-ca-file',
                        required=False,
                        type=str,
                        help='path to CA file used for TLS connection')

    args = parser.parse_args()

    if not (args.dump_indexes or args.restore_indexes or args.show_issues
            or args.show_compatible):
        message = "must specify one of [--dump-indexes | --restore-indexes | --show-issues | --show-compatible]"
        parser.error(message)

    if args.dir is not None:
        if not os.path.isdir(args.dir):
            parser.error("--dir must specify a directory")

    if args.dump_indexes is True:
        if args.restore_indexes is True:
            parser.error("cannot dump and restore indexes simultaneously")

    if any([args.username, args.password]):
        if not all([args.username, args.password]):
            parser.error(
                "both --username amd --password are required if providing MongoDB credentials."
            )

    if args.auth_db is not None and not all([args.username, args.password]):
        parser.error("--auth-db requires both --username and --password.")

    indextool = DocumentDbIndexTool(args)
    indextool.run()


if __name__ == "__main__":
    main()
