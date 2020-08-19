import os
from common.logger import get_logger
from signal import signal, SIGINT
from sys import exit
import json
from bson.json_util import loads, dumps

from helpers.change_manager import ChangeManager
from helpers.dynamodb_helper import DynamodbHelper
from helpers.tokens_manager import TokensManager
from migrators.ClusterMigrator import ClusterMigrator
from commandline_parser import CommandLineParser
from common.application_exception import ApplicationException

logger = get_logger(__name__)

def exit_handler(signal_received, frame):
    # Handle any cleanup here
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    exit(0)

def check_environment_variables(variables):
    for variable in variables:
        if variable not in os.environ:
            logger.fatal("Environment variable %s is required but not set.", variable)
            logger.error("The following environment variables are required: %s", json.dumps(variables, indent=2))
            exit(1)

# Tell Python to run the handler() function when SIGINT is recieved
signal(SIGINT, exit_handler)

migrator = None
writer = None
change_manager = None
try:
    # check if the required environment variables are set or not
    names = ["AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "S3_CHANGE_FEED_BUCKET_NAME", "SOURCE_URI"]
    check_environment_variables(names)
    
    parser = CommandLineParser()
    commandline_options = parser.get_options()

    cluster_name = commandline_options["cluster_name"]

    # get the active namespaces from the cluster
    source_connection_string = os.environ['SOURCE_URI']
    migrator = ClusterMigrator(cluster_name, source_connection_string)
    namespaces = migrator.get_namespaces()
    logger.info("Found the following namespaces on cluster_name: %s. Namespaces: %s", cluster_name, dumps(namespaces))

    # load the resume tokens for the given namespaces
    dynamo_helper = DynamodbHelper(cluster_name)
    tokens_manager = TokensManager(dynamo_helper)
    tokens = tokens_manager.load(namespaces)

    # # save the list of databases being tracked
    dynamo_helper.save_namespaces(namespaces)

    change_manager = ChangeManager(cluster_name, dynamo_helper, tokens)
    migrator.watch(tokens, change_manager.on_change_event)
except ApplicationException as ae:
    logger.error("%s", ae)
    exit(1)
finally:
    if migrator is not None:
        migrator.close()
    if change_manager is not None:
        change_manager.close()
