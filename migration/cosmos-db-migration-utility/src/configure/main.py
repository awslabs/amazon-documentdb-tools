import json
from application import Application
from common.logger import get_logger
from commandline_parser import CommandLineParser
from common.application_exception import ApplicationException
from signal import signal, SIGINT
from sys import exit
import os

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

try:
    # check if the required environment variables are set or not
    names = ["AWS_DEFAULT_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"]
    check_environment_variables(names)
    
    parser = CommandLineParser()
    commandline_options = parser.get_options()
except ApplicationException as ae:
    logger.error("%s", ae)
    exit(1)
except Exception as e:
    logger.error("Exception occurred while processing the request", exc_info=True)
    exit(1)

logger.info("Starting to configure application components with commandline_options: %s", json.dumps(commandline_options))
app = Application(commandline_options["cluster_name"])
if commandline_options["command"] == "connection_string":
    app.set_connection_string(commandline_options["connection_string"])
elif commandline_options["command"] == "event_writer":
    app.set_event_writer(commandline_options["event_writer"])
elif commandline_options["command"] == "status":
    app.print_status()
elif commandline_options["command"] == "watch_status":
    app.watch_status()
logger.info("Successfully completed configuring the application components.")