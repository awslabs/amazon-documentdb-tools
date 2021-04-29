import argparse
import json
from common.logger import get_logger
from common.application_exception import ApplicationException

logger = get_logger(__name__)

class  CommandLineParser():

  def get_options(self):
    config = self.__validate_arguments()
    return config
  
  def __get_parser(self):
    parser = argparse.ArgumentParser(description='Setup or configure the AWS services.')
    parser.add_argument('--cluster-name', '-n', help='Identifies the name of the cluster being migrated', required=True)
    parser.add_argument('--connection-string', '-c', help='Sets the connection string for the DocumentDB cluster.')
    parser.add_argument('--event-writer', '-e', help='Sets the status of the event writer. Values: stop or start.')
    parser.add_argument('--status', '-s', help='Displays the migration status and time gap details.', action='store_true')
    parser.add_argument('--watch-status', '-w', help='Watch the migration status and time gap details in a loop.', action='store_true')
    return parser
  

  def __validate_arguments(self):
    parser = self.__get_parser()
    config = vars(parser.parse_args())
    logger.info("Command line arguments given: " + json.dumps(config))
    
    # Verify necessary components are supplied in command line arguments
    command = []
    if config["connection_string"]:
      command.append("connection_string")
    if config["status"]:
      command.append("status")
    if config["watch_status"]:
      command.append("watch_status")
    if not config["event_writer"] is None:
      command.append("event_writer")

    if len(command) == 0:
      raise ApplicationException("Missing input argument for command. Specify --connection-string or --event-writer.")
    if len(command) > 1:
      raise ApplicationException("Please specify only one of the commands: --connection-string, --event_writer, --status or --watch-status arguments.")

    config["command"] = command[0]
    logger.info("Validated Command line arguments are: " + json.dumps(config))
    
    if config["command"] == "event_writer":
      if config["event_writer"] != "start" and config["event_writer"] != "stop":
        raise ApplicationException("Given value for event-writer is not valid: {}. Valid values are stop or start".format(config["event_writer"]))
    elif config["command"] == "connection_string":
      if config["connection_string"] == "" or  config["connection_string"] == None:
        raise ApplicationException("Given value for connection-string is not valid: [{}].".format(config["connection_string"]))
    return config