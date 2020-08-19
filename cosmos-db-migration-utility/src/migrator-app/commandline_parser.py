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
    parser = argparse.ArgumentParser(description='Start watching the change events on Cosmos Cluster')
    parser.add_argument('--cluster-name', '-n', help='Identifies the name of the cluster being migrated', required=True)
    return parser
  

  def __validate_arguments(self):
    parser = self.__get_parser()
    config = vars(parser.parse_args())
    logger.info("Command line arguments given: " + json.dumps(config))
    return config