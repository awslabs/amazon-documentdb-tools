import logging
import os
import tempfile
import traceback

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class FileHelper:
    """
    A helper class to work with local file system. The class methods
    offer methods to create a temporary files and delete the files 
    on local file system.
    """

    def create_file(self):
        """
        Creates a named temporary file on local file system
        :rtype: str
        :return: Returns a file path of temporary file
        """
        temp_file = tempfile.NamedTemporaryFile(mode='w+b', delete=False)
        logger.info("Successfully created a temporary file: %s", temp_file.name)
        return temp_file

    def delete_file(self, file_path):
        """
        Deletes a file located on local file system
        :param file_path A file path for the file being deleted
        """
        try:
            os.unlink(file_path)
            logger.info("Successfully deleted the file: %s", file_path)
        except Exception as e:
            stack_trace = traceback.format_stack()
            logger.error("Exception while deleting file: %s. Error: %s", file_path, e)
