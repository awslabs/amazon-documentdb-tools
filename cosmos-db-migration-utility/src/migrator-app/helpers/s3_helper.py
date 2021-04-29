import logging

import boto3

from .file_helper import FileHelper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class S3Helper:
    """
    A helper class to work with s3. The class offers various
    methods to download files from s3 to local file system 
    as well as upload local files to the s3 bucket.

    Assumption: The AWS_SECRET_ACCESS_KEY, AWS_ACCESS_KEY_ID are
    assumed to be loaded in the environment variables. 
    """

    def __init__(self):
        self.__fh = FileHelper()

    def download(self, bucket_name, key_name):
        """
        Downloads the file from s3 to a local temporary file
        :param bucket_name A string representing of s3 bucket name
        :param key_name A string representing of s3 key name
        :rtype: str
        :return: Returns a local file path of downloaded s3 file
        """
        temp_file = self.__fh.create_file()
        s3 = boto3.client('s3')
        logger.info("Starting to download s3 file - bucket_name: %s, key_name: %s to local file: %s",
                    bucket_name, key_name, temp_file.name)
        s3.download_file(Bucket=bucket_name, Key=key_name, Filename=temp_file.name)
        logger.info("Successfully downloaded s3 contents into file: %s", temp_file.name)
        return temp_file.name

    def upload(self, file_path, bucket_name, key_name):
        """
        Uploads a local file to s3 bucket
        :param file_path A file path for the file being uploaded
        :param bucket_name A string representing of s3 bucket name
        :param key_name A string representing of s3 key name
        """
        s3 = boto3.client('s3')
        logger.info("Starting to upload file: %s to s3", file_path)
        s3.upload_file(file_path, bucket_name, key_name)
        logger.info("Successfully uploaded file contents: %s into s3", file_path)
