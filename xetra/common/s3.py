"""Connector and methods accessing s3"""
import os
import logging
import boto3
import pandas as pd
from io import StringIO, BytesIO

from xetra.common.constants import S3FileTypes
from xetra.common.custom_exceptions import WrongFormatException

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """

    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector
        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        #self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
        #                             aws_secret_access_key=os.environ[secret_key])
        #self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)

        self.s3 = boto3.resource('s3')
        self._bucket = self.s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        listing all files with a prefix on the S3 bucket
        :param prefix: prefix on the S3 bucket that should be filtered with
        :return: files: list of all files containing the prefix
        """
        
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, encoding: str = 'utf-8', sep: str = ','):
        """
        reading a csv from bucket S3 to df
        :param key:key of the file that should be read
        :param encoding: encoding of the data inside the csv
        :param sep: seperator of the csv file
        :return: df
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, sep=sep)
        return data_frame

    def write_df_to_s3(self, data_frame: pd.DataFrame, key: str, file_format: str, sep: str = ','):
        """
        writing a pandas df to S3
        supported formats: .csv, .parquet

        :param data_fram: pandas df that should be written
        :param key: target key
        :param file_format: file format
        """
        if data_frame.empty:
            self._logger.info('The dataframe is empty! No file will be written!')
            return None
        if file_format == S3FileTypes.CSV.value:
            out_buffer = StringIO()
            data_frame.to_csv(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        if file_format == S3FileTypes.PARQUET.value:
            out_buffer = BytesIO()
            data_frame.to_parquet(out_buffer, index=False)
            return self.__put_object(out_buffer, key)
        self._logger.info('The file format %s is not '
        'supported to be written to s3!', file_format)
        raise WrongFormatException

    def __put_object(self, out_buffer: StringIO or BytesIO, key: str):
        """
        Helper function for self.write_df_s3
        :param out_buffer: StringIO | BytesIO
        :param key: target key
        """
        self._logger.info('Writing file to %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
        return True