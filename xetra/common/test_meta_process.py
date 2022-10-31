import unittest
import os
import unittest
import boto3
from moto import mock_s3
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess
from xetra.common.constants import MetaProcessFormat
from datetime import datetime, timedelta


class TestMetaProcessMethods(unittest.TestCase):
    """
    Testing the MetaProcess class
    --in progress--
    """

    def setUp(self) -> None:
        """
                Setting up the environment
                """
        # mocking s3 connecton start
        self.mock_s3 = mock_s3()
        self.mock_s3.start()
        # Defining class arg
        self._s3_access_key = 'AWS_ACCESS_KEY_ID'
        self._s3_secret_key = 'AWS_SECRET_ACESS_KEY'
        self.s3_endpoint_url = 'https://s3.eu-central-1.amazonaws.com'
        self.s3_bucket_name = 'test_bucket'
        # Creating s3 access keys as environemntal variables
        os.environ[self._s3_access_key] = 'KEY1'
        os.environ[self._s3_secret_key] = 'KEY2'
        # Creating a bucket on the mocked S3
        self.s3 = boto3.resource(service_name='s3', endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(Bucket=self.s3_bucket_name,
                              CreateBucketConfiguration={
                                  'LocationConstraint': 'eu-central-1'
                              })
        self.s3_bucket_meta = S3BucketConnector
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(
            self._s3_access_key, self._s3_secret_key, self.s3_endpoint_url, self.s3_bucket_name
        )
        self.dates = [(datetime.today().date() - timedelta(days=day)) \
                          .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(8)]

    def tearDown(self):
        """
        Executing after unitests
        """
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_update_meta_file_no_meta_file(self):
        pass

    def test_update_meta_file_empty_date_list(self):
        pass

    def test_return_date_list_no_meta_file(self):
        """
        Tests teh return_date_list method with no meta file
        """
        # Expected results
        date_list_exp = [
            (datetime.today().date() - timedelta(days=day)) \
                .strftime(MetaProcessFormat.META_DATE_FORMAT.value) for day in range(4)
        ]
        min_date_exp = (datetime.today().date() - timedelta(days=2)).strftime(MetaProcessFormat.META_DATE_FORMAT.value)
        # Test init
        first_date = min_date_exp
        meta_key = 'meta.csv'
        # Method exec
        min_date_return, date_list_return = MetaProcess.return_date_list(first_date, meta_key, self.s3_bucket_meta)
        # Test after method exec
        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        pass

    def test_return_date_list_meta_file_wrong(self):
        pass


