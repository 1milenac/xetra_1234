"""
TestS3BucketConnectorMethods
"""
import os
import unittest
import boto3
from moto import mock_s3
import pandas as pd
from xetra.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
    """
    Testing the S3BucketConnector class
    """

    def setUp(self):
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
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        # Creating a testing instance
        self.s3_bucket_conn = S3BucketConnector(
            self._s3_access_key, self._s3_secret_key, self.s3_endpoint_url, self.s3_bucket_name
        )

    def tearDown(self):
        """
        Executing after unitests
        """
        # mocking s3 connection stop
        self.mock_s3.stop()

    def test_list_files_in_prefix_ok(self):
        """
        Tests the list_files_in_prefix method for getting a list of files based on a prefix
        """
        # Expected results
        prefix_exp = 'prefix/'
        key1_exp = f'{prefix_exp}test1.csv'
        key2_exp = f'{prefix_exp}test2.csv'
        # Test init
        csv_content = """ col1,col2
        valA, valB
        """
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key1_exp
                    },
                    {
                        'Key': key2_exp
                    }
                ]
            }
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        """
        Tests the list_files_in_prefix method in case of wrong prefix
        """
        # Expected results
        prefix_exp = 'no-prefix/'
        # Method execution
        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)
        # Tests after method execution
        self.assertTrue(not list_result)

    def test_read_csv_to_df(self):
        """
        Tests the read_csv_to_df method
        """
        # Expected results
        key_exp = 'test.csv'
        col1_exp = 'col1'
        col2_exp = 'col2'
        val1_exp = 'val1'
        val2_exp = 'val2'
        log_exp = f'Reading file {self.s3_endpoint_url}/{self.s3_bucket_name}/{key_exp}'
        # Test init
        csv_content = f'{col1_exp},{col2_exp}\n{val1_exp},{val2_exp}'
        self.s3_bucket.put_object(Body=csv_content, Key=key_exp)
        # Method execution
        with self.assertLogs() as logm:
            df_result = self.s3_bucket_conn.read_csv_to_df(key_exp)
            # log test after the method execution
            self.assertIn(log_exp, logm.output[0])
        # Test after method execution
        self.assertEqual(df_result.shape[0], 1)
        self.assertEqual(df_result.shape[1], 2)
        self.assertEqual(val1_exp, df_result[col1_exp][0])
        self.assertEqual(val2_exp, df_result[col2_exp][0])
        # Cleanup after tests
        self.s3_bucket.delete_objects(
            Delete={
                'Objects': [
                    {
                        'Key': key_exp
                    }
                ]
            }
        )

    def test_write_df_to_s3_empty(self):
        """
        Tests the write_df_to_s3 method with an empty df as an inpit
        """
        # Expected results
        return_exp = None
        log_exp = 'The dataframe is empty! No file will be writem'
        # test init
        df_empty = pd.DataFrame()
        key = 'key.csv'
        file_format = 'csv'
        # Method execution
        with self.assertLogs() as logm:
            result = self.s3_bucket_conn.write_df_to_s3(df_empty, key, file_format)
            # log test after method exec
            self.assertIn(log_exp, logm.output[0])
        # test after method exec
        self.assertEqual(return_exp, result)


if __name__ == '__main__':
    unittest.main()
