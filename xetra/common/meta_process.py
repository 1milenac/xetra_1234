"""
Methods for processing the meta file
"""
import collections
from datetime import datetime
from xetra.common.s3 import S3BucketConnector
from xetra.common.constants import MetaProcessFormat
from xetra.common.custom_exceptions import WrongMetaFileException
import pandas as pd

class MetaProcess:
    """
    class for working with the meta file
    """
    @staticmethod
    def update_meta_file(extract_date_list: list, meta_key: str, s3_bucket_meta: S3BucketConnector):
        """
        Updating the meta file with the processing date
        :param extract_date_list: list of dates extracted from the source
        :param meta_key: key of the meta file
        :param s3_bucket_meta: S3BucketConnector for the bucket with meta file
        """
        # creating an empty df using meta file column names
        df_new = pd.DataFrame(columns=[
            MetaProcessFormat.META_SOURCE_DATE_COL.value,
            MetaProcessFormat.META_PROCESS_COL.value
        ])
        # filling up the data column with extract_date_list
        df_new[MetaProcessFormat.META_SOURCE_DATE_COL.value] = extract_date_list
        # filling the processed column
        df_new[MetaProcessFormat.META_PROCESS_COL.value] = \
        datetime.today().strftime(MetaProcessFormat.META_PROCESS_DATE_FORMAT.value)
        try:
            # if meta file exists
            df_old = s3_bucket_meta.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(df_new.columns):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except s3_bucket_meta.session.client('s3').exceptions.NoSuchKey:
            # meta file does not exist
            df_all = df_new
        s3_bucket_meta.write_df_to_s3(df_all, meta_key, MetaProcessFormat.META_FILE_FORMAT.value)
        return True




    @staticmethod
    def return_date_list():
        pass