"""Xetra ETL Component"""
from typing import NamedTuple
import logging
from datetime import datetime, timedelta
import pandas as pd
from xetra.common.s3 import S3BucketConnector
from xetra.common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    """
    Class for source configuration data

    src_first_extract_date: determines the date for extracting the source
    src_columns: source column names
    src_col_date: column name for date in source
    src_col_isin: column name for isin in source
    src_col_time: column name for time in source
    src_col_start_price: column name for starting price in source
    src_col_min_price: column name for minimum price in source
    src_col_max_price: column name for maximum price in source
    src_col_traded_vol: column name for traded volumne in source
    """
    src_first_extract_date: str
    src_columns: list
    src_col_date: str
    src_col_isin: str
    src_col_time: str
    src_col_start_price: str
    src_col_min_price: str
    src_col_max_price: str
    src_col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    """
    Class for target configuration data

    trg_col_isin: column name for isin in target
    trg_col_date: column name for date in target
    trg_col_op_price: column name for opening price in target
    trg_col_clos_price: column name for closing price in target
    trg_col_min_price: column name for minimum price in target
    trg_col_max_price: column name for maximum price in target
    trg_col_dail_trad_vol: column name for daily traded volume in target
    trg_col_ch_prev_clos: column name for change to previous day's closing price in target
    trg_key: basic key of target file
    trg_key_date_format: date format of target file key
    trg_format: file format of the target file
    """
    trg_col_isin: str
    trg_col_date: str
    trg_col_op_price: str
    trg_col_clos_price: str
    trg_col_min_price: str
    trg_col_max_price: str
    trg_col_dail_trad_vol: str
    trg_col_ch_prev_clos: str
    trg_key: str
    trg_key_date_format: str
    trg_format: str


class XetraETL():
    """
    Reads the Xetra data, transforms and writes the transformed to target
    """

    def __init__(self, s3_bucket_src: S3BucketConnector,
                 s3_bucket_trg: S3BucketConnector, meta_key: str,
                 src_args: XetraSourceConfig, trg_args: XetraTargetConfig):
        """
        Constructor for XetraTransformer

        :param s3_bucket_src: connection to source S3 bucket
        :param s3_bucket_trg: connection to target S3 bucket
        :param meta_key: used as self.meta_key -> key of meta file
        :param src_args: NamedTouple class with source configuration data
        :param trg_args: NamedTouple class with target configuration data
        """
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(self.src_args.src_first_extract_date,
                                                                                 self.meta_key, self.s3_bucket_trg)
        self.meta_update_list = [date for date in self.extract_date_list if date >= self.extract_date]

    def extract(self):
        """
        Read the source data and concat them into one pandas df
        :return:
        data frame: pandas df with the extracted dates
        """
        self._logger.info('Extracting Xetra source files started...')
        files = [key for date in self.extract_date_list for key in self.s3_bucket_src.list_files_in_prefix(date)]
        if not files:
            data_frame = pd.DataFrame()
        else:
            data_frame = pd.concat([self.s3_bucket_sr.cread_csv_to_df(file) for file in files], ignore_index=True)
        self._logger.info('Extracting Xetra source files finished...')
        return data_frame

    def transform_report1(self, data_frame: pd.DataFrame):
        """
        Applied tranformation to create a repoer 1
        :param data_Frame: pandas sd
        :return: data_frame: transformed pd df
        """
        if data_frame.empty:
            self._logger.info('The df is empty. No transformation will be applied')
            return data_frame
        self._logger.info('Applying transformations to Xetra source data for report 1...')
        # Filtering source columns
        data_frame = data_frame.loc[:, self.src_args.src_columns]
        # Removing rows with missing values
        data_frame.dropna(inplace=True)
        # calc opening price per ISIN and day
        data_frame[self.trg_args.trg_col_op_price] = data_frame.sort_values(by=[self.src_args.src_col_time]).groupby(
            [self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price].transform(
            'first')
        # calc closing price per ISIN and day
        data_frame[self.trg_args.trg_col_clos_price] = data_frame.sort_values(by=[self.src_args.src_col_time]).groupby(
            [self.src_args.src_col_isin, self.src_args.src_col_date])[self.src_args.src_col_start_price].transform(
            'last')
        # agg per ISIn and day -> open & closing price
        # min max price, traded volume
        data_frame = data_frame.groupby([self.src_args.src_col_isin, self.src_args.src_col_date], as_index=False).agg({
            self.trg_args.trg_col_op_price: 'min',
            self.trg_args.trg_col_clos_price: 'min',
            self.trg_args.trg_col_min_price: 'min',
            self.trg_args.trg_col_max_price: 'max',
            self.trg_args.trg_col_dail_trad_vol: 'sum'})
        # change of current days closing price compared to the previous trading days closing price in %
        data_frame[self.trg_args.trg_col_ch_prev_clos] = \
        data_frame.sort_values(by=[self.src_args.src_col_date]).groupby([self.src_args.src_col_isin])[
            self.trg_args.trg_col_op_price].shift(1)
        data_frame[self.trg_args.trg_col_ch_prev_clos] = (data_frame[self.trg_args.trg_col_op_price] - data_frame[
            self.trg_args.trg_col_ch_prev_clos]) / data_frame[
                                                             self.trg_args.trg_col_ch_prev_clos] * 100
        # rounding
        data_frame = data_frame.round(decimals=2)
        # removing the day before extract date
        data_frame = data_frame[data_frame.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info('Applying transformations to Xetra source data finished')
        return data_frame

    def load(self, data_frame: pd.DataFrame):
        """
        Saves a pd df to the target
        :param data_frame: df as input
        """
        # creating target key
        target_key = (
            f'{self.trg_args.trg_key}'
            f'{datetime.today().strftime(self.trg_args.trg_key_date_format)}.'
            f'{self.trg_args.trg_format}'
        )
        # Writing a target
        self.s3_bucket_trg.write_df_to_s3(data_frame, target_key, self.trg_args.trg_format)
        self._logger.info('Xetra target data loaded')
        # updating meta file
        MetaProcess.update_meta_file(self.meta_update_list, self.meta_key, self.s3_bucket_trg)
        self._logger.info('Xetra meta file updated')
        return True

    def etl_report1(self):
        """
        Extract, transform and load
        """
        # extr
        data_frame = self.extract()
        # transform
        data_frame = self.transform_report1(data_frame)
        # load
        self.load(data_frame)
        return True
