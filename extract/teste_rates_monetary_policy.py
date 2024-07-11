import pandas as pd
import os
from datetime import datetime, timedelta, date
import pdblp
import settings
from helpers.aux_functions import upload_dataframe_to_postgresql
from helpers.aux_functions import *


class BbgRatesMonetaryPolicyUpdatePrices():
    CLASS_NAME = 'Rates MP'

    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, port=8194, timeout=60000).start()
        self.dict_path = f'{settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.start_date = start_date
        self.end_date = end_date

    def __get_rates_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Rates MP')
        return tickers_df['bbg_ticker'].drop_duplicates().to_list()

    def update(self, ticker_list=[]):
        """
        ticker_list: Specific list of index to extract.
        """
        chunk_size = 30
        tickers = self.__get_rates_tickers()
        for i in range(0, len(tickers), chunk_size):
            ticker_list = tickers[i: i+chunk_size]
            print(ticker_list)
            print('[DOWNLOAD] PX_Last')
            px_last_df = self.con.bdh(ticker_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "DAILY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_RATES_MONETARY_POLICY_TABLE)
        return True


if __name__ == '__main__':
    print(f"[START] {datetime.now()}")
    bbg_update_prices = BbgRatesMonetaryPolicyUpdatePrices(
        start_date='20240611',
        end_date='20240630'
    ).update()
    print(f"[END] {datetime.now()}")
    pass