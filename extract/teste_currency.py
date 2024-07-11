import pandas as pd
import os
from datetime import datetime, timedelta, date
import pdblp
import settings
from helpers.aux_functions import upload_dataframe_to_postgresql
from helpers.aux_functions import *

# TODO Quebrar extraçao currrency por periodos
class BbgCurrencyUpdatePrices():
    CLASS_NAME = 'Currency'
    CHUNK_SIZE = 10
    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, port=8194, timeout=60000).start()
        self.dict_path = f'{settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.start_date = start_date
        self.end_date = end_date

    def __get_current_account_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Currency')
        return tickers_df['CA'].drop_duplicates().to_list()
    
    def __get_current_price_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Currency')
        return tickers_df['Current price'].drop_duplicates().to_list()
    
    def __get_implied_volatility_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Currency')
        return tickers_df['Implied Vol 1m'].drop_duplicates().to_list() + tickers_df['Implied Vol 3m'].drop_duplicates().to_list() + tickers_df['Implied Vol 6m'].drop_duplicates().to_list() + tickers_df['Implied Vol 12m'].drop_duplicates().to_list()
    
    def __get_points_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Currency')
        return tickers_df['Points 1m'].drop_duplicates().to_list() + tickers_df['Points 3m'].drop_duplicates().to_list() + tickers_df['Points 6m'].drop_duplicates().to_list() + tickers_df['Points 12m'].drop_duplicates().to_list()

    def update_current_account(self):
        tickers = self.__get_current_account_tickers()
        for i in range(0, len(tickers), self.CHUNK_SIZE):
            ticker_list = tickers[i: i+self.CHUNK_SIZE]
            # PX_Last
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
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_CURRENCY_CURRENT_ACCOUNT_TABLE)
        return True
    
    def update_current_price(self):
        tickers = self.__get_current_price_tickers()
        for i in range(0, len(tickers), self.CHUNK_SIZE):
            ticker_list = tickers[i: i+self.CHUNK_SIZE]
            # PX_Last
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
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_CURRENCY_CURRENT_PRICE_TABLE)
        return True
    
    def update_implied_volatility(self):
        tickers = self.__get_implied_volatility_tickers()
        for i in range(0, len(tickers), self.CHUNK_SIZE):
            ticker_list = tickers[i: i+self.CHUNK_SIZE]
            # PX_Last
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
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_CURRENCY_IMPLIED_VOLATILITY_TABLE)
        return True
    
    def update_points(self):
        tickers = self.__get_points_tickers()
        for i in range(0, len(tickers), self.CHUNK_SIZE):
            ticker_list = tickers[i: i+self.CHUNK_SIZE]
            # PX_Last
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
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_CURRENCY_POINTS_TABLE)
        return True

    def update(self):
        # self.update_current_account()
        # self.update_current_price()
        # self.update_implied_volatility()
        self.update_points()
        self.con.stop()
        return True

if __name__ == '__main__':
    print(f"[START] {datetime.now()}")
    bbg_update_prices = BbgCurrencyUpdatePrices(
        start_date='20240611',
        end_date='20240630'
    ).update()
    print(f"[END] {datetime.now()}")
    pass