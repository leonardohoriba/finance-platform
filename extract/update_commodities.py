import pandas as pd
from datetime import datetime, date
import pdblp
import settings
from helpers.aux_functions import upload_dataframe_to_postgresql
from helpers.aux_functions import *


class BbgCommoditiesUpdatePrices():
    CLASS_NAME = 'Commodities'

    def __init__(
            self, 
            start_date=(pd.to_datetime(datetime.now()) - pd.DateOffset(days=1 if date.today().weekday() != 0 else 3)).strftime('%Y%m%d'), 
            end_date=datetime.now().strftime('%Y%m%d')
        ) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, port=8194, timeout=60000).start()
        self.dict_df = pd.read_excel(settings.DICT_BBG_PATH, sheet_name=self.CLASS_NAME)
        self.start_date = start_date
        self.end_date = end_date

    def update(self):
        """
        ticker_list: Specific list of index to extract.
        """
        daily_df = self.dict_df[self.dict_df['frequency'] == 'DAILY']
        weekly_df = self.dict_df[self.dict_df['frequency'] == 'WEEKLY']
        monthly_df = self.dict_df[self.dict_df['frequency'] == 'MONTHLY']

        daily_tickers_list = daily_df['bbg_ticker'].dropna().drop_duplicates().to_list()
        weekly_tickers_list = weekly_df['bbg_ticker'].dropna().drop_duplicates().to_list()
        monthly_tickers_list = monthly_df['bbg_ticker'].dropna().drop_duplicates().to_list()

        # DAILY
        if len(daily_tickers_list) > 0:
            daily_start_date = (pd.to_datetime(self.start_date, format='%Y%m%d') - pd.DateOffset(days=1 if date.today().weekday() != 0 else 3)).strftime('%Y%m%d')
            # PX_Last
            print('[DOWNLOAD] PX_Last')
            px_last_df = self.con.bdh(daily_tickers_list, 'PX_Last', start_date=daily_start_date, end_date=self.end_date, elms=[("periodicitySelection", "DAILY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_COMMODITIES_TABLE)

        
        # WEEKLY
        weekly_start_date = (pd.to_datetime(self.start_date, format='%Y%m%d') - pd.DateOffset(weeks=1)).strftime('%Y%m%d')    
        if len(weekly_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] PX_Last')
            px_last_df = self.con.bdh(weekly_tickers_list, 'PX_Last', start_date=weekly_start_date, end_date=self.end_date, elms=[("periodicitySelection", "WEEKLY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_COMMODITIES_TABLE)
    
        # MONTHLY
        monthly_start_date = (pd.to_datetime(self.start_date, format='%Y%m%d') - pd.DateOffset(months=1)).strftime('%Y%m%d')
        if len(monthly_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] PX_Last')
            px_last_df = self.con.bdh(monthly_tickers_list, 'PX_Last', start_date=monthly_start_date, end_date=self.end_date, elms=[("periodicitySelection", "MONTHLY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=settings.BBG_COMMODITIES_TABLE)
        return True


if __name__ == '__main__':
    print(f"[START] {datetime.now()}")
    bbg_update_prices = BbgCommoditiesUpdatePrices().update()
    print(f"[END] {datetime.now()}")
    pass