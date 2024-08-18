import pandas as pd
import os
from datetime import datetime, timedelta, date
import pdblp
import utils.settings
from helpers.aux_functions import *


class BbgEquityIndexExtract():
    CLASS_NAME = 'Equity Index'

    def __init__(self, start_date='20000101', end_date=datetime.now().strftime('%Y%m%d')) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_path = f'{utils.settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.index_tickers = self.__get_index_tickers()
        self.fields_df = self.__get_fields_df()
        self.overrides_df = self.__get_overrides_df()
        self.start_date = start_date
        self.end_date = end_date

    def __get_index_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Tickers')
        tickers_df = tickers_df[tickers_df['class'] == self.CLASS_NAME]
        return tickers_df['bbg_ticker'].to_list()

    def __get_fields_df(self):
        fields_df = pd.read_excel(self.dict_path, sheet_name='Fields')
        fields_df = fields_df[fields_df['class'] == self.CLASS_NAME]
        return fields_df
    
    def __get_overrides_df(self):
        overrides_df = pd.read_excel(self.dict_path, sheet_name='Overrides')
        return overrides_df

    def extract(self, index_list=[]):
        """
        index_list: Specific list of index to extract.
        """
        for ticker in (index_list or self.index_tickers):
            ## Download Data
            print(ticker)

            # ESTIMATE
            print('[DOWNLOAD] Estimate Data')
            estimate_df = pd.DataFrame()
            df_aux = self.fields_df[self.fields_df['override'] == 'Y']
            df_aux = df_aux[df_aux['bbg_per'] == 'DAILY']
            estimate_fields = df_aux['bbg_fields'].to_list()
            if estimate_fields:
                for ovr in self.overrides_df['bbg_override_period'].to_list():
                    temp_df = self.con.bdh(ticker, estimate_fields, self.start_date, self.end_date, elms=[("periodicitySelection", "DAILY")], ovrds=[('BEST_FPERIOD_OVERRIDE', ovr)])
                    temp_df = temp_df.rename(columns={field: f'{field}_{ovr}' for field in estimate_fields}, level=1)
                    estimate_df = pd.concat([estimate_df, temp_df], axis=1)
            if not estimate_df.empty: 
                # Upload Database
                estimate_df = estimate_df.melt(ignore_index=False).dropna(subset=['value'])
                estimate_df = estimate_df.reset_index()
                estimate_df['extraction_date'] = datetime.now()
                estimate_df['override_period'] = estimate_df['field'].apply(lambda field:
                                field.split('_')[-1])
                estimate_df['field'] = estimate_df['field'].apply(lambda field:
                                '_'.join(field.split('_')[:-1]))
                estimate_df['override'] = 'BEST_FPERIOD_OVERRIDE'
                estimate_ticker_list = estimate_df['ticker'].drop_duplicates().dropna().to_list()
                for estimate_ticker in estimate_ticker_list:
                    estimate_aux_df = estimate_df[estimate_df['ticker'] == estimate_ticker]
                    upload_dataframe_to_postgresql(estimate_aux_df, table_name=f"bbg_equity_{estimate_ticker.lower().replace(' ', '__')}")


            # DAILY
            print('[DOWNLOAD] Daily Data')
            df_aux = self.fields_df[self.fields_df['override'] == 'N']
            df_aux = df_aux[df_aux['bbg_per'] == 'DAILY']
            daily_fields = df_aux['bbg_fields'].to_list()
            if daily_fields:
                daily_df = self.con.bdh(ticker, daily_fields, self.start_date, self.end_date, elms=[("periodicitySelection", "DAILY")])
                if not daily_df.empty: 
                    # Upload Database
                    daily_df = daily_df.melt(ignore_index=False).dropna(subset=['value'])
                    daily_df = daily_df.reset_index()
                    daily_df['extraction_date'] = datetime.now()
                    daily_ticker_list = daily_df['ticker'].drop_duplicates().dropna().to_list()
                    for daily_ticker in daily_ticker_list:
                        daily_aux_df = daily_df[daily_df['ticker'] == daily_ticker]
                        upload_dataframe_to_postgresql(daily_aux_df, table_name=f"bbg_equity_{daily_ticker.lower().replace(' ', '__')}")
                    
                    # Upload database variations
                    ## Only for Total Return at the moment
                    if 'TOT_RETURN_INDEX_GROSS_DVDS' in daily_df['field'].dropna().to_list(): 
                        variation_df = daily_df[daily_df['field'] == 'TOT_RETURN_INDEX_GROSS_DVDS'].sort_values('date')
                        variation_df['value'] = variation_df['value'].pct_change()
                        variation_df = variation_df.dropna(subset=['value'])
                        variation_ticker_list = variation_df['ticker'].drop_duplicates().dropna().to_list()
                        variation_df['field'] = variation_df['field'] + "_VARIATION"
                        for variation_ticker in variation_ticker_list:
                            variation_aux_df = variation_df[variation_df['ticker'] == variation_ticker]
                            upload_dataframe_to_postgresql(variation_aux_df, table_name=f"bbg_equity_{variation_ticker.lower().replace(' ', '__')}")
            
            # QUARTERLY
            print('[DOWNLOAD] Quarterly Data')
            df_aux = self.fields_df[self.fields_df['override'] == 'N']
            df_aux = df_aux[df_aux['bbg_per'] == 'QUARTERLY']
            quarterly_fields = df_aux['bbg_fields'].to_list()
            if quarterly_fields:
                quarterly_df = self.con.bdh(ticker, quarterly_fields, self.start_date, self.end_date, elms=[("periodicitySelection", "QUARTERLY")])
                if not quarterly_df.empty: 
                    # Upload Database
                    quarterly_df = quarterly_df.melt(ignore_index=False).dropna(subset=['value'])
                    quarterly_df = quarterly_df.reset_index()
                    quarterly_df['extraction_date'] = datetime.now()
                    quarterly_ticker_list = quarterly_df['ticker'].drop_duplicates().dropna().to_list()
                    for quarterly_ticker in quarterly_ticker_list:
                        quarterly_aux_df = quarterly_df[quarterly_df['ticker'] == quarterly_ticker]
                        upload_dataframe_to_postgresql(quarterly_aux_df, table_name=f"bbg_equity_{quarterly_ticker.lower().replace(' ', '__')}")
        
        self.con.stop()
        return True

class BbgRatesUpdatePrices():
    CLASS_NAME = 'Rates'
    CHUNK_SIZE = 30
    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_path = f'{utils.settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.start_date = start_date
        self.end_date = end_date

    def __get_rates_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Rates')
        return tickers_df['bbg_ticker'].drop_duplicates().to_list()

    def extract(self, ticker_list=[]):
        tickers = self.__get_rates_tickers()
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_RATES_TABLE)
        self.con.stop()
        return True
    
class BbgRatesMonetaryPolicyUpdatePrices():
    CLASS_NAME = 'Rates MP'
    CHUNK_SIZE = 30
    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_path = f'{utils.settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.start_date = start_date
        self.end_date = end_date

    def __get_rates_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name='Rates MP')
        return tickers_df['bbg_ticker'].drop_duplicates().to_list()

    def extract(self, ticker_list=[]):
        """
        ticker_list: Specific list of index to extract.
        """
        tickers = self.__get_rates_tickers()
        for i in range(0, len(tickers), self.CHUNK_SIZE):
            ticker_list = tickers[i: i+self.CHUNK_SIZE]
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_RATES_MONETARY_POLICY_TABLE)
        self.con.stop()
        return True

class BbgHousingUpdatePrices():
    CLASS_NAME = 'Housing'
    CHUNK_SIZE = 5
    def __init__(self, start_date=datetime.now().strftime('%Y%m%d'), end_date=datetime.now().strftime('%Y%m%d')) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_df = pd.read_excel(utils.settings.DICT_BBG_PATH, sheet_name=self.CLASS_NAME)
        self.start_date = start_date
        self.end_date = end_date

    def extract(self):
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
        # PX_Last
        print('[DOWNLOAD] PX_Last')
        for i in range(0, len(daily_tickers_list), self.CHUNK_SIZE):
            chunked_daily_tickers_list = daily_tickers_list[i:i+self.CHUNK_SIZE]
            print(chunked_daily_tickers_list)
            px_last_df = self.con.bdh(chunked_daily_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "DAILY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_HOUSING_TABLE)

        
        # WEEKLY
        # PX_Last
        print('[DOWNLOAD] PX_Last')
        for i in range(0, len(weekly_tickers_list), self.CHUNK_SIZE):
            chunked_weekly_tickers_list = weekly_tickers_list[i:i+self.CHUNK_SIZE]
            print(chunked_weekly_tickers_list)
            px_last_df = self.con.bdh(chunked_weekly_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "WEEKLY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_HOUSING_TABLE)
    
        # MONTHLY
        # PX_Last
        print('[DOWNLOAD] PX_Last')
        for i in range(0, len(monthly_tickers_list), self.CHUNK_SIZE):
            chunked_monthly_tickers_list = monthly_tickers_list[i:i+self.CHUNK_SIZE]
            print(chunked_monthly_tickers_list)
            px_last_df = self.con.bdh(chunked_monthly_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "MONTHLY")])
            if not px_last_df.empty: 
                px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                for index in px_last_wide_df:
                    index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                    index_df['ticker'] = index
                    index_df['field'] = 'PX_Last'
                    index_df['extraction_date'] = datetime.now()
                    index_df = index_df.dropna(subset=['value'])
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_HOUSING_TABLE)
        self.con.stop()
        return True

class BbgEconomicsUpdatePrices():
    CLASS_NAME = 'Economics'
    CHUNK_SIZE = 30
    def __init__(
            self, 
            start_date=datetime.now().strftime('%Y%m%d'), 
            end_date=datetime.now().strftime('%Y%m%d')
        ) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_df = pd.read_excel(utils.settings.DICT_BBG_PATH, sheet_name=self.CLASS_NAME)
        self.start_date = start_date
        self.end_date = end_date

    def extract(self):
        """
        ticker_list: Specific list of index to extract.
        """
        daily_df = self.dict_df[self.dict_df['period'] == 'D']
        monthly_df = self.dict_df[self.dict_df['period'] == 'M']
        quarterly_df = self.dict_df[self.dict_df['period'] == 'Q']
        yearly_df = self.dict_df[self.dict_df['period'] == 'Y']

        daily_tickers_list = daily_df['bbg_ticker'].dropna().drop_duplicates().to_list()
        monthly_tickers_list = monthly_df['bbg_ticker'].dropna().drop_duplicates().to_list()
        quarterly_tickers_list = quarterly_df['bbg_ticker'].dropna().drop_duplicates().to_list()
        yearly_tickers_list = yearly_df['bbg_ticker'].dropna().drop_duplicates().to_list()

        # DAILY
        if len(daily_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] DAILY')
            for i in range(0, len(daily_tickers_list), self.CHUNK_SIZE):
                ticker_list = daily_tickers_list[i: i+self.CHUNK_SIZE]
                print(ticker_list)
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
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_ECONOMICS_TABLE)

        # MONTHLY
        if len(monthly_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] MONTHLY')
            for i in range(0, len(monthly_tickers_list), self.CHUNK_SIZE):
                ticker_list = monthly_tickers_list[i: i+self.CHUNK_SIZE]
                print(ticker_list)
                px_last_df = self.con.bdh(ticker_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "MONTHLY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_ECONOMICS_TABLE)
        
        # QUARTERLY
        if len(quarterly_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] QUARTERLY')
            for i in range(0, len(quarterly_tickers_list), self.CHUNK_SIZE):
                ticker_list = quarterly_tickers_list[i: i+self.CHUNK_SIZE]
                print(ticker_list)
                px_last_df = self.con.bdh(ticker_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "QUARTERLY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_ECONOMICS_TABLE)
        
        # YEARLY
        if len(yearly_tickers_list) > 0:
            # PX_Last
            print('[DOWNLOAD] YEARLY')
            for i in range(0, len(yearly_tickers_list), self.CHUNK_SIZE):
                ticker_list = yearly_tickers_list[i: i+self.CHUNK_SIZE]
                print(ticker_list)
                px_last_df = self.con.bdh(ticker_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "YEARLY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_ECONOMICS_TABLE)
        self.con.stop()
        return True

class BbgCurrencyUpdatePrices():
    CLASS_NAME = 'Currency'
    CHUNK_SIZE = 10
    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_path = f'{utils.settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
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

    def extract_current_account(self):
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_CURRENCY_CURRENT_ACCOUNT_TABLE)
        return True
    
    def extract_current_price(self):
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_CURRENCY_CURRENT_PRICE_TABLE)
        return True
    
    def extract_implied_volatility(self):
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_CURRENCY_IMPLIED_VOLATILITY_TABLE)
        return True
    
    def extract_points(self):
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_CURRENCY_POINTS_TABLE)
        return True

    def extract(self):
        # self.update_current_account()
        # self.update_current_price()
        # self.update_implied_volatility()
        self.update_points()
        self.con.stop()
        return True

class BbgCommoditiesUpdatePrices():
    CLASS_NAME = 'Commodities'
    CHUNK_SIZE = 30
    def __init__(
            self, 
            start_date=datetime.now().strftime('%Y%m%d'), 
            end_date=datetime.now().strftime('%Y%m%d')
        ) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_df = pd.read_excel(utils.settings.DICT_BBG_PATH, sheet_name=self.CLASS_NAME)
        self.start_date = start_date
        self.end_date = end_date

    def extract(self):
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
            # PX_Last
            for i in range(0, len(daily_tickers_list), self.CHUNK_SIZE):
                chunked_daily_tickers_list = daily_tickers_list[i: i+self.CHUNK_SIZE]
                print(chunked_daily_tickers_list)
                px_last_df = self.con.bdh(chunked_daily_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "DAILY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_COMMODITIES_TABLE)

        
        # WEEKLY
        if len(weekly_tickers_list) > 0:
            # PX_Last
            for i in range(0, len(weekly_tickers_list), self.CHUNK_SIZE):
                chunked_weekly_tickers_list = weekly_tickers_list[i: i+self.CHUNK_SIZE]
                print(chunked_weekly_tickers_list)
                px_last_df = self.con.bdh(chunked_weekly_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "WEEKLY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_COMMODITIES_TABLE)
    
        # MONTHLY
        if len(monthly_tickers_list) > 0:
            # PX_Last
            for i in range(0, len(monthly_tickers_list), self.CHUNK_SIZE):
                chunked_monthly_tickers_list = monthly_tickers_list[i: i+self.CHUNK_SIZE]
                print(chunked_monthly_tickers_list)
                px_last_df = self.con.bdh(chunked_monthly_tickers_list, 'PX_Last', start_date=self.start_date, end_date=self.end_date, elms=[("periodicitySelection", "MONTHLY")])
                if not px_last_df.empty: 
                    px_last_df = px_last_df.melt(ignore_index=False).dropna(subset=['value'])
                    px_last_wide_df = px_last_df.pivot(columns='ticker', values='value')
                    for index in px_last_wide_df:
                        index_df = px_last_wide_df[index].reset_index().rename(columns={index: 'value'})
                        index_df['ticker'] = index
                        index_df['field'] = 'PX_Last'
                        index_df['extraction_date'] = datetime.now()
                        index_df = index_df.dropna(subset=['value'])
                        upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_COMMODITIES_TABLE)
        self.con.stop()
        return True

class BbgBondsUpdatePrices():
    CLASS_NAME = 'Bonds'
    CHUNK_SIZE = 5
    def __init__(self, start_date, end_date) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, host='host.docker.internal', port=8194, timeout=60000).start()
        self.dict_path = f'{utils.settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
        self.start_date = start_date
        self.end_date = end_date

    def __get_bonds_tickers(self):
        # Get Dict Data
        tickers_df = pd.read_excel(self.dict_path, sheet_name=self.CLASS_NAME)
        return tickers_df['bbg_ticker'].drop_duplicates().to_list()
    
    def extract(self, ticker_list=[]):
        tickers = self.__get_bonds_tickers()
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
                    upload_dataframe_to_postgresql(index_df, table_name=utils.settings.BBG_BONDS_TABLE)
        self.con.stop()
        return True
