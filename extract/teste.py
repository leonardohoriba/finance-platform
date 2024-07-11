import pandas as pd
import os
from datetime import datetime, timedelta, date
import pdblp
import settings
from helpers.aux_functions import upload_dataframe_to_postgresql


class BbgEquityIndexExtract():
    CLASS_NAME = 'Equity Index'

    def __init__(self, start_date='20000101', end_date=datetime.now().strftime('%Y%m%d')) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, port=8194, timeout=60000).start()
        self.dict_path = f'{settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx'
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
    
    def __get_output_path(self, ticker: str):
        output_path = f'{settings.DATALAKE_PATH}\\{datetime.now().strftime("%Y%m%d")}'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        output_path = f'{output_path}\{self.CLASS_NAME}'
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        output_path = f'{output_path}\\{ticker}'
        # Create Index Folder
        if not os.path.exists(output_path):
            os.makedirs(output_path)
        return output_path

    def extract(self, index_list=[]):
        """
        index_list: Specific list of index to extract.
        """
        for ticker in (index_list or self.index_tickers):
            # output_path = self.__get_output_path(ticker)

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
                # estimate_df.to_csv(f'{output_path}\\estimate.csv', encoding='utf-8', sep=';')
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
                    # daily_df.to_csv(f'{output_path}\\daily.csv', encoding='utf-8', sep=';')
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

if __name__ == '__main__':
    res = BbgEquityIndexExtract(start_date='20240611', end_date='20240630').extract()