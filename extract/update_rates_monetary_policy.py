import pandas as pd
import os
from datetime import datetime, timedelta, date
import pdblp
import settings
from helpers.aux_functions import upload_dataframe_to_postgresql
from helpers.aux_functions import *


class BbgRatesMonetaryPolicyUpdatePrices():
    CLASS_NAME = 'Rates MP'

    def __init__(self) -> None:
        # Open connection
        self.con = pdblp.BCon(debug=False, port=8194, timeout=60000).start()

    def update(self, ticker_list=[]):
        """
        ticker_list: Specific list of index to extract.
        """
        df = execute_postgresql_query(f"""
            SELECT DISTINCT 
                ticker, 
                max("date") AS max_date
            FROM public.{settings.BBG_RATES_MONETARY_POLICY_TABLE}
            GROUP BY ticker;
        """)
        for start_date, ticker_list in df.groupby('max_date')['ticker'].apply(list).items():
            start_date = (pd.to_datetime(start_date) - pd.DateOffset(days=1)).strftime('%Y%m%d')
            # PX_Last
            print('[DOWNLOAD] PX_Last')
            px_last_df = self.con.bdh(ticker_list, 'PX_Last', start_date=start_date, end_date=datetime.now().strftime('%Y%m%d'), elms=[("periodicitySelection", "DAILY")])
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
    bbg_update_prices = BbgRatesMonetaryPolicyUpdatePrices().update()
    print(f"[END] {datetime.now()}")
    pass