from new_automations.bbg_equity_extract import BbgEquityExtract
from new_automations.bbg_equity_index_extract import BbgEquityIndexExtract
from new_automations.bbg_rates_extract import BbgRatesExtract
from new_automations.index_transform import index_transform
from new_automations.index_sheet_v2 import update_index_sheets
from datetime import datetime, date, timedelta
import pandas as pd
import settings


# Update Index data
def update_index_data():
    # start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    start_date = '20240610'
    equity_index_extract = BbgEquityIndexExtract(start_date=start_date).extract()
    return True
# Update Equity data
def update_equity_data():
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    equity_extract = BbgEquityExtract(start_date=start_date).extract()
    return True

def update_rates_data():
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y%m%d')
    # start_date = '19900101'
    rates_extract = BbgRatesExtract(start_date=start_date).extract()
    return True

def transform_equity_index():
    # Equity Index transform
    tickers_df = pd.read_excel(f"{settings.DRIVE_BLOOMBERG_PATH}\\Dict_bbg.xlsx", sheet_name='Tickers')
    index_list = tickers_df[tickers_df['class'] == 'Equity Index']['bbg_ticker'].drop_duplicates().to_list()
    print('[START]', datetime.now().strftime('%H:%M:%S'))
    for index in index_list:
        print(f"START: {datetime.now().strftime('%H:%M:%S')}", index)
        index_transform(index)
        print(f"END: {datetime.now().strftime('%H:%M:%S')}", index)
    print('[END]', datetime.now().strftime('%H:%M:%S'))
    return True

def handle_update_index_sheets():
    return update_index_sheets()

if __name__ == '__main__':
    # Execute all after bbg routine.

    # Update 
    """
        Create Index.xlsx for each index paste on network.
    """
    print(f"[START] transform_equity_index {datetime.now().strftime('%H:%M:%S')}")
    transform_equity_index()
    print(f"[END] transform_equity_index {datetime.now().strftime('%H:%M:%S')}")

    # Update index sheet
    """
        Create xlsx concat all xlsx on network.
    """
    print(f"[START] handle_update_index_sheets {datetime.now().strftime('%H:%M:%S')}")
    handle_update_index_sheets()
    print(f"[END] handle_update_index_sheets {datetime.now().strftime('%H:%M:%S')}")
    pass