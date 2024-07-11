from new_automations.index_transform import index_transform
from new_automations.index_sheet_v2 import update_index_sheets
from datetime import datetime, date, timedelta
from helpers.aux_functions import upload_dataframe_to_postgresql, execute_postgresql_query
import pandas as pd
import settings
from new_automations.factset_index_transform import factset_index_transform
from update_dictionary import UpdateDictionary

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

def handle_factset_index_transform():
    return factset_index_transform()

def handle_dict_to_db():
    return UpdateDictionary().update_all()

if __name__ == '__main__':

    # Update dict to db
    print(f"[START] handle_dict_to_db {datetime.now().strftime('%H:%M:%S')}")
    handle_dict_to_db()
    print(f"[END] handle_dict_to_db {datetime.now().strftime('%H:%M:%S')}")


    ## UPDATE FACTSET INDEX TRANSFORM
    print(f"[START] handle_factset_index_transform {datetime.now().strftime('%H:%M:%S')}")
    handle_factset_index_transform()
    print(f"[END] handle_factset_index_transform {datetime.now().strftime('%H:%M:%S')}")

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