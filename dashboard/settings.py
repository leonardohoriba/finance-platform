import os
from sqlalchemy import create_engine
from urllib.parse import quote_plus

root = os.getcwd()

DRIVE_BLOOMBERG_PATH = 'Pesquisa Sistemática\\Bloomberg Data'
DATALAKE_PATH = 'Datalake'
DICT_BBG_PATH = os.path.join(root, 'utils', 'Dict_bbg.xlsx')
ADD_IN_TEMPLATE_PATH = os.path.join(root, 'utils', 'template.xlsm')
ENGINE = create_engine(f'postgresql://{quote_plus(os.environ["DATABASE_USER"])}:{quote_plus(os.environ["DATABASE_PASSWORD"])}@{quote_plus(os.environ["DATABASE_HOST"])}:{quote_plus(os.environ["DATABASE_PORT"])}/{quote_plus(os.environ["DATABASE_NAME"])}')

## DATABASE
SCHEMA = 'public'
BBG_DICT_FIELDS = 'bbg_dict_fields'
BBG_DICT_TICKERS = 'bbg_dict_tickers'
BBG_DICT_OVERRIDES = 'bbg_dict_overrides'
BBG_EQUITY_TABLE = 'bbg_equity'
BBG_EQUITY_INDEX_TABLE = 'bbg_equity_index'