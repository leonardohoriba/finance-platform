import os
from sqlalchemy import create_engine
from decouple import config
from urllib.parse import quote_plus

root = os.getcwd()

# Paths
DRIVE_BLOOMBERG_PATH = os.path.join(root, 'utils')
DATALAKE_PATH = f'Datalake'
DICT_BBG_PATH = os.path.join(root, 'utils', 'Dict_bbg.xlsx')
ADD_IN_TEMPLATE_PATH = os.path.join(root, 'utils', 'template.xlsm')

# Postgres Engine
ENGINE = create_engine(f'postgresql://{quote_plus(config("DATABASE_USER"))}:{quote_plus(config("DATABASE_PASSWORD"))}@{quote_plus(config("DATABASE_HOST"))}:{quote_plus(config("DATABASE_PORT"))}/{quote_plus(config("DATABASE_NAME"))}')

# Namespaces
SCHEMA = 'public'
BBG_DICT_FIELDS = 'bbg_dict_fields'
BBG_DICT_TICKERS = 'bbg_dict_tickers'
BBG_DICT_OVERRIDES = 'bbg_dict_overrides'
BBG_EQUITY_TABLE = 'bbg_equity'
BBG_CURRENCY_CURRENT_PRICE_TABLE = 'bbg_currency_current_price'
BBG_CURRENCY_IMPLIED_VOLATILITY_TABLE = 'bbg_currency_implied_volatility'
BBG_CURRENCY_CURRENT_ACCOUNT_TABLE = 'bbg_current_account'
BBG_CURRENCY_JP_TOTAL_RETURN_INDEX_TABLE = 'bbg_jp_total_return_index'
BBG_CURRENCY_POINTS_TABLE = 'bbg_currency_points'
BBG_EQUITY_VARIATION_TABLE = 'bbg_equity_variation'
BBG_EQUITY_INDEX_TABLE = 'bbg_equity_index'
BBG_EQUITY_INDEX_VARIATION_TABLE = 'bbg_equity_index_variation'
BBG_RATES_TABLE = 'bbg_rates'
BBG_RATES_MONETARY_POLICY_TABLE = 'bbg_rates_monetary_policy'
BBG_BONDS_TABLE = 'bbg_bonds'
BBG_REFERENCE_TABLE = 'bbg_reference_table'
BBG_HOUSING_TABLE = 'bbg_housing'
BBG_COMMODITIES_TABLE = 'bbg_commodities'
BBG_ECONOMICS_TABLE = 'bbg_economics'
