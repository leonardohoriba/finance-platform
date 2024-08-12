import pandas as pd
from utils.settings import *
from datetime import datetime, date, timedelta
import os
from helpers.aux_functions import execute_postgresql_query, upload_dataframe_to_postgresql


class UpdateDictionary():
    def __init__(self) -> None:
        self.tickers_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Tickers')
        self.fields_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Fields')
        self.overrides_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Overrides')
        self.currency_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Currency')
        self.rates_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Rates')
        self.rates_monetary_policy_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Rates MP')
        self.bonds_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Bonds')
        self.housing_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Housing')
        self.commodities_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Commodities')
        self.economics_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Economics')
        self.proprietary_economic_index_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Proprietary Economic Index')
        self.update_equity_vol_assets_df = pd.read_excel(DICT_BBG_PATH, sheet_name='Equity Vol Assets')
        # Factset
        self.fs_fields_df = pd.read_excel(DICT_BBG_PATH, sheet_name='FS_Fields')
        # Datastream
        self.ds_economics_df = pd.read_excel(DICT_BBG_PATH, sheet_name='DS_Economics')
        pass

    def update_tickers(self):
        # Update dict tickers
        tickers_df = self.tickers_df.rename(columns={
            'bbg_ticker': 'ticker',
            'GICS_SECTOR_NAME': 'gics_sector_name',
            'GICS_INDUSTRY_NAME': 'gics_industry_name',
            'GICS_SUB_INDUSTRY_NAME': 'gics_sub_industry_name',
        })
        upload_dataframe_to_postgresql(tickers_df, table_name='bbg_dict_tickers', if_exists='replace')
        return True

    def update_fields(self):
        # Update dict Fields
        fields_df = self.fields_df.replace('N', False).replace('Y', True)
        fields_df = fields_df.rename(columns={
            'bbg_fields': 'field',
            'bbg_per': 'period',
        })
        fields_df = fields_df[['field', 'class', 'period', 'override', 'annualized', 'aggregation_type', 'ratio']]
        upload_dataframe_to_postgresql(fields_df, table_name='bbg_dict_fields', if_exists='replace')
        return True

    def update_overrides(self):
        # Update dict overrides
        overrides_df = self.overrides_df.rename(columns={
            'bbg_override': 'override',
            'bbg_override_period': 'period',
        })
        upload_dataframe_to_postgresql(overrides_df, table_name='bbg_dict_overrides', if_exists='replace')
        return True

    def update_currency(self):
        # Update dict currency
        upload_dataframe_to_postgresql(self.currency_df, table_name='bbg_dict_currency', if_exists='replace')
        return True


    def update_rates(self):
        # Update dict rates
        upload_dataframe_to_postgresql(self.rates_df, table_name='bbg_dict_rates', if_exists='replace')
        return True

    def update_rates_monetary_policy(self):
        # Update dict rates monetary policy
        upload_dataframe_to_postgresql(self.rates_monetary_policy_df, table_name='bbg_dict_rates_monetary_policy', if_exists='replace')
        return True

    def update_bonds(self):
        # Update dict bonds
        upload_dataframe_to_postgresql(self.bonds_df, table_name='bbg_dict_bonds', if_exists='replace')
        return True
    
    def update_housing(self):
        # Update dict housing
        upload_dataframe_to_postgresql(self.housing_df, table_name='bbg_dict_housing', if_exists='replace')
        return True
    
    def update_commodities(self):
        # Update dict commodities
        upload_dataframe_to_postgresql(self.commodities_df, table_name='bbg_dict_commodities', if_exists='replace')
        return True
    
    def update_economics(self):
        # Update dict economics
        upload_dataframe_to_postgresql(self.economics_df, table_name='bbg_dict_economics', if_exists='replace')
        return True
    
    def update_proprietary_economic_index(self):
        # Update dict proprietary_economic_index
        upload_dataframe_to_postgresql(self.proprietary_economic_index_df, table_name='bbg_dict_proprietary_economic_index', if_exists='replace')
        return True
    
    def update_equity_vol_assets(self):
        # Update dict equity_vol_assets
        upload_dataframe_to_postgresql(self.update_equity_vol_assets_df, table_name='bbg_dict_equity_vol_assets', if_exists='replace')
        return True

    # Factset
    def update_fs_fields(self):
        # Update dict Fields
        upload_dataframe_to_postgresql(self.fs_fields_df, table_name='factset_dict_fields', if_exists='replace')
        return True
    
    # Datastream
    def update_ds_economics(self):
        # Update dict Fields
        upload_dataframe_to_postgresql(self.ds_economics_df, table_name='datastream_dict_economics', if_exists='replace')
        return True

    def create_reference_table(self):
        query = """
            -- Equity
            SELECT DISTINCT
                t.ticker,
                t."class",
                CONCAT('public.bbg_equity_', REPLACE(LOWER(COALESCE(t."index", t.ticker)), ' ', '__')) AS "table"
            FROM public.bbg_dict_tickers t
            UNION
            -- Equity Factset
            SELECT DISTINCT
                t.factset_ticker,
                t."class",
                CONCAT('public.factset_equity_', REPLACE(LOWER(COALESCE(t."index", t.factset_ticker)), '-', '__')) AS "table"
            FROM public.bbg_dict_tickers t
            WHERE t."class" = 'Equity Index'
            AND t.factset_ticker IS NOT NULL
            UNION
            -- Bonds
            SELECT DISTINCT
                b.bbg_ticker AS "ticker",
                'Bonds' AS "class",
                'public.bbg_bonds' AS "table"
            FROM public.bbg_dict_bonds b
            UNION
            -- Rates
            SELECT DISTINCT
                r.bbg_ticker AS "ticker",
                'Rates' AS "class",
                'public.bbg_rates' AS "table"
            FROM public.bbg_dict_rates r
            UNION
            -- Rates MP
            SELECT DISTINCT
                rmp.bbg_ticker AS "ticker",
                'Rates MP' AS "class",
                'public.bbg_rates_monetary_policy' AS "table"
            FROM public.bbg_dict_rates_monetary_policy rmp
            UNION
            SELECT DISTINCT
                eva.bbg_ticker AS "ticker",
                'Equity Vol Assets' AS "class",
                CONCAT('public.', eva."table") AS "table"
            FROM public.bbg_dict_equity_vol_assets eva
            UNION
            -- Currency
            (
                WITH tickers AS (
                    SELECT
                        "Current price" AS "ticker",
                       	'Currency' AS "class",
                        'public.bbg_currency_current_price' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Points 1m" AS "ticker",
                       	'Currency' AS "class",
                        'public.bbg_currency_points' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Points 3m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_points' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Points 6m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_points' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Points 12m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_points' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Implied Vol 1m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_implied_volatility' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Implied Vol 3m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_implied_volatility' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Implied Vol 6m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_implied_volatility' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT
                        "Implied Vol 12m" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_implied_volatility' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT DISTINCT
                        "CA" AS "ticker",
                       	'Currency' AS "class",       
                        'public.bbg_currency_current_account' AS "table"
                    FROM public.bbg_dict_currency
                    UNION
                    SELECT DISTINCT
                        "JP Total Return Index vs USD" AS "ticker",
                        'Currency' AS "class",       
                        'public.bbg_jp_total_return_index' AS "table"
                        FROM public.bbg_dict_currency
                    UNION
                    SELECT DISTINCT
                        "BNP Positioning Index" AS "ticker",
                        'Currency' AS "class",       
                        'public.bbg_bnp_fx_positioning_index' AS "table"
                        FROM public.bbg_dict_currency
                    UNION
                    SELECT DISTINCT
                        h.bbg_ticker AS "ticker",
                        'Housing' AS "class",
                        'public.bbg_housing' AS "table"
                    FROM public.bbg_dict_housing h
                    UNION
                    SELECT DISTINCT
                        c.bbg_ticker AS "ticker",
                        'Commodities' AS "class",
                        'public.bbg_commodities' AS "table"
                    FROM public.bbg_dict_commodities c
                    UNION
                    SELECT DISTINCT
                        p.bbg_ticker AS "ticker",
                        'Proprietary Economic Index' AS "class",
                        'public.bbg_proprietary_economic_index' AS "table"
                    FROM public.bbg_dict_proprietary_economic_index p
                    UNION
                    SELECT DISTINCT
                        e.bbg_ticker AS "ticker",
                        'Economics' AS "class",
                        'public.bbg_economics' AS "table"
                    FROM public.bbg_dict_economics e
                    UNION
                    -- Datastream Economics
                    SELECT 
                        ticker,
                        'DS_Economics' AS "class",
                        REPLACE(concat('public.datastream_economics_', LOWER("class")), ' ', '__')  AS "table"
                    FROM public.datastream_dict_economics
                )
                SELECT * FROM tickers t
            )
            ORDER BY ticker, "class";
        """
        df = execute_postgresql_query(query=query)
        upload_dataframe_to_postgresql(df, "bbg_reference_table", if_exists='replace')
        return True
    def update_all(self):
        self.update_tickers()
        self.update_fields()
        self.update_overrides()
        self.update_currency()
        self.update_rates()
        self.update_rates_monetary_policy()
        self.update_bonds()
        self.update_housing()
        self.update_commodities()
        self.update_economics()
        self.update_proprietary_economic_index()
        self.update_equity_vol_assets()
        self.update_fs_fields()
        self.update_ds_economics()
        # Update Add-In Reference Table
        self.create_reference_table()
        return True

if __name__ == '__main__':
    update_dictionary = UpdateDictionary().update_all()