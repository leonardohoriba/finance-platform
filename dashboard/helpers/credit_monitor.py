from helpers.aux_functions import calcular_metricas, execute_postgresql_query
import pandas as pd
from datetime import datetime, date, timedelta
import settings


def get_ice_bofa_data():
    dict_df = execute_postgresql_query("""
        SELECT
            *
        FROM public.factset_bonds_ice_bofa_dict;
    """)
    df = execute_postgresql_query("""
        WITH dict AS (
            SELECT
                adj_name,
                geography,
                description
            FROM public.factset_bonds_ice_bofa_dict
        ),
        bonds AS (
            SELECT 
                t1.date, 
                t1.ticker, 
                t1.field, 
                t1.value 
            FROM public.factset_bonds_ice_bofa t1
            JOIN (
                SELECT date, ticker, field, MAX(extraction_date) AS latest_extraction_date
                FROM public.factset_bonds_ice_bofa
                GROUP BY date, ticker, field
            ) t2 ON t1.date = t2.date AND t1.ticker = t2.ticker AND t1.field = t2.field AND t1.extraction_date = t2.latest_extraction_date
            WHERE t1.value IS NOT NULL
            ORDER BY t1.date, t1.ticker, t1.field
        )
        SELECT
            b."date",
            d.description AS ticker,
            b.field,
            b.value
        FROM bonds b
        INNER JOIN dict d ON b.ticker = d.adj_name
        ORDER BY b."date", b.ticker, b.field;
    """
    )
    ML_CONVEX_EFF_df = df[df['field'] == 'ML_CONVEX_EFF'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_DUR_EFF_df = df[df['field'] == 'ML_DUR_EFF'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_OAS_df = df[df['field'] == 'ML_OAS'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_PRICE_df = df[df['field'] == 'ML_PRICE'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_TOT_RET_df = df[df['field'] == 'ML_TOT_RET'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_YTW_df = df[df['field'] == 'ML_YTW'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()
    ML_CPN_MKT_df = df[df['field'] == 'ML_CPN_MKT'].pivot(index='date', columns='ticker', values='value').dropna(how='all').sort_index()

    # Calculate Returns
    ML_CONVEX_EFF_series = ML_CONVEX_EFF_df.apply(lambda col: calcular_metricas(col)).T
    ML_CONVEX_EFF_returns = pd.DataFrame(list(ML_CONVEX_EFF_series), index=ML_CONVEX_EFF_series.index).rename(columns={
            '1D (%)': 'CONVEX_EFF (1D)', 
            '1W (%)': 'CONVEX_EFF (1W)', 
            'MTD (%)': 'CONVEX_EFF (MTD)', 
            '1M (%)': 'CONVEX_EFF (1M)', 
            '3M (%)': 'CONVEX_EFF (3M)', 
            '6M (%)': 'CONVEX_EFF (6M)', 
            '12M (%)': 'CONVEX_EFF (12M)', 
            'YTD (%)': 'CONVEX_EFF (YTD)',
        })
    ML_CONVEX_EFF_returns = ML_CONVEX_EFF_returns*100 # Percentage
    
    ML_DUR_EFF_series = ML_DUR_EFF_df.apply(lambda col: calcular_metricas(col)).T
    ML_DUR_EFF_returns = pd.DataFrame(list(ML_DUR_EFF_series), index=ML_DUR_EFF_series.index).rename(columns={
            '1D (%)': 'DUR_EFF (1D)', 
            '1W (%)': 'DUR_EFF (1W)', 
            'MTD (%)': 'DUR_EFF (MTD)', 
            '1M (%)': 'DUR_EFF (1M)', 
            '3M (%)': 'DUR_EFF (3M)', 
            '6M (%)': 'DUR_EFF (6M)', 
            '12M (%)': 'DUR_EFF (12M)', 
            'YTD (%)': 'DUR_EFF (YTD)',
        })
    ML_DUR_EFF_returns = ML_DUR_EFF_returns*100 # Percentage
    
    ML_OAS_series = ML_OAS_df.apply(lambda col: calcular_metricas(col)).T
    ML_OAS_returns = pd.DataFrame(list(ML_OAS_series), index=ML_OAS_series.index).rename(columns={
            '1D (%)': 'OAS (1D)', 
            '1W (%)': 'OAS (1W)', 
            'MTD (%)': 'OAS (MTD)', 
            '1M (%)': 'OAS (1M)', 
            '3M (%)': 'OAS (3M)', 
            '6M (%)': 'OAS (6M)', 
            '12M (%)': 'OAS (12M)', 
            'YTD (%)': 'OAS (YTD)',
        })
    ML_OAS_returns = ML_OAS_returns*100 # Percentage
    
    ML_PRICE_series = ML_PRICE_df.apply(lambda col: calcular_metricas(col)).T
    ML_PRICE_returns = pd.DataFrame(list(ML_PRICE_series), index=ML_PRICE_series.index).rename(columns={
            '1D (%)': 'P (1D)', 
            '1W (%)': 'P (1W)', 
            'MTD (%)': 'P (MTD)', 
            '1M (%)': 'P (1M)', 
            '3M (%)': 'P (3M)', 
            '6M (%)': 'P (6M)', 
            '12M (%)': 'P (12M)', 
            'YTD (%)': 'P (YTD)',
        })
    ML_PRICE_returns = ML_PRICE_returns*100 # Percentage
    
    ML_TOT_RET_series = ML_TOT_RET_df.apply(lambda col: calcular_metricas(col)).T
    ML_TOT_RET_returns = pd.DataFrame(list(ML_TOT_RET_series), index=ML_TOT_RET_series.index).rename(columns={
            '1D (%)': 'TR (1D)', 
            '1W (%)': 'TR (1W)', 
            'MTD (%)': 'TR (MTD)', 
            '1M (%)': 'TR (1M)', 
            '3M (%)': 'TR (3M)', 
            '6M (%)': 'TR (6M)', 
            '12M (%)': 'TR (12M)', 
            'YTD (%)': 'TR (YTD)',
        })
    ML_TOT_RET_returns = ML_TOT_RET_returns*100 # Percentage
    
    ML_YTW_series = ML_YTW_df.apply(lambda col: calcular_metricas(col)).T
    ML_YTW_returns = pd.DataFrame(list(ML_YTW_series), index=ML_YTW_series.index).rename(columns={
            '1D (%)': 'YTW (1D)', 
            '1W (%)': 'YTW (1W)', 
            'MTD (%)': 'YTW (MTD)', 
            '1M (%)': 'YTW (1M)', 
            '3M (%)': 'YTW (3M)', 
            '6M (%)': 'YTW (6M)', 
            '12M (%)': 'YTW (12M)', 
            'YTD (%)': 'YTW (YTD)',
        })
    ML_YTW_returns = ML_YTW_returns*100 # Percentage
    
    ML_CPN_MKT_series = ML_CPN_MKT_df.apply(lambda col: calcular_metricas(col)).T
    ML_CPN_MKT_returns = pd.DataFrame(list(ML_CPN_MKT_series), index=ML_CPN_MKT_series.index).rename(columns={
            '1D (%)': 'Coupon (1D)', 
            '1W (%)': 'Coupon (1W)', 
            'MTD (%)': 'Coupon (MTD)', 
            '1M (%)': 'Coupon (1M)', 
            '3M (%)': 'Coupon (3M)', 
            '6M (%)': 'Coupon (6M)', 
            '12M (%)': 'Coupon (12M)', 
            'YTD (%)': 'Coupon (YTD)',
        })
    ML_CPN_MKT_returns = ML_CPN_MKT_returns*100 # Percentage

    returns_df = pd.concat([
        pd.DataFrame(ML_PRICE_df.iloc[-1].rename('P')), 
        pd.DataFrame(ML_CONVEX_EFF_df.iloc[-1].rename('CONVEX_EFF')), 
        pd.DataFrame(ML_DUR_EFF_df.iloc[-1].rename('DUR_EFF')), 
        pd.DataFrame(ML_OAS_df.iloc[-1].rename('OAS')), 
        pd.DataFrame(ML_YTW_df.iloc[-1].rename('YTW')), 
        pd.DataFrame(ML_TOT_RET_df.iloc[-1].rename('TR')), 
        pd.DataFrame(ML_CPN_MKT_df.iloc[-1].rename('Coupon')),
        ML_CONVEX_EFF_returns,
        ML_DUR_EFF_returns,
        ML_OAS_returns,
        ML_PRICE_returns,
        ML_TOT_RET_returns,
        ML_YTW_returns,
        ML_CPN_MKT_returns,
    ], axis=1)

    returns_df['P Date'] = pd.to_datetime(ML_PRICE_df.iloc[-1].name).strftime("%Y-%m-%d")
    returns_df = returns_df.reset_index().rename(columns={'ticker': 'Index'})

    # merge with dict
    returns_df = pd.merge(left=returns_df, right=dict_df, left_on='Index', right_on='description')

    return {
        'Convexity Effective': ML_CONVEX_EFF_df,
        'Duration Effective': ML_DUR_EFF_df,
        'OAS': ML_OAS_df,
        'Price': ML_PRICE_df,
        'Total Return': ML_TOT_RET_df,
        'Yield to Worst': ML_YTW_df,
        'Coupon Market Weighted': ML_CPN_MKT_df
    }, returns_df
    