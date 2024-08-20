import streamlit as st
from datetime import datetime, date
from helpers.equity_index_monitor import equity_index_monitor_df, get_rates
import pandas as pd
import numpy as np
from PIL import Image
import matplotlib.pyplot as plt
import settings
from helpers.aux_functions import plot_series, plot_boxplot, plot_series_multiple_y_axis, plot_scatter, index_number, start_end_dates, execute_postgresql_query
import plotly.express as px
# Config
logo = Image.open("images/favicon.png")
st.set_page_config(page_title='Equity Index', page_icon=logo, layout='wide')

table_styles = [
    dict(selector="tr:hover", props=[("background", "#8A8A8A")]),
    dict(
        selector="theadth",
        props=[
            ("border", "1pxsolid#000000"),
            ("border-collapse", "collapse"),
            ("text-align", "center"),
            ("font-size", "15px"),
            ("width", "100px"),
        ],
    ),
    dict(
        selector="tbodyth",
        props=[
            ("border", "1pxsolid#000000"),
            ("border-collapse", "collapse"),
            ("text-align", "center"),
            ("font-size", "15px"),
            ("white-space", "nowrap"),
        ],
    ),
    dict(
        selector="td",
        props=[
            ("padding", "0px0px"),
            ("text-align", "center"),
            ("border-collapse", "collapse"),
            ("font-size", "14px"),
        ],
    ),
]
# Multiselect colors
st.markdown(
    """
<style>
span[data-baseweb="tag"] {
  background-color: #539ecd !important;
}
</style>
""",
    unsafe_allow_html=True,
)

default_columns = [
    'Index',
    'Country',
    'Description',
    'P Date',
    'Cur',
    'P ',
    'TR (1D)',
    'TR (1W)',
    'TR (1M)',
    'TR (YTD)',
    'TR (12M)',
    'P/B ',
    '*E/Y T12M ',
    '*E/Y 1BF ',
    '*E/Y 2BF ',
    'EPS T12M ',
    'EPS 1BF ',
    'EPS 2BF ',
    'Sales T12M ',
    '*EPS/Sales T12M ', 
    '*EPS/Sales 1BF ', 
    '*EPS/Sales 2BF ',
    'EPS 1BF (1M)',
    'EPS 1BF (3M)',
    'EPS 1BF (12M)',
    'EPS 2BF (1M)',
    'EPS 2BF (3M)',
    'EPS 2BF (12M)',
    'EPS T12M (1M)',
    'EPS T12M (3M)',
    'EPS T12M (12M)',
    'ΔEPS 1BF-T12M',
    'ΔEPS 2BF-T12M',
]

# Style
with open('style.css') as f:
    st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html = True)

def highlight_none(val):
    return 'background-color: transparent' if pd.isna(val) else ''

@st.cache_data
def handle_rates_tickers_dict():
    df = execute_postgresql_query(query=f"""
            SELECT * FROM public.bbg_dict_tickers;
    """)
    df = df.rename(columns={
        'ticker': 'bbg_ticker',
        'gics_sector_name': 'GICS_SECTOR_NAME',
        'gics_industry_name': 'GICS_INDUSTRY_NAME',
        'gics_sub_industry_name': 'GICS_SUB_INDUSTRY_NAME',
    })
    df = df[df['class'] == 'Equity Index'][['bbg_ticker', 'CRNCY']]
    rates_df = execute_postgresql_query(query=f"""
            SELECT * FROM public.bbg_dict_rates;
    """)
    merge_df = df.merge(rates_df, left_on='CRNCY', right_on='currency').rename(columns={'bbg_ticker_x': 'bbg_ticker'})
    return merge_df[['bbg_ticker', 'currency']].drop_duplicates().dropna()
rates_tickers_dict_df = handle_rates_tickers_dict()

@st.cache_data
def handle_equity_index_monitor_df():
    (
        heatmap_df,
        PX_Last_df, 
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df, 
        BEST_EPS_1BF_df, 
        BEST_EPS_2BF_df, 
        TRAIL_12M_EPS_df, 
        T12M_DIL_EPS_CONT_OPS_df, 
        PX_TO_BOOK_RATIO_df, 
        PE_RATIO_df, 
        TRAIL_12M_SALES_PER_SH_df, 
        EY_T12M_df, 
        EY_1BF_df, 
        EY_2BF_df,
        EPS_T12M_per_SALES_df,
        EPS_1BF_per_SALES_df,
        EPS_2BF_per_SALES_df,
        DELTA_EPS_1BF_T12M_df,
        DELTA_EPS_2BF_T12M_df,
        EY_T12M_CONT_OPS_df,
        EPS_T12M_CONT_OPS_per_SALES_df,
        DELTA_EPS_1BF_T12M_CONT_OPS_df,
        DELTA_EPS_2BF_T12M_CONT_OPS_df
    ) = equity_index_monitor_df()
    return (
        heatmap_df,
        PX_Last_df, 
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df, 
        BEST_EPS_1BF_df, 
        BEST_EPS_2BF_df, 
        TRAIL_12M_EPS_df, 
        T12M_DIL_EPS_CONT_OPS_df, 
        PX_TO_BOOK_RATIO_df,
        PE_RATIO_df,
        TRAIL_12M_SALES_PER_SH_df, 
        EY_T12M_df, 
        EY_1BF_df, 
        EY_2BF_df,
        EPS_T12M_per_SALES_df,
        EPS_1BF_per_SALES_df,
        EPS_2BF_per_SALES_df,
        DELTA_EPS_1BF_T12M_df,
        DELTA_EPS_2BF_T12M_df,
        EY_T12M_CONT_OPS_df,
        EPS_T12M_CONT_OPS_per_SALES_df,
        DELTA_EPS_1BF_T12M_CONT_OPS_df,
        DELTA_EPS_2BF_T12M_CONT_OPS_df
    )

# Define dataframes
(
    heatmap_df,
    PX_Last_df, 
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df, 
    BEST_EPS_1BF_df, 
    BEST_EPS_2BF_df, 
    TRAIL_12M_EPS_df, 
    T12M_DIL_EPS_CONT_OPS_df, 
    PX_TO_BOOK_RATIO_df,
    PE_RATIO_df,
    TRAIL_12M_SALES_PER_SH_df, 
    EY_T12M_df, 
    EY_1BF_df, 
    EY_2BF_df,
    EPS_T12M_per_SALES_df,
    EPS_1BF_per_SALES_df,
    EPS_2BF_per_SALES_df,
    DELTA_EPS_1BF_T12M_df,
    DELTA_EPS_2BF_T12M_df,
    EY_T12M_CONT_OPS_df,
    EPS_T12M_CONT_OPS_per_SALES_df,
    DELTA_EPS_1BF_T12M_CONT_OPS_df,
    DELTA_EPS_2BF_T12M_CONT_OPS_df
) = handle_equity_index_monitor_df()

# weights_df, economy_df = handle_factset_weight_df()

#
def format_heatmap(df, tr_start_date, tr_end_date):
    global default_columns
    tr_date_delta = (tr_end_date - tr_start_date).days
    
    ### TOT_RETURN Decomposition
    # # Insert flexible TOT_RETURN_INDEX_GROSS_DVDS_VARIATION
    # TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.reindex(pd.date_range(tr_start_date, tr_end_date, freq='D'), method='ffill').ffill()
    # TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series = ((TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_end_date)] / TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_start_date)] - 1)*100).replace(0, np.nan).dropna().rename(f'*TR {tr_date_delta}D')
    # default_columns.insert(7, f'*TR {tr_date_delta}D')
    # df = df.merge(TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series, left_on='bbg_ticker', right_index=True, how='left')
    
    
    # # Insert flexible PE_RATIO
    # PE_RATIO_formated = PE_RATIO_df.reindex(pd.date_range(tr_start_date, tr_end_date, freq='D'), method='ffill').ffill()
    # PE_RATIO_series = np.log((PE_RATIO_formated.loc[pd.to_datetime(tr_end_date)] / PE_RATIO_formated.loc[pd.to_datetime(tr_start_date)]).replace(0, np.nan).dropna()).rename(f'[PE {tr_date_delta}D]') * 100
    # # default_columns.insert(11, f'[PE {tr_date_delta}D]')
    # df = df.merge(PE_RATIO_series, left_on='bbg_ticker', right_index=True, how='left')
    
    # # Insert flexible TRAIL_12M_EPS
    # TRAIL_12M_EPS_formated = TRAIL_12M_EPS_df.reindex(pd.date_range(tr_start_date, tr_end_date, freq='D'), method='ffill').ffill()
    # TRAIL_12M_EPS_series = np.log((TRAIL_12M_EPS_formated.loc[pd.to_datetime(tr_end_date)] / TRAIL_12M_EPS_formated.loc[pd.to_datetime(tr_start_date)]).replace(0, np.nan).dropna()).rename(f'[EPS {tr_date_delta}D]') * 100
    # # default_columns.insert(12, f'[EPS {tr_date_delta}D]')
    # df = df.merge(TRAIL_12M_EPS_series, left_on='bbg_ticker', right_index=True, how='left')
    
    # # Calculated TR
    # calculated_tr_series = (df[f'[PE {tr_date_delta}D]'] + df[f'[EPS {tr_date_delta}D]']).rename(f'[#TR {tr_date_delta}D]')
    # # default_columns.insert(13, f'[#TR {tr_date_delta}D]')
    # df = df.merge(calculated_tr_series, left_index=True, right_index=True, how='left')
    
    # # Insert flexible TOT_RETURN_INDEX_GROSS_DVDS_VARIATION
    # TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.reindex(pd.date_range(tr_start_date, tr_end_date, freq='D'), method='ffill').ffill()
    # TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series =np.log((TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_end_date)] / TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_start_date)]).replace(0, np.nan).dropna()).rename(f'[TR {tr_date_delta}D]') * 100
    # default_columns.insert(11, f'[TR {tr_date_delta}D]')
    # # default_columns.insert(14, f'[TR {tr_date_delta}D]')
    # df = df.merge(TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series, left_on='bbg_ticker', right_index=True, how='left')
    
    # Insert flexible TOT_RETURN_INDEX_GROSS_DVDS_VARIATION
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.reindex(pd.date_range(tr_start_date, tr_end_date, freq='D'), method='ffill').ffill()
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series = (((TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_end_date)] / TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_formated.loc[pd.to_datetime(tr_start_date)]).replace(0, np.nan).dropna()).rename(f'[TR {tr_date_delta}D]') - 1)* 100
    default_columns.insert(11, f'[TR {tr_date_delta}D]')
    df = df.merge(TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_series, left_on='bbg_ticker', right_index=True, how='left')
    
    
    # # Error
    # error_series = (df[f'[TR {tr_date_delta}D]'] - df[f'[#TR {tr_date_delta}D]']).rename(f'[Err {tr_date_delta}D]')
    # default_columns.insert(15, f'[Err {tr_date_delta}D]')
    # df = df.merge(error_series, left_index=True, right_index=True, how='left')

    ### ----
    # EM
    em_df = df[(df['country_classification'] == 'EM') & (~df['Description'].str.contains("MSCI"))].drop(columns=['country_classification', 'bbg_ticker']).sort_values('P (1D)', ascending=False)
    # DM
    dm_df = df[(df['country_classification'] == 'DM') & (~df['Description'].str.contains("MSCI"))].drop(columns=['country_classification', 'bbg_ticker']).sort_values('P (1D)', ascending=False)
    us_df = dm_df[dm_df['Country'] == 'United States']
    us_ticker_list = dm_df[dm_df['Country'] == 'United States']['Index'].dropna().drop_duplicates().to_list()
    filter_us_ticker_list = [ticker for ticker in us_ticker_list if ticker not in [
        'SPX', # S&P500
        'SP5T5', # S&P top 50
        'SPR', # S&P1500
        'NDX' # Nasdaq
    ]]
    dm_df = dm_df[~dm_df['Index'].isin(filter_us_ticker_list)]
    # MSCI
    MSCI_df = df[df['Description'].str.contains("MSCI")].drop(columns=['country_classification', 'bbg_ticker']).sort_values('P (1D)', ascending=False)
    # Concat MSCI EM in em_df
    em_df = pd.concat([MSCI_df[MSCI_df['Index'] == 'MXEF'], em_df], axis=0)
    return (
        em_df, 
        dm_df, 
        MSCI_df, 
        us_df
    )

@st.cache_data
def format_returns(
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df, 
    TRAIL_12M_EPS_df, 
    T12M_DIL_EPS_CONT_OPS_df, 
    BEST_EPS_1BF_df, 
    BEST_EPS_2BF_df):
    # TOT_RETURN_INDEX_GROSS_DVDS
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1W_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(weeks=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1W)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1M_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(months=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1M)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_3M_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(months=3))*100).dropna(how='all').rename(columns=lambda col: f"{col} (3M)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_6M_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(months=6))*100).dropna(how='all').rename(columns=lambda col: f"{col} (6M)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_12M_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(years=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (12M)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df = (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.pct_change(freq=pd.DateOffset(days=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1D)")
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df = pd.concat([
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df,
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1W_df,
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1M_df,
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_3M_df,
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_6M_df,
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_12M_df,], axis=1)
    
    # TRAIL_12M_EPS
    TRAIL_12M_EPS_VARIATION_1M_df = (TRAIL_12M_EPS_df.pct_change(freq=pd.DateOffset(months=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1M)")
    TRAIL_12M_EPS_VARIATION_3M_df = (TRAIL_12M_EPS_df.pct_change(freq=pd.DateOffset(months=3))*100).dropna(how='all').rename(columns=lambda col: f"{col} (3M)")
    TRAIL_12M_EPS_VARIATION_12M_df = (TRAIL_12M_EPS_df.pct_change(freq=pd.DateOffset(years=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (12M)")
    TRAIL_12M_EPS_VARIATIONS_df = pd.concat([
        TRAIL_12M_EPS_VARIATION_1M_df,
        TRAIL_12M_EPS_VARIATION_3M_df,
        TRAIL_12M_EPS_VARIATION_12M_df,], axis=1)
    
    # T12M_DIL_EPS_CONT_OPS
    T12M_DIL_EPS_CONT_OPS_VARIATION_1M_df = (T12M_DIL_EPS_CONT_OPS_df.pct_change(freq=pd.DateOffset(months=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1M)")
    T12M_DIL_EPS_CONT_OPS_VARIATION_3M_df = (T12M_DIL_EPS_CONT_OPS_df.pct_change(freq=pd.DateOffset(months=3))*100).dropna(how='all').rename(columns=lambda col: f"{col} (3M)")
    T12M_DIL_EPS_CONT_OPS_VARIATION_12M_df = (T12M_DIL_EPS_CONT_OPS_df.pct_change(freq=pd.DateOffset(years=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (12M)")
    T12M_DIL_EPS_CONT_OPS_VARIATIONS_df = pd.concat([
        T12M_DIL_EPS_CONT_OPS_VARIATION_1M_df,
        T12M_DIL_EPS_CONT_OPS_VARIATION_3M_df,
        T12M_DIL_EPS_CONT_OPS_VARIATION_12M_df,], axis=1)
    
    # BEST_EPS_1BF
    BEST_EPS_1BF_VARIATION_1M_df = (BEST_EPS_1BF_df.pct_change(freq=pd.DateOffset(months=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1M)")
    BEST_EPS_1BF_VARIATION_3M_df = (BEST_EPS_1BF_df.pct_change(freq=pd.DateOffset(months=3))*100).dropna(how='all').rename(columns=lambda col: f"{col} (3M)")
    BEST_EPS_1BF_VARIATION_12M_df = (BEST_EPS_1BF_df.pct_change(freq=pd.DateOffset(years=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (12M)")
    BEST_EPS_1BF_VARIATIONS_df = pd.concat([
        BEST_EPS_1BF_VARIATION_1M_df,
        BEST_EPS_1BF_VARIATION_3M_df,
        BEST_EPS_1BF_VARIATION_12M_df,], axis=1)
    
    # BEST_EPS_2BF
    BEST_EPS_2BF_VARIATION_1M_df = (BEST_EPS_2BF_df.pct_change(freq=pd.DateOffset(months=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (1M)")
    BEST_EPS_2BF_VARIATION_3M_df = (BEST_EPS_2BF_df.pct_change(freq=pd.DateOffset(months=3))*100).dropna(how='all').rename(columns=lambda col: f"{col} (3M)")
    BEST_EPS_2BF_VARIATION_12M_df = (BEST_EPS_2BF_df.pct_change(freq=pd.DateOffset(years=1))*100).dropna(how='all').rename(columns=lambda col: f"{col} (12M)")
    BEST_EPS_2BF_VARIATIONS_df = pd.concat([
        BEST_EPS_2BF_VARIATION_1M_df,
        BEST_EPS_2BF_VARIATION_3M_df,
        BEST_EPS_2BF_VARIATION_12M_df,], axis=1)
    
    return (
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df,
        TRAIL_12M_EPS_VARIATIONS_df,
        T12M_DIL_EPS_CONT_OPS_VARIATIONS_df,
        BEST_EPS_1BF_VARIATIONS_df,
        BEST_EPS_2BF_VARIATIONS_df
    )

(
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df, 
    TRAIL_12M_EPS_VARIATIONS_df,
    T12M_DIL_EPS_CONT_OPS_VARIATIONS_df,
    BEST_EPS_1BF_VARIATIONS_df, 
    BEST_EPS_2BF_VARIATIONS_df
) = format_returns(
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df, 
    TRAIL_12M_EPS_df, 
    T12M_DIL_EPS_CONT_OPS_df, 
    BEST_EPS_1BF_df, 
    BEST_EPS_2BF_df
)

@st.cache_data
def handle_get_rates():
    return get_rates()
rates_df = handle_get_rates()


# Dataframes
all_equities_dfs = {
    # Bloomberg
    'PX_Last (Bloomberg)': {
        'df': PX_Last_df,
        'name': 'Price'
    },
    'TOT_RETURN_INDEX_GROSS_DVDS - Returns (Bloomberg)': {
        'df': TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df,
        'name': 'TR'
    },
    'TRAIL_12M_EPS (Bloomberg)': {
        'df': TRAIL_12M_EPS_df,
        'name': 'EPS T12M'
    },
    'T12M_DIL_EPS_CONT_OPS (Bloomberg)': {
        'df': T12M_DIL_EPS_CONT_OPS_df,
        'name': 'EPS T12M CONT_OPS'
    },
    'BEST_EPS 1BF (Bloomberg)': {
        'df': BEST_EPS_1BF_df,
        'name': 'EPS 1BF'
    },
    'BEST_EPS 2BF (Bloomberg)': {
        'df': BEST_EPS_2BF_df,
        'name': 'EPS 2BF'
    },
    '*Earnings Yield T12M (Bloomberg)': {
        'df': EY_T12M_df,
        'name': '*E/Y T12M'
    }, 
    '*Earnings Yield 1BF (Bloomberg)': {
        'df': EY_1BF_df,
        'name': '*E/Y 1BF'
    }, 
    '*Earnings Yield 2BF (Bloomberg)': {
        'df': EY_2BF_df,
        'name': '*E/Y 2BF' 
    },
    'PX_TO_BOOK_RATIO (Bloomberg)': {
        'df': PX_TO_BOOK_RATIO_df,
        'name': 'P/B'
    },
    'PE_RATIO (Bloomberg)': {
        'df': PE_RATIO_df,
        'name': 'PE'
    },
    'TRAIL_12M_SALES_PER_SH (Bloomberg)': {
        'df': TRAIL_12M_SALES_PER_SH_df,
        'name': 'Sales T12M'
    },
    'ΔEPS 1BF-T12M (Bloomberg)': {
        'df': DELTA_EPS_1BF_T12M_df,
        'name': 'ΔEPS 1BF-T12M'
    },
    'ΔEPS 2BF-T12M (Bloomberg)': {
        'df': DELTA_EPS_2BF_T12M_df,
        'name': 'ΔEPS 2BF-T12M'
    },
    '*EPS/Sales T12M (Bloomberg)': {
        'df': EPS_T12M_per_SALES_df,
        'name': 'EPS/Sales T12M'
    },
    '*EPS/Sales 1BF (Bloomberg)': {
        'df': EPS_1BF_per_SALES_df,
        'name': 'EPS/Sales 1BF'
    },
    '*EPS/Sales 2BF (Bloomberg)': {
        'df': EPS_2BF_per_SALES_df,
        'name': 'EPS/Sales 2BF'
    },
    'TRAIL_12M_SALES_PER_SH (Bloomberg)': {
        'df': TRAIL_12M_SALES_PER_SH_df,
        'name': 'Sales T12M'
    },
    'TRAIL_12M_EPS - Variations (Bloomberg)': {
        'df': TRAIL_12M_EPS_VARIATIONS_df,
        'name': 'EPS T12M change'
    },
    'T12M_DIL_EPS_CONT_OPS - Variations (Bloomberg)': {
        'df': T12M_DIL_EPS_CONT_OPS_VARIATIONS_df,
        'name': 'EPS T12M CONT_OPS change'
    },
    'BEST_EPS_1BF - Variations (Bloomberg)': {
        'df': BEST_EPS_1BF_VARIATIONS_df,
        'name': 'EPS 1BF change'
    },
    'BEST_EPS_2BF - Variations (Bloomberg)': {
        'df': BEST_EPS_2BF_VARIATIONS_df,
        'name': 'EPS 1BF change'
    },
    '*Earnings Yield T12M CONT_OPS (Bloomberg)': {
        'df': EY_T12M_CONT_OPS_df,
        'name': '*E/Y T12M C/O'
    },
    'EPS_T12M_CONT_OPS_PER_SALES (Bloomberg)': {
        'df': EPS_T12M_CONT_OPS_per_SALES_df,
        'name': 'EPS T12M(C/O)/Sales'
    },
    'ΔEPS 1BF-T12M CONT_OPS (Bloomberg)': {
        'df': DELTA_EPS_1BF_T12M_CONT_OPS_df,
        'name': 'ΔEPS 1BF-T12M(C/O)'
    },
    'ΔEPS 2BF-T12M CONT_OPS (Bloomberg)': {
        'df': DELTA_EPS_2BF_T12M_CONT_OPS_df,
        'name': 'ΔEPS 2BF-T12M(C/O)'
    },
}


# Clear Cache
if st.sidebar.button("Update data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_data.clear()
    st.experimental_rerun()


# Title
st.title('🗺️ Equity Index')


st.divider()
# -=-=-=-=-=-=-=-=-= EM -=-=-=-=-=-=-=-=-=-=
tr_col1, tr_col2 = st.columns(2)
tr_start_date = tr_col1.date_input("TR Start Date", value=np.datetime64(pd.to_datetime(datetime.now())-pd.DateOffset(years=1)).astype(date), min_value=date(1980,1,1), max_value=datetime.now(), key='tr-start_date')
tr_end_date = tr_col2.date_input("TR End Date", value=np.datetime64(pd.to_datetime(datetime.now())).astype(date), min_value=date(1980,1,1), max_value=datetime.now(), key='tr-end_date')
(em_df, dm_df, MSCI_df, us_df) = format_heatmap(heatmap_df, tr_start_date, tr_end_date)

st.header('Emerging Markets (EM)')

em_all_columns = em_df.columns.to_list()
em_default_columns = em_all_columns
em_default_columns = default_columns
# em_default_columns = [x for x in em_all_columns if x not in not_default_columns]
em_fixed_columns = ['Index', 'Country']
em_filtered_df = em_df[em_default_columns]
# Caixa de seleção para escolher quais colunas serão exibidas
em_selected_columns = st.multiselect('EM Columns:', options=em_all_columns, default=list(dict.fromkeys(em_fixed_columns + em_default_columns)), key='em-multiselect-columns')
em_selected_columns = em_fixed_columns + em_selected_columns if not all(ele in em_selected_columns for ele in em_fixed_columns) else em_selected_columns

# Definir ordem dos em_fixed_columns
for index, element in enumerate(em_fixed_columns):
    em_selected_columns.insert(index, element)

em_selected_columns = list(dict.fromkeys(em_selected_columns))

# Verifica se pelo menos uma coluna foi selecionada antes de aplicar o filtro
if em_selected_columns:
    em_filtered_df = em_df[em_selected_columns]
else:
    em_filtered_df = em_df

em_placeholder = st.empty()

em_all = st.checkbox("Select/Clear all", value=True, key='checkbox-em')
em_container = st.container()
 
if em_all:
    em_selected_funds = em_container.multiselect("Countries:",
         em_filtered_df['Country'].drop_duplicates(), em_filtered_df['Country'], key='multiselect-em-all')
else:
    em_selected_funds =  em_container.multiselect("Countries:",
        em_filtered_df['Country'].drop_duplicates(), key='multiselect-em')


# Filtrar o DataFrame com base nas linhas selecionadas
em_filtered_df = em_filtered_df[em_filtered_df['Country'].isin(em_selected_funds)]
em_filtered_df = em_filtered_df.drop_duplicates()

# Inicialize listas vazias para armazenar os valores mínimos e máximos de colunas numéricas
em_valores_minimos = []
em_valores_maximos = []
# Calcule os valores mínimos e máximos de colunas numéricas
for col in em_filtered_df.columns:
    if pd.api.types.is_numeric_dtype(em_filtered_df[col]):
        em_vmin = em_filtered_df[col].min()
        em_vmax = em_filtered_df[col].max()
        em_valores_minimos.append(em_vmin)
        em_valores_maximos.append(em_vmax)
    else:
        em_valores_minimos.append(None)
        em_valores_maximos.append(None)
# Defina o mapa de cores
cmap = plt.colormaps['Blues']
# Crie o estilo de gradiente de fundo com valores em_vmin e em_vmax personalizados
em_styled_df = em_filtered_df.set_index('Index').style
for col, em_vmin, em_vmax in zip(em_filtered_df.columns[1:], em_valores_minimos[1:], em_valores_maximos[1:]):
    if em_vmin is not None and em_vmax is not None:
        em_styled_df = em_styled_df.background_gradient(cmap=cmap, vmin=em_vmin, vmax=em_vmax, subset=col)
        em_styled_df.applymap(highlight_none, subset=col)
em_placeholder.dataframe(em_styled_df.format(precision=1))

st.divider()

# -=-=-=-=-=-=-=-=-= MSCI -=-=-=-=-=-=-=-=-=-=
st.header('MSCI EM')

MSCI_all_columns = MSCI_df.columns.to_list()
MSCI_default_columns = MSCI_all_columns
# MSCI_default_columns = [x for x in MSCI_all_columns if x not in not_default_columns]
MSCI_default_columns = default_columns
MSCI_fixed_columns = ['Index', 'Country']
MSCI_filtered_df = MSCI_df[MSCI_default_columns]

# Caixa de seleção para escolher quais colunas serão exibidas
MSCI_selected_columns = st.multiselect('MSCI Columns:', options=MSCI_all_columns, default=list(dict.fromkeys(MSCI_fixed_columns + MSCI_default_columns)), key='MSCI-multiselect-columns')
MSCI_selected_columns = MSCI_fixed_columns + MSCI_selected_columns if not all(ele in MSCI_selected_columns for ele in MSCI_fixed_columns) else MSCI_selected_columns

# Definir ordem dos MSCI_fixed_columns
for index, element in enumerate(MSCI_fixed_columns):
    MSCI_selected_columns.insert(index, element)

MSCI_selected_columns = list(dict.fromkeys(MSCI_selected_columns))

# Verifica se pelo menos uma coluna foi selecionada antes de aplicar o filtro
if MSCI_selected_columns:
    MSCI_filtered_df = MSCI_df[MSCI_selected_columns]
else:
    MSCI_filtered_df = MSCI_df
    
MSCI_placeholder = st.empty()

MSCI_all = st.checkbox("Select/Clear all", value=True, key='checkbox-MSCI')
MSCI_container = st.container()
 
if MSCI_all:
    MSCI_selected_funds = MSCI_container.multiselect("Countries:",
         MSCI_filtered_df['Country'].drop_duplicates(),MSCI_filtered_df['Country'], key='multiselect-MSCI-all')
else:
    MSCI_selected_funds =  MSCI_container.multiselect("Countries:",
        MSCI_filtered_df['Country'].drop_duplicates(), key='multiselect-MSCI')


# Filtrar o DataFrame com base nas linhas selecionadas
MSCI_filtered_df = MSCI_filtered_df[MSCI_filtered_df['Country'].isin(MSCI_selected_funds)]
MSCI_filtered_df = MSCI_filtered_df.drop_duplicates()

# Inicialize listas vazias para armazenar os valores mínimos e máximos de colunas numéricas
MSCI_valores_minimos = []
MSCI_valores_maximos = []
# Calcule os valores mínimos e máximos de colunas numéricas
for col in MSCI_filtered_df.columns:
    if pd.api.types.is_numeric_dtype(MSCI_filtered_df[col]):
        MSCI_vmin = MSCI_filtered_df[col].min()
        MSCI_vmax = MSCI_filtered_df[col].max()
        MSCI_valores_minimos.append(MSCI_vmin)
        MSCI_valores_maximos.append(MSCI_vmax)
    else:
        MSCI_valores_minimos.append(None)
        MSCI_valores_maximos.append(None)
# Defina o mapa de cores
cmap = plt.colormaps['Blues']
# Crie o estilo de gradiente de fundo com valores MSCI_vmin e MSCI_vmax personalizados
MSCI_styled_df = MSCI_filtered_df.set_index('Index').style
for col, MSCI_vmin, MSCI_vmax in zip(MSCI_filtered_df.columns[1:], MSCI_valores_minimos[1:], MSCI_valores_maximos[1:]):
    if MSCI_vmin is not None and MSCI_vmax is not None:
        MSCI_styled_df = MSCI_styled_df.background_gradient(cmap=cmap, vmin=MSCI_vmin, vmax=MSCI_vmax, subset=col)
        MSCI_styled_df.applymap(highlight_none, subset=col)
# Exiba o DataFrame estilizado no Streamlit
MSCI_placeholder.dataframe(MSCI_styled_df.format(precision=1))

st.divider()

# -=-=-=-=-=-=-=-=-= DM -=-=-=-=-=-=-=-=-=-=
st.header('Developed Markets (DM)')

dm_all_columns = dm_df.columns.to_list()
dm_default_columns = default_columns

dm_fixed_columns = ['Index', 'Country']
dm_filtered_df = dm_df[dm_default_columns]

# Caixa de seleção para escolher quais colunas serão exibidas
dm_selected_columns = st.multiselect('DM Columns:', options=dm_all_columns, default=list(dict.fromkeys(dm_fixed_columns + dm_default_columns)), key='dm-multiselect-columns')
dm_selected_columns = dm_fixed_columns + dm_selected_columns if not all(ele in dm_selected_columns for ele in dm_fixed_columns) else dm_selected_columns

# Definir ordem dos dm_fixed_columns
for index, element in enumerate(dm_fixed_columns):
    dm_selected_columns.insert(index, element)

dm_selected_columns = list(dict.fromkeys(dm_selected_columns))

# Verifica se pelo menos uma coluna foi selecionada antes de aplicar o filtro
if dm_selected_columns:
    dm_filtered_df = dm_df[dm_selected_columns]
else:
    dm_filtered_df = dm_df

dm_placeholder = st.empty()

dm_all = st.checkbox("Select/Clear all", value=True, key='checkbox-dm')
dm_container = st.container()
 
if dm_all:
    dm_selected_funds = dm_container.multiselect("Countries:",
         dm_filtered_df['Country'].drop_duplicates(),dm_filtered_df['Country'].drop_duplicates(), key='multiselect-dm-all')
else:
    dm_selected_funds =  dm_container.multiselect("Countries:",
        dm_filtered_df['Country'].drop_duplicates(), key='multiselect-dm')


# Filtrar o DataFrame com base nas linhas selecionadas
dm_filtered_df = dm_filtered_df[dm_filtered_df['Country'].isin(dm_selected_funds)]
dm_filtered_df = dm_filtered_df.drop_duplicates()

# Inicialize listas vazias para armazenar os valores mínimos e máximos de colunas numéricas
dm_valores_minimos = []
dm_valores_maximos = []
# Calcule os valores mínimos e máximos de colunas numéricas
for col in dm_filtered_df.columns:
    if pd.api.types.is_numeric_dtype(dm_filtered_df[col]):
        dm_vmin = dm_filtered_df[col].min()
        dm_vmax = dm_filtered_df[col].max()
        dm_valores_minimos.append(dm_vmin)
        dm_valores_maximos.append(dm_vmax)
    else:
        dm_valores_minimos.append(None)
        dm_valores_maximos.append(None)
# Defina o mapa de cores
cmap = plt.colormaps['Blues']
# Crie o estilo de gradiente de fundo com valores dm_vmin e dm_vmax personalizados
dm_styled_df = dm_filtered_df.set_index('Index').style
for col, dm_vmin, dm_vmax in zip(dm_filtered_df.columns[1:], dm_valores_minimos[1:], dm_valores_maximos[1:]):
    if dm_vmin is not None and dm_vmax is not None:
        dm_styled_df = dm_styled_df.background_gradient(cmap=cmap, vmin=dm_vmin, vmax=dm_vmax, subset=col)
        dm_styled_df.applymap(highlight_none, subset=col)
# Exiba o DataFrame estilizado no Streamlit
dm_placeholder.dataframe(dm_styled_df.format(precision=1))

st.divider()

# -=-=-=-=-=-=-=-=-= US -=-=-=-=-=-=-=-=-=-=
st.header('United States (US)')

us_all_columns = us_df.columns.to_list()
us_default_columns = default_columns
us_default_columns.remove('Country')
us_fixed_columns = ['Index']
us_filtered_df = us_df[us_default_columns]

# Caixa de seleção para escolher quais colunas serão exibidas
us_selected_columns = st.multiselect('US Columns:', options=us_all_columns, default=list(dict.fromkeys(us_fixed_columns + us_default_columns)), key='us-multiselect-columns')
us_selected_columns = us_fixed_columns + us_selected_columns if not all(ele in us_selected_columns for ele in us_fixed_columns) else us_selected_columns

# Definir ordem dos us_fixed_columns
for index, element in enumerate(us_fixed_columns):
    us_selected_columns.insert(index, element)

us_selected_columns = list(dict.fromkeys(us_selected_columns))

# Verifica se pelo menos uma coluna foi selecionada antes de aplicar o filtro
if us_selected_columns:
    us_filtered_df = us_df[us_selected_columns]
else:
    us_filtered_df = us_df

us_placeholder = st.empty()

us_all = st.checkbox("Select/Clear all", value=True, key='checkbox-us')
us_container = st.container()
 
if us_all:
    us_selected_funds = us_container.multiselect("Index:",
         us_filtered_df['Index'].drop_duplicates(),us_filtered_df['Index'].drop_duplicates(), key='multiselect-us-all')
else:
    us_selected_funds =  us_container.multiselect("Index:",
        us_filtered_df['Index'].drop_duplicates(), key='multiselect-us')


# Filtrar o DataFrame com base nas linhas selecionadas
us_filtered_df = us_filtered_df[us_filtered_df['Index'].isin(us_selected_funds)]
us_filtered_df = us_filtered_df.drop_duplicates()

# Inicialize listas vazias para armazenar os valores mínimos e máximos de colunas numéricas
us_valores_minimos = []
us_valores_maximos = []
# Calcule os valores mínimos e máximos de colunas numéricas
for col in us_filtered_df.columns:
    if pd.api.types.is_numeric_dtype(us_filtered_df[col]):
        us_vmin = us_filtered_df[col].min()
        us_vmax = us_filtered_df[col].max()
        us_valores_minimos.append(us_vmin)
        us_valores_maximos.append(us_vmax)
    else:
        us_valores_minimos.append(None)
        us_valores_maximos.append(None)
# Defina o mapa de cores
cmap = plt.colormaps['Blues']
# Crie o estilo de gradiente de fundo com valores us_vmin e us_vmax personalizados
us_styled_df = us_filtered_df.set_index('Index').style
for col, us_vmin, us_vmax in zip(us_filtered_df.columns[1:], us_valores_minimos[1:], us_valores_maximos[1:]):
    if us_vmin is not None and us_vmax is not None:
        us_styled_df = us_styled_df.background_gradient(cmap=cmap, vmin=us_vmin, vmax=us_vmax, subset=col)
        us_styled_df.applymap(highlight_none, subset=col)
# Exiba o DataFrame estilizado no Streamlit
us_placeholder.dataframe(us_styled_df.format(precision=1))

st.divider()

# # -=-=-=-=-=-=-=-=-= Time Series  Bloomberg -=-=-=-=-=-=-=-=-=-=
st.title('Time Series')
# st.write(us_recession_df)
# Crie uma caixa de seleção para escolher o DataFrame
bbg_multiselect = st.multiselect("Field:", list(all_equities_dfs.keys()), key='bbg-multiselect')

line_graph_df = pd.DataFrame()
title_list = []
# Selecione o DataFrame com base na escolha do usuário e plote o gráfico
for idx, fld in enumerate(bbg_multiselect):
    title_list.append(all_equities_dfs[fld]['name'])
    # Crie uma caixa de seleção para escolher os tickers
    fld_selected_tickers = st.multiselect(fld, key=f'{idx}-fld-multiselect-index-key', options=all_equities_dfs[fld]['df'].columns, default=[])
    # Filtrar o DataFrame com base nos tickers selecionados
    fld_filtered_df = all_equities_dfs[fld]['df'][fld_selected_tickers]
    fld_filtered_df = fld_filtered_df.rename(columns=lambda col: f"{col} - {all_equities_dfs[fld]['name']}")
    fld_filtered_df.index = pd.to_datetime(fld_filtered_df.index)
    line_graph_df = pd.concat([line_graph_df, fld_filtered_df], axis=1)


line_graph_col1, line_graph_col2 = st.columns(2)
start_date = date(2000,1,1)
with line_graph_col1:
    line_graph_start_date_checkbox = st.checkbox("Apply evolution", value=False, key='checkbox-evolution-line-graph')
    line_graph_log_checkbox = st.checkbox("Apply log", value=False, key='checkbox-line_graph')
with line_graph_col2:
    if line_graph_start_date_checkbox and not line_graph_df.empty:
        max_min_df = line_graph_df.dropna(axis=0, how='any')
        evolution_min_date = max_min_df.index[0]
        evolution_max_date = max_min_df.index[-1]
        start_date = st.date_input("Start Date (100 base)", evolution_max_date , min_value=evolution_min_date, max_value=evolution_max_date, key='line_graph-start_date')

if line_graph_start_date_checkbox:
    line_graph_df = line_graph_df.apply(lambda col: index_number(start_date, col), axis=0)

if line_graph_log_checkbox:
    line_graph_df = line_graph_df.apply(lambda x: np.log(x) if np.issubdtype(x.dtype, np.number) else x)
st.plotly_chart(plot_series(line_graph_df, ' x '.join(list(title_list)), 'Date', 'Value (log)' if line_graph_log_checkbox else "Value"))
# st.plotly_chart(plot_series(line_graph_df, ' x '.join(list(title_list)), 'Date', 'Value (log)' if line_graph_log_checkbox else "Value", recession_dates=us_recession_df))

st.divider()

# # # -=-=-=-=-=-=-=-=-= Scatter Plot  Bloomberg -=-=-=-=-=-=-=-=-=-=
# Crie uma caixa de seleção para escolher o DataFrame
st.header("Scatter Plot")

scatter_plot_col1, scatter_plot_col2 = st.columns(2)
with scatter_plot_col1:
    x_scatter_plot_selected_fields = st.selectbox("Field (x):", list(all_equities_dfs.keys()), key='x_scatter_plot-fields')
    x_scatter_plot_df = all_equities_dfs[x_scatter_plot_selected_fields]['df']
    x_scatter_plot_df.index.name = 'date'
    x_scatter_plot_name = all_equities_dfs[x_scatter_plot_selected_fields]['name']
with scatter_plot_col2:
    y_scatter_plot_selected_fields = st.selectbox("Field (y):", list(all_equities_dfs.keys()), key='y_scatter_plot-fields')
    y_scatter_plot_df = all_equities_dfs[y_scatter_plot_selected_fields]['df']
    y_scatter_plot_df.index.name = 'date'
    y_scatter_plot_name = all_equities_dfs[y_scatter_plot_selected_fields]['name']

# Checkbox
scatter_plot_columns_available = list(set(x_scatter_plot_df.columns) & set(y_scatter_plot_df.columns))
scatter_plot_tickers_placeholder = st.empty()
scatter_plot_default_values_col1, scatter_plot_default_values_col2, scatter_plot_default_values_col3, scatter_plot_default_values_col4,  = st.columns(4)
scatter_plot_selected_tickers = []
with scatter_plot_default_values_col1:
    em_scatter_plot_default_values_checkbox = st.checkbox("EM", value=False, key='scatter_plot-checkbox-em_default_values')
    last_values_scatter_plot_checkbox = st.checkbox("Last Values", value=False, key='scatter_plot-checkbox-last_values')
    if em_scatter_plot_default_values_checkbox:
        scatter_plot_selected_tickers = (em_filtered_df['Index'] + " Index").to_list()
with scatter_plot_default_values_col2:
    MSCI_scatter_plot_default_values_checkbox = st.checkbox("MSCI", value=False, key='scatter_plot-checkbox-MSCI_default_values')
    # style2_scatter_plot_checkbox = st.checkbox("Style2", value=False, key='scatter_plot-checkbox-style2')
    if MSCI_scatter_plot_default_values_checkbox:
        scatter_plot_selected_tickers = (MSCI_filtered_df['Index'] + " Index").to_list()
with scatter_plot_default_values_col3:
    dm_scatter_plot_default_values_checkbox = st.checkbox("DM", value=False, key='scatter_plot-checkbox-dm_default_values')
    # style3_scatter_plot_checkbox = st.checkbox("Style3", value=False, key='scatter_plot-checkbox-style3')
    if dm_scatter_plot_default_values_checkbox:
        scatter_plot_selected_tickers = (dm_filtered_df['Index'] + " Index").to_list()
with scatter_plot_default_values_col4:
    us_scatter_plot_default_values_checkbox = st.checkbox("US", value=False, key='scatter_plot-checkbox-us_default_values')
    # # style4_scatter_plot_checkbox = st.checkbox("Style4", value=False, key='scatter_plot-checkbox-style4')
    if us_scatter_plot_default_values_checkbox:
        scatter_plot_selected_tickers = (us_filtered_df['Index'] + " Index").to_list()

scatter_plot_selected_tickers = list(set(scatter_plot_selected_tickers) & set(scatter_plot_columns_available))

scatter_plot_tickers_options = list(set(x_scatter_plot_df.columns) & set(y_scatter_plot_df.columns))
scatter_plot_tickers = scatter_plot_tickers_placeholder.multiselect("Index", options=scatter_plot_tickers_options + scatter_plot_selected_tickers, default=scatter_plot_selected_tickers, key='scatter_plot-tickers')

# TODO date input
x_scatter_plot_df_stacked = x_scatter_plot_df[scatter_plot_tickers].dropna(how='all').stack().reset_index().rename(columns={'level_1': 'Tickers', 'level_0': 'x', 0: 'x', 'field': 'Tickers'})
y_scatter_plot_df_stacked = y_scatter_plot_df[scatter_plot_tickers].dropna(how='all').stack().reset_index().rename(columns={'level_1': 'Tickers', 'level_0': 'y', 0: 'y', 'field': 'Tickers'})

if scatter_plot_tickers and (x_scatter_plot_selected_fields != y_scatter_plot_selected_fields):
    scatter_df_combinado = pd.merge(x_scatter_plot_df_stacked, y_scatter_plot_df_stacked, on=['date', 'Tickers'])
    # Get most recent occurrence
    scatter_df_combinado = scatter_df_combinado.groupby('Tickers').max().reset_index()
    
    st.plotly_chart(plot_scatter(
        scatter_df_combinado,
        x_column='x',
        y_column='y',
        categories='Tickers',
        title=f"{x_scatter_plot_name}  |  {y_scatter_plot_name} ",
        xaxis_title=x_scatter_plot_name,
        yaxis_title=y_scatter_plot_name
    ))


st.divider()

# # -=-=-=-=-=-=-=-=-= BoxPlot  Bloomberg -=-=-=-=-=-=-=-=-=-=
# Crie uma caixa de seleção para escolher o DataFrame
st.header("Boxplot")
bp_selected_df = st.selectbox("Field:", list(all_equities_dfs.keys()), key='bp-selectbox')


boxplot_df = all_equities_dfs[bp_selected_df]['df']
boxplot_col1, boxplot_col2 = st.columns(2)
with boxplot_col1:
    boxplot_start_date = st.date_input("Start Date", value=np.datetime64(boxplot_df.index.min(), 'D').astype(date), min_value=date(1990,1,1), max_value=datetime.now(),key='boxplot-start_date')
with boxplot_col2:
    boxplot_end_date = st.date_input("End Date", value=np.datetime64(boxplot_df.index.max(), 'D').astype(date), min_value=date(1990,1,1), max_value=datetime.now(), key='boxplot-end_date')

default_values_col1, default_values_col2, default_values_col3, default_values_col4,  = st.columns(4)

boxplot_selected_tickers_placeholder = st.empty()
boxplot_selected_tickers = []
with default_values_col1:
    em_default_values_checkbox = st.checkbox("EM", value=False, key='bp-checkbox-em_default_values')
    if em_default_values_checkbox:
        boxplot_selected_tickers = em_filtered_df['Index'] + " Index"
with default_values_col2:
    MSCI_default_values_checkbox = st.checkbox("MSCI", value=False, key='bp-checkbox-MSCI_default_values')
    if MSCI_default_values_checkbox:
        boxplot_selected_tickers = MSCI_filtered_df['Index'] + " Index"
with default_values_col3:
    dm_default_values_checkbox = st.checkbox("DM", value=False, key='bp-checkbox-dm_default_values')
    if dm_default_values_checkbox:
        boxplot_selected_tickers = dm_filtered_df['Index'] + " Index"
with default_values_col4:
    us_default_values_checkbox = st.checkbox("US", value=False, key='bp-checkbox-us_default_values')
    if us_default_values_checkbox:
        boxplot_selected_tickers = us_filtered_df['Index'] + " Index"

if (em_default_values_checkbox or MSCI_default_values_checkbox or dm_default_values_checkbox or us_default_values_checkbox) and 'return' in bp_selected_df.lower():
    boxplot_returns_col1, boxplot_returns_col2, boxplot_returns_col3, boxplot_returns_col4, boxplot_returns_col5  = st.columns(5)
    with boxplot_returns_col1:
        boxplot_tot_return_1d = st.checkbox("1D", value=False, key='boxplot_returns-boxplot-1D')
    with boxplot_returns_col2:
        boxplot_tot_return_1w = st.checkbox("1W", value=False, key='boxplot_returns-boxplot-1W') 
    with boxplot_returns_col3:
        boxplot_tot_return_3m = st.checkbox("3M", value=False, key='boxplot_returns-boxplot-3M') 
    with boxplot_returns_col4:
        boxplot_tot_return_6m = st.checkbox("6M", value=False, key='boxplot_returns-boxplot-6M') 
    with boxplot_returns_col5:
        boxplot_tot_return_12m = st.checkbox("12M", value=False, key='boxplot_returns-boxplot-12M')
    boxplot_selected_tickers += " (1D)" if boxplot_tot_return_1d else " (1W)" if boxplot_tot_return_1w else " (3M)" if boxplot_tot_return_3m else " (6M)" if boxplot_tot_return_6m else " (12M)"

boxplot_selected_tickers = boxplot_selected_tickers_placeholder.multiselect(
    "Index:", 
    options=boxplot_df.columns, 
    default=list(set(boxplot_selected_tickers) & set(boxplot_df.columns)), 
    # default=boxplot_selected_tickers, 
    key='boxplot-bp_selected_tickers'
)

# Filtrar o DataFrame com base nos tickers selecionados
boxplot_filtered_df = boxplot_df[boxplot_selected_tickers]
boxplot_checkbox = st.checkbox("Apply log", value=False, key='bp-checkbox-boxplot')
if boxplot_checkbox:
    boxplot_filtered_log_df = boxplot_filtered_df.copy()
    boxplot_filtered_log_df = boxplot_filtered_log_df.apply(lambda x: np.log(x) if np.issubdtype(x.dtype, np.number) else x)
    st.plotly_chart(plot_boxplot(boxplot_filtered_log_df, all_equities_dfs[bp_selected_df]['name'], 'Index', 'Value (log)', start_date=boxplot_start_date, end_date=boxplot_end_date))
else:
    st.plotly_chart(plot_boxplot(boxplot_filtered_df, all_equities_dfs[bp_selected_df]['name'], 'Index', 'Value', start_date=boxplot_start_date, end_date=boxplot_end_date))

# Inicio das series
st.dataframe(boxplot_filtered_df.apply(start_end_dates, axis=0))

st.divider()

# # -=-=-=-=-=-=-=-=-= Betas -=-=-=-=-=-=-=-=-=-=
st.title('Betas')

return_window_list = ['1D', '1W', '1M', '3M', '6M', '12M',]
beta_windows = {
    '1M': 22, 
    '3M': 22*3, 
    '6M': 22*6,
    '1Y': 252, 
    '5Y': 252*5,
}

st.subheader('Beta Index')
beta_col1, beta_col2= st.columns(2)

beta_line_graph_df = pd.DataFrame()
returns_beta_df = pd.DataFrame()

beta_line_graph_df = pd.DataFrame()
returns_beta_df = pd.DataFrame()


tot_return_tickers = [elem.split(' (')[0] for elem in TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df.columns.to_list()]
tot_return_tickers = list(set(tot_return_tickers))
with beta_col1:
    selected_beta_ticker = st.selectbox("Ticker:", tot_return_tickers, index=tot_return_tickers.index('SPX Index'), key='beta-selectbox-ticker')
    return_ticker_window = st.selectbox("Return Window:", list(return_window_list), index=0, key='beta-return_window')
with beta_col2:
    selected_beta_market = st.selectbox("Market:", tot_return_tickers, index=tot_return_tickers.index('SPX Index'), key='beta-selectbox-market')

if selected_beta_ticker != selected_beta_market:
    selected_beta_ticker = f"{selected_beta_ticker} ({return_ticker_window})"
    selected_beta_market = f"{selected_beta_market} ({return_ticker_window})"
    returns_beta_df = TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df[[selected_beta_ticker, selected_beta_market]]
    returns_beta_df = returns_beta_df[returns_beta_df!=0].dropna()

    for window_name, window_period in beta_windows.items():
        # Calcule a covariância entre os retornos do ativo e do mercado
        covariances = returns_beta_df.rolling(window=window_period).cov().unstack()[(selected_beta_ticker, selected_beta_market)].dropna()
        # Calcule a variância dos retornos do mercado
        market_variances = returns_beta_df[selected_beta_market].rolling(window=window_period).var().dropna()

        # Calcule o beta
        betas = covariances / market_variances

        betas = betas.dropna().rename(window_name)
        beta_line_graph_df = pd.concat([beta_line_graph_df, betas], axis=1)

    st.plotly_chart(plot_series(beta_line_graph_df, f'Beta | {selected_beta_ticker} x {selected_beta_market}', 'Date', "Value"))
    st.plotly_chart(plot_boxplot(beta_line_graph_df, f'Beta | {selected_beta_ticker} x {selected_beta_market}', xaxis_title='Beta Window', yaxis_title='Value'))

st.subheader('Beta Compare')
beta_compare_col1, beta_compare_col2 = st.columns(2)

beta_compare_line_graph_df = pd.DataFrame()
returns_beta_compare_df = pd.DataFrame()

beta_compare_line_graph_df = pd.DataFrame()
returns_beta_compare_df = pd.DataFrame()


tot_return_tickers = [elem.split(' (')[0] for elem in TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df.columns.to_list()]
tot_return_tickers = list(set(tot_return_tickers))
with beta_compare_col1:
    # selected_beta_compare_ticker = st.selectbox("Ticker:", tot_return_tickers, index=9, key='beta-compare-selectbox-ticker')
    selected_beta_compare_ticker = st.multiselect('Index:', options=tot_return_tickers, default=[], key='beta-compare-multiselect-ticker')
    return_window = st.selectbox("Return Window:", list(return_window_list), index=0, key='beta-compare-return_window')
with beta_compare_col2:
    selected_beta_compare_market = st.selectbox("Market:", tot_return_tickers, index=tot_return_tickers.index('SPX Index'), key='beta-compare-selectbox-market')
    beta_compare_window = st.selectbox("Beta Window:", list(beta_windows.keys()), index=0, key='beta-compare-window')

if selected_beta_compare_ticker and selected_beta_compare_market not in selected_beta_compare_ticker:
    selected_beta_compare_ticker = [f"{elem} ({return_window})" for elem in selected_beta_compare_ticker]
    selected_beta_compare_market = f"{selected_beta_compare_market} ({return_window})"
    returns_beta_compare_df = TOT_RETURN_INDEX_GROSS_DVDS_VARIATIONS_df[selected_beta_compare_ticker + [selected_beta_compare_market]]
    returns_beta_compare_df = returns_beta_compare_df[returns_beta_compare_df!=0].dropna()
    # # Calcule a covariância entre os retornos do ativo e do mercado
    # covariances = returns_beta_compare_df.rolling(window=beta_windows[beta_compare_window]).cov().unstack()[(selected_beta_compare_ticker + [selected_beta_compare_market])].dropna()
    # # Calcule a variância dos retornos do mercado
    # market_variances = returns_beta_compare_df[selected_beta_compare_market].rolling(window=beta_windows[beta_compare_window]).var().dropna()

    # # Calcule o beta
    # betas = covariances / market_variances

    # betas = betas.dropna().rename(beta_compare_window)
    # beta_compare_line_graph_df = pd.concat([beta_compare_line_graph_df, betas], axis=1)

    for beta_compare_ticker in selected_beta_compare_ticker:
        # Calcule a covariância entre os retornos do ativo e do mercado
        covariances = returns_beta_compare_df.rolling(window=beta_windows[beta_compare_window]).cov().unstack()[(beta_compare_ticker, selected_beta_compare_market)].dropna()
        # Calcule a variância dos retornos do mercado
        market_variances = returns_beta_compare_df[selected_beta_compare_market].rolling(window=beta_windows[beta_compare_window]).var().dropna()

        # Calcule o beta
        betas = covariances / market_variances

        betas = betas.dropna().rename(beta_compare_ticker)
        beta_compare_line_graph_df = pd.concat([beta_compare_line_graph_df, betas], axis=1)

    # for window_name, window_period in beta_windows.items():
    #     # Calcule a covariância entre os retornos do ativo e do mercado
    #     covariances = returns_beta_compare_df.rolling(window=window_period).cov().unstack()[(selected_beta_compare_ticker, selected_beta_compare_market)].dropna()
    #     # Calcule a variância dos retornos do mercado
    #     market_variances = returns_beta_compare_df[selected_beta_compare_market].rolling(window=window_period).var().dropna()

    #     # Calcule o beta
    #     betas = covariances / market_variances

    #     betas = betas.dropna().rename(window_name)
    #     beta_compare_line_graph_df = pd.concat([beta_compare_line_graph_df, betas], axis=1)
    # st.plotly_chart(plot_series(beta_compare_line_graph_df, f'Beta | {selected_beta_compare_ticker} x {selected_beta_compare_market}', 'Date', "Value"))
    st.plotly_chart(plot_series(beta_compare_line_graph_df, f'Beta - ({" | ".join(selected_beta_compare_ticker)}) x {selected_beta_compare_market}', 'Date', "Value"))

st.divider()
# -=-=-=-=-=-=-=-=-= ERP -=-=-=-=-=-=-=-=-=-=
st.header('Equity Risk Premium')

st.subheader('ERP Compare')
cur_df = pd.DataFrame()
erp_df = pd.DataFrame()
erp_compare_col1, erp_compare_col2 = st.columns(2)
with erp_compare_col1:
    erp_EY_select_placeholder = st.empty()
with erp_compare_col2:
    erp_multiselect_placeholder = st.empty()
#
erp_boxplot_selected_tickers = []
erp_default_values_col1, erp_default_values_col2, erp_default_values_col3, erp_default_values_col4,  = st.columns(4)
with erp_default_values_col1:
    erp_em_default_values_checkbox = st.checkbox("EM", value=False, key='erp-bp-checkbox-em_default_values')
    y1_rates_default_values_checkbox = st.checkbox("1Y", value=False, key='erp-bp-checkbox-1Y_default_values')
    if erp_em_default_values_checkbox:
        erp_boxplot_selected_tickers = em_filtered_df['Index'] + " Index"
with erp_default_values_col2:
    erp_MSCI_default_values_checkbox = st.checkbox("MSCI", value=False, key='erp-bp-checkbox-MSCI_default_values')
    y2_rates_default_values_checkbox = st.checkbox("2Y", value=False, key='erp-bp-checkbox-2Y_default_values')
    if erp_MSCI_default_values_checkbox:
        erp_boxplot_selected_tickers = MSCI_filtered_df['Index'] + " Index"
with erp_default_values_col3:
    erp_dm_default_values_checkbox = st.checkbox("DM", value=False, key='erp-bp-checkbox-dm_default_values')
    y5_rates_default_values_checkbox = st.checkbox("5Y", value=False, key='erp-bp-checkbox-5Y_default_values')
    if erp_dm_default_values_checkbox:
        erp_boxplot_selected_tickers = dm_filtered_df['Index'] + " Index"
with erp_default_values_col4:
    erp_us_default_values_checkbox = st.checkbox("US", value=False, key='erp-bp-checkbox-us_default_values')
    y10_rates_default_values_checkbox = st.checkbox("10Y", value=False, key='erp-bp-checkbox-10Y_default_values')
    if erp_us_default_values_checkbox:
        erp_boxplot_selected_tickers = us_filtered_df['Index'] + " Index"
#
erp_EY_selectbox = erp_EY_select_placeholder.selectbox(
    "Earnings Yield:", 
    ['*Earnings Yield 1BF (Bloomberg)', '*Earnings Yield 2BF (Bloomberg)', '*Earnings Yield 1BF (Factset)', '*Earnings Yield 2BF (Factset)'], 
    key='erp-EY-selectbox'
)
ERP_EY_df = all_equities_dfs[erp_EY_selectbox]['df']
erp_multiselect = erp_multiselect_placeholder.multiselect("Ticker:", ERP_EY_df.columns.to_list()*5, default=list(set(ERP_EY_df.columns.to_list()*5) & set(erp_boxplot_selected_tickers)), key='erp-multiselect')
rates_dict = {}

if erp_em_default_values_checkbox or erp_dm_default_values_checkbox or erp_us_default_values_checkbox or erp_MSCI_default_values_checkbox:
    for idx, ticker in enumerate(erp_multiselect):
        cur_df = rates_tickers_dict_df[rates_tickers_dict_df['bbg_ticker'] == ticker]
        if not cur_df.empty:
            currency = cur_df['currency'].drop_duplicates().dropna().iloc[0]
            rate_period = '1y' if y1_rates_default_values_checkbox else '2y' if y2_rates_default_values_checkbox else '5y' if y5_rates_default_values_checkbox else '10y' if y10_rates_default_values_checkbox else '1y'
            aux_select_box = [col for col in rates_df.columns if (currency in col and f'Spot {rate_period}' in col)][0]
            label = f"{ticker.replace(' Index', '')} {aux_select_box}"
            erp_df[label] = ERP_EY_df[ticker] - rates_df[aux_select_box]
else:
    if y1_rates_default_values_checkbox or y2_rates_default_values_checkbox or y5_rates_default_values_checkbox or y10_rates_default_values_checkbox:
        for idx, ticker in enumerate(erp_multiselect):
            cur_df = rates_tickers_dict_df[rates_tickers_dict_df['bbg_ticker'] == ticker]
            if not cur_df.empty:
                currency = cur_df['currency'].drop_duplicates().dropna().iloc[0]
                rate_period = '1y' if y1_rates_default_values_checkbox else '2y' if y2_rates_default_values_checkbox else '5y' if y5_rates_default_values_checkbox else '10y' if y10_rates_default_values_checkbox else '1y'
                aux_select_box = [col for col in rates_df.columns if (currency in col and f'Spot {rate_period}' in col)][0]
                label = f"{ticker.replace(' Index', '')} {aux_select_box}"
                erp_df[label] = ERP_EY_df[ticker] - rates_df[aux_select_box]
    else:
        for idx, ticker in enumerate(erp_multiselect):
            cur_df = rates_tickers_dict_df[rates_tickers_dict_df['bbg_ticker'] == ticker]
            if not cur_df.empty:
                currency = cur_df['currency'].drop_duplicates().dropna().iloc[0]
                aux_select_box = st.selectbox(f"{ticker}:", [col for col in rates_df.columns if currency in col], key=f"{idx}-{ticker}")
                label = f"{ticker.replace(' Index', '')} {aux_select_box}"
                erp_df[label] = ERP_EY_df[ticker] - rates_df[aux_select_box]

if not erp_df.empty:
    st.plotly_chart(plot_series(erp_df.rename(columns=lambda col: f"ERP {col}"), title='Equity Risk Premium', xaxis_title='Date', yaxis_title='Value', source='Source: Bloomberg'))


st.subheader('Boxplot')
erp_boxplot_col1, erp_boxplot_col2 = st.columns(2)
if not erp_df.empty:
    with erp_boxplot_col1:
        erp_boxplot_start_date = st.date_input("Start Date", value=np.datetime64(erp_df.index.min(), 'D').astype(date), min_value=date(1980,1,1), max_value=datetime.now(),key='erp_boxplot-start_date')
    with erp_boxplot_col2:
        erp_boxplot_end_date = st.date_input("End Date", value=np.datetime64(erp_df.index.max(), 'D').astype(date), min_value=date(1980,1,1), max_value=datetime.now(), key='erp_boxplot-end_date')
    st.plotly_chart(plot_boxplot(erp_df, 'Equity Risk Premium', 'Date', 'Value', start_date=erp_boxplot_start_date, end_date=erp_boxplot_end_date))

st.divider()
st.subheader('ERP vs EY')
erp_col1, erp_col2 = st.columns(2)
cur_df = pd.DataFrame()
with erp_col1:
    selected_ey_ticker = st.selectbox("Ticker:", EY_1BF_df.columns.to_list(), index=1)
with erp_col2:
    cur_df = rates_tickers_dict_df[rates_tickers_dict_df['bbg_ticker'] == selected_ey_ticker]
    if not cur_df.empty:
        currency = cur_df['currency'].drop_duplicates().dropna().iloc[0]
        selected_rates = st.selectbox("Rate:", [col for col in rates_df.columns if currency in col])

erp_df = pd.DataFrame()
if not cur_df.empty:
    erp_df['EY 1BF'] = EY_1BF_df[selected_ey_ticker]
    erp_df[selected_rates] = rates_df[selected_rates]
    erp_df['ERP'] = erp_df['EY 1BF'] - erp_df[selected_rates]
    st.plotly_chart(plot_series_multiple_y_axis(erp_df, title='Equity Risk Premium', xaxis_title='Date', yaxis_title='E/Y & Spot', y2axis_title='ERP', source='Source: Bloomberg'))

st.divider()


# # # -=-=-=-=-=-=-=-=-= Weights -=-=-=-=-=-=-=-=-=-=
# # Pizza graph
# st.title('Weights')


# economy_all_columns = economy_df.columns.to_list()
# economy_default_columns = economy_all_columns
# economy_filtered_df = economy_df[economy_default_columns]

# # Caixa de seleção para escolher quais colunas serão exibidas
# economy_selected_columns = st.multiselect('Index:', options=economy_all_columns, default=economy_default_columns, key='economy-multiselect-columns')


# # Verifica se pelo menos uma coluna foi selecionada antes de aplicar o filtro
# if economy_selected_columns:
#     economy_filtered_df = economy_df[economy_selected_columns]
# else:
#     economy_filtered_df = economy_df

# economy_placeholder = st.empty()

# cmap = plt.colormaps['Blues']
# economy_placeholder.dataframe(economy_filtered_df.fillna(0).style \
#                       .background_gradient(cmap=cmap,vmin=0,vmax=100,axis=None) \
#                       .format(precision=3), 
#             # width=1400, 
#             # height=1300
#         )
# st.divider()

# selected_weight = st.selectbox("Index:", weights_df['index'].drop_duplicates().to_list(), index=1)
# filtered_weight_df = weights_df[weights_df['index'] == selected_weight]

# # st.markdown('<div style="text-align: right;">source: Factset</div>', unsafe_allow_html=True)
# # Gráfico setorial de pizza por setor
# fs_economy = px.pie(
#     filtered_weight_df.dropna(subset=['factset_economy']), 
#     values='weightClose', 
#     names='factset_economy', 
#     title='Factset Economy',
#     labels={'factset_economy': 'Economy', 'weightClose': 'Weight (%)'},
#     template='plotly'
# )
# fs_economy.update_traces(textinfo='percent+label')
# # Aumentar o tamanho da fonte no gráfico
# fs_economy.update_layout(
#     font=dict(size=16),
#     margin=dict(l=0, r=0, t=50, b=0),  # Ajustar margens para acomodar o título maior,
#     width=400,  # Defina a largura em pixels
#     height=600,  # Defina a altura em pixels
# )
# # st.plotly_chart(fs_economy, use_container_width=True)

# st.markdown(f'<div style="text-align: left;">Updated at: {filtered_weight_df["date"].min()}</div>', unsafe_allow_html=True)
# st.markdown('<div style="text-align: right;">source: Factset</div>', unsafe_allow_html=True)
# # Gráfico setorial de pizza por setor
# fs_sector = px.pie(
#     filtered_weight_df.dropna(subset=['factset_sector']), 
#     values='weightClose', 
#     names='factset_sector', 
#     title='Factset Sectors',
#     labels={'factset_sector': 'Sectors', 'weightClose': 'Weight (%)'},
#     template='plotly'
# )

# fs_sector.update_traces(textinfo='percent+label')
# # Aumentar o tamanho da fonte no gráfico
# fs_sector.update_layout(
#     font=dict(size=16),
#     margin=dict(l=0, r=0, t=50, b=0),  # Ajustar margens para acomodar o título maior,
#     width=600,  # Defina a largura em pixels
#     height=700,  # Defina a altura em pixels
# )
# # st.plotly_chart(fs_sector, use_container_width=True)
# st.plotly_chart(fs_economy, use_container_width=True)
# st.plotly_chart(fs_sector, use_container_width=True)

