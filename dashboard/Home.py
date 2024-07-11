# Libraries
import streamlit as st
from PIL import Image
from helpers.aux_functions import execute_postgresql_query
import settings

# Config
logo = Image.open("images/favicon.png")
st.set_page_config(page_title='PH', page_icon=logo, layout='wide')

# Clear Cache
if st.sidebar.button("Update data"):
    # Clear values from *all* all in-memory and on-disk data caches:
    st.cache_data.clear()
    st.experimental_rerun()

# Functions
@st.cache_data
def handle_tickers_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_tickers;
    """)
    return df[df['class'] == 'Equity Index'][['ticker', 'description', 'CRNCY', 'country', 'factset_ticker']].sort_values(['country', 'ticker']).reset_index(drop=True)
tickers_df = handle_tickers_dict()

@st.cache_data
def handle_fields_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_fields;
    """)
    df = df[df['class'] == 'Equity Index'][['field', 'period', 'annualized', 'override']].sort_values('field').reset_index(drop=True)
    df = df.replace("TOT_RETURN_INDEX_GROSS_DVDS", "TOT_RETURN_INDEX_GROSS_DVDS_VARIATION") # TR was stored as variation on database
    return df[['field', 'period']]
fields_df = handle_fields_dict()

@st.cache_data
def handle_currency_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_currency;
    """)
    return df.sort_values('Country').reset_index(drop=True)
currency_df = handle_currency_dict()

@st.cache_data
def handle_rates_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_rates;
    """)
    return df[['bbg_ticker', 'rate', 'currency', 'country']].sort_values(['country', 'rate']).reset_index(drop=True)
rates_df = handle_rates_dict()

@st.cache_data
def handle_rates_monetary_policy_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_rates_monetary_policy;
    """)
    return df.sort_values('bbg_ticker').reset_index(drop=True)
rates_monetary_policy_df = handle_rates_monetary_policy_dict()

@st.cache_data
def handle_bonds_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_bonds;
    """)
    return df.sort_values('bbg_ticker').reset_index(drop=True)
bonds_df = handle_bonds_dict()

@st.cache_data
def handle_housing_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_housing;
    """)
    return df.sort_values('bbg_ticker').reset_index(drop=True)
housing_df = handle_housing_dict()

@st.cache_data
def handle_commodities_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_commodities;
    """)
    return df.sort_values('bbg_ticker').reset_index(drop=True)[['bbg_ticker', 'frequency']]
commodities_df = handle_commodities_dict()

@st.cache_data
def handle_economics_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.bbg_dict_economics;
    """)
    return df.sort_values(['description', 'bbg_ticker']).reset_index(drop=True)[
        ['bbg_ticker', 'description', 'period', 'country', 'seasonality', 'seasonal_adjustment_method', 'original_seasonality', 'type']]
economics_df = handle_economics_dict()


@st.cache_data
def handle_datastream_economics_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.datastream_dict_economics;
    """)
    return df.sort_values(['class', 'ticker']).reset_index(drop=True)[['ticker', 'class', 'country', 'period', 'seasonality', 'seasonal_adjustment_method', 'original_seasonality', 'currency', 'unity']]
datastream_economics_df = handle_datastream_economics_dict()


@st.cache_data
def handle_factset_fields_dict():
    df = execute_postgresql_query("""
        SELECT * FROM public.factset_dict_fields;
    """)
    return df.sort_values(['class', 'fs_fields'], ascending=[False, True]).reset_index(drop=True)[['class', 'fs_fields', 'short_description']]
factset_fields_df = handle_factset_fields_dict()


# Title
st.title('Dashboard')

st.write(
    """
    Step into our Streamlit-based platform, where we present you with a powerful toolset to conduct in-depth analyses and data visualizations that will steer your decisions.
    """
)
st.divider()

# Excel
excel_logo = Image.open("images/excel.png")
excel_col1, excel_col2 = st.columns([1, 20])
excel_col1.image(excel_logo, width=50)
excel_col2.header("Excel Add-in (xlwings based)")

with open(settings.ADD_IN_TEMPLATE_PATH, 'rb') as file:
    template_bytes = file.read()
    st.download_button(label='Download Template', data=template_bytes, file_name='template.xlsm', key='template-download_button')

st.subheader("⚙️ Setup")
st.markdown("""
    1. Enable `Trust access` to the VBA project object model under `File > Options > Trust Center > Trust Center Settings > Macro Settings`. You only need to do this once. Also, this is only required for importing the functions, i.e. end users won’t need to bother about this.
    2. Open windows cmd. Copy and paste the commands below:
    - `pip install -r excel_functions\\requirements.txt`
    - `xlwings addin install`
    3. Open excel and save as xlsm (macro enabled).
    4. In excel, open up the Developer Console (Alt-F11). First, click on your xlsm project name on the left side. Second, click on Tools and then References... Finally, select xlwings and save.
    5. On excel xlwings tab, set options:
    - Interpreter: `C:\Python37\python.exe` (or your python path. On cmd, write `where python`)
    - PYTHONPATH: `excel_functions`
    - UDF Modules: `excel_functions`
    6. On excel xlwings tab:
    - Click on "Import Functions"
    7. Finally, try: 
            
        =PH_BDH("SPX Index")
            
        =PH_WEIGHTS("SPX Index")
""")
st.write(f':red[IMPORTANT: If you want to use the add-in on a worksheet that already exists, you need to repeat steps 4 and 6 the first time.]')

st.subheader("📰 Documentation")
st.markdown("""


""")
st.markdown("""
    ##### PH_BDH(ticker; field; start_date; end_date; override_period; frequency; fill)
            Description:
                Same functionality as Bloomberg BDH. Query on Datalake Database.
        Parameters:
            1. ticker: Bloomberg Ticker. E.g: "SPX Index".
            2. field: Bloomberg Field. Default (Blank): PX_Last.
            3. start_date: Start Date in excel date format. Default (Blank): 1990-01-01 
            4. end_date: End Date in excel date format. Default (Blank): Today. 
            5. override_period: Bloomberg Override Period for Consensus Field. E.g: "1BF", "2BF", "1GY", "2GY", "3GY".
            6. frequency: Data frequency (last ocurrence). E.g: "D", "W", "M", "Q", "Y".
            7. fill: Data fill #N/A values. Default: True.

        Examples:
            =PH_BDH("BZSTSETA Index")  |  PX_Last for Brazil Selic Target Rate.
            =PH_BDH("DAX Index"; "PX_TO_BOOK_RATIO")  |  PX_TO_BOOK_RATIO for DAX Index, complete series on Database.
            =PH_BDH("BRL Curncy";;"2000-01-01")  |  PX_Last for BRL Curncy with start_date 2000-01-01.
            =PH_BDH("USSO1 BGN Curncy";;"2000-01-01";"2023-01-01")  |  PX_Last for USSO1 BGN Curncy with start_date 2000-01-01 and end_date 2023-01-01.
            =PH_BDH("RTY Index";"BEST_EPS";;;"1BF")  |  BEST_EPS 1BF for RTY Index complete series.
            =PH_BDH("BZAA10Y Index";;;;;"M")  |  PX_Last for BZAA10Y Index complete series Monthly.
    
    ##### PH_WEIGHTS(ticker)
            
        Description: 
            Get last weights for an Index. Source: Factset.
        
        Parameters:
            1. ticker: Bloomberg Ticker. E.g: "SPX Index".

        Examples:
            =PH_WEIGHTS("NKY Index")
""")
st.divider()

st.header('📕 Dictionary')
st.subheader('Equity Index')
equity_index_col1, equity_index_col2, equity_index_col3 = st.columns(3)
equity_index_col1.subheader('Tickers')
equity_index_col1.dataframe(tickers_df, width=1500, height=400, hide_index=True)
equity_index_col2.subheader('Bloomberg Fields')
equity_index_col2.dataframe(fields_df, width=600, hide_index=True)
equity_index_col3.subheader('Factset Fields')
equity_index_col3.dataframe(factset_fields_df, width=500, hide_index=True)
st.subheader('Currency')
st.dataframe(currency_df)
st.subheader('Rates')
st.dataframe(rates_df)
st.subheader('Rates Monetary Policy')
st.dataframe(rates_monetary_policy_df)
st.subheader('Bonds')
st.dataframe(bonds_df)
st.subheader('Housing')
st.dataframe(housing_df)
st.subheader('Commodities')
st.dataframe(commodities_df)
st.subheader('Economics')
st.dataframe(economics_df)
st.subheader('Datastream Economics')
st.dataframe(datastream_economics_df)
st.divider()

