from helpers.aux_functions import calcular_metricas, execute_postgresql_query
import pandas as pd
from datetime import datetime, date, timedelta
import settings
import numpy as np
from helpers.aux_functions import extract_ticker

def equity_index_monitor_df():
    return_col = [
        '1D (%)', 
        '1W (%)', 
        'MTD (%)', 
        '1M (%)', 
        '3M (%)', 
        '6M (%)', 
        '12M (%)', 
        'YTD (%)'
    ]

    tickers = pd.read_sql(
        sql=f"""
            SELECT DISTINCT 
                ticker
            FROM bbg_dict_tickers bdt
            WHERE "class" = 'Equity Index'
        """,
        con=settings.ENGINE
    )['ticker'].to_list()

    ## ------------------- PX_Last
    PX_Last_df_list = []
    for ticker in tickers:
        PX_Last_df_list.append(extract_ticker(index=ticker, field='PX_Last'))
    PX_Last_df = pd.concat(PX_Last_df_list, axis=1)

    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    PX_Last_return_df = pd.DataFrame()
    for col in PX_Last_df:
        aux_df = pd.DataFrame({col: calcular_metricas(PX_Last_df[col])}).T
        PX_Last_return_df = pd.concat([PX_Last_return_df, aux_df], axis=0)
    PX_Last_return_df = PX_Last_return_df.apply(lambda col: col * 100, axis=1)
    PX_Last_return_df = PX_Last_return_df[return_col]
    PX_Last_return_df = PX_Last_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # Last price
    ultimos_valores = {}
    for coluna in PX_Last_df.columns:
        valores_sem_nan = PX_Last_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    PX_Last_date_df = pd.DataFrame(ultimos_valores).T

    # Price
    PX_Last_output_df = pd.concat([PX_Last_date_df, PX_Last_return_df], axis=1).copy()
    PX_Last_output_df.columns = pd.MultiIndex.from_product([['P'], PX_Last_output_df.columns])
    PX_Last_output_df

    ## ------------------- TOT_RETURN_INDEX_GROSS_DVDS_VARIATION
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_df_list = []
    for ticker in tickers:
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_df_list.append(extract_ticker(index=ticker, field='TOT_RETURN_INDEX_GROSS_DVDS_VARIATION'))
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_df = pd.concat(TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_df_list, axis=1)   

    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df =  (TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_df + 1).cumprod()
    

    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df = pd.DataFrame()
    for col in TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df:
        aux_df = pd.DataFrame({col: calcular_metricas(TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df[col])}).T
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df = pd.concat([TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df, aux_df], axis=0)
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df.apply(lambda col: col * 100, axis=1)
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df[return_col]
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # Last price
    ultimos_valores = {}
    for coluna in TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df.columns:
        valores_sem_nan = TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_date_df = pd.DataFrame(ultimos_valores).T

    # Price
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_output_df = pd.concat([TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_date_df, TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_return_df], axis=1).copy()
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_output_df.columns = pd.MultiIndex.from_product([['TR'], TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_output_df.columns])
    TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_output_df

    ## ------------------ BEST_EPS_1BF
    BEST_EPS_1BF_df_list = []
    for ticker in tickers:
        BEST_EPS_1BF_df_list.append(extract_ticker(index=ticker, field='BEST_EPS', override_period='1BF'))
    BEST_EPS_1BF_df = pd.concat(BEST_EPS_1BF_df_list, axis=1) 
    # # NASDAQ Outliers
    # nasdaq_aux = BEST_EPS_1BF_df['CCMP Index']
    # nasdaq_aux = nasdaq_aux[(nasdaq_aux < 10000) & (nasdaq_aux > -1200)]
    # BEST_EPS_1BF_df['CCMP Index'] = nasdaq_aux

    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    BEST_EPS_1BF_return_df = pd.DataFrame()
    for col in BEST_EPS_1BF_df:
        aux_df = pd.DataFrame({col: calcular_metricas(BEST_EPS_1BF_df[col])}).T
        BEST_EPS_1BF_return_df = pd.concat([BEST_EPS_1BF_return_df, aux_df], axis=0)
    BEST_EPS_1BF_return_df = BEST_EPS_1BF_return_df.apply(lambda col: col * 100, axis=1)
    BEST_EPS_1BF_return_df = BEST_EPS_1BF_return_df[return_col]
    BEST_EPS_1BF_return_df = BEST_EPS_1BF_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # BEST_EPS_1BF
    ultimos_valores = {}
    for coluna in BEST_EPS_1BF_df.columns:
        valores_sem_nan = BEST_EPS_1BF_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    BEST_EPS_1BF_date_df = pd.DataFrame(ultimos_valores).T

    # Concat
    BEST_EPS_1BF_output_df = pd.concat([BEST_EPS_1BF_date_df, BEST_EPS_1BF_return_df], axis=1).copy()
    # Format multindex
    BEST_EPS_1BF_output_df.columns = pd.MultiIndex.from_product([['EPS 1BF'], BEST_EPS_1BF_output_df.columns])
    BEST_EPS_1BF_output_df

    ## ------------------ BEST_EPS_2BF
    BEST_EPS_2BF_df_list = []
    for ticker in tickers:
        BEST_EPS_2BF_df_list.append(extract_ticker(index=ticker, field='BEST_EPS', override_period='2BF'))
    BEST_EPS_2BF_df = pd.concat(BEST_EPS_2BF_df_list, axis=1) 
    # NASDAQ Outliers
    nasdaq_aux = BEST_EPS_2BF_df['CCMP Index']
    nasdaq_aux = nasdaq_aux[(nasdaq_aux < 10000) & (nasdaq_aux > -1200)]
    BEST_EPS_2BF_df['CCMP Index'] = nasdaq_aux
    
    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    BEST_EPS_2BF_return_df = pd.DataFrame()
    for col in BEST_EPS_2BF_df:
        aux_df = pd.DataFrame({col: calcular_metricas(BEST_EPS_2BF_df[col])}).T
        BEST_EPS_2BF_return_df = pd.concat([BEST_EPS_2BF_return_df, aux_df], axis=0)
    BEST_EPS_2BF_return_df = BEST_EPS_2BF_return_df.apply(lambda col: col * 100, axis=1)
    BEST_EPS_2BF_return_df = BEST_EPS_2BF_return_df[return_col]
    BEST_EPS_2BF_return_df = BEST_EPS_2BF_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # BEST_EPS_2BF
    ultimos_valores = {}
    for coluna in BEST_EPS_2BF_df.columns:
        valores_sem_nan = BEST_EPS_2BF_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    BEST_EPS_2BF_date_df = pd.DataFrame(ultimos_valores).T

    # Concat
    BEST_EPS_2BF_output_df = pd.concat([BEST_EPS_2BF_date_df, BEST_EPS_2BF_return_df], axis=1).copy()
    # Format multindex
    BEST_EPS_2BF_output_df.columns = pd.MultiIndex.from_product([['EPS 2BF'], BEST_EPS_2BF_output_df.columns])
    BEST_EPS_2BF_output_df

    ## ------------------ TRAIL_12M_EPS
    TRAIL_12M_EPS_df_list = []
    for ticker in tickers:
        TRAIL_12M_EPS_df_list.append(extract_ticker(index=ticker, field='TRAIL_12M_EPS'))
    TRAIL_12M_EPS_df = pd.concat(TRAIL_12M_EPS_df_list, axis=1) 

    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    TRAIL_12M_EPS_return_df = pd.DataFrame()
    for col in TRAIL_12M_EPS_df:
        aux_df = pd.DataFrame({col: calcular_metricas(TRAIL_12M_EPS_df[col])}).T
        TRAIL_12M_EPS_return_df = pd.concat([TRAIL_12M_EPS_return_df, aux_df], axis=0)
    TRAIL_12M_EPS_return_df = TRAIL_12M_EPS_return_df.apply(lambda col: col * 100, axis=1)
    TRAIL_12M_EPS_return_df = TRAIL_12M_EPS_return_df[return_col]
    TRAIL_12M_EPS_return_df = TRAIL_12M_EPS_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # TRAIL_12M_EPS
    ultimos_valores = {}
    for coluna in TRAIL_12M_EPS_df.columns:
        valores_sem_nan = TRAIL_12M_EPS_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    TRAIL_12M_EPS_date_df = pd.DataFrame(ultimos_valores).T

    # Concat
    TRAIL_12M_EPS_output_df = pd.concat([TRAIL_12M_EPS_date_df, TRAIL_12M_EPS_return_df], axis=1).copy()
    # Format multindex
    TRAIL_12M_EPS_output_df.columns = pd.MultiIndex.from_product([['EPS T12M'], TRAIL_12M_EPS_output_df.columns])
    TRAIL_12M_EPS_output_df

    ## ------------------ T12M_DIL_EPS_CONT_OPS
    T12M_DIL_EPS_CONT_OPS_df_list = []
    for ticker in tickers:
        T12M_DIL_EPS_CONT_OPS_df_list.append(extract_ticker(index=ticker, field='T12M_DIL_EPS_CONT_OPS'))
    T12M_DIL_EPS_CONT_OPS_df = pd.concat(T12M_DIL_EPS_CONT_OPS_df_list, axis=1) 

    # Retornos 1D, 1W, 3M, 6M, 12M, YTD, ...
    T12M_DIL_EPS_CONT_OPS_return_df = pd.DataFrame()
    for col in T12M_DIL_EPS_CONT_OPS_df:
        aux_df = pd.DataFrame({col: calcular_metricas(T12M_DIL_EPS_CONT_OPS_df[col])}).T
        T12M_DIL_EPS_CONT_OPS_return_df = pd.concat([T12M_DIL_EPS_CONT_OPS_return_df, aux_df], axis=0)
    T12M_DIL_EPS_CONT_OPS_return_df = T12M_DIL_EPS_CONT_OPS_return_df.apply(lambda col: col * 100, axis=1)
    T12M_DIL_EPS_CONT_OPS_return_df = T12M_DIL_EPS_CONT_OPS_return_df[return_col]
    T12M_DIL_EPS_CONT_OPS_return_df = T12M_DIL_EPS_CONT_OPS_return_df.rename(columns={
        '1D (%)': '(1D)', 
        '1W (%)': '(1W)', 
        'MTD (%)': '(MTD)', 
        '1M (%)': '(1M)', 
        '3M (%)': '(3M)', 
        '6M (%)': '(6M)', 
        '12M (%)': '(12M)', 
        'YTD (%)': '(YTD)',
    })
    # T12M_DIL_EPS_CONT_OPS
    ultimos_valores = {}
    for coluna in T12M_DIL_EPS_CONT_OPS_df.columns:
        valores_sem_nan = T12M_DIL_EPS_CONT_OPS_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    T12M_DIL_EPS_CONT_OPS_date_df = pd.DataFrame(ultimos_valores).T

    # Concat
    T12M_DIL_EPS_CONT_OPS_output_df = pd.concat([T12M_DIL_EPS_CONT_OPS_date_df, T12M_DIL_EPS_CONT_OPS_return_df], axis=1).copy()
    # Format multindex
    T12M_DIL_EPS_CONT_OPS_output_df.columns = pd.MultiIndex.from_product([['EPS T12M C/O'], T12M_DIL_EPS_CONT_OPS_output_df.columns])
    T12M_DIL_EPS_CONT_OPS_output_df


    ## ------------------ PX_TO_BOOK_RATIO
    PX_TO_BOOK_RATIO_df_list = []
    for ticker in tickers:
        PX_TO_BOOK_RATIO_df_list.append(extract_ticker(index=ticker, field='PX_TO_BOOK_RATIO'))
    PX_TO_BOOK_RATIO_df = pd.concat(PX_TO_BOOK_RATIO_df_list, axis=1)

    ultimos_valores = {}
    for coluna in PX_TO_BOOK_RATIO_df.columns:
        valores_sem_nan = PX_TO_BOOK_RATIO_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    PX_TO_BOOK_RATIO_date_df = pd.DataFrame(ultimos_valores).T

    PX_TO_BOOK_RATIO_output_df = PX_TO_BOOK_RATIO_date_df.copy()
    # Format multindex
    PX_TO_BOOK_RATIO_output_df.columns = pd.MultiIndex.from_product([['P/B'], PX_TO_BOOK_RATIO_output_df.columns])
    PX_TO_BOOK_RATIO_output_df

    ## ------------------ TRAIL_12M_SALES_PER_SH
    TRAIL_12M_SALES_PER_SH_df_list = []
    for ticker in tickers:
        TRAIL_12M_SALES_PER_SH_df_list.append(extract_ticker(index=ticker, field='TRAIL_12M_SALES_PER_SH'))
    TRAIL_12M_SALES_PER_SH_df = pd.concat(TRAIL_12M_SALES_PER_SH_df_list, axis=1) 

    ultimos_valores = {}
    for coluna in TRAIL_12M_SALES_PER_SH_df.columns:
        valores_sem_nan = TRAIL_12M_SALES_PER_SH_df[coluna].dropna()
        if not valores_sem_nan.empty:
            ultimos_valores[coluna] = {
                'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
                '': valores_sem_nan.iloc[-1]
            }

    # Resultado
    TRAIL_12M_SALES_PER_SH_date_df = pd.DataFrame(ultimos_valores).T

    TRAIL_12M_SALES_PER_SH_output_df = TRAIL_12M_SALES_PER_SH_date_df.copy()
    # Format multindex
    TRAIL_12M_SALES_PER_SH_output_df.columns = pd.MultiIndex.from_product([['Sales T12M'], TRAIL_12M_SALES_PER_SH_output_df.columns])
    TRAIL_12M_SALES_PER_SH_output_df

    # ## ------------------ BEST_DIV_YLD_1BF
    # BEST_DIV_YLD_1BF_df = pd.read_excel(f"{index_path}\\BEST_DIV_YLD_1BF.xlsx", index_col=0)

    # ultimos_valores = {}
    # for coluna in BEST_DIV_YLD_1BF_df.columns:
    #     valores_sem_nan = BEST_DIV_YLD_1BF_df[coluna].dropna()
    #     if not valores_sem_nan.empty:
    #         ultimos_valores[coluna] = {
    #             'Date': valores_sem_nan.index[-1].strftime("%Y-%m-%d"),
    #             '': valores_sem_nan.iloc[-1]
    #         }

    # # Resultado
    # BEST_DIV_YLD_1BF_date_df = pd.DataFrame(ultimos_valores).T

    # BEST_DIV_YLD_1BF_output_df = BEST_DIV_YLD_1BF_date_df.copy()
    # # Format multindex
    # BEST_DIV_YLD_1BF_output_df.columns = pd.MultiIndex.from_product([['D/Y 1BF'], BEST_DIV_YLD_1BF_output_df.columns])
    # BEST_DIV_YLD_1BF_output_df

    # PE RATIO
    PE_RATIO_df_list = []
    for ticker in tickers:
        PE_RATIO_df_list.append(extract_ticker(index=ticker, field='PE_RATIO'))
    PE_RATIO_df = pd.concat(PE_RATIO_df_list, axis=1).dropna(how='all')
    
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-= Final Dataframe-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    final_df = pd.concat([
        PX_Last_output_df, 
        TOT_RETURN_INDEX_GROSS_DVDS_VARIATION_1D_output_df, 
        BEST_EPS_1BF_output_df, 
        BEST_EPS_2BF_output_df, 
        TRAIL_12M_EPS_output_df, 
        T12M_DIL_EPS_CONT_OPS_output_df, 
        PX_TO_BOOK_RATIO_output_df, 
        TRAIL_12M_SALES_PER_SH_output_df
    ], axis=1)
    final_df.columns = final_df.columns.get_level_values(0) + ' ' + final_df.columns.get_level_values(1)
    final_df = final_df.rename_axis('Index').reset_index()

    # Remove list of index
    index_to_remove = [
        'ESA Index',
        'H11137 Index',
        'CSI1000 Index'
    ]
    final_df = final_df[~final_df['Index'].isin(index_to_remove)]
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-= Calculating Dataframes (TRANSFORM) -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    
    EY_T12M_df = ((TRAIL_12M_EPS_df/PX_Last_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EY_T12M_CONT_OPS_df = ((T12M_DIL_EPS_CONT_OPS_df/PX_Last_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EY_1BF_df = ((BEST_EPS_1BF_df/PX_Last_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EY_2BF_df = ((BEST_EPS_2BF_df/PX_Last_df)*100).dropna(how='all').dropna(axis=1, how='all')
    
    EPS_T12M_per_SALES_df = ((TRAIL_12M_EPS_df/TRAIL_12M_SALES_PER_SH_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EPS_T12M_CONT_OPS_per_SALES_df = ((T12M_DIL_EPS_CONT_OPS_df/TRAIL_12M_SALES_PER_SH_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EPS_1BF_per_SALES_df = ((BEST_EPS_1BF_df/TRAIL_12M_SALES_PER_SH_df)*100).dropna(how='all').dropna(axis=1, how='all')
    EPS_2BF_per_SALES_df = ((BEST_EPS_2BF_df/TRAIL_12M_SALES_PER_SH_df)*100).dropna(how='all').dropna(axis=1, how='all')

    DELTA_EPS_1BF_T12M_df = ((BEST_EPS_1BF_df/TRAIL_12M_EPS_df - 1)*100).dropna(how='all').dropna(axis=1, how='all')
    DELTA_EPS_2BF_T12M_df = ((BEST_EPS_2BF_df/TRAIL_12M_EPS_df - 1)*100).dropna(how='all').dropna(axis=1, how='all')
    DELTA_EPS_1BF_T12M_CONT_OPS_df = ((BEST_EPS_1BF_df/T12M_DIL_EPS_CONT_OPS_df - 1)*100).dropna(how='all').dropna(axis=1, how='all')
    DELTA_EPS_2BF_T12M_CONT_OPS_df = ((BEST_EPS_2BF_df/T12M_DIL_EPS_CONT_OPS_df - 1)*100).dropna(how='all').dropna(axis=1, how='all')

 
    # -=-=-=-=-=-=-=-=-=-=-=-=-=-= Final Dataframe (TRANSFORM) -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    
    final_df['*E/Y T12M '] = (final_df['EPS T12M '] / final_df['P '])*100  
    final_df['*E/Y T12M C/O'] = (final_df['EPS T12M C/O '] / final_df['P '])*100  
    final_df['*E/Y 1BF '] = (final_df['EPS 1BF '] / final_df['P '])*100  
    final_df['*E/Y 2BF '] = (final_df['EPS 2BF '] / final_df['P '])*100
    
    final_df['*EPS/Sales T12M '] = (final_df['EPS T12M '] / final_df['Sales T12M '])*100  
    final_df['*EPS/Sales T12M C/O '] = (final_df['EPS T12M C/O '] / final_df['Sales T12M '])*100  
    final_df['*EPS/Sales 1BF '] = (final_df['EPS 1BF '] / final_df['Sales T12M '])*100  
    final_df['*EPS/Sales 2BF '] = (final_df['EPS 2BF '] / final_df['Sales T12M '])*100

    final_df['ΔEPS 1BF-T12M'] = ((final_df['EPS 1BF '] / final_df['EPS T12M ']) - 1)*100
    final_df['ΔEPS 2BF-T12M'] = ((final_df['EPS 2BF '] / final_df['EPS T12M ']) - 1)*100
    final_df['ΔEPS 1BF-T12M C/O'] = ((final_df['EPS 1BF '] / final_df['EPS T12M C/O ']) - 1)*100
    final_df['ΔEPS 2BF-T12M C/O'] = ((final_df['EPS 2BF '] / final_df['EPS T12M C/O ']) - 1)*100
    # breakpoint()

    # final_df['ln PE 1BF'] = np.log((100/final_df['*E/Y 1BF ']).astype(float))
    # final_df['ln EPS 1BF/EPS T12M'] = np.log((final_df['EPS 1BF '] / final_df['EPS T12M ']).astype(float))
    # final_df['ln EPS/EPS T12M'] = np.log((TRAIL_12M_EPS_df.resample('Q').mean().iloc[-1]/TRAIL_12M_EPS_df.resample('Q').mean().iloc[-5]).astype(float))
    # final_df['ln PE T12M'] = np.log((100/final_df['*E/Y T12M ']).astype(float))
    # final_df['ln TR 1BF'] = final_df['ln PE 1BF'] + final_df['ln EPS 1BF/EPS T12M'] - final_df['ln PE T12M']
    # final_df['ln TR'] = final_df['ln PE 1BF'] + final_df['ln EPS/EPS T12M'] - final_df['ln PE T12M']
    # final_df['TR 1BF'] = (np.exp(final_df['ln TR 1BF'])-1) * 100


    # -=-=-=-=-=-=-=-=-=-=-=-=-=-= -=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=
    # Merge dict
    dict_df = execute_postgresql_query(query=f"""
            SELECT * FROM public.bbg_dict_tickers;
    """)
    dict_df = dict_df.rename(columns={
        'ticker': 'bbg_ticker',
        'gics_sector_name': 'GICS_SECTOR_NAME',
        'gics_industry_name': 'GICS_INDUSTRY_NAME',
        'gics_sub_industry_name': 'GICS_SUB_INDUSTRY_NAME',
    })
    dict_df = dict_df[dict_df['class'] == 'Equity Index']
    em_dict_df = dict_df[['country', 'bbg_ticker', 'country_classification', 'description', 'CRNCY']]
    final_df = pd.merge(right=final_df, right_on='Index', left=em_dict_df, left_on='bbg_ticker', how='inner').drop_duplicates().rename(columns={
        'country': 'Country',
        'description': 'Description',
        'CRNCY': 'Cur'
    })
    final_df['Index'] = final_df['Index'].str.replace(" Index", "")

    string_columns = ['Index', 'Country', 'Description', 'Cur', 'bbg_ticker', 'country_classification'] + [col for col in final_df.columns.to_list() if "Date" in col]
    numeric_columns = [column for column in final_df.columns.to_list() if column not in string_columns]
    final_df[numeric_columns] = final_df[numeric_columns].apply(pd.to_numeric)
    return (
        final_df, 
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

def get_rates():
    dict_df = execute_postgresql_query(query=f"""
            SELECT * FROM public.bbg_dict_rates;
    """)
    dict_df['name'] = dict_df['rate'] + ' (' + dict_df['currency'] + ')'
    column_dict = dict_df[['bbg_ticker', 'name']].set_index('bbg_ticker').to_dict()['name']
    query = """
        SELECT * FROM public.bbg_rates;
    """
    df = execute_postgresql_query(query=query)
    # Drop duplicated columns
    df = df.loc[:,~df.columns.duplicated()].copy()

    # Primeiro, classifique o DataFrame por 'date' e 'extraction_date' em ordem decrescente.
    dff = df.sort_values(by=['date', 'extraction_date'], ascending=[True, False])

    # Em seguida, use o método drop_duplicates para manter apenas a primeira ocorrência de cada combinação única de colunas.
    # Isso garantirá que você esteja mantendo as ocorrências mais recentes de 'extraction_date'.
    dff = dff.drop_duplicates(subset=['date', 'ticker', 'field'], keep='first')
    dff = dff.sort_index()
    pivot_df = dff.pivot_table(index=['date'], columns='ticker', values='value').reset_index()

    pivot_df['date'] = pd.to_datetime(pivot_df['date'])
    pivot_df.set_index('date', inplace=True)
    pivot_df = pivot_df.rename(columns=column_dict)
    return pivot_df

def get_us_recession():
    df = execute_postgresql_query("""
        SELECT
            *
        FROM public.fred_nber_recession
        WHERE ticker = 'USREC'
            AND value = '1';
    """)
    df['date'] = pd.to_datetime(df['date'])
    recession_periods = []
    start_date = None

    for date in df['date']:
        if start_date is None:
            start_date = date
        elif (date - start_date).days != 30:  # Verifique se há um mês de diferença (aproximadamente)
            end_date = start_date + pd.DateOffset(months=1) - pd.DateOffset(days=1)
            recession_periods.append({'start_date': start_date, 'end_date': end_date})
            start_date = None
        else:
            start_date = date

    # Crie o DataFrame de recession_dates
    recession_dates = pd.DataFrame(recession_periods)
    return recession_dates
