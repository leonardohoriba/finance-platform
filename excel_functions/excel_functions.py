import xlwings as xw
import pandas as pd
from sqlalchemy import create_engine
from urllib.parse import quote_plus
from datetime import datetime, date
import numpy as np
from src.utils.settings import *
import os

def execute_postgresql_query(query, schema_name=SCHEMA, database=None):
    """
    Executa uma consulta SQL no banco de dados PostgreSQL e retorna os resultados como um DataFrame.

    :param query: A consulta SQL a ser executada.
    :param db_config: Um dicionário contendo as configurações de conexão com o banco de dados.
    :return: Um DataFrame contendo os resultados da consulta.
    """
    try:
        engine = create_engine(f'postgresql://{quote_plus(os.environ["DATABASE_USER"])}:{quote_plus(os.environ["DATABASE_PASSWORD"])}@{quote_plus(os.environ["DATABASE_HOST"])}:{quote_plus(os.environ["DATABASE_PORT"])}/{database or quote_plus(os.environ["DATABASE_NAME"])}')
        # Defina temporariamente o search_path para o schema <schema_name>
        with engine.connect() as conn:
            conn.execute(f'SET search_path TO {schema_name}')
            # Executa a consulta SQL e carrega os resultados em um DataFrame
            result_df = pd.read_sql_query(query, engine)
        
        return result_df

    except Exception as e:
        print(f'Ocorreu um erro: {e}')
        return pd.DataFrame()

def format_frequency(frequency):
    if frequency == 'D' or 'daily' in frequency.lower():
        return 'D' 
    elif frequency == 'W' or 'week' in frequency.lower():
        return 'W'
    elif frequency == 'M' or 'month' in frequency.lower():
        return 'M'
    elif frequency == 'Q' or 'quarter' in frequency.lower():
        return 'Q'
    elif frequency == 'Y' or 'year' in frequency.lower():
        return 'Y'
    else:
        return None

    
def main():
    wb = xw.Book.caller()
    sheet = wb.sheets[0]
    if sheet["A1"].value == "Hello xlwings!":
        sheet["A1"].value = "Bye xlwings!"
    else:
        sheet["A1"].value = "Hello xlwings!"

@xw.func
def get_caller_address(caller):
    # caller will not be exposed in Excel, so use it like so:
    # =get_caller_address()
    return caller.address

""""
    Help info
"""
@xw.func
@xw.ret(expand='table', index=None, header=None)
@xw.arg('param')
def PH_HELP(param=None):
    if not param:
        return pd.DataFrame(
            {
                "PH_HELP(\"tickers\")": "List of all Tickers in Database.",
                "PH_HELP(\"fields\")": "List of all Fields in Database.",
                "PH_WEIGHTS(Index)": "Weights and constituents for an index.",
                "PH_BDH(Index, Field, Start Date, End Date, Override Period)": "BDH Data. E.g:  =PH_BDH(\"SPX Index\", \"BEST_EPS\", \"2023-01-01\", \"2023-02-20\", \"1BF\")",
            }.items(),
            columns=['Formula', 'Description']
        )
    
    if 'ticker' in param.lower():
        query = f"""
            SELECT
                ticker,
                "class"
            FROM public.bbg_reference_table;    
        """
        return execute_postgresql_query(query=query)
    
    elif 'field' in param.lower():
        query = f"""
            SELECT
                field,
                "class"
            FROM public.bbg_dict_fields
            ORDER BY "class", field;
        """
        return execute_postgresql_query(query=query)
    else:
        return

""""
    Extract Factset Weights
"""
@xw.func
@xw.ret(expand='table', index=False, header=False)
@xw.arg('ticker', doc='Bloomberg Ticker')
def PH_WEIGHTS(ticker):
    "Return last index weight. Source: Factset"
    query = f"""
        WITH bbg_dict AS (
            SELECT
                factset_ticker
            FROM public.bbg_dict_tickers dt
            WHERE dt."class" = 'Equity Index'
            AND dt.ticker = '{ticker}'
            AND factset_ticker IS NOT NULL
        ),
        max_date_table AS (
            SELECT 
                w."requestId", MAX(w."date") AS max_date
            FROM public.factset_weights w
            WHERE w."requestId" = (SELECT factset_ticker FROM bbg_dict)
            GROUP BY w."requestId"
        ),
        weights AS (
            SELECT
                w."date",
                w."securityTicker",
                w."securityName",
                w."weightClose"
            FROM public.factset_weights w
            INNER JOIN max_date_table mdt 
                ON w."requestId" = mdt."requestId" AND w."date" = mdt.max_date
            WHERE w."issueType" NOT IN (
                'Cash/Repo/MM', 'Open-End Mutual Fund', 'Future Agreement', 'Warrant/Right'
            )
        ),
        companies AS (
            SELECT DISTINCT
                *
            FROM weights w
            ORDER BY w."weightClose" DESC
        )
        SELECT DISTINCT
            c.*,
            s."FG_FACTSET_ECONOMY"
        FROM companies c
        LEFT JOIN public.factset_sectors s ON c."securityTicker"= s."requestId"
        ORDER BY c."weightClose" DESC;
    """
    df = execute_postgresql_query(query=query)
    df = df.drop_duplicates(subset=[elem for elem in df.columns.to_list() if elem != 'weightClose'], keep='last')
    return df


""""
    Extract Bloomberg Data
"""
@xw.func
@xw.arg('ticker', doc='Bloomberg Ticker')
@xw.arg('field', doc='Bloomberg Field')
@xw.arg('start_date', doc='Start Date. Format: (YYYY-mm-dd)')
@xw.arg('end_date', doc='End Date. Format: (YYYY-mm-dd)')
@xw.arg('override_period', doc='Override. eg: 1BF, 2BF, 1GY, 2GY, 3GY')
@xw.arg('frequency', doc='Frequency (Last value of period). eg: D, W, M, Q, Y')
@xw.arg('fill', doc='Data forward fill. default: True')
@xw.ret(expand='table', header=None)
def PH_BDH(ticker, field=None, start_date=date(1900,1,1), end_date=datetime.now(), override_period=None, frequency=None, fill=True):
    output_df = pd.DataFrame()
    start_date = start_date.strftime('%Y-%m-%d')
    end_date = end_date.strftime('%Y-%m-%d')
    
    if isinstance(ticker, str):
        bd_table = execute_postgresql_query(
            f"""
                SELECT
                    "table"
                FROM public.bbg_reference_table
                WHERE ticker = '{ticker.replace("%", "%%")}'
            """
        )
        if bd_table.empty:
            return "Ticker not found."
        elif bd_table.shape[0] > 1:
            return "Database Reference Table Error."
        table = bd_table['table'].iloc[0]
        
        if not field:
            if 'factset' in table:
                field = 'P_PRICE'
            else:
                field = 'PX_Last'

        if isinstance(field, str):
            query = f"""
                WITH field_query AS (
                    SELECT
                        t."date",
                        t.ticker,
                        {"CASE WHEN t.override_period IS NULL THEN t.field ELSE CONCAT(t.field, '_', t.override_period) END" if 'equity' in table and 'bbg' in table else "t.field"} AS field,
                        t.value,
                        t.extraction_date
                    FROM {table} t
                    WHERE t.field = '{field}'
                        AND t.ticker = '{ticker.replace("%", "%%")}'
                        AND t."date" >= '{start_date}' 
                        AND t."date" <= '{end_date}'
                        {"AND t.override_period = " + "'"+ override_period + "'" if override_period else "AND (t.override_period IS NULL OR t.override_period = '1BF')" if 'equity' in table and 'bbg' in table else ""}
                    ORDER BY t."date", t.extraction_date
                ),
                max_date AS (
                    SELECT
                        "date",
                        MAX(extraction_date) AS extraction_date
                    FROM field_query
                    WHERE ticker = '{ticker.replace("%", "%%")}'
                    GROUP BY "date"
                )
                SELECT DISTINCT
                    fq."date",
                    fq.value
                FROM field_query fq
                INNER JOIN max_date md ON fq."date" = md."date" AND fq.extraction_date = md.extraction_date;
            """
            df = execute_postgresql_query(query=query)
            if not df.empty:
                df = df.dropna(subset=['value'])
                df = df.set_index('date').rename(columns={'value': ticker}).sort_index()
                df.index = pd.to_datetime(df.index)
                if fill:
                    df = df.ffill()
                if frequency:
                    df = df.resample(format_frequency(frequency)).last()
                return df
        
        elif isinstance(field, list):
            for fld in field:
                query = f"""
                    WITH field_query AS (
                        SELECT
                            t."date",
                            t.ticker,
                            {"CASE WHEN t.override_period IS NULL THEN t.field ELSE CONCAT(t.field, '_', t.override_period) END" if 'equity' in table and 'bbg' in table else "t.field"} AS field,
                            t.value,
                            t.extraction_date
                        FROM {table} t
                        WHERE t.field = '{fld}'
                            AND t.ticker = '{ticker}'
                            AND t."date" >= '{start_date}' 
                            AND t."date" <= '{end_date}'
                            {"AND t.override_period = " + "'"+ override_period + "'" if override_period else "AND (t.override_period IS NULL OR t.override_period = '1BF')" if 'equity' in table and 'bbg' in table else ""}

                        ORDER BY t."date", t.extraction_date
                    ),
                    max_date AS (
                        SELECT
                            "date",
                            MAX(extraction_date) AS extraction_date
                        FROM field_query
                        WHERE ticker = '{ticker}'
                        GROUP BY "date"
                    )
                    SELECT DISTINCT
                        fq."date",
                        fq.value
                    FROM field_query fq
                    INNER JOIN max_date md ON fq."date" = md."date" AND fq.extraction_date = md.extraction_date;
                """
                aux_df = execute_postgresql_query(query=query)
                if not aux_df.empty:
                    aux_df = aux_df.dropna(subset=['value'])
                    aux_df = aux_df.set_index('date').rename(columns={'value': ticker}).sort_index()
                    aux_df.index = pd.to_datetime(aux_df.index)
                    if frequency:
                        aux_df = aux_df.resample(format_frequency(frequency)).last()
                    # output_df = pd.concat([output_df, aux_df], axis=1)
                    output_df = pd.merge(left=output_df, right=aux_df, left_index=True, right_index=True, how='outer')
        output_df = output_df.sort_index(ascending=True)
        if fill:
            output_df = output_df.ffill()
        return output_df
    
    elif (isinstance(field, str) or not field) and isinstance(ticker, list):
        for tick in ticker:
            bd_table = execute_postgresql_query(
                f"""
                    SELECT
                        "table"
                    FROM public.bbg_reference_table
                    WHERE ticker = '{tick.replace("%", "%%")}'
                """
            )
            if bd_table.empty:
                output_df[tick] = np.nan 
                continue
                # return "Ticker not found."
            elif bd_table.shape[0] > 1:
                return "Database Reference Table Error."
            table = bd_table['table'].iloc[0]

            if not field:
                if 'factset' in table:
                    field = 'P_PRICE'
                else:
                    field = 'PX_Last'

            query = f"""
                WITH field_query AS (
                    SELECT
                        t."date",
                        t.ticker,
                        {"CASE WHEN t.override_period IS NULL THEN t.field ELSE CONCAT(t.field, '_', t.override_period) END" if 'equity' in table and 'bbg' in table else "t.field"} AS field,
                        t.value,
                        t.extraction_date
                    FROM {table} t
                    WHERE t.field = '{field}'
                        AND t.ticker = '{tick.replace("%", "%%")}'
                        AND t."date" >= '{start_date}' 
                        AND t."date" <= '{end_date}'
                        {"AND t.override_period = " + "'"+ override_period + "'" if override_period else "AND (t.override_period IS NULL OR t.override_period = '1BF')" if 'equity' in table and 'bbg' in table else ""}
                    ORDER BY t."date", t.extraction_date
                ),
                max_date AS (
                    SELECT
                        "date",
                        MAX(extraction_date) AS extraction_date
                    FROM field_query
                    WHERE ticker = '{tick.replace("%", "%%")}'
                    GROUP BY "date"
                )
                SELECT DISTINCT
                    fq."date",
                    fq.value
                FROM field_query fq
                INNER JOIN max_date md ON fq."date" = md."date" AND fq.extraction_date = md.extraction_date;
            """
            aux_df = execute_postgresql_query(query=query)
            if not aux_df.empty:
                aux_df = aux_df.dropna(subset=['value'])
                aux_df = aux_df.set_index('date').rename(columns={'value': tick}).sort_index()
                aux_df.index = pd.to_datetime(aux_df.index)
                if frequency:
                    aux_df = aux_df.resample(format_frequency(frequency)).last()
                # output_df = pd.concat([output_df, aux_df], axis=1)
                output_df = pd.merge(left=output_df, right=aux_df, left_index=True, right_index=True, how='outer')
        output_df = output_df.sort_index(ascending=True)
        if fill:
            output_df = output_df.ffill()
        return output_df
    else:
        return "Formula Error"
    



if __name__ == "__main__":
    xw.Book("excel_functions.xlsm").set_mock_caller()
    main()
