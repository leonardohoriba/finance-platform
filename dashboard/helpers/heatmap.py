from helpers.aux_functions import execute_postgresql_query, calcular_metricas
import pandas as pd
import ffn


def equity_index_price():
    query = """
        WITH index_prices AS (
            SELECT *
            FROM public.bbg_equity_index e
            WHERE e.field = 'PX_Last'
                AND e."date" > (current_date - INTERVAL '2 year')
        )
        SELECT * FROM index_prices;
    """
    df = execute_postgresql_query(
        query=query
    )

    dict_query = """
        WITH dict AS (
            SELECT
                t.ticker,
                t."class",
                t."index",
                t.country_iso,
                t.description,
                c.name AS country
            FROM public.bbg_dict_tickers t
            INNER JOIN public.country_codes c
                ON t.country_iso = c."alpha-2"
            WHERE t."class" = 'Equity Index'
        )
        SELECT * FROM dict;
    """
    dict_df = execute_postgresql_query(
        query=dict_query
    )



    df.index = pd.to_datetime(df['date'])

    # Agrupe o DataFrame por 'ticker' e aplique a função de cálculo
    resultados = df.groupby('ticker')['value'].agg(calcular_metricas).reset_index()

    # Expanda a coluna 'value' em colunas separadas para cada período
    resultados_expandidos = pd.DataFrame(resultados['value'].tolist(), index=resultados.index)

    # Concatene os DataFrames resultados e resultados_expandidos
    resultados = pd.concat([resultados, resultados_expandidos], axis=1)

    # Remova a coluna 'value' original se necessário
    resultados.drop('value', axis=1, inplace=True)

    # Calcular percentuais
    resultados = resultados.apply(lambda x: x * 100 if x.name != 'ticker' else x)

    resultados = resultados.sort_values(by='MTD (%)', ascending=False)
    # O DataFrame 'resultados' agora contém os tickers, métricas calculadas e colunas expandidas para cada período

    res_df = pd.merge(left=resultados, how='left', left_on='ticker', right=dict_df, right_on='ticker')
    res_df = res_df[[
        'country_iso',
        'description',
        'ticker',
        'MTD (%)',
        '3M (%)',
        '6M (%)',
        '12M (%)',
        'YTD (%)',
    ]]
    res_df = res_df.rename(columns={
        'description': 'Index',
        'ticker': 'Ticker',
        'country_iso': 'Country'
    })

    return res_df.drop_duplicates()
