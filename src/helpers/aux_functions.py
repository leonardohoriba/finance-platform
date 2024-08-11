import pandas as pd
from sqlalchemy import create_engine
import os
from urllib.parse import quote_plus
import ffn
import plotly.graph_objects as go
import plotly.express as px
import numpy as np
import statsmodels.api as sm
from sklearn.linear_model import LinearRegression
from sklearn.metrics import r2_score

def accumulated_mean(arr):
    if not pd.isnull(arr.iloc[-1]):  # Verifica se o valor de referência (último valor) não é NaN
        return arr.dropna().mean()  # Exemplo: soma dos valores não-NaN
    return arr.iloc[-1]  # Retorna o valor de referência se for NaN

def accumulated_sum(arr):
    if not pd.isnull(arr.iloc[-1]):  # Verifica se o valor de referência (último valor) não é NaN
        return arr.dropna().sum()  # Exemplo: soma dos valores não-NaN
    return arr.iloc[-1]  # Retorna o valor de referência se for NaN

def index_number(start_date, series):
    start_date = pd.to_datetime(start_date)
    series = series.dropna()
    # Verifique se a data especificada está no índice
    if start_date not in series.index:
        while start_date not in series.index:
            # Se a data não estiver presente, adicione um dia
            start_date += pd.DateOffset(days=1)
            if start_date > series.index.max():
                start_date = series.index.min()
    res_series = (series / series.loc[start_date]) * 100
    return res_series

def interpolate_column(col):
    first_valid_index = col.first_valid_index()
    last_valid_index = col.last_valid_index()

    if first_valid_index is not None and last_valid_index is not None:
        col[first_valid_index:last_valid_index] = col[first_valid_index:last_valid_index].interpolate()

    return col

# Função para calcular as datas iniciais e finais de uma serie
def start_end_dates(series):
    start_date = pd.to_datetime(series.first_valid_index()).strftime('%Y-%m-%d')
    end_date = pd.to_datetime(series.last_valid_index()).strftime('%Y-%m-%d')
    return pd.Series([start_date, end_date], index=['Start Date', 'End Date'])

def upload_dataframe_to_postgresql(df, table_name, schema_name='public', if_exists='append'):
    """
    Carrega um DataFrame em uma tabela do banco de dados PostgreSQL.

    :param df: O DataFrame a ser carregado.
    :param table_name: O nome da tabela no banco de dados.
    :param schema_name: Seleciona o schema do banco de dados.
    :param if_exists: How to behave if the table already exists.
            fail: Raise a ValueError.
            replace: Drop the table before inserting new values.
            append: Insert new values to the existing table..
    """
    try:
        # engine = create_engine(f'postgresql://postgres:postgres@localhost:5432/datalake')
        engine = create_engine(f'postgresql://{quote_plus(os.environ["DATABASE_USER"])}:{quote_plus(os.environ["DATABASE_PASSWORD"])}@{quote_plus(os.environ["DATABASE_HOST"])}:{quote_plus(os.environ["DATABASE_PORT"])}/{quote_plus(os.environ["DATABASE_NAME"])}',
                               connect_args={'options': '-csearch_path={}'.format(schema_name)})
        
        # Defina temporariamente o search_path para o schema <schema_name>
        with engine.begin() as conn:
            # conn.execute(f'SET search_path TO {schema_name}')
            # Carrega o DataFrame na tabela
            df.to_sql(table_name, con=conn, if_exists=if_exists, index=False, schema=schema_name)
        
        print(f'DataFrame carregado com sucesso na tabela {table_name} do banco de dados PostgreSQL!')

    except Exception as e:
        print(f'Ocorreu um erro: {e}')

def execute_postgresql_query(query, schema_name='public', database=None):
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
        return None


# Função para calcular todas as métricas de desempenho de uma vez
def calcular_metricas(coluna):
    stats = ffn.PerformanceStats(coluna)
    filtered_coluna = coluna.dropna()
    return {
        '1D (%)': (filtered_coluna.iloc[-1]/filtered_coluna.iloc[-2]) - 1 if 0 <= len(filtered_coluna) - 2 < len(filtered_coluna) else np.nan,
        '1W (%)': (filtered_coluna.iloc[-1]/filtered_coluna.iloc[-5]) - 1 if 0 <= len(filtered_coluna) - 5 < len(filtered_coluna) else np.nan,
        'MTD (%)': stats.mtd,
        '1M (%)':  (filtered_coluna.iloc[-1]/filtered_coluna.iloc[-22]) - 1 if 0 <= len(filtered_coluna) - 22 < len(filtered_coluna) else np.nan,
        '3M (%)': stats.stats['three_month'],
        '6M (%)': stats.stats['six_month'],
        '12M (%)': stats.stats['one_year'],
        'YTD (%)': stats.ytd,
    }


def plot_series(df, title, xaxis_title, yaxis_title, source=None, height=600, width=1500, title_font_size=22, legend_font_size=16, xaxis_rangeslider_visible=False, shadow=False, recession_dates=None):
    """
    > df.index = date
    > columns = lines 
    """
    df = df.dropna(how='all')
    if isinstance(df, pd.DataFrame):
        df = df.dropna(axis=1, how='all')
    # Crie um gráfico interativo com Plotly
    fig = go.Figure()

    df.index = pd.to_datetime(df.index)
    x = df.index
    ########
    # Adicione a região sombreada, se fornecida
    if recession_dates is not None and not recession_dates.empty:
        fig.add_shape(
        type='rect',
        x0=pd.to_datetime('2000-01-01'),
        x1=pd.to_datetime('2004-01-01'),
        y0=min(df),
        y1=max(df),
        fillcolor='rgba(169,169,169,0.3)',
        layer='below',
        line=dict(width=0),
        name='Recessão'
    )

    #######
    # Adicione cada série temporal como uma curva no gráfico
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            # Interpole os valores para tornar o gráfico contínuo
            # y = df[col].interpolate(method='linear')
            y = interpolate_column(df[col])
            fig.add_trace(go.Scatter(
                x=x,  # Use o índice de data e hora como eixo x
                y=y,
                mode='lines',
                name=col,
                stackgroup='one' if shadow else None
            ))
    else: # Series
        # Interpole os valores para tornar o gráfico contínuo
        # y = df.interpolate(method='linear')
        y = df.apply(interpolate_column)
        # y = df.apply(interpolate_column, axis=0)
        fig.add_trace(go.Scatter(
            x=x,  # Use o índice de data e hora como eixo x
            y=y,
            mode='lines',
            name='data',
            stackgroup='one' if shadow else None
        ))

    # Personalize o layout do gráfico
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.9, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
        xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.5, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )
    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

    # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )
    
    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))

    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                go.layout.Annotation(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10, color='black')  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico interativo
    return fig


def plot_series_multiple_y_axis(df, title, xaxis_title, yaxis_title, y2axis_title, source=None, height=600, width=1500, title_font_size=22, legend_font_size=16, xaxis_rangeslider_visible=False):
    """
    > df.index = date
    > columns = lines 
    """
    df = df.dropna(how='all')
    if isinstance(df, pd.DataFrame):
        df = df.dropna(axis=1, how='all')
    # Crie um gráfico interativo com Plotly
    fig = go.Figure()

    df.index = pd.to_datetime(df.index)
    x = df.index
    
    # Adicione cada série temporal como uma curva no gráfico
    if isinstance(df, pd.DataFrame):
        for i, col in enumerate(df.columns):
            # Interpole os valores para tornar o gráfico contínuo
            y = interpolate_column(df[col])
            # y = df[col].interpolate(method='linear')
            if i < 2:
                yaxis = 'y'
            else:
                yaxis = 'y2'
            fig.add_trace(go.Scatter(
                x=x,  # Use o índice de data e hora como eixo x
                y=y,
                mode='lines',
                name=col,
                yaxis=yaxis
            ))
    
    # Personalize o layout do gráfico com dois eixos y
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.9, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis=dict(title=yaxis_title, titlefont=dict(size=legend_font_size)),
        yaxis2=dict(title=y2axis_title, overlaying='y', side='right'),
        xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.5, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )
    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

    # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )
    
    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))
    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                go.layout.Annotation(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10)  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico interativo
    return fig


def plot_boxplot(df, title, xaxis_title, yaxis_title, source=None, height=600, width=1500, title_font_size=22, legend_font_size=16, start_date=None, end_date=None):
    # Filtrar o DataFrame com base nas datas, se fornecidas
    if start_date is not None and end_date is not None:
        df = df[(df.index >= pd.to_datetime(start_date)) & (df.index <= pd.to_datetime(end_date))]

    # Inicialize uma figura
    fig = go.Figure()
    
    # Dicionário para armazenar os valores mais recentes
    most_recent_values = {}
    
    # Adicione um boxplot para cada coluna
    for col in df.columns:
        series = df[col].dropna()
        fig.add_trace(go.Box(y=series, name=col, boxpoints=False))
        
        # Calcule o valor mais recente para cada coluna
        most_recent_values[col] = series.iloc[-1]
    # Adicione marcadores para os valores mais recentes 
    for col, value in most_recent_values.items():
        fig.add_trace(go.Scatter(x=[col], y=[value], mode='markers', name=None, showlegend=False, marker=dict(symbol='x', color='red', size=10)))

    # Personalize o gráfico
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.9, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.4, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )
    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

    # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )
    
    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))
    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                go.layout.Annotation(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10)  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico
    return fig


# def plot_scatter(df, 
#     x_column, 
#     y_column, 
#     categories, 
#     title="", 
#     xaxis_title="", 
#     yaxis_title="",
#     source=None, 
#     marker_size=20,
#     height=600, 
#     width=1500, 
#     title_font_size=22, 
#     legend_font_size=16, 
#     trendline='ols', 
#     xaxis_rangeslider_visible=False
# ):
#     """
#     Scatter plot.
#     """
#     df[categories] = df[categories].str.replace(" Index", "")
#     # Crie um gráfico interativo com Plotly Express
#     if not trendline:
#         fig = px.scatter(df, x=x_column, y=y_column, color=categories)
#     else:
#         fig = px.scatter(df, x=x_column, y=y_column, color=categories, trendline=trendline)

#     fig.update_traces(marker_size=20, marker_line_width=1)
    
#     # Adicione uma linha de 45 graus (x=y)
#     fig.update_layout(
#         shapes=[
#             dict(
#                 type='line',
#                 x0=df[x_column].min(),
#                 y0=df[x_column].min(),
#                 x1=df[x_column].max(),
#                 y1=df[x_column].max(),
#                 line=dict(color='black', width=2, dash='dash'),
#             )
#         ]
#     )
    
#     # Personalize o layout do gráfico
#     fig.update_layout(
#         title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.95, xanchor='center', yanchor='top'),
#         xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
#         yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
#         xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
#         height=height,  # Defina a altura desejada em pixels
#         width=width,   # Defina a largura desejada em pixels
#         legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.3, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
#     )

#     # Adicione descrição de legenda ao lado de cada ponto
#     for i, row in df.iterrows():
#         fig.add_annotation(
#             x=row[x_column],  # coordenada x do ponto
#             y=row[y_column],  # coordenada y do ponto
#             text=str(row[categories]),  # descrição da legenda
#             showarrow=False,
#             font=dict(size=16, color='black'),  # tamanho da fonte do texto
#             yanchor='bottom',
#             yshift=12
#         )
    
#     # Alinhe o texto horizontalmente na legenda
#     fig.update_layout(legend=dict(orientation="h"))

#      # Adicione mais informações aos eixos
#     fig.update_xaxes(
#         showgrid=True,
#         gridwidth=1,
#         gridcolor='LightGray',
#         title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
#     )
#     fig.update_yaxes(
#         showgrid=True,
#         gridwidth=1,
#         gridcolor='LightGray',
#         title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
#     )
    
#     # Escurece a fonte dos eixos
#     fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
#     fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))
#     # Adicione o texto na região externa ao gráfico (legenda)
#     if source:
#         fig.update_layout(
#             annotations=[
#                 dict(
#                     text=source,
#                     xref="paper", yref="paper",
#                     x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
#                     showarrow=False,
#                     font=dict(size=10)  # Tamanho da fonte do texto
#                 )
#             ]
#         )

#     # Exiba o gráfico interativo
#     return fig

def plot_scatter(df, 
    x_column, 
    y_column, 
    categories, 
    title="", 
    xaxis_title="", 
    yaxis_title="",
    source=None, 
    marker_size=20,
    height=600, 
    width=1500, 
    title_font_size=22, 
    legend_font_size=16, 
    trendline='ols', 
    xaxis_rangeslider_visible=False
):
    """
    Scatter plot.
    """
    df[categories] = df[categories].str.replace(" Index", "")
    
    # Crie um gráfico interativo com Plotly Express
    if not trendline:
        fig = px.scatter(df, x=x_column, y=y_column, color=categories)
    else:
        # Adicione a linha de fit
        X = sm.add_constant(df[x_column])
        model = sm.OLS(df[y_column], X).fit()
        df['fit'] = model.predict(X)
        
        fig = px.scatter(df, x=x_column, y=y_column, color=categories, trendline=trendline)
        fig.add_trace(px.line(df, x=x_column, y='fit').data[0])

    fig.update_traces(marker_size=20, marker_line_width=1)
    
    # Adicione uma linha de 45 graus (x=y)
    fig.update_layout(
        shapes=[
            dict(
                type='line',
                x0=df[x_column].min(),
                y0=df[x_column].min(),
                x1=df[x_column].max(),
                y1=df[x_column].max(),
                line=dict(color='black', width=2, dash='dash'),
            )
        ]
    )
    
    # Personalize o layout do gráfico
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.95, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
        xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.3, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )

    # Adicione descrição de legenda ao lado de cada ponto
    for i, row in df.iterrows():
        fig.add_annotation(
            x=row[x_column],  # coordenada x do ponto
            y=row[y_column],  # coordenada y do ponto
            text=str(row[categories]),  # descrição da legenda
            showarrow=False,
            font=dict(size=16, color='black'),  # tamanho da fonte do texto
            yanchor='bottom',
            yshift=12
        )
    
    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

     # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )
    
    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))
    
    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                dict(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10)  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico interativo
    return fig

def plot_beta_scatter_plot(df, 
    market_column, 
    stock_column,
    title="", 
    xaxis_title="", 
    yaxis_title="",
    source=None,
    height=600, 
    width=1500, 
    title_font_size=22, 
    legend_font_size=16,
    xaxis_rangeslider_visible=False
):
    """
    Scatter plot.
    """
    fig = px.scatter(df, x=market_column, y=stock_column, title=title,
                 labels={market_column: f'{market_column} returns', stock_column: f'{stock_column} returns'})

    # Adicione a linha de regressão linear
    model = LinearRegression()
    X = df[market_column].values.reshape(-1, 1)
    y = df[stock_column].values.reshape(-1, 1)
    model.fit(X, y)
    beta = model.coef_[0][0]

    # Calcule o R²
    y_pred = model.predict(X)
    r2 = r2_score(y, y_pred)

    # Crie pontos na linha de regressão para plotar
    line_x = np.linspace(df[market_column].min(), df[market_column].max(), 100).reshape(-1, 1)
    line_y = model.predict(line_x)

    fig.add_trace(
        go.Scatter(
            x=line_x.flatten(),
            y=line_y.flatten(),
            mode='lines',
            name=f'Linear Regression'
        )
    )

    # Adicione anotações para exibir beta e R² perto da figura
    fig.add_annotation(
        x=0.15,
        y=0.9,
        xref='paper',
        yref='paper',
        text=f'Beta: {beta:.2f}',
        showarrow=False,
        font=dict(size=22)
    )

    fig.add_annotation(
        x=0.15,
        y=0.95,
        xref='paper',
        yref='paper',
        text=f'R²: {r2:.2f}',
        showarrow=False,
        font=dict(size=22)
    )
    # Personalize o layout do gráfico
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.95, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
        xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.3, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )

    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

     # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )
    
    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))
    
    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                dict(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10)  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico interativo
    return fig



def plot_bars(df, title, xaxis_title, yaxis_title, source=None, height=600, width=1500, title_font_size=22, legend_font_size=16, xaxis_rangeslider_visible=False):
    """
    > df.index = date
    > columns = bars
    """
    df = df.dropna(how='all')
    if isinstance(df, pd.DataFrame):
        df = df.dropna(axis=1, how='all')
        df.index = pd.to_datetime(df.index)
    
    # Crie um gráfico interativo com Plotly
    fig = go.Figure()
    df = df.sort_values(ascending=False)
    x = df.index

    # Adicione cada série temporal como uma barra no gráfico
    if isinstance(df, pd.DataFrame):
        for col in df.columns:
            # Interpole os valores para tornar o gráfico contínuo
            # y = interpolate_column(df[col])
            fig.add_trace(go.Bar(
                x=x,  # Use o índice de data e hora como eixo x
                y=df,
                name=col,
            ))
    else:  # Series
        # Interpole os valores para tornar o gráfico contínuo
        # y = df.apply(interpolate_column)
        fig.add_trace(go.Bar(
            x=x,  # Use o índice de data e hora como eixo x
            y=df,
            name='data'
        ))

    # Personalize o layout do gráfico
    fig.update_layout(
        title=dict(text=title, font=dict(size=title_font_size), x=0.5, y=0.9, xanchor='center', yanchor='top'),
        xaxis_title=dict(text=xaxis_title, font=dict(size=legend_font_size)),
        yaxis_title=dict(text=yaxis_title, font=dict(size=legend_font_size)),
        xaxis_rangeslider_visible=xaxis_rangeslider_visible,  # Adiciona controle de zoom
        height=height,  # Defina a altura desejada em pixels
        width=width,   # Defina a largura desejada em pixels
        legend=dict(font=dict(size=legend_font_size), x=0.5, y=-0.5, xanchor='center', yanchor='middle'),  # Ajuste x e y para mover a legenda
    )
    # Alinhe o texto horizontalmente na legenda
    fig.update_layout(legend=dict(orientation="h"))

    # Adicione mais informações aos eixos
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo x
    )
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor='LightGray',
        title=dict(font=dict(size=legend_font_size+4, color='black'))  # Adiciona título ao eixo y
    )

    # Escurece a fonte dos eixos
    fig.update_xaxes(tickfont=dict(color='black', size=legend_font_size))
    fig.update_yaxes(tickfont=dict(color='black', size=legend_font_size))

    # Adicione o texto na região externa ao gráfico (legenda)
    if source:
        fig.update_layout(
            annotations=[
                go.layout.Annotation(
                    text=source,
                    xref="paper", yref="paper",
                    x=1.112, y=0.0,  # Ajuste essas coordenadas conforme necessário
                    showarrow=False,
                    font=dict(size=10, color='black')  # Tamanho da fonte do texto
                )
            ]
        )

    # Exiba o gráfico interativo
    return fig

