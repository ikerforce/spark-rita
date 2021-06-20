#!/usr/bin/env python
# coding: utf-8


# Preparacion de ambiente
# -----------------------------------------------------------------------------------
# Importacion de librerias
import pandas as pd # Libreria para manejo de datos en forma de DataFrame
import numpy as np # Libreria para operaciones matemaicas
from sqlalchemy import create_engine # Libreria para conectar con MySQL
import plotly.express as px # Libreria para crear graficos
from dash import Dash # Libreria para desplegar el dashboard
import dash_core_components as dcc # Libreria para agregar componentes al dashboard
import dash_html_components as html # Libreria para usar elementos de html en python 
from dash.dependencies import Input, Output # Libreria para 
from plotly.subplots import make_subplots # Libreria para hacer subgraficas
import plotly.graph_objects as go # Libreria para agregar graficas al dashboard 
from plotly.colors import sequential # Libreria para agregar paletas de colores
import time # Libreria para medir y saber tiempo
import datetime # Libreria para conocer la fecha
import os # Libreria para manipular el sistema operativo
import argparse
import json
Oranges = sequential.Oranges
Greys = sequential.Greys

samples = ['10K', '100K', '1M', '10M', 'TOTAL']
ambientes = ['local', 'cluster']

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--creds", help="Ruta hacia archivo de configuracion.")
parser.add_argument("--env", help="Conjunto de tablas que se leerán.")
args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
with open(args.creds) as json_file:
        creds = json.load(json_file)
user = creds["user"] # Usuario de mysql
password = creds["password"] # Password de mysql
database = creds["database"] # Base de datos en la que almaceno resultados y tiempo de ejecucion
db_url = creds["db_url"] # URL de la base de datos
host = creds['host']

if args.env != 'test':
    tabla_tiempo_dask = 'registro_de_tiempo_dask'
    tabla_tiempo_spark = 'registro_de_tiempo_spark'
else:
    tabla_tiempo_dask = 'registro_de_tiempo_dask_test'
    tabla_tiempo_spark = 'registro_de_tiempo_spark_test'

os.environ['TZ'] = 'America/Mexico_City' # Cambiamos a la zona horaria actual
time.tzset() # Ponemos la zona horaria correcta
# -----------------------------------------------------------------------------------


# Conexion a base de datos
# ---------------------------------------------------------------------------------
db_connection_str = 'mysql+pymysql://' + user + ':' + password + '@' + host + ':3306/' + database # String de conexion a MySQL
db_connection = create_engine(db_connection_str) # Conectamos con la base de datos de MySQL
# ---------------------------------------------------------------------------------


# ---------------------------------------------------------------------------------
query_spark = """SELECT DISTINCT process
                FROM {tabla_dask}
                UNION
                SELECT DISTINCT process
                FROM {tabla_spark}""".format(tabla_dask=tabla_tiempo_dask, tabla_spark=tabla_tiempo_spark)

procesos = pd.read_sql(query_spark, con=db_connection)
procesos['process'] = procesos['process'].str.replace('_spark', '')
procesos['process'] = procesos['process'].str.replace('_dask', '')
procesos = procesos['process'].values
# ---------------------------------------------------------------------------------


# Dashboard
# -----------------------------------------------------------------------------------
app = Dash(__name__, routes_pathname_prefix='/monitoreo/') # Ruta en la que se expondra el dashboard
server = app.server

# Definimos el layout del dashboard
# Se compone de 4 componentes independientes
app.layout = html.Div([
    html.Div([
        html.Div(className='dropdown-barras',
            children=[
                html.Div(className='barras',
                    children=[
                        html.Label(['Nombre del proceso:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-proceso-barras',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                html.Div(className='barras-sample',
                    children=[
                        html.Label(['Tamaño de la muestra:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-barras-sample',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in samples],
                            value='100K',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                html.Div(className='barras-ambiente',
                    children=[
                        html.Label(['Ambiente de ejecución:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-barras-ambiente',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in ambientes],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                ],
            style=dict(display='flex')
        ),

        dcc.Graph('resumen-duracion', config={'displayModeBar': False}),

        html.Div(className='menus-desplegables2',
            children=[

                html.Div(className='proceso-movil',
                    children=[
                        html.Label(['Proceso:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-proceso-ventana',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='numero-registros-movil',
                    children=[
                        html.Label(['Número de ejecuciones:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-numero-registros-movil',
                            options=[{'label': i, 'value': i} for i in range(20, 201, 20)] + [{'label':'Todos', 'value':'4382378943324'}],
                            value='20',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='ventana-sample-size',
                    children=[
                        html.Label(['Tamaño de la muestra:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-ventana-sample-size',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in samples],
                            value='100K',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='ventana-ambiente',
                    children=[
                        html.Label(['Ambiente de ejecución:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-ventana-ambiente',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in ambientes],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='ventana',
                    children=[
                        html.Label(['Registros en ventana móvil:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-ventana',
                            options=[{'label':1, 'value':0}] + [{'label':i, 'value':i-1} for i in range(5, 51, 5)],
                            value='4',
                            clearable=False)
                    ],
                    style=dict(width='20%')),
                ],
            style=dict(display='flex')
        ),

        dcc.Graph('promedio-movil', config={'displayModeBar': False}),

        html.Div(className='menus-desplegables',
            children=[

                html.Div(className='proceso',
                    children=[
                        html.Label(['Nombre del proceso:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-proceso-escalamiento',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='demoras_aerolinea',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='lineas-ambiente',
                    children=[
                        html.Label(['Ambiente de ejecución:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-lineas-ambiente',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in ambientes],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                ],
            style=dict(display='flex')
        ),

        dcc.Graph('escalamiento-datos', config={'displayModeBar': False}),

        html.Div(className='menus-desplegables3',
            children=[
                html.Div(className='histograma-proceso',
                    children=[
                        html.Label(['Nombre del proceso:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-histograma-proceso',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='histograma-sample-size',
                    children=[
                        html.Label(['Tamaño de la muestra:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-histograma-sample-size',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':s, 'value':s} for s in samples],
                            value='100K',
                            clearable=False)
                    ],
                    style=dict(width='20%')),

                html.Div(className='numero-registros-movil',
                    children=[
                        html.Label(['Número de barras:'], style={'font-weight': 'bold', "text-align": "center"}),
                        dcc.Dropdown(
                            id='dropdown-histograma-numero-bins',
                            options=[{'label':'1', 'value': 1}] + [{'label': i, 'value': i} for i in range(10, 91, 10)] + [{'label': i, 'value': i} for i in range(100, 251, 50)],
                            value='20',
                            clearable=False)
                    ],
                    style=dict(width='20%')),
                ],
            style=dict(display='flex')
        ),

        dcc.Graph('histograma-duracion', config={'displayModeBar': False}),

        dcc.Interval(id='interval-component', interval=1*1000)])])

# Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
@app.callback(
    Output('resumen-duracion', 'figure'),
    [Input('dropdown-proceso-barras', 'value')
    , Input('dropdown-barras-sample', 'value')
    , Input('dropdown-barras-ambiente', 'value')])
def update_graph(proceso, sample_size, env):

    query_spark = """SELECT REPLACE(process, '_spark', '') as process
                        , REPLACE(REPLACE(REPLACE(process, '_spark', ''), '_command_time', ''), '_write_time', '') as general_process
                        , AVG(duration) as avg_duration
                        , STDDEV(duration) as stddev_duration
                    FROM {tabla}
                    WHERE process LIKE CONCAT('{process}', '_spark%%')
                    AND sample_size LIKE '{sample_size}'
                    AND env LIKE '{env}'
                    GROUP BY process
                    ORDER BY process""".format(process=proceso, tabla=tabla_tiempo_spark, sample_size=sample_size, env=env)

    query_dask = """SELECT REPLACE(process, '_dask', '') as process
                        , REPLACE(REPLACE(REPLACE(process, '_dask', ''), '_command_time', ''), '_write_time', '') as general_process
                        , AVG(duration) as avg_duration
                        , STDDEV(duration) as stddev_duration
                    FROM {tabla}
                    WHERE process LIKE CONCAT('{process}', '_dask%%')
                    AND sample_size LIKE '{sample_size}'
                    AND env LIKE '{env}'
                    GROUP BY process
                    ORDER BY process""".format(process=proceso, tabla=tabla_tiempo_dask, sample_size=sample_size, env=env)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection)
    tiempo_spark_write_time = tiempo_spark[tiempo_spark['process'].str.contains('write') == True]
    tiempo_spark_command_time = tiempo_spark[tiempo_spark['process'].str.contains('command') == True]
    tiempo_spark_total = tiempo_spark[(tiempo_spark['process'].str.contains('command') == False) & (tiempo_spark['process'].str.contains('write') == False)]

    tiempo_dask = pd.read_sql(query_dask, con=db_connection)
    tiempo_dask_write_time = tiempo_dask[tiempo_dask['process'].str.contains('write') == True]
    tiempo_dask_command_time = tiempo_dask[tiempo_dask['process'].str.contains('command') == True]
    tiempo_dask_total = tiempo_dask[(tiempo_dask['process'].str.contains('command') == False) & (tiempo_dask['process'].str.contains('write') == False)]

    # Grafica de barras que resume duracion de procesos
    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])

    fig.add_trace(
        go.Bar(
            x=tiempo_spark_total.general_process
            , y=tiempo_spark_total.avg_duration
            # , error_y=dict(type='data', array=tiempo_spark_total.stddev_duration)
            , marker=dict(color=Greys[4])
            , name='Spark - Ejecución'
            , hovertext=tiempo_spark_total.process
            , offset=-0.3
            , width=0.3
            , base=0
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_dask_total.general_process
            , y=tiempo_dask_total.avg_duration
            # , error_y=dict(type='data', array=tiempo_dask_total.stddev_duration)
            , marker=dict(color=Oranges[4])
            , name='Dask - Ejecución'
            , hovertext=tiempo_dask_total.process
            , offset=0.0
            , base=0
            , width=0.3
            )
        , row=1
        , col=1)

    fig.add_trace(
        go.Bar(
            x=tiempo_spark_write_time.general_process
            , y=tiempo_spark_write_time.avg_duration
            # , error_y=dict(type='data', array=tiempo_spark_write_time.stddev_duration)
            , marker=dict(color=Greys[3])
            , name='Spark - Write'
            , hovertext=tiempo_spark_write_time.process
            , offset=-0.3
            , width=0.3
            , base=0
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_dask_write_time.general_process
            , y=tiempo_dask_write_time.avg_duration
            # , error_y=dict(type='data', array=tiempo_dask_write_time.stddev_duration)
            , marker=dict(color=Oranges[3])
            , name='Dask - Write'
            , hovertext=tiempo_dask_write_time.process
            , offset=0.0
            , width=0.3
            , base=0
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_spark_command_time.general_process
            , y=tiempo_spark_command_time.avg_duration
            # , error_y=dict(type='data', array=tiempo_spark_command_time.stddev_duration)
            , marker=dict(color=Greys[1])
            , name='Spark - Command'
            , hovertext=tiempo_spark_command_time.process
            , offset=-0.3
            , width=0.3
            , base=tiempo_spark_write_time.avg_duration
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_dask_command_time.general_process
            , y=tiempo_dask_command_time.avg_duration
            # , error_y=dict(type='data', array=tiempo_dask_command_time.stddev_duration)
            , marker=dict(color=Oranges[1])
            , name='Dask - Command'
            , hovertext=tiempo_dask_command_time.process
            , offset=0.0
            , width=0.3
            , base=tiempo_dask_write_time.avg_duration
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_spark_total.general_process
            , y=tiempo_spark_total.avg_duration
            , error_y=dict(type='data', array=tiempo_spark_total.stddev_duration)
            , marker=dict(color=Greys[6])
            , name='Spark - Total'
            , hovertext=tiempo_spark_total.process
            , offset=-0.3
            , width=0.2
            , base=0
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Bar(
            x=tiempo_dask_total.general_process
            , y=tiempo_dask_total.avg_duration
            , error_y=dict(type='data', array=tiempo_dask_total.stddev_duration)
            , marker=dict(color=Oranges[6])
            , name='Dask - Total'
            , hovertext=tiempo_dask_total.process
            , offset=0.0
            , base=0
            , width=0.2
            )
        , row=1
        , col=1)

    fig.update_xaxes(title_text='Proceso', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    fig.update_layout(title_text="Resumen"
        , title_font={'size':25}
        , title_x=0.5
        , height=500
        , autosize=True
        , barmode='stack'
        # , width=1400
        , template='plotly_white'
        , legend=dict(orientation="h"
            , yanchor="bottom"
            , y=-0.25
            , xanchor="left"
            , x=0.2)
        )

    # Regresamos el bloque de graficos 
    return fig

# # -------------------------------------------------------------------------------------------

@app.callback(
    Output('escalamiento-datos', 'figure'),
    [Input('dropdown-proceso-escalamiento', 'value')
    , Input('dropdown-lineas-ambiente', 'value')])
def update_graph(process, env):

    query_spark = """SELECT sample_size
                        , avg_duration
                        , stddev_duration
                        , avg_duration - stddev_duration as duration_low
                        , avg_duration + stddev_duration as duration_high
                    FROM
                    (
                    SELECT process
                        , sample_size
                        , avg(duration) AS avg_duration
                        , stddev(duration) AS stddev_duration
                        , count(*) AS count
                    FROM {tabla}
                    WHERE process LIKE CONCAT('{process}', '_spark')
                    GROUP BY sample_size, process
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(sample_size, 'M', '000000'), 'K', '000'), 'TOTAL', '80000000') AS UNSIGNED)
                    ) tabla_temp""".format(process=process, tabla=tabla_tiempo_spark)

    query_dask = """SELECT sample_size
                        , avg_duration
                        , stddev_duration
                        , avg_duration - stddev_duration as duration_low
                        , avg_duration + stddev_duration as duration_high
                    FROM
                    (
                    SELECT process
                        , sample_size
                        , avg(duration) AS avg_duration
                        , stddev(duration) AS stddev_duration
                        , count(*) AS count
                    FROM {tabla}
                    WHERE process LIKE CONCAT('{process}', '_dask')
                    GROUP BY sample_size, process
                    ORDER BY CAST(REPLACE(REPLACE(REPLACE(sample_size, 'M', '000000'), 'K', '000'), 'TOTAL', '80000000') AS UNSIGNED)
                    ) tabla_temp""".format(process=process, tabla=tabla_tiempo_dask)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web
    tiempo_dask = pd.read_sql(query_dask, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web

    # Iniciamos dos graficos de barras. Uno con la informacion de usuarios conectados por alcaldia y otro con las paginas web que registraron mas visitas
    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])

    fig.add_trace(
        go.Scatter(
            name="Spark Bounds"
            , x=list(tiempo_spark.sample_size) + list(tiempo_spark.sample_size[::-1])
            , y=list(tiempo_spark.duration_high) + list(tiempo_spark.duration_low[::-1])
            , mode='lines'
            , fill='toself'
            , opacity=0.7
            , fillcolor=Greys[2]
            , line=dict(width=0)
            , showlegend=False
            )
        , row=1
        , col=1)

    fig.add_trace(
        go.Scatter(
            name="Dask Bounds"
            , x=list(tiempo_dask.sample_size) + list(tiempo_dask.sample_size[::-1])
            , y=list(tiempo_dask.duration_high) + list(tiempo_dask.duration_low[::-1])
            , mode='lines'
            , fill='toself'
            , fillcolor=Oranges[2]
            , opacity=0.7
            , line=dict(width=0)
            , showlegend=False
            )
        , row=1
        , col=1)

    fig.add_trace(
        go.Scatter(
            x=tiempo_spark.sample_size
            , y=tiempo_spark.avg_duration
            , marker=dict(color=Greys[4])
            , mode='lines+markers'
            , name='Spark'
            )
        , row=1
        , col=1)


    fig.add_trace(
        go.Scatter(
            x=tiempo_dask.sample_size
            , y=tiempo_dask.avg_duration
            , marker=dict(color=Oranges[4])
            , mode='lines+markers'
            , name='Dask'
            )
        , row=1
        , col=1)


    fig.update_xaxes(title_text='Fecha de ejecución', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    fig.update_layout(title_text="Duración del proceso"
        , title_font={'size':25}
        , title_x=0.5
        , height=500
        , autosize=True
        # , width=1400
        , template='plotly_white'
        , legend=dict(orientation="h"
            , yanchor="bottom"
            , y=-0.3
            , xanchor="left"
            , x=0.45))

    # Regresamos el bloque de graficos 
    return fig

# # -------------------------------------------------------------------------------------------

@app.callback(
    Output('promedio-movil', 'figure'),
    [Input('dropdown-proceso-ventana', 'value')
    , Input('dropdown-numero-registros-movil', 'value')
    , Input('dropdown-ventana-sample-size', 'value')
    , Input('dropdown-ventana-ambiente', 'value')
    , Input('dropdown-ventana', 'value')])
def update_graph(process, n_registros, sample_size, env, ventana):

    process = "'" + process + "'"


    query_spark =  """SELECT num
                        , sample_size
                        , env
                        , duration
                        , stddev_duration
                        , duration - stddev_duration as duration_low
                        , duration + stddev_duration as duration_high
                    FROM
                    (
                        SELECT (@row_number:=@row_number + 1) as num
                            , process
                            , sample_size
                            , env
                            , AVG(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as duration
                            , STDDEV(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as stddev_duration
                        FROM {tabla}, (SELECT @row_number:=0) AS t
                        WHERE process LIKE CONCAT({process}, '_spark')
                        AND sample_size LIKE '{sample_size}'
                        AND env LIKE '{env}'
                        ORDER BY insertion_ts DESC
                    ) temp
                    LIMIT {n_registros}""".format(process=process, ventana=ventana, n_registros=n_registros, tabla=tabla_tiempo_spark, env=env, sample_size=sample_size)

    query_dask =  """SELECT num
                        , sample_size
                        , env
                        , duration
                        , stddev_duration
                        , duration - stddev_duration as duration_low
                        , duration + stddev_duration as duration_high
                    FROM
                    (
                        SELECT (@row_number:=@row_number + 1) as num
                            , process
                            , sample_size
                            , env
                            , AVG(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as duration
                            , STDDEV(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as stddev_duration
                        FROM {tabla}, (SELECT @row_number:=0) AS t
                        WHERE process LIKE CONCAT({process}, '_dask')
                        AND sample_size LIKE '{sample_size}'
                        AND env LIKE '{env}'
                        ORDER BY insertion_ts DESC
                    ) temp
                    LIMIT {n_registros}""".format(process=process, ventana=ventana, n_registros=n_registros, tabla=tabla_tiempo_dask, env=env, sample_size=sample_size)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection)
    tiempo_dask = pd.read_sql(query_dask, con=db_connection)

    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])
    fig.add_trace(
        go.Scatter(
            name="Spark Bounds"
            , x=list(tiempo_spark.num) + list(tiempo_spark.num[::-1])
            , y=list(tiempo_spark.duration_high) + list(tiempo_spark.duration_low[::-1])
            , mode='lines'
            , fill='toself'
            , opacity=0.7
            , fillcolor=Greys[2]
            , line=dict(width=0)
            , showlegend=False
            )

        , row=1
        , col=1)
    fig.add_trace(
        go.Scatter(
            name="Spark Duration"
            , x=tiempo_spark.num
            , y=tiempo_spark.duration
            # , error_y=dict(type='data', array=tiempo_spark.stddev_duration)
            , marker=dict(color=Greys[4])
            , mode='lines+markers'
            )
        , row=1
        , col=1)

    fig.add_trace(
        go.Scatter(
            name="Dask Bounds"
            , x=list(tiempo_dask.num) + list(tiempo_dask.num[::-1])
            , y=list(tiempo_dask.duration_high) + list(tiempo_dask.duration_low[::-1])
            , mode='lines'
            , fill='toself'
            , fillcolor=Oranges[2]
            , opacity=0.7
            , line=dict(width=0)
            , showlegend=False
            )
        , row=1
        , col=1)

    fig.add_trace(
        go.Scatter(
            name="Dask Duration"
            , x=tiempo_dask.num
            , y=tiempo_dask.duration
            # , error_y=dict(type='data', array=tiempo_dask.stddev_duration)
            , marker=dict(color=Oranges[4])
            , mode='lines+markers'
            )
        , row=1
        , col=1)

    fig.update_xaxes(title_text='Fecha de ejecución', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    if int(ventana) >= 2:
        registros = "registros"
    else:
        registros = "registro"

    fig.update_layout(title_text="Promedio móvil de la duración del proceso (en ventana de {ventana} {registros})".format(ventana=int(ventana)+1, registros=registros)
        , title_font={'size':25}
        , title_x=0.5
        , autosize=True
        , height=500
        # , width=1400
        , template='plotly_white'
        , legend=dict(orientation="h"
            , yanchor="bottom"
            , y=-0.3
            , xanchor="left"
            , x=0.45)
        )

    # Regresamos el bloque de graficos 
    return fig

@app.callback(
    Output('histograma-duracion', 'figure'),
    [Input('dropdown-histograma-proceso', 'value')
    , Input('dropdown-histograma-numero-bins', 'value')
    , Input('dropdown-histograma-sample-size', 'value')])
def update_graph(process, numero_bins, sample_size):

    query_spark = """SELECT 1 as num
                        , REPLACE(process, '_spark', '') as process
                        , duration
                    FROM {tabla}, (SELECT @row_number:=0) AS t
                    WHERE process LIKE CONCAT('{process}', '_spark')
                    AND sample_size LIKE '{sample_size}'
                    ORDER BY insertion_ts DESC""".format(process=process, tabla=tabla_tiempo_spark, sample_size=sample_size)

    query_dask = """SELECT 1 as num
                        , REPLACE(process, '_dask', '') as process
                        , duration
                    FROM {tabla}, (SELECT @row_number:=0) AS t
                    WHERE process LIKE CONCAT('{process}', '_dask')
                    AND sample_size LIKE '{sample_size}'
                    ORDER BY insertion_ts DESC""".format(process=process, tabla=tabla_tiempo_dask, sample_size=sample_size)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web
    tiempo_dask = pd.read_sql(query_dask, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web

    # Iniciamos dos graficos de barras. Uno con la informacion de usuarios conectados por alcaldia y otro con las paginas web que registraron mas visitas
    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])

    numero_bins=int(numero_bins)

    fig.add_trace(
        go.Histogram(
            x=tiempo_spark.duration
            , y=tiempo_spark.num
            , nbinsx=numero_bins
            , marker_color=Greys[4]
            # , mode='lines+markers'
            , name='Spark'
            )
        , row=1
        , col=1)
    fig.add_trace(
        go.Histogram(
            x=tiempo_dask.duration
            , y=tiempo_dask.num
            , nbinsx=numero_bins
            , marker_color=Oranges[4]
            # , mode='lines+markers'
            , name='Dask'
            )
        , row=1
        , col=1)


    fig.update_yaxes(title_text='Número de ejecuciones', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_xaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    fig.update_layout(title_text="Histograma de la duración del proceso."
        , title_font={'size':25}
        , title_x=0.5
        , height=500
        , autosize=True
        # , width=1400
        , template='plotly_white'
        , legend=dict(orientation="h"
            , yanchor="bottom"
            , y=-0.3
            , xanchor="left"
            , x=0.45))

    # Regresamos el bloque de graficos 
    return fig

if __name__ == '__main__':
        app.run_server(debug=True, port=9092)
