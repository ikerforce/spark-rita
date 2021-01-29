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

procesos = """demoras_aerolinea
flota
demoras_aeropuerto_destino
demoras_aeropuerto_origen
demoras_ruta_mktid
demoras_ruta_aeropuerto""".split("\n")

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--config", help="Ruta hacia archivo de configuracion")
args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
with open(args.config) as json_file:
        config = json.load(json_file)
user = config["user"] # Usuario de mysql
password = config["password"] # Password de mysql
database = config["database"] # Base de datos en la que almaceno resultados y tiempo de ejecucion
db_url = config["db_url"] # URL de la base de datos

os.environ['TZ'] = 'America/Mexico_City' # Cambiamos a la zona horaria actual
time.tzset() # Ponemos la zona horaria correcta
# -----------------------------------------------------------------------------------


# Conexion a base de datos
# ---------------------------------------------------------------------------------
db_connection_str = 'mysql+pymysql://' + user + ':' + password + '@localhost:3306/' + database # String de conexion a MySQL
db_connection = create_engine(db_connection_str) # Conectamos con la base de datos de MySQL
# ---------------------------------------------------------------------------------


# Dashboard
# -----------------------------------------------------------------------------------
app = Dash(__name__, routes_pathname_prefix='/monitoreo/') # Ruta en la que se expondra el dashboard
server = app.server

# Definimos el layout del dashboard
# Se compone de 4 componentes independientes
app.layout = html.Div([
    html.Div([
        dcc.Graph('resumen-duracion', config={'displayModeBar': False}),
        html.Div(className='menus-desplegables',
            children=[
                html.Div(className='proceso',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-proceso',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                html.Div(className='numero-registros',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-numero-registros',
                            options=[{'label': i, 'value': i} for i in range(20, 201, 20)],
                            value='20',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                ],
            style=dict(display='flex')
        ),
        dcc.Graph('proceso-en-tiempo', config={'displayModeBar': False}),
        html.Div(className='menus-desplegables2',
            children=[
                html.Div(className='proceso-movil',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-proceso-movil',
                            options=[{'label':'Todos', 'value':'%%'}] + [{'label':p, 'value':p} for p in procesos],
                            value='%%',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                html.Div(className='ventana',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-ventana',
                            options=[{'label':1, 'value':0}] + [{'label':i, 'value':i-1} for i in range(5, 51, 5)],
                            value='5',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                html.Div(className='numero-registros-movil',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-numero-registros-movil',
                            options=[{'label': i, 'value': i} for i in range(20, 201, 20)],
                            value='20',
                            clearable=False)
                    ],
                    style=dict(width='25%')),
                ],
            style=dict(display='flex')
        ),
        dcc.Graph('promedio-movil', config={'displayModeBar': False}),
        dcc.Graph('perfilamiento-perc2', config={'displayModeBar': False}),
        dcc.Interval(id='interval-component', interval=1*1000)])])

# Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
@app.callback(
    Output('resumen-duracion', 'figure'),
    [Input('interval-component', 'interval')])

# Este es el metodo que actualiza la informacion de MySQL y genera los dashboards de visitas y usuarios conectados por sexo
def update_graph(proceso):

    query_spark = """SELECT REPLACE(process, '_spark', '') as process
                        , AVG(duration) as avg_duration
                        , STDDEV(duration) as stddev_duration
                    FROM registro_de_tiempo_spark
                    GROUP BY process"""

    query_dask = """SELECT REPLACE(process, '_dask', '') as process
                        , AVG(duration) as avg_duration
                        , STDDEV(duration) as stddev_duration
                    FROM registro_de_tiempo_dask
                    GROUP BY process"""

    tiempo_spark = pd.read_sql(query_spark, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web
    tiempo_dask = pd.read_sql(query_dask, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web

    # Iniciamos dos graficos de barras. Uno con la informacion de usuarios conectados por alcaldia y otro con las paginas web que registraron mas visitas
    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])

    fig.add_trace(
        go.Bar(
            x=tiempo_spark.process
            , y=tiempo_spark.avg_duration
            , error_y=dict(type='data', array=tiempo_spark.stddev_duration)
            , marker=dict(color=Greys[4])
            , name='Spark'
            )
        , row=1
        , col=1)
    fig.add_trace(
        go.Bar(
            x=tiempo_dask.process
            , y=tiempo_dask.avg_duration
            , error_y=dict(type='data', array=tiempo_dask.stddev_duration)
            , marker=dict(color=Oranges[6])
            , name='Dask'
            )
        , row=1
        , col=1)


    fig.update_xaxes(title_text='Proceso', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    fig.update_layout(title_text="Resumen", title_font={'size':25}, title_x=0.5, height=500, width=1400, template='plotly_white', legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="left", x=0.45))

    # Regresamos el bloque de graficos 
    return fig

# # -------------------------------------------------------------------------------------------

@app.callback(
    Output('proceso-en-tiempo', 'figure'),
    [Input('dropdown-proceso', 'value')
    , Input('dropdown-numero-registros', 'value')])

def update_graph(process, n_registros):

    process = "'" + process + "'"

    query_spark = """SELECT CAST(insertion_ts AS CHAR) as insertion_ts
                        , REPLACE(process, '_spark', '') as process
                        , duration
                    FROM registro_de_tiempo_spark
                    WHERE process LIKE CONCAT({process}, '_spark')
                    ORDER BY insertion_ts DESC
                    LIMIT {n_registros}""".format(process=process, n_registros=n_registros)

    query_dask = """SELECT CAST(insertion_ts AS CHAR) as insertion_ts
                        , REPLACE(process, '_dask', '') as process
                        , duration
                    FROM registro_de_tiempo_dask
                    WHERE process LIKE CONCAT({process}, '_dask')
                    ORDER BY insertion_ts DESC
                    LIMIT {n_registros}""".format(process=process, n_registros=n_registros)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web
    tiempo_dask = pd.read_sql(query_dask, con=db_connection) # Consultamos la tabla que tiene la informacion de de visitas a paginas web

    # Iniciamos dos graficos de barras. Uno con la informacion de usuarios conectados por alcaldia y otro con las paginas web que registraron mas visitas
    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])

    fig.add_trace(
        go.Scatter(
            x=tiempo_spark.insertion_ts
            , y=tiempo_spark.duration
            , marker=dict(color=Greys[4])
            , mode='lines+markers'
            , name='Spark'
            )
        , row=1
        , col=1)
    fig.add_trace(
        go.Scatter(
            x=tiempo_dask.insertion_ts
            , y=tiempo_dask.duration
            , marker=dict(color=Oranges[6])
            , mode='lines+markers'
            , name='Dask'
            )
        , row=1
        , col=1)


    fig.update_xaxes(title_text='Fecha de ejecución', title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text='Duración (segundos)', title_font={'size':12}, showgrid=False, row=1, col=1)
    # Actualizamos el tamano y titulo del grafico y ubicacion de los titulos y posicion del bloque
    fig.update_layout(title_text="Duración del proceso", title_font={'size':25}, title_x=0.5, height=500, width=1400, template='plotly_white', legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="left", x=0.45))

    # Regresamos el bloque de graficos 
    return fig

# # -------------------------------------------------------------------------------------------

@app.callback(
    Output('promedio-movil', 'figure'),
    [Input('dropdown-proceso-movil', 'value')
    , Input('dropdown-ventana', 'value')
    , Input('dropdown-numero-registros-movil', 'value')])

def update_graph(process, ventana, n_registros):

    process = "'" + process + "'"


    query_spark =  """SELECT insertion_ts
                        , duration
                        , stddev_duration
                        , duration - stddev_duration as duration_low
                        , duration + stddev_duration as duration_high
                    FROM
                    (
                        SELECT CAST(insertion_ts AS CHAR) as insertion_ts
                            , REPLACE(process, '_spark', '') as process
                            , AVG(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as duration
                            , STDDEV(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as stddev_duration
                        FROM registro_de_tiempo_spark
                        WHERE process LIKE CONCAT({process}, '_spark')
                        ORDER BY insertion_ts DESC
                    ) temp
                    LIMIT {n_registros}""".format(process=process, ventana=ventana, n_registros=n_registros)

    query_dask =  """SELECT insertion_ts
                        , duration
                        , stddev_duration
                        , duration - stddev_duration as duration_low
                        , duration + stddev_duration as duration_high
                    FROM
                    (
                        SELECT CAST(insertion_ts AS CHAR) as insertion_ts
                            , REPLACE(process, '_spark', '') as process
                            , AVG(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as duration
                            , STDDEV(duration) OVER(ORDER BY insertion_ts DESC ROWS BETWEEN {ventana} PRECEDING AND CURRENT ROW) as stddev_duration
                        FROM registro_de_tiempo_dask
                        WHERE process LIKE CONCAT({process}, '_dask')
                        ORDER BY insertion_ts DESC
                    ) temp
                    LIMIT {n_registros}""".format(process=process, ventana=ventana, n_registros=n_registros)

    tiempo_spark = pd.read_sql(query_spark, con=db_connection)
    tiempo_dask = pd.read_sql(query_dask, con=db_connection)

    fig = make_subplots(rows=1, cols=1,
                        specs=[[{"type":"bar"}]])
    fig.add_trace(
        go.Scatter(
            name="Spark Bounds"
            , x=list(tiempo_spark.insertion_ts) + list(tiempo_spark.insertion_ts[::-1])
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
            , x=tiempo_spark.insertion_ts
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
            , x=list(tiempo_dask.insertion_ts) + list(tiempo_dask.insertion_ts[::-1])
            , y=list(tiempo_dask.duration_high) + list(tiempo_dask.duration_low[::-1])
            , mode='lines'
            , fill='toself'
            , fillcolor=Oranges[1]
            , opacity=0.7
            , line=dict(width=0)
            , showlegend=False
            )
        , row=1
        , col=1)
    fig.add_trace(
        go.Scatter(
            name="Dask Duration"
            , x=tiempo_dask.insertion_ts
            , y=tiempo_dask.duration
            # , error_y=dict(type='data', array=tiempo_dask.stddev_duration)
            , marker=dict(color=Oranges[6])
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
    fig.update_layout(title_text="Promedio móvil de la duración del proceso (en ventana de {ventana} {registros})".format(ventana=int(ventana)+1, registros=registros), title_font={'size':25}, title_x=0.5, height=500, width=1400, template='plotly_white', legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="left", x=0.45))

    # Regresamos el bloque de graficos 
    return fig

if __name__ == '__main__':
        app.run_server(debug=True, port=9092)
