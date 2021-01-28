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
Greens = sequential.Greens
Blues = sequential.Blues

meses = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

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
app = Dash(__name__, routes_pathname_prefix='/spark-rita/') # Ruta en la que se expondra el dashboard
server = app.server

# Definimos el layout del dashboard
# Se compone de 4 componentes independientes
app.layout = html.Div([
    html.Div([
        html.Div(className='two-cols',
            children=[
                html.Div(className='ciudad',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-ciudad',
                            options=[
                                {'label': 'TUL', 'value': 'TUL'},
                                {'label': 'Dallas', 'value': 'DFW'},
                                {'label': 'Dallas 2', 'value': 'DTW'}],
                            value='NULL',
                            clearable=True)
                    ],
                    style=dict(width='10%')),
                html.Div(className='year',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-year',
                            options=[
                                {'label': '2015', 'value': '2015'},
                                {'label': '2013', 'value': '2013'},
                                {'label': '2014', 'value': '2014'}],
                            value='2008',
                            clearable=True)
                    ],
                    style=dict(width='10%')),
                html.Div(className='month',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-month',
                            options=[{'label' : m , 'value' : str(index + 1)} for index, m in enumerate(meses)],
                            value='2008',
                            clearable=True)
                    ],
                    style=dict(width='10%')),
                html.Div(className='day',
                    children=[
                        dcc.Dropdown(
                            id='dropdown-day',
                            options=[{'label' : i + 1, 'value' : str(i + 1)} for i in range(31)],
                            value='2008',
                            clearable=True)
                    ],
                    style=dict(width='10%')),
                ],
            style=dict(display='flex')
        ),
        dcc.Graph('perfilamiento-hm', config={'displayModeBar': False}),
        dcc.Graph('perfilamiento-edades', config={'displayModeBar': False}),
        dcc.Graph('perfilamiento-perc1', config={'displayModeBar': False}),
        dcc.Graph('perfilamiento-perc2', config={'displayModeBar': False}),
        dcc.Interval(id='interval-component', interval=1*1000)])])

# Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
@app.callback(
    Output('perfilamiento-hm', 'figure'),
    [Input('dropdown-ciudad', 'value')
    , Input('dropdown-year', 'value')
    , Input('dropdown-month', 'value')
    , Input('dropdown-day', 'value')])

# Este es el metodo que actualiza la informacion de MySQL y genera los dashboards de visitas y usuarios conectados por sexo
def update_graph(ciudad, year, month, day):

    if ciudad != None:
        ciudad = "'" + ciudad + "'"
    else:
        ciudad = 'NULL'
    if year != None:
        year = "'" + year + "'"
    else:
        year = 'NULL'
    if month != None:
        month = "'" + month + "'"
    else:
        month = 'NULL'
    if day != None:
        day = "'" + day + "'"
    else:
        day = 'NULL'

    query= """SELECT CONCAT(
                        IFNULL(YEAR, ''), '-'
                        , IFNULL(LPAD(QUARTER, 2, '0'), ''), '-'
                        , IFNULL(LPAD(MONTH, 2, '0'), ''), '-'
                        , IFNULL(LPAD(DAY_OF_MONTH, 2, '0'), '')
                    ) AS FECHA
                    , N_FLIGHTS
                    , ARR_DELAY
                    , DEP_DELAY
                FROM demoras_aeropuerto_origen_spark
                WHERE 1 = 0
                OR ORIGIN = {ciudad}
                AND YEAR LIKE {year}
                AND QUARTER LIKE '%%'
                AND MONTH LIKE {month}
                AND DAY_OF_MONTH LIKE {day}
                ORDER BY FECHA""".format(ciudad=ciudad, year=year, month=month, day=day)

    demoras_por_dia = pd.read_sql(query, con=db_connection) # Lectura de datos de demoras diarias
    demoras_por_aerolinea = pd.read_sql('SELECT * FROM demoras_aerolinea_dask ORDER BY FL_DATE LIMIT 20', con=db_connection) # Lectura de datos de demoras por aerolinea
    flota = pd.read_sql('SELECT * FROM flota_dask ORDER BY TAIL_NUM DESC LIMIT 20', con=db_connection) # Lectura del tamano de la flota de las aerolineas
    # destinos_fantasma = pd.read_sql('SELECT CONCAT("A", DEST_AIRPORT_ID) AS DEST_AIRPORT_ID, count FROM destinos_fantasma ORDER BY count DESC LIMIT 20', con=db_connection) # Lectura de los destinos con mas vuelos fantasma
    vuelos_origen_demoras = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask_ubicacion', con=db_connection) # Aeropuertos de origen con mas demoras
    # Definicion de layout de dashboards
    fig = make_subplots(rows=1
                    , cols=2
                    , subplot_titles=("Retraso por aeropuerto", None)
                    , specs=[[
                        {"type": "scatter", "colspan": 2}, None]]
                    )

    fig.add_trace(go.Scatter(x=demoras_por_dia.FECHA
                            , y=demoras_por_dia.ARR_DELAY
                            , mode='lines+markers'
                            , fill='tozeroy'
                            , name='Llegadas'
                            , marker=dict(color=Blues[6]))
                            , row=1
                            , col=1)
    fig.add_trace(go.Scatter(x=demoras_por_dia.FECHA
                            , y=demoras_por_dia.DEP_DELAY
                            , mode='lines+markers'
                            , fill='tozeroy'
                            , name='Salidas'
                            , marker=dict(color=Greens[6]))
                            , row=1
                            , col=1)
    fig.update_layout(height=450, width=1500, template='plotly_dark', legend=dict(orientation="h", yanchor="bottom", y=-0.6, xanchor="left", x=0.415))
    fig.update_xaxes(title_text="Fecha", title_font={'size':12}, showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text="Retraso promedio", showgrid=False, row=1, col=1)
    # Regresamos el bloque de graficos 
    return fig

# # -------------------------------------------------------------------------------------------

# # Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
# @app.callback(
#     Output('perfilamiento-edades', 'figure'),
#     [Input('interval-component', 'interval')])

# # Este es el metodo que actualiza la informacion de MySQL y genera los dashboards de distribucion de edad y usuarios conectados por AP
# def update_graph(grpname):

#     demoras_por_dia = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask WHERE DAY_OF_MONTH IS NOT NULL and ORIGIN = "TUL" ORDER BY YEAR, MONTH, DAY_OF_MONTH DESC LIMIT 365', con=db_connection) # Lectura de datos de demoras diarias
#     demoras_por_aerolinea = pd.read_sql('SELECT * FROM demoras_aerolinea_dask ORDER BY FL_DATE LIMIT 20', con=db_connection) # Lectura de datos de demoras por aerolinea
#     flota = pd.read_sql('SELECT * FROM flota_dask ORDER BY TAIL_NUM DESC LIMIT 20', con=db_connection) # Lectura del tamano de la flota de las aerolineas
#     # destinos_fantasma = pd.read_sql('SELECT CONCAT("A", DEST_AIRPORT_ID) AS DEST_AIRPORT_ID, count FROM destinos_fantasma ORDER BY count DESC LIMIT 20', con=db_connection) # Lectura de los destinos con mas vuelos fantasma
#     vuelos_origen_demoras = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask_ubicacion', con=db_connection) # Aeropuertos de origen con mas demoras
#     demoras_por_dia['FECHA'] = demoras_por_dia.fillna('').apply(lambda row: row['YEAR'] + '-' + row['MONTH'] + '-' + row['DAY_OF_MONTH'], axis=1)
#     # Definicion de layout de dashboards
#     fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.FL_DATE, marker=dict(color=Greens[6]), name='vuelos_totales',showlegend=False), row=2, col=2)
#     fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.ARR_DELAY, marker=dict(color=Greens[4]), name='Demoras en llegadas', showlegend=False), row=2, col=2)
#     fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.DEP_DELAY, marker=dict(color=Greens[2]), name='Demoras en salidas', showlegend=False), row=2, col=2)
#     # Regresamos el bloque de graficos
#     return fig

# # -------------------------------------------------------------------------------------------

# # Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
# @app.callback(
#     Output('perfilamiento-perc1', 'figure'),
#     [Input('interval-component', 'interval')])

# # Este es el metodo que actualiza la informacion de MySQL y genera el dashboard de percentiles de desplazamientos entre APs
# def update_graph(grpname):
        
#     demoras_por_dia = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask WHERE DAY_OF_MONTH IS NOT NULL and ORIGIN = "TUL" ORDER BY YEAR, MONTH, DAY_OF_MONTH DESC LIMIT 365', con=db_connection) # Lectura de datos de demoras diarias
#     demoras_por_aerolinea = pd.read_sql('SELECT * FROM demoras_aerolinea_dask ORDER BY FL_DATE LIMIT 20', con=db_connection) # Lectura de datos de demoras por aerolinea
#     flota = pd.read_sql('SELECT * FROM flota_dask ORDER BY TAIL_NUM DESC LIMIT 20', con=db_connection) # Lectura del tamano de la flota de las aerolineas
#     # destinos_fantasma = pd.read_sql('SELECT CONCAT("A", DEST_AIRPORT_ID) AS DEST_AIRPORT_ID, count FROM destinos_fantasma ORDER BY count DESC LIMIT 20', con=db_connection) # Lectura de los destinos con mas vuelos fantasma
#     vuelos_origen_demoras = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask_ubicacion', con=db_connection) # Aeropuertos de origen con mas demoras
#     demoras_por_dia['FECHA'] = demoras_por_dia.fillna('').apply(lambda row: row['YEAR'] + '-' + row['MONTH'] + '-' + row['DAY_OF_MONTH'], axis=1)
#     # Definicion de layout de dashboards
#     fig = make_subplots(rows=7, cols=2,
#                         subplot_titles=("Flota por aerolínea", "Destinos fantasma", "Aeropuertos en Estados Unidos","Demoras por aerolínea", "Demoras en llegadas", "Demoras en salidas"),
#                         specs=[[{"type":"bar"}, {"type": "bar"}],
#                               [{"type":"scattergeo"}, {"type": "bar"}],
#                               [{"type": "scatter", "colspan": 2}, None],
#                               [{"type": "scatter", "colspan": 2}, None],
#                               [{"type": "scattergeo", "colspan": 2, "rowspan":3}, None],
#                                [None, None],
#                               [None, None]])

#     fig.add_trace(go.Bar(x=flota.OP_UNIQUE_CARRIER, y=flota.TAIL_NUM, marker=dict(color=Greens[4]), name='Flota'), row=1, col=1) # Grafico de tamano de las mayores flotas
        
#     # Regresamos el grafico
#     return fig

# # -------------------------------------------------------------------------------------------

# # Callback: A partir de aqui se hace la actualizacion de los datos cada que un usuario visita o actualiza la pagina
# @app.callback(
#     Output('perfilamiento-perc2', 'figure'),
#     [Input('interval-component', 'interval')])

# # Este es el metodo que actualiza la informacion de MySQL y genera el dashboard de percentiles de usuarios detectados en APs
# def update_graph(grpname):

#     demoras_por_dia = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask WHERE DAY_OF_MONTH IS NOT NULL and ORIGIN = "TUL" ORDER BY YEAR, MONTH, DAY_OF_MONTH DESC LIMIT 365', con=db_connection) # Lectura de datos de demoras diarias
#     demoras_por_aerolinea = pd.read_sql('SELECT * FROM demoras_aerolinea_dask ORDER BY FL_DATE LIMIT 20', con=db_connection) # Lectura de datos de demoras por aerolinea
#     flota = pd.read_sql('SELECT * FROM flota_dask ORDER BY TAIL_NUM DESC LIMIT 20', con=db_connection) # Lectura del tamano de la flota de las aerolineas
#     # destinos_fantasma = pd.read_sql('SELECT CONCAT("A", DEST_AIRPORT_ID) AS DEST_AIRPORT_ID, count FROM destinos_fantasma ORDER BY count DESC LIMIT 20', con=db_connection) # Lectura de los destinos con mas vuelos fantasma
#     vuelos_origen_demoras = pd.read_sql('SELECT * FROM demoras_aeropuerto_origen_dask_ubicacion', con=db_connection) # Aeropuertos de origen con mas demoras
#     demoras_por_dia['FECHA'] = demoras_por_dia.fillna('').apply(lambda row: row['YEAR'] + '-' + row['MONTH'] + '-' + row['DAY_OF_MONTH'], axis=1)
#     # Definicion de layout de dashboards
#     fig = make_subplots(rows=7, cols=2,
#                         subplot_titles=("Flota por aerolínea", "Destinos fantasma", "Aeropuertos en Estados Unidos","Demoras por aerolínea", "Demoras en llegadas", "Demoras en salidas"),
#                         specs=[[{"type":"bar"}, {"type": "bar"}],
#                               [{"type":"scattergeo"}, {"type": "bar"}],
#                               [{"type": "scatter", "colspan": 2}, None],
#                               [{"type": "scatter", "colspan": 2}, None],
#                               [{"type": "scattergeo", "colspan": 2, "rowspan":3}, None],
#                                [None, None],
#                               [None, None]])

#     fig.add_trace(go.Bar(x=flota.OP_UNIQUE_CARRIER, y=flota.TAIL_NUM, marker=dict(color=Greens[4]), name='Flota'), row=1, col=1) # Grafico de tamano de las mayores flotas
        
#     # Regresamos el grafico
#     return fig

# # -------------------------------------------------------------------------------------------


if __name__ == '__main__':
        app.run_server(debug=True, port=9092)
