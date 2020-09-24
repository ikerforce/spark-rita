# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de python
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import plotly.express as px
import math
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
from plotly.subplots import make_subplots
import plotly.graph_objects as go
from plotly.colors import sequential
import argparse
import json
Greens = sequential.Blues

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
# ----------------------------------------------------------------------------------------------------


# DEFINICION DEL SERVICIO
# ----------------------------------------------------------------------------------------------------
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    html.Div([dcc.Graph('vuelos', config={'displayModeBar': False}),
             dcc.Interval(id='interval-component', interval=1*1000)])])

@app.callback(
    Output('vuelos', 'figure'),
    [Input('interval-component', 'interval')])
# ----------------------------------------------------------------------------------------------------

# DEFINICION DE FUNCIONES
# ----------------------------------------------------------------------------------------------------
def update_graph(grpname):
    db_connection_str = 'mysql+pymysql://' + user + ':' + password + '@localhost:3306/' + database # String de conexion a MySQL
    db_connection = create_engine(db_connection_str) # Conectamos con la base de datos de MySQL

    demoras_por_dia = pd.read_sql('SELECT * FROM demoras_por_dia ORDER BY FL_DATE DESC LIMIT 100', con=db_connection) # Lectura de datos de demoras diarias
    demoras_por_aerolinea = pd.read_sql('SELECT * FROM demoras_por_aerolinea ORDER BY n_vuelos LIMIT 20', con=db_connection) # Lectura de datos de demoras por aerolinea
    flota = pd.read_sql('SELECT * FROM flota ORDER BY N_AVIONES DESC LIMIT 20', con=db_connection) # Lectura del tamano de la flota de las aerolineas
    destinos_fantasma = pd.read_sql('SELECT CONCAT("A", DEST_AIRPORT_ID) AS DEST_AIRPORT_ID, count FROM destinos_fantasma ORDER BY count DESC LIMIT 20', con=db_connection) # Lectura de los destinos con mas vuelos fantasma
    vuelos_origen_demoras = pd.read_sql('SELECT * FROM vuelos_origen_demoras', con=db_connection) # Aeropuertos de origen con mas demoras

    # Definicion de layout de dashboards
    fig = make_subplots(rows=7, cols=2,
                        subplot_titles=("Flota por aerolínea", "Destinos fantasma", "Aeropuertos en Estados Unidos","Demoras por aerolínea", "Demoras en llegadas", "Demoras en salidas"),
                        specs=[[{"type":"bar"}, {"type": "bar"}],
                              [{"type":"scattergeo"}, {"type": "bar"}],
                              [{"type": "scatter", "colspan": 2}, None],
                              [{"type": "scatter", "colspan": 2}, None],
                              [{"type": "scattergeo", "colspan": 2, "rowspan":3}, None],
                               [None, None],
                              [None, None]])

    fig.add_trace(go.Bar(x=flota.OP_UNIQUE_CARRIER, y=flota.N_AVIONES, marker=dict(color=Greens[4]), name='Flota'), row=1, col=1) # Grafico de tamano de las mayores flotas
    
    fig.add_trace(go.Bar(x=destinos_fantasma.DEST_AIRPORT_ID.astype(str), y=destinos_fantasma['count'], marker=dict(color=Greens[4]), showlegend=False), row=1, col=2) # Grafico de aeropuertos con mas vuelos fantasma

    fig.add_trace(go.Scattergeo(
        lon = vuelos_origen_demoras['LONGITUDE'],
        lat = vuelos_origen_demoras['LATITUDE'],
        mode = 'markers', marker_color=vuelos_origen_demoras['COLOR_ARR_DELAY']), row=2, col=1) # Mapa de aeropuertos de origen con mas demoras
    
    fig.update_layout(geo_scope='usa') # Cambio de layout para que el mapa de arriba sea solo de US
    
    fig.add_trace(go.Scattergeo(
        lon = vuelos_origen_demoras['LONGITUDE'],
        lat = vuelos_origen_demoras['LATITUDE'],
        mode = 'markers', marker_color=vuelos_origen_demoras['COLOR_ARR_DELAY']), row=5, col=1) # Mapa de aeropuertos de origen con mas demoras

    fig.update_layout(geo_scope='usa') # Cambio de layout para que el mapa de arriba sea solo de US
    
    # Grfico de barras que presenta el numero de vuelos, el numero de demoras en llegadas y el numero de demoras en salidas por aerolinea
    fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.n_vuelos, marker=dict(color=Greens[6]), name='vuelos_totales',showlegend=False), row=2, col=2)
    fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.n_dem_llegadas, marker=dict(color=Greens[4]), name='Demoras en llegadas', showlegend=False), row=2, col=2)
    fig.add_trace(go.Bar(x=demoras_por_aerolinea.OP_UNIQUE_CARRIER, y=demoras_por_aerolinea.n_dem_salidas, marker=dict(color=Greens[2]), name='Demoras en salidas', showlegend=False), row=2, col=2)
    
    # Demoras en llegadas vs numero de vuelos por dia del mes
    fig.add_trace(go.Scatter(x=demoras_por_dia.FL_DATE, y=demoras_por_dia.n_vuelos, mode='lines+markers', fill='tozeroy', name='Vuelos diarios', marker=dict(color=Greens[2]), showlegend=False), row=3, col=1)
    fig.add_trace(go.Scatter(x=demoras_por_dia.FL_DATE, y=demoras_por_dia.n_dem_llegadas, mode='lines+markers', fill='tozeroy', name='Llegadas con demora', marker=dict(color=Greens[2]), showlegend=False), row=3, col=1)

    # Demoras en llegadas vs numero de vuelos por dia del mes
    fig.add_trace(go.Scatter(x=demoras_por_dia.FL_DATE, y=demoras_por_dia.n_vuelos, mode='lines+markers', fill='tozeroy', name='Vuelos diarios', marker=dict(color=Greens[2]), showlegend=False), row=4, col=1)
    fig.add_trace(go.Scatter(x=demoras_por_dia.FL_DATE, y=demoras_por_dia.n_dem_salidas, mode='lines+markers', fill='tozeroy', name='Salidas con demora', marker=dict(color=Greens[2]), showlegend=False), row=4, col=1)

    # Ajuste de titulos del eje de los graficos
    fig.update_yaxes(title_text="Número de aviones", showgrid=False, row=1, col=1)
    fig.update_yaxes(title_text="Número de vuelos", showgrid=False, row=1, col=2)
    fig.update_yaxes(title_text="Aeropuertos en Estados Unidos", showgrid=False, row=2, col=1)
    fig.update_yaxes(title_text="Numero de usuarios detectados", showgrid=False, row=2, col=2)
    fig.update_yaxes(title_text="Número de vuelos", showgrid=False, row=3, col=1)
    fig.update_yaxes(title_text="Número de vuelos", showgrid=False, row=4, col=1)

    # Cambio en titulo del grafico y tamano total
    fig.update_layout(title_text="Información de vuelos en Estados Unidos", height=1800, width=1900, template='plotly_dark', geo_scope='usa')

    # Regresamos el dashboard
    return fig
# ----------------------------------------------------------------------------------------------------


# INICIO DEL SERVICIO
# ----------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    app.run_server(debug=False, port=9092)
# ----------------------------------------------------------------------------------------------------


