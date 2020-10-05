# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion
import dask.dataframe as dd # Utilizado para el procesamiento de los datos
from sqlalchemy import create_engine

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
time_table = config["time_table"] # Tabla en la que almaceno el tiempo de ejecucion de la tarea
results_table_origen = config["results_table_origen"] # Tabla en la que almaceno el resultado de los vuelos de origen
results_table_destino = config["results_table_destino"] # Tabla en la que almaceno el resultado de los vuelos de destino
input_table = config["input_table"] # Tabla con los datos necesarios para la ejecucion del algoritmo
time_table_mode = config["time_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
results_table_mode = config["results_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
db_url = config["db_url"] # URL de la base de datos
db_driver = config["db_driver"] # Driver de la base de datos
db_numPartitions = config["db_numPartitions"] # Numero de particiones en las que escribimos la base de datos
exec_desc = config["description"] # Descripcion breve de la ejecucion que estamos haciendo
resources = config["resources"] # Descripcion breve de los recursos utilizados para la ejecucion

# mysql+pymysql://ikerolarra:mysql2014@localhost:3306/TRANSTAT
uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(user, password, "3306", database)
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

df = dd.read_sql_table('RITA_10', uri=uri, index_col='ID')
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCIONES
# ----------------------------------------------------------------------------------------------------
def tiempo_ejecucion(t_inicial):
    tiempo_segundos = time.time() - t_inicial
    tiempo = {}
    tiempo['horas'] = int(tiempo_segundos // 3600)
    tiempo['minutos'] = int(tiempo_segundos % 3600 // 60)
    tiempo['segundos'] = tiempo_segundos % 3600 % 60
    return tiempo

def conjuntos_rollup(columnas):
    conjuntos = list(map(lambda x: columnas[0:x+1], range(len(columnas))))
    return conjuntos

def group_by_rollup(df, columnas_agregacion, columnas_totales):
    columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
    resultado = df.groupby(columnas_agregacion).agg(agregaciones).reset_index().compute()
    for columna in columnas_nulas:
        resultado[columna] = -1
    return resultado

def rollup(df, columnas, agregaciones):
    df = df[list(set(columnas + list(agregaciones.keys())))]
    conjuntos_columnas = conjuntos_rollup(columnas)
    dataframes = list(map(lambda X: group_by_rollup(df, X, columnas), conjuntos_columnas[1:]))
    return dataframes
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
conjuntos_rollup(['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'])

agregaciones = {'TAIL_NUM' : 'count'}

lista_df = rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)

for resultado in lista_df:
	resultado.to_sql('flota_dask', uri, if_exists='append', index=False)

print(lista_df[1].head())

print(tiempo_ejecucion(t_inicio))
# ----------------------------------------------------------------------------------------------------

# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------

# ----------------------------------------------------------------------------------------------------