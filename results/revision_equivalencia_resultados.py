import json
import pandas as pd
from sqlalchemy import create_engine
import numpy as np
import dask.dataframe as dd
import os
import glob


with open('conf/mysql_creds.json') as json_file:
        creds = json.load(json_file)

uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(creds["user"], creds["password"], "3306", creds["database"]) # String de conexion a MySQL
db_connection = create_engine(uri) # Conectamos con la base de datos de MySQL


def tablas_equivalentes_mysql(query_spark, query_dask, imprimir_head=False, columnas_ordenar=None):
    """Esta función prepara dos DataFrames y los convierte en arrays ordenados de la misma forma 
    y con los mismos tipos de datos para después poder compararlos."""
    print('+-------------- SPARK --------------+')
    print('query spark:\n' + query_spark)
    tabla_spark = pd.read_sql(query_spark, con=uri)
    print('Tamaño del arreglo: ' + str(tabla_spark.shape))
    tipo_datos_spark = tabla_spark.dtypes.to_dict()
    columnas_spark = tabla_spark.columns.to_list()
    columnas_spark.sort()
    if columnas_ordenar == None:
        tabla_spark_lista = tabla_spark[columnas_spark].sort_values(by=columnas_spark, ascending=False).fillna(0)
    else:
        tabla_spark_lista = tabla_spark[columnas_spark].sort_values(by=columnas_ordenar, ascending=False).fillna(0)
    
    if imprimir_head:
        print(tabla_spark_lista.head())
    
    print('+-------------- DASK --------------+')
    print('query dask:\n' + query_dask)
    tabla_dask = pd.read_sql(query_dask, con=uri)
    print('Tamaño del arreglo: ' + str(tabla_dask.shape))
    columnas_dask = tabla_dask.columns.to_list()
    columnas_dask.sort()
    for columna in tipo_datos_spark.keys():
        if tipo_datos_spark[columna] == 'float64':
            tabla_dask[columna] = tabla_dask[columna].astype(tipo_datos_spark[columna]).round(7)
        else:
            tabla_dask[columna] = tabla_dask[columna].astype(tipo_datos_spark[columna])
    if columnas_ordenar == None:
        tabla_dask_lista = tabla_dask[columnas_dask].sort_values(by=columnas_dask, ascending=False).fillna(0)
    else:
        tabla_dask_lista = tabla_dask[columnas_dask].sort_values(by=columnas_ordenar, ascending=False).fillna(0)
    
    if imprimir_head:
        print(tabla_dask_lista.head())
    
    return tabla_spark_lista.values, tabla_dask_lista.values


def tablas_equivalentes_parquet(ruta_spark, ruta_dask, imprimir_head=False, columnas_ordenar=None):
    """Esta función prepara dos DataFrames y los convierte en arrays ordenados de la misma forma 
    y con los mismos tipos de datos para después poder compararlos."""
    print('+-------------- SPARK --------------+')
    print('ruta spark:\n' + ruta_spark)
    tabla_spark = dd.read_parquet(ruta_spark, engine='pyarrow').compute()
    print('Tamaño del arreglo: ' + str(tabla_spark.shape))
    tipo_datos_spark = tabla_spark.dtypes.to_dict()
    columnas_spark = tabla_spark.columns.to_list()
    columnas_spark.sort()
    if columnas_ordenar == None:
        tabla_spark_lista = tabla_spark[columnas_spark].sort_values(by=columnas_spark, ascending=False).fillna(0)
    else:
        tabla_spark_lista = tabla_spark[columnas_spark].sort_values(by=columnas_ordenar, ascending=False).fillna(0)
    
    if imprimir_head:
        print(tabla_spark_lista.head())
    
    print('+-------------- DASK --------------+')
    print('ruta dask:\n' + ruta_dask)
    tabla_dask = dd.read_parquet(ruta_dask, engine='pyarrow').compute()
    print('Tamaño del arreglo: ' + str(tabla_dask.shape))
    columnas_dask = tabla_dask.columns.to_list()
    columnas_dask.sort()
    for columna in tipo_datos_spark.keys():
        if tipo_datos_spark[columna] == 'float64':
            tabla_dask[columna] = tabla_dask[columna].astype(tipo_datos_spark[columna]).round(7)
        else:
            tabla_dask[columna] = tabla_dask[columna].astype(tipo_datos_spark[columna])
    if columnas_ordenar == None:
        tabla_dask_lista = tabla_dask[columnas_dask].sort_values(by=columnas_dask, ascending=False).fillna(0)
    else:
        tabla_dask_lista = tabla_dask[columnas_dask].sort_values(by=columnas_ordenar, ascending=False).fillna(0)
    
    if imprimir_head:
        print(tabla_dask_lista.head())
    
    return tabla_spark_lista.values, tabla_dask_lista.values


def confirma_igualdad(arreglo_spark, arreglo_dask, print_examples=False):
    """Esta función compara dos dataframes obtenidos a partir de los queries de SQL 
    y regresa True si son iguales y False en otro caso"""
    if np.array_equal(arreglo_spark, arreglo_dask):
        print('\n\tLos arreglos coinciden.\n')
    else:
        if print_examples:
            print('\n\tLos arreglos presentan diferencias en al menos los siguientes elementos:\n')
            i = 0
            j = 0
            for x in zip(arreglo_spark, arreglo_dask):
                j+=1
                if not np.array_equal(x[0], x[1]):
                    print(i, j)
                    print('spark')
                    print(x[0])
                    print('dask')
                    print(x[1])
                    print('\n')
                    i+=1
                if i > 10:
                    break;
        else:
            print('\n\tLos arreglos presentan diferencias.\n')



def get_latest(process):
    """Esta función resibe el nombre de un directorio y devuelve el último archivo escrito."""
    path = 'resultados/{process}/*'.format(process=process)
    list_of_files = glob.glob(path)
    latest_file = max(list_of_files, key=os.path.getctime)
    if 'dask' in process:
        return '{latest_file}/*.parquet'.format(process=process, latest_file=latest_file)
    else:
        return '{latest_file}/*.parquet'.format(process=process, latest_file=latest_file)


ruta_dask = get_latest("demoras_aerolinea_dask")
ruta_spark = get_latest("demoras_aerolinea_spark")
arr_spark, arr_dask = tablas_equivalentes_parquet(ruta_spark, ruta_dask)
confirma_igualdad(arr_spark, arr_dask)

ruta_dask = get_latest("demoras_aeropuerto_destino_dask")
ruta_spark = get_latest("demoras_aeropuerto_destino_spark")
arr_spark, arr_dask = tablas_equivalentes_parquet(ruta_spark, ruta_dask)
confirma_igualdad(arr_spark, arr_dask)

query_dask = """SELECT * from demoras_aeropuerto_origen_dask"""
query_spark = """SELECT * from demoras_aeropuerto_origen_spark"""
arr_spark, arr_dask = tablas_equivalentes_mysql(query_spark, query_dask)
confirma_igualdad(arr_spark, arr_dask)

ruta_dask = get_latest("demoras_ruta_aeropuerto_dask")
ruta_spark = get_latest("demoras_ruta_aeropuerto_spark")
arr_spark, arr_dask = tablas_equivalentes_parquet(ruta_spark, ruta_dask)
confirma_igualdad(arr_spark, arr_dask)

query_dask = """SELECT * from dijkstra_dask"""
query_spark = """SELECT * from dijkstra_spark"""
arr_spark, arr_dask = tablas_equivalentes_mysql(query_spark, query_dask)
confirma_igualdad(arr_spark, arr_dask)

query_dask = """SELECT * from flota_dask"""
query_spark = """SELECT * from flota_spark"""
arr_spark, arr_dask = tablas_equivalentes_mysql(query_spark, query_dask)
confirma_igualdad(arr_spark, arr_dask)

query_dask = """SELECT * from demoras_ruta_mktid_dask"""
query_spark = """SELECT * from demoras_ruta_mktid_spark"""
columnas_ordenar = ['ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT']
arr_spark, arr_dask = tablas_equivalentes_mysql(query_spark, query_dask, columnas_ordenar=columnas_ordenar)
confirma_igualdad(arr_spark, arr_dask)