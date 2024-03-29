# -*- coding: utf-8 -*-
import time # Utilizado para medir el timpo de ejecucion
t_inicio = time.time() # Inicia tiempo de ejecucion

# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
from dask.distributed import Client
# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import dask.dataframe as dd # Utilizado para el procesamiento de los datos
import pandas as pd # Utilizado para crear dataframe que escribe la informacion del tiempo en MySQL
from sqlalchemy import create_engine
import sys # Ayuda a agregar archivos al path
from os import getcwdb # Nos permite conocer el directorio actual
curr_path = getcwdb().decode() # Obtenemos el directorio actual
sys.path.insert(0, curr_path) # Agregamos el directioro en el que se encuentra el directorio src
from src import utils # Estas son las funciones definidas por mi

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
parser.add_argument("--env", help="Puede ser local o cluster. Esto determina los recursos utilizados y los archivos de configuración que se utilizarán.")
parser.add_argument("--command_time", help="Hora a la que fue enviada la ejecución del proceso.")
parser.add_argument("--scheduler", help="Direccion IP y puerto del scheduler.")
args = parser.parse_args()

if args.env != 'cluster':
    config = utils.lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
else:
    config = utils.lee_config_csv(path="conf/base/configs_cluster.csv", sample_size=args.sample_size, process=args.process)
with open(args.creds) as json_file:
    creds = json.load(json_file)

command_time = float(args.command_time)

uri = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(creds["user"], creds["password"], creds["host"], "3306", creds["database"])

if __name__ == '__main__':

    if args.scheduler != None:
        
        client = Client(args.scheduler)
    
    else:
        
        client = Client()
    # ----------------------------------------------------------------------------------------------------


    # LECTURA DE DATOS
    # ----------------------------------------------------------------------------------------------------

    # df = dd.read_sql_table(config["input_table"], uri=uri, index_col=config["partition_column"])
    # df = dd.read_sql_table("RITA_100K", uri=uri
    #     , index_col=config["partition_column"]
    #     , columns=['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])

    # print(df.dtypes)

    # df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'].astype(float)
    # df['ARR_DELAY'] = df['ARR_DELAY'].astype(float)
    # df['DEP_DELAY'] = df['DEP_DELAY'].astype(float)
    # df['TAXI_IN'] = df['TAXI_IN'].astype(float)
    # df['TAXI_OUT'] = df['TAXI_OUT'].astype(float)

    # print(df.dtypes)

    # df = df.repartition(1)

    # print(df.divisions)

    # df = client.persist(df)

    # print(df.count().compute())

    # ----------------------------------------------------------------------------------------------------


    # EJECUCION
    # ----------------------------------------------------------------------------------------------------
    process = config["results_table"] # Tabla en la que almaceno el resultado (resumen de flota por aerolinea)
    print('\tLos resultados se escribirán en la tabla: ' + process)

    if process == 'demoras_aerolinea_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_parquet(lista_df, process, env=args.env)
    
    elif process == 'demoras_aeropuerto_origen_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'max', 'DEP_DELAY':'max', 'ACTUAL_ELAPSED_TIME':'max', 'TAXI_IN':'max', 'TAXI_OUT':'max'}
        lista_df = utils.rollup(df, ['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_mysql(lista_df, uri, process)
    
    elif process == 'demoras_aeropuerto_destino_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'min', 'DEP_DELAY':'min', 'ACTUAL_ELAPSED_TIME':'min', 'TAXI_IN':'min', 'TAXI_OUT':'min'}
        lista_df = utils.rollup(df, ['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_parquet(lista_df, process, env=args.env)
    
    elif process == 'demoras_ruta_aeropuerto_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST'])
        df = df.repartition(12)
        df = utils.unir_columnas(df, "ORIGIN", "DEST", "ROUTE_AIRPORTS")
        df = client.persist(df)
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_parquet(lista_df, process, env=args.env)
        del df
    
    elif process == 'demoras_ruta_mktid_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])
        df = df.repartition(12)
        df = utils.unir_columnas(df, "ORIGIN_CITY_MARKET_ID", "DEST_CITY_MARKET_ID", "ROUTE_MKT_ID")
        df = client.persist(df)
        # df = df.set_index('ROUTE_MKT_ID')
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'std', 'DEP_DELAY':'std', 'ACTUAL_ELAPSED_TIME':'std', 'TAXI_IN':'std', 'TAXI_OUT':'std'}
        lista_df = utils.rollup(df, ['ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_mysql(lista_df, uri, process)
        # del df
    
    elif process == 'flota_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'TAIL_NUM'])
        agregaciones = {'TAIL_NUM' : 'nunique'}
        lista_df = utils.rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
        write_time = time.time()
        utils.write_result_to_mysql(lista_df, uri, process)

    elif process == 'elimina_nulos_dask':
        df = utils.read_df_from_parquet(config['input_path'], columns=['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])
        df = utils.elimina_nulos(df)
        write_time = 0.0
        print('Conteo sin nulos: ' + str(df.shape[0].compute()))
    
    else:
        print('\n\n\tEl nombre del proceso: ' + process + ' no es válido.\n\n')

    t_final = time.time() # Tiempo de finalizacion de la ejecucion
    # ----------------------------------------------------------------------------------------------------

    # REGISTRO DE TIEMPO
    # ----------------------------------------------------------------------------------------------------
    info_tiempo = [[process + '_command_time', command_time, t_inicio, t_inicio - command_time, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')],
                [process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')],
                [process + '_write_time', write_time, t_final, t_final - write_time, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')]]
    df_tiempo = pd.DataFrame(data=info_tiempo, columns=['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size', 'env', 'insertion_ts'])
    df_tiempo.to_sql(config['time_table'], uri, if_exists=config["time_table_mode"], index=False)

    print('\tTiempo de ejecución: ' + str(time.time() - t_inicio))
    print('\tFIN DE LA EJECUCION')
    # ----------------------------------------------------------------------------------------------------
