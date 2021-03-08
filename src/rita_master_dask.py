# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
from dask.distributed import Client

if __name__ == '__main__':

    client = Client(n_workers=4)

    # Importaciones de Python
    import argparse # Utilizado para leer archivo de configuracion
    import json # Utilizado para leer archivo de configuracion
    import time # Utilizado para medir el timpo de ejecucion
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
    parser.add_argument("--config", help="Ruta hacia archivo de configuracion")
    parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos")
    args = parser.parse_args()
    # Leemos las credenciales de la ruta especificada
    with open(args.config) as json_file:
        config = json.load(json_file)
    with open(args.creds) as json_file:
        creds = json.load(json_file)

    uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(creds["user"], creds["password"], "3306", creds["database"])
    # ----------------------------------------------------------------------------------------------------


    # LECTURA DE DATOS
    # ----------------------------------------------------------------------------------------------------
    t_inicio = time.time() # Inicia tiempo de ejecucion

    # df = dd.read_sql_table(config["input_table"], uri=uri, index_col=config["partition_column"])
    df = dd.read_parquet('data_dask_50'
                , infer_divisions=False
                , engine='pyarrow'
                , columns=['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID']
                , index=False
                , dtype={'ACTUAL_ELAPSED_TIME' : float, 'ARR_DELAY' : float, 'DEP_DELAY' : float, 'TAXI_IN' : float, 'TAXI_OUT' : float}
            )
    df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'].astype(float)
    df['ARR_DELAY'] = df['ARR_DELAY'].astype(float)
    df['DEP_DELAY'] = df['DEP_DELAY'].astype(float)
    df['TAXI_IN'] = df['TAXI_IN'].astype(float)
    df['TAXI_OUT'] = df['TAXI_OUT'].astype(float)
    # ----------------------------------------------------------------------------------------------------


    # EJECUCION
    # ----------------------------------------------------------------------------------------------------
    process = config["results_table"] # Tabla en la que almaceno el resultado (resumen de flota por aerolinea)
    print('\n\n\tLos resultados se escribirán en la tabla: ' + process + '\n\n')
    if process == 'demoras_aerolinea_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_aeropuerto_origen_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_aeropuerto_destino_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_ruta_aeropuerto_dask':
        df = utils.unir_columnas(df, "ORIGIN", "DEST", "ROUTE_AIRPORTS")
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_ruta_mktid_dask':
        df = utils.unir_columnas(df, "ORIGIN_CITY_MARKET_ID", "DEST_CITY_MARKET_ID", "ROUTE_MKT_ID")
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = utils.rollup(df, ['ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'flota_dask':
        agregaciones = {'TAIL_NUM' : 'nunique'}
        lista_df = utils.rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    else:
        print('\n\n\tEl nombre del proceso: ' + process + ' no es válido.\n\n')


    lista_df[0].to_sql(process, uri, if_exists='replace', index=False) # En la primera escritura borro los resultados anteriores
    for resultado in lista_df[1:]:
        resultado.to_sql(process, uri, if_exists='append', index=False) # Desupés solo hago append


    t_final = time.time() # Tiempo de finalizacion de la ejecucion
    # ----------------------------------------------------------------------------------------------------

    # REGISTRO DE TIEMPO
    # ----------------------------------------------------------------------------------------------------
    info_tiempo = [[process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], time.strftime('%Y-%m-%d %H:%M:%S')]]
    df_tiempo = pd.DataFrame(data=info_tiempo, columns=['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'insertion_ts'])
    df_tiempo.to_sql("registro_de_tiempo_dask", uri, if_exists=config["time_table_mode"], index=False)
    # ----------------------------------------------------------------------------------------------------