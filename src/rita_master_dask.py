# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
from dask.distributed import Client

if __name__ == '__main__':

    client = Client(n_workers=2)

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
    # from src import utils # Estas son las funciones definidas por mi

    # Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
    parser = argparse.ArgumentParser()
    parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
    parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
    parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
    args = parser.parse_args()
    # Leemos las credenciales de la ruta especificada

    def lee_config_csv(path, sample_size, process):
        file = open(path, "r").read().splitlines()
        nombres = file[0]
        info = filter(lambda row: row.split("|")[0] == sample_size and row.split("|")[1] == process, file[1:])
        parametros = dict(zip(nombres.split('|'), list(info)[0].split('|')))
        return parametros

    config = lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
    with open(args.creds) as json_file:
        creds = json.load(json_file)

    uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(creds["user"], creds["password"], "3306", creds["database"])
    # ----------------------------------------------------------------------------------------------------


    # DEFINICION DE FUNCIONES
    # ----------------------------------------------------------------------------------------------------
    nunique = dd.Aggregation('nunique', lambda s: s.nunique(), lambda s0: s0.sum()) # Definimos como hacer la agregacion para contar elementos únicos

    def tiempo_ejecucion(t_inicial):
        """Esta función mide el tiempo transcurrido entre t_inicial y el momento en el que se llama la función.
        El resultado es un JSON con los campos: horas, minutos y segundos."""
        tiempo_segundos = time.time() - t_inicial
        tiempo = {}
        tiempo['horas'] = int(tiempo_segundos // 3600)
        tiempo['minutos'] = int(tiempo_segundos % 3600 // 60)
        tiempo['segundos'] = tiempo_segundos % 3600 % 60
        return tiempo

    def unir_columnas(df, col1, col2, col_resultante, meta=pd.DataFrame):
        """Esta función recibe como argumento un dataframe y dos columnas (col1, col2) cuyos valores concatenará en una columna nueva cuyo resultado
        corresponde al cuarto argumento. (col resultante)"""
        df[col_resultante] = df.apply(lambda row: str(row[col1]) + '-' + str(row[col2]), axis=1, meta='str')
        return df.drop(col1, axis=1).drop(col2, axis=1)

    def conjuntos_rollup(columnas): # Hay que ver la forma de que se haga la agregación total
        conjuntos = list(map(lambda x: columnas[0:x+1], range(len(columnas))))
        return conjuntos

    def group_by_rollup(df, columnas_agregacion, columnas_totales, agregaciones, meta=pd.DataFrame):
        columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
        try:
            resultado = df.groupby(columnas_agregacion).agg(agregaciones).reset_index().compute()
        except Exception as e:
            if 'nunique' in list(agregaciones.values()):
                resultado = df.groupby(columnas_agregacion).agg(nunique).reset_index().compute()
            else:
                print("\nHubo un error en la agregación.\n")
                print(e)
                print('\n')
        for columna in columnas_nulas:
            resultado[columna] = None
        return resultado.drop(columns=['X'])

    def rollup(df, columnas, agregaciones, meta=list):
        df = df[list(set(columnas + list(agregaciones.keys())))]
        df['X'] = 1 # Esta es una columna temporal para hacer la agregación total
        columnas_totales = ["X"] + columnas  # Esta es una columna temporal que se utiliza para la agregacion total.
        conjuntos_columnas = conjuntos_rollup(columnas_totales)
        dataframes = list(map(lambda X: group_by_rollup(df, X, columnas_totales, agregaciones), conjuntos_columnas))
        return dataframes
    # ----------------------------------------------------------------------------------------------------


    # LECTURA DE DATOS
    # ----------------------------------------------------------------------------------------------------
    t_inicio = time.time() # Inicia tiempo de ejecucion

    # df = dd.read_sql_table(config["input_table"], uri=uri, index_col=config["partition_column"])
    # df = dd.read_sql_table("RITA_100K", uri=uri, index_col=config["partition_column"])
    df = dd.read_parquet(config['input_path']
                # , infer_divisions=False
                , engine='pyarrow'
                , columns=['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID']
                # , index='ID'
                , dtype={'ACTUAL_ELAPSED_TIME' : float, 'ARR_DELAY' : float, 'DEP_DELAY' : float, 'TAXI_IN' : float, 'TAXI_OUT' : float}
            )
    df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'].astype(float)
    df['ARR_DELAY'] = df['ARR_DELAY'].astype(float)
    df['DEP_DELAY'] = df['DEP_DELAY'].astype(float)
    df['TAXI_IN'] = df['TAXI_IN'].astype(float)
    df['TAXI_OUT'] = df['TAXI_OUT'].astype(float)

    # df = df.repartition(4)

    df = client.persist(df)

    # print(df.count().compute())

    # ----------------------------------------------------------------------------------------------------


    # EJECUCION
    # ----------------------------------------------------------------------------------------------------
    process = config["results_table"] # Tabla en la que almaceno el resultado (resumen de flota por aerolinea)
    print('\n\n\tLos resultados se escribirán en la tabla: ' + process + '\n\n')
    if process == 'demoras_aerolinea_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_aeropuerto_origen_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = rollup(df, ['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_aeropuerto_destino_dask':
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = rollup(df, ['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_ruta_aeropuerto_dask':
        df = unir_columnas(df, "ORIGIN", "DEST", "ROUTE_AIRPORTS")
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = rollup(df, ['ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'demoras_ruta_mktid_dask':
        df = unir_columnas(df, "ORIGIN_CITY_MARKET_ID", "DEST_CITY_MARKET_ID", "ROUTE_MKT_ID")
        agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}
        lista_df = rollup(df, ['ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
    elif process == 'flota_dask':
        agregaciones = {'TAIL_NUM' : 'nunique'}
        lista_df = rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)
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

    print()
    print(time.time() - t_inicio)
    print()

    print('\n\t\tFIN DE LA EJECUCION')
    # ----------------------------------------------------------------------------------------------------