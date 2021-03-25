# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU

from dask.distributed import Client

if __name__ == '__main__':
    client = Client(n_workers=4)
    # user code follows

    # PREPARACION DE AMBIENTE
    # ----------------------------------------------------------------------------------------------------
    # Importaciones de Python
    import argparse # Utilizado para leer archivo de configuracion
    import json # Utilizado para leer archivo de configuracion
    import time # Utilizado para medir el timpo de ejecucion
    import pandas as pd # Utilizado para crear dataframe que escribe la informacion del tiempo en MySQL
    from sqlalchemy import create_engine
    import datetime
    import sys # Ayuda a agregar archivos al path
    import os # Nos permite conocer el directorio actual
    curr_path = os.getcwdb().decode() # Obtenemos el directorio actual
    sys.path.insert(0, curr_path) # Agregamos el directioro en el que se encuentra el directorio src
    from src import utils # Estas son las funciones definidas por mi

    # Configuracion de dask
    import dask.dataframe as dd # Utilizado para el procesamiento de los datos
    import dask

    t_inicio = time.time()
    # ----------------------------------------------------------------------------------------------------


    # LECTURA DE DATOS
    # ----------------------------------------------------------------------------------------------------
    t_inicio = time.time() # Inicia tiempo de ejecucion
    t_ant = t_inicio

    # df = dd.read_sql_table(config["input_table"], uri=uri, index_col=config["partition_column"])\
    #     .dropna(subset=['FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME'])
    # df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'] * 60

    df = dd.read_parquet('samples/data_10K'
            , engine='pyarrow'
            , gather_statistics=False
            # , columns=['ACTUAL_ELAPSED_TIME', 'ORIGIN', 'DEST']
            )# , infer_divisions=False, engine='pyarrow')\

    # lista_parquets = [x for x in os.listdir(path='data_dask') if 'parquet' in x]

    # def read_parquet_file_from_s3(filename):
    #     with open('data_dask/' + filename, mode='rb') as f:
    #         return pd.read_parquet(f, engine='pyarrow') # also tried fastparquet, pyarrow was faster

    # df = dd.from_delayed(map(dask.delayed(read_parquet_file_from_s3), lista_parquets))

    # print(df.count().compute())

    df.repartition(1).to_parquet('samples/data_dask_10K', engine='pyarrow')

    print(time.time()-t_inicio)