# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
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
user = creds["user"] # Usuario de mysql
password = creds["password"] # Password de mysql
database = creds["database"] # Base de datos en la que almaceno resultados y tiempo de ejecucion
time_table = config["time_table"] # Tabla en la que almaceno el tiempo de ejecucion de la tarea
results_table = config["results_table"] # Tabla en la que almaceno el resultado
input_table = config["input_table"] # Tabla con los datos necesarios para la ejecucion del algoritmo
time_table_mode = config["time_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
results_table_mode = config["results_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
exec_desc = config["description"] # Descripcion breve de la ejecucion que estamos haciendo
resources = config["resources"] # Descripcion breve de los recursos utilizados para la ejecucion
partition_col = config["partition_column"] # Columna a traves de la cual vamos a particionar

uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(user, password, "3306", database)
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

df = dd.read_sql_table(input_table, uri=uri, index_col=partition_col)
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
df['ROUTE_AIRPORTS'] = df.apply(lambda row: row['ORIGIN'] + '-' + row['DEST'], axis=1, meta='str') # Agregamos una columna que sea el concatenado de el origen y destino para así tener una ruta
print(df['ROUTE_AIRPORTS'].compute().head())
# Cálculo de demoras en el origen
agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}

lista_df = utils.rollup(df, ['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)

lista_df[0].to_sql(results_table_origen, uri, if_exists='replace', index=False) # En la primera escritura borro los resultados anteriores
for resultado in lista_df[1:]:
    resultado.to_sql(results_table_origen, uri, if_exists='append', index=False) # Despupes solo hago append

# Cálculo de demoras en el destino
agregaciones = {'FL_DATE':'count', 'ARR_DELAY':'mean', 'DEP_DELAY':'mean', 'ACTUAL_ELAPSED_TIME':'mean', 'TAXI_IN':'mean', 'TAXI_OUT':'mean'}

lista_df = utils.rollup(df, ['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)

lista_df[0].to_sql(results_table_destino, uri, if_exists='replace', index=False) # En la primera escritura borro los resultados anteriores
for resultado in lista_df[1:]:
    resultado.to_sql(results_table_destino, uri, if_exists='append', index=False) # Despupes solo hago append

t_final = time.time() # Tiempo de finalizacion de la ejecucion
# ----------------------------------------------------------------------------------------------------


# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------
info_tiempo = [[t_inicio, t_final, t_final - t_inicio, exec_desc, resources, time.strftime('%Y-%m-%d %H:%M:%S')]]
df_tiempo = pd.DataFrame(data=info_tiempo, columns=['start_ts', 'end_ts', 'duration', 'description', 'resources', 'insertion_ts'])
df_tiempo.to_sql(time_table, uri, if_exists=time_table_mode, index=False)
# ----------------------------------------------------------------------------------------------------





# DEFINICION DE FUNCIONES
# ----------------------------------------------------------------------------------------------------
def principales_rutas_fecha(df):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    \nLa entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df_resp = df.groupBy('ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID','ORIGIN', 'DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE')\
        .agg(F.count("FL_DATE").alias("N_FLIGHTS"), F.avg('ARR_DELAY').alias("ARR_DELAY"), F.avg('DEP_DELAY').alias("DEP_DELAY"), F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"))\
        .withColumn('ROUTE_AIRPORTS', F.array('ORIGIN', 'DEST'))\
        .withColumn('ROUTE_MKT_ID', F.array('ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_MKT_ID', 'ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(F.grouping_id().alias('GROUPING_ID'), F.count("FL_DATE").alias("N_FLIGHTS"), F.avg('ARR_DELAY').alias("AVG_ARR_DELAY"), F.avg('DEP_DELAY').alias("AVG_DEP_DELAY"), F.avg('ACTUAL_ELAPSED_TIME').alias("AVG_ACTUAL_ELAPSED_TIME"))\
        .orderBy(F.desc('GROUPING_ID'))\
        .withColumn('ORIGIN', F.expr('ROUTE_AIRPORTS[0]'))\
        .withColumn('DEST', F.expr('ROUTE_AIRPORTS[1]'))\
        .withColumn('ORIGIN_MKT_ID', F.expr('ROUTE_MKT_ID[0]'))\
        .withColumn('DEST_MKT_ID', F.expr('ROUTE_MKT_ID[1]'))\
        .drop('ROUTE_AIRPORTS')\
        .drop('ROUTE_MKT_ID')
    return df_resp
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
df_resp = principales_rutas_fecha(df_rita) # Calculo de demoras en cada ruta
df_resp.write.format("jdbc")\
    .options(
        url=db_url + database,
        driver=db_driver,
        dbtable=results_table,
        user=user,
        password=password,
        numPartitions=db_numPartitions)\
    .mode(results_table_mode)\
    .save()
t_final = time.time() # Tiempo de finalizacion de la ejecucion
# ----------------------------------------------------------------------------------------------------


# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------
rdd_time = sc.parallelize([[t_inicio, t_final, t_final - t_inicio, exec_desc, resources]]) # Almacenamos infomracion de ejecucion en rdd
df_time = rdd_time.toDF(['start_ts', 'end_ts', 'duration', 'description', 'resources'])\
    .withColumn("insertion_ts", F.current_timestamp())
df_time.write.format("jdbc")\
    .options(
        url=db_url + database,
        driver=db_driver,
        dbtable=time_table,
        user=user,
        password=password)\
    .mode(time_table_mode)\
    .save()
# ----------------------------------------------------------------------------------------------------