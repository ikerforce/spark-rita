# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de PySpark
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion

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
results_table = config["results_table"] # Tabla en la que almaceno el resultado de los vuelos de origen
input_table = config["input_table"] # Tabla con los datos necesarios para la ejecucion del algoritmo
time_table_mode = config["time_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
results_table_mode = config["results_table_mode"] # Forma en la que escribo el resultado en la tabla (append o overwrite)
db_url = config["db_url"] # URL de la base de datos
db_driver = config["db_driver"] # Driver de la base de datos
db_numPartitions = config["db_numPartitions"] # Numero de particiones en las que escribimos la base de datos
exec_desc = config["description"] # Descripcion breve de la ejecucion que estamos haciendo
resources = config["resources"] # Descripcion breve de los recursos utilizados para la ejecucion
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

# Lectura de datos de MySQL
df_rita = spark.read.format("jdbc")\
    .options(
        url=db_url + database,
        driver=db_driver,
        dbtable="(SELECT * FROM " + input_table + " LIMIT 10000) df_rita",
        user=user,
        password=password)\
    .load()
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCIONES
# ----------------------------------------------------------------------------------------------------
def aeropuerto_demoras_aerolinea(df):
    """Esta funcion calcula, tomando como referencia el cada aerolínea:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df_resp = df.rollup('OP_CARRIER_AIRLINE_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"),
            F.avg('ARR_DELAY').alias("AVG_ARR_DELAY"),
            F.avg('DEP_DELAY').alias("AVG_DEP_DELAY"),
            F.avg('ACTUAL_ELAPSED_TIME').alias("AVG_ACTUAL_ELAPSED_TIME"),
            F.avg('TAXI_IN').alias("AVG_TAXI_IN"),
            F.avg('TAXI_OUT').alias("AVG_TAXI_OUT")
            )
    return df_resp
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
df_resp_origen = aeropuerto_demoras_aerolinea(df_rita) # Calculo de demoras en cada ruta
df_resp_origen.write.format("jdbc")\
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