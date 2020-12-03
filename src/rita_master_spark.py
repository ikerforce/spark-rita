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
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos")
args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
with open(args.config) as json_file:
    config = json.load(json_file)
with open(args.creds) as json_file:
    creds = json.load(json_file)
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

# Lectura de datos de MySQL
df_rita = spark.read.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="(SELECT * FROM " + config["input_table"] + " LIMIT 10000) df_rita",
        user=creds["user"],
        password=creds["password"])\
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
    df_resp = df.rollup('OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("FL_DATE"),
            F.avg('ARR_DELAY').alias("ARR_DELAY"),
            F.avg('DEP_DELAY').alias("DEP_DELAY"),
            F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.avg('TAXI_IN').alias("TAXI_IN"),
            F.avg('TAXI_OUT').alias("TAXI_OUT")
            )
    return df_resp

def aeropuerto_demoras_origen(df):
    """Esta funcion calcula, tomando como referencia el aeropuerto de origen:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df_resp = df.rollup('ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE"),
            F.avg('ARR_DELAY'),
            F.avg('DEP_DELAY'),
            F.avg('ACTUAL_ELAPSED_TIME'),
            F.avg('TAXI_IN'),
            F.avg('TAXI_OUT')
            )
    return df_resp

def aeropuerto_demoras_destino(df):
    """Esta funcion calcula, tomando como referencia el aeropuerto de destino:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df_resp = df.rollup('DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE"),
            F.avg('ARR_DELAY'),
            F.avg('DEP_DELAY'),
            F.avg('ACTUAL_ELAPSED_TIME'),
            F.avg('TAXI_IN'),
            F.avg('TAXI_OUT')
            )
    return df_resp

def principales_rutas_aeropuerto_fecha(df):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    \nLa entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df_resp = df.groupBy('ORIGIN', 'DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"),
            F.avg('ARR_DELAY').alias("ARR_DELAY"),
            F.avg('DEP_DELAY').alias("DEP_DELAY"),
            F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME")
            )\
        .withColumn('ROUTE_AIRPORTS', F.array('ORIGIN', 'DEST'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"), 
            F.avg('ARR_DELAY').alias("AVG_ARR_DELAY"), 
            F.avg('DEP_DELAY').alias("AVG_DEP_DELAY"), 
            F.avg('ACTUAL_ELAPSED_TIME').alias("AVG_ACTUAL_ELAPSED_TIME")
            )\
        .withColumn('ORIGIN', F.expr('ROUTE_AIRPORTS[0]'))\
        .withColumn('DEST', F.expr('ROUTE_AIRPORTS[1]'))\
        .drop('ROUTE_AIRPORTS')
    return df_resp


def principales_rutas_mktid_fecha(df):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    \nLa entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df_resp = df.groupBy('ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"),
            F.avg('ARR_DELAY').alias("ARR_DELAY"),
            F.avg('DEP_DELAY').alias("DEP_DELAY"),
            F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME")
            )\
        .withColumn('ROUTE_MKT_ID', F.array('ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"), 
            F.avg('ARR_DELAY').alias("AVG_ARR_DELAY"), 
            F.avg('DEP_DELAY').alias("AVG_DEP_DELAY"), 
            F.avg('ACTUAL_ELAPSED_TIME').alias("AVG_ACTUAL_ELAPSED_TIME")
            )\
        .withColumn('ORIGIN_MKT_ID', F.expr('ROUTE_MKT_ID[0]'))\
        .withColumn('DEST_MKT_ID', F.expr('ROUTE_MKT_ID[1]'))\
        .drop('ROUTE_MKT_ID')
    return df_resp


def tamano_flota_aerolinea(df):
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df.rollup('OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(F.expr('COUNT(DISTINCT TAIL_NUM)').alias('TAIL_NUM'))
    return df_resp
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
process = config["results_table"]
print('\n\n\tLos resultados se escribirán en la tabla: ' + process + '\n\n')
if process == 'demoras_aerolinea_spark':
	df_resp = aeropuerto_demoras_aerolinea(df_rita) # Calculo de demoras en cada ruta
elif process == 'demoras_aeropuerto_origen_spark':
	df_resp = aeropuerto_demoras_origen(df_rita) # Calculo de demoras en cada ruta
elif process == 'demoras_aeropuerto_destino_spark':
	df_resp = aeropuerto_demoras_destino(df_rita) # Calculo de demoras en cada ruta basados en destino
elif process == 'demoras_ruta_aeropuerto_spark':
	df_resp = principales_rutas_aeropuerto_fecha(df_rita) # Calculo de demoras en cada ruta
elif process == 'demoras_ruta_mktid_spark':
    df_resp = principales_rutas_mktid_fecha(df_rita) # Calculo de demoras en cada ruta
elif process == 'flota_spark':
	df_resp = tamano_flota_aerolinea(df_rita) # Calculo del tamano de la flota
else:
	print('\n\n\tEl nombre del proceso: ' + process + ' no es válido.\n\n')

df_resp.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable=process,
        user=creds["user"],
        password=creds["password"],
        numPartitions=config["db_numPartitions"])\
    .mode(config["results_table_mode"])\
    .save()
t_final = time.time() # Tiempo de finalizacion de la ejecucion
# ----------------------------------------------------------------------------------------------------


# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------
rdd_time = sc.parallelize([[process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"]]]) # Almacenamos infomracion de ejecucion en rdd
df_time = rdd_time.toDF(['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources'])\
    .withColumn("insertion_ts", F.current_timestamp())
df_time.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="registro_de_tiempo_spark",
        user=creds["user"],
        password=creds["password"])\
    .mode(config["time_table_mode"])\
    .save()

print('\n\n\tFIN DE LA EJECUCIÓN\n\n')
# ----------------------------------------------------------------------------------------------------