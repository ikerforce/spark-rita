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

parser = argparse.ArgumentParser()
parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")

def lee_config_csv(path, sample_size, process):
    """Esta función lee un archivo de configuración y obtiene la información para un proceso y tamaño de muestra específico."""
    file = open(path, "r").read().splitlines()
    nombres = file[0]
    info = filter(lambda row: row.split("|")[0] == sample_size and row.split("|")[1] == process, file[1:])
    parametros = dict(zip(nombres.split('|'), list(info)[0].split('|')))
    return parametros

args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
config = lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
with open(args.creds) as json_file:
    creds = json.load(json_file)
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

# Lectura de datos de MySQL
df_rita = spark.read\
    .format('parquet')\
    .load(config['input_path'])\
    .select(*['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])

df_rita.cache()

t_intermedio = time.time()
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
            F.count("FL_DATE").alias("N_FLIGHTS"),
            F.max('ARR_DELAY').alias("ARR_DELAY"),
            F.max('DEP_DELAY').alias("DEP_DELAY"),
            F.max('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.max('TAXI_IN').alias("TAXI_IN"),
            F.max('TAXI_OUT').alias("TAXI_OUT")
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
            F.count("FL_DATE").alias("N_FLIGHTS"),
            F.min('ARR_DELAY').alias("ARR_DELAY"),
            F.min('DEP_DELAY').alias("DEP_DELAY"),
            F.min('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.min('TAXI_IN').alias("TAXI_IN"),
            F.min('TAXI_OUT').alias("TAXI_OUT")
            )
    return df_resp

def principales_rutas_aeropuerto_fecha(df):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    \nLa entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df_resp = df\
        .withColumn('ROUTE_AIRPORTS', F.array('ORIGIN', 'DEST'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"), 
            F.avg('ARR_DELAY').alias("ARR_DELAY"), 
            F.avg('DEP_DELAY').alias("DEP_DELAY"), 
            F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.avg('TAXI_IN').alias("TAXI_IN"),
            F.avg('TAXI_OUT').alias("TAXI_OUT")
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
    df_resp = df\
        .withColumn('ROUTE_MKT_ID', F.array('ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("N_FLIGHTS"), 
            F.stddev_pop('ARR_DELAY').alias("ARR_DELAY"), 
            F.stddev_pop('DEP_DELAY').alias("DEP_DELAY"), 
            F.stddev_pop('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.stddev_pop('TAXI_IN').alias("TAXI_IN"),
            F.stddev_pop('TAXI_OUT').alias("TAXI_OUT")
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
rdd_time_1 = sc.parallelize([[process + '_p1', t_inicio, t_intermedio, t_intermedio - t_inicio, config["description"], config["resources"], args.sample_size]])
rdd_time_2 = sc.parallelize([[process + '_p2', t_intermedio, t_final, t_final - t_intermedio, config["description"], config["resources"], args.sample_size]]) # Almacenamos informacion de ejecucion en rdd
df_time_1 = rdd_time_1.toDF(['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size'])\
    .withColumn("insertion_ts", F.current_timestamp())
df_time_2 = rdd_time_2.toDF(['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size'])\
    .withColumn("insertion_ts", F.current_timestamp())
df_time_1.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="registro_de_tiempo_spark",
        user=creds["user"],
        password=creds["password"])\
    .mode(config["time_table_mode"])\
    .save()

df_time_2.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="registro_de_tiempo_spark",
        user=creds["user"],
        password=creds["password"])\
    .mode(config["time_table_mode"])\
    .save()

print('\n\n\tTiempo ejecución: {t}\n\n'.format(t = t_final - t_inicio))
print('\n\n\tFIN DE LA EJECUCIÓN\n\n')
# ----------------------------------------------------------------------------------------------------