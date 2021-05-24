# -*- coding: utf-8 -*-
import time # Utilizado para medir el timpo de ejecucion
t_inicio = time.time() # Inicia tiempo de ejecucion
# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de PySpark
from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
spark.sparkContext.setLogLevel("ERROR")
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion

parser = argparse.ArgumentParser()
parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
parser.add_argument("--env", help="Puede ser local o cluster. Esto determina los recursos utilizados y los archivos de configuración que se utilizarán.")
parser.add_argument("--command_time", help="Hora a la que fue enviada la ejecución del proceso.")
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
if args.env != 'cluster':
    config = lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
else:
    config = lee_config_csv(path="conf/base/configs_cluster.csv", sample_size=args.sample_size, process=args.process)
with open(args.creds) as json_file:
    creds = json.load(json_file)

command_time = float(args.command_time)
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCIONES
# ----------------------------------------------------------------------------------------------------
def read_df_from_parquet(path, columns=None):
    """Esta función lee los datos desde el path proprocionado y lee las columnas especificadas. Si no se especifica lista de columnas entonces se leen todas."""
    if columns != None:
        df_rita = spark.read\
            .format('parquet')\
            .load(path)\
            .select(*columns)

    else:
        df_rita = spark.read\
            .format('parquet')\
            .load(path)

    return df_rita


def elimina_nulos(path):
    """Esta funcion elimina los valores nulos de todas las columnas."""
    df = read_df_from_parquet(path=path, columns=['TAIL_NUM', 'OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])

    return df.na.drop("any")

def aeropuerto_demoras_aerolinea(path):
    """Esta funcion calcula, tomando como referencia el cada aerolínea:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df = read_df_from_parquet(path=path, columns=['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])

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

def aeropuerto_demoras_origen(path):
    """Esta funcion calcula, tomando como referencia el aeropuerto de origen:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df = read_df_from_parquet(path=path, columns=['ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
    
    df_resp = df.rollup('ORIGIN', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("FL_DATE"),
            F.max('ARR_DELAY').alias("ARR_DELAY"),
            F.max('DEP_DELAY').alias("DEP_DELAY"),
            F.max('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.max('TAXI_IN').alias("TAXI_IN"),
            F.max('TAXI_OUT').alias("TAXI_OUT")
            )
    return df_resp

def aeropuerto_demoras_destino(path):
    """Esta funcion calcula, tomando como referencia el aeropuerto de destino:
    - El retraso promedio en la salida de los vuelos.
    - El retraso promedio en la llegada de los vuelos.
    - La duración promedio de los vuelos.
    - El tiempo de rodaje del avión desde el aterrizaje hasta la puerta de desembarco.
    - El tiempo de rodaje del avión desde la puerta de rodaje hasta el despegue.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    """
    df = read_df_from_parquet(path=path, columns=['DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
    
    df_resp = df.rollup('DEST', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("FL_DATE"),
            F.min('ARR_DELAY').alias("ARR_DELAY"),
            F.min('DEP_DELAY').alias("DEP_DELAY"),
            F.min('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.min('TAXI_IN').alias("TAXI_IN"),
            F.min('TAXI_OUT').alias("TAXI_OUT")
            )
    return df_resp

def principales_rutas_aeropuerto_fecha(path):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    \nLa entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df = read_df_from_parquet(path=path, columns=['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN', 'DEST'])
    
    df_resp = df\
        .withColumn('ROUTE_AIRPORTS', F.concat('ORIGIN', F.lit('-'), 'DEST'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_AIRPORTS', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("FL_DATE"), 
            F.avg('ARR_DELAY').alias("ARR_DELAY"), 
            F.avg('DEP_DELAY').alias("DEP_DELAY"), 
            F.avg('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.avg('TAXI_IN').alias("TAXI_IN"),
            F.avg('TAXI_OUT').alias("TAXI_OUT")
            )\
        # .withColumn('ORIGIN', F.expr('ROUTE_AIRPORTS[0]'))\
        # .withColumn('DEST', F.expr('ROUTE_AIRPORTS[1]'))\
        # .drop('ROUTE_AIRPORTS')
    return df_resp


def principales_rutas_mktid_fecha(path):
    """Esta funcion calcula el retraso promedio en la salida, llegada y la duracion promedio para cada ruta.
    Los resultados se presentan para cada dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR).
    La entrada es un dataframe que contiene los datos de lugar, fecha, duracion y retraso de cada vuelo."""
    # Obtencion de ruta por dia
    df = read_df_from_parquet(path=path, columns=['YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT', 'ORIGIN_CITY_MARKET_ID', 'DEST_CITY_MARKET_ID'])
    
    df_resp = df\
        .withColumn('ROUTE_MKT_ID', F.concat('ORIGIN_CITY_MARKET_ID', F.lit('-'), 'DEST_CITY_MARKET_ID'))
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df_resp.rollup('ROUTE_MKT_ID', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(
            F.count("FL_DATE").alias("FL_DATE"), 
            F.stddev('ARR_DELAY').alias("ARR_DELAY"), 
            F.stddev('DEP_DELAY').alias("DEP_DELAY"), 
            F.stddev('ACTUAL_ELAPSED_TIME').alias("ACTUAL_ELAPSED_TIME"),
            F.stddev('TAXI_IN').alias("TAXI_IN"),
            F.stddev('TAXI_OUT').alias("TAXI_OUT")
            )\
        .fillna(0, subset=['FL_DATE', 'ARR_DELAY', 'DEP_DELAY', 'ACTUAL_ELAPSED_TIME', 'TAXI_IN', 'TAXI_OUT'])
        # .withColumn('ORIGIN_MKT_ID', F.expr('ROUTE_MKT_ID[0]'))\
        # .withColumn('DEST_MKT_ID', F.expr('ROUTE_MKT_ID[1]'))\
        # .drop('ROUTE_MKT_ID')
    return df_resp


def tamano_flota_aerolinea(path):
    df = read_df_from_parquet(path=path, columns=['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH', 'TAIL_NUM'])
    
    # Calculo de indicadores por dia (DAY), cada mes (MONTH), cada trimestre (QUARTER) y cada ano (YEAR)
    df_resp = df.rollup('OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH')\
        .agg(F.expr('COUNT(DISTINCT TAIL_NUM)').alias('TAIL_NUM'))
    return df_resp


def write_result_to_mysql(df_resp, creds, config, process):
    df_resp.write.format("jdbc")\
        .options(
            url=creds["db_url"] + creds["database"],
            driver=creds["db_driver"],
            dbtable=process,
            user=creds["user"],
            password=creds["password"],
            rewriteBatchedStatements=True,
            numPartitions=config["db_numPartitions"])\
        .mode(config["results_table_mode"])\
        .save()


def write_result_to_parquet(df_resp, process, env=args.env):
    """Esta función escribe el resultado a la dirección especificada en formato parquet."""
    date = time.strftime("%Y_%m_%d_%H_%M_%S", time.gmtime(time.time()))
    if env != 'cluster':
        full_path = 'resultados/' + process + '/' + date
    else:
        full_path = 'hdfs://mycluster/resultados/' + process + '/' + date
    df_resp.coalesce(1).write.format('parquet').save(full_path)
# ----------------------------------------------------------------------------------------------------


# EJECUCION
# ----------------------------------------------------------------------------------------------------
process = config["results_table"]
print('\tLos resultados se escribirán en la tabla: ' + process)

if process == 'demoras_aerolinea_spark':
    df_resp = aeropuerto_demoras_aerolinea(config['input_path']) # Calculo de demoras en cada ruta
    write_result_to_parquet(df_resp, process)

elif process == 'demoras_aeropuerto_origen_spark':
    df_resp = aeropuerto_demoras_origen(config['input_path']) # Calculo de demoras en cada ruta
    write_result_to_mysql(df_resp, creds, config, process)

elif process == 'demoras_aeropuerto_destino_spark':
    df_resp = aeropuerto_demoras_destino(config['input_path']) # Calculo de demoras en cada ruta basados en destino
    write_result_to_parquet(df_resp, process)

elif process == 'demoras_ruta_aeropuerto_spark':
    df_resp = principales_rutas_aeropuerto_fecha(config['input_path']) # Calculo de demoras en cada ruta
    write_result_to_parquet(df_resp, process)

elif process == 'demoras_ruta_mktid_spark':
    df_resp = principales_rutas_mktid_fecha(config['input_path']) # Calculo de demoras en cada ruta
    write_result_to_mysql(df_resp, creds, config, process)

elif process == 'flota_spark':
    df_resp = tamano_flota_aerolinea(config['input_path']) # Calculo del tamano de la flota
    write_result_to_mysql(df_resp, creds, config, process)

elif process == 'elimina_nulos_spark':
    df = elimina_nulos(config['input_path'])
    print('Conteo sin nulos: ' + str(df.count()))

else:
    print('\n\n\tEl nombre del proceso: ' + process + ' no es válido.\n\n')

t_final = time.time() # Tiempo de finalizacion de la ejecucion
# ----------------------------------------------------------------------------------------------------


# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------
rdd_time = sc.parallelize([[process + '_command_time', command_time, t_inicio, t_inicio - command_time, config["description"], config["resources"], args.sample_size],
                        [process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], args.sample_size]])
df_time = rdd_time.toDF(['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size'])\
    .withColumn("insertion_ts", F.current_timestamp())
df_time.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable=config['time_table'],
        user=creds["user"],
        password=creds["password"])\
    .mode(config["time_table_mode"])\
    .save()

print('\tTiempo ejecución: {t}'.format(t = t_final - t_inicio))
print('\tFIN DE LA EJECUCIÓN')
# ----------------------------------------------------------------------------------------------------
