from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
spark.sparkContext.setCheckpointDir('temp_dir')
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import subprocess
import argparse
import math

parser = argparse.ArgumentParser()
parser.add_argument("--file", help="Los archivos parquet que se van a procesar.")
args = parser.parse_args()

input_file = args.file

def define_particiones(size, chunksize_mb):
    return math.ceil(int(size.replace('MB', '')) / chunksize_mb)

size = subprocess.check_output(['du','--max-depth=0', '--block-size=MB', input_file]).split()[0].decode('utf-8') # Obtencion del tamaño del directorio
n_partitions = define_particiones(size, chunksize_mb=150) # Obtencion del numero de particiones considerando particiones de alrededor de 150 MB

print("Archivo: " + input_file)
print("Tamaño del archivo: " + size)
print("Número de particiones: " + str(n_partitions))

df = spark.read.format('parquet').load(input_file).repartition(n_partitions)

new_dtypes = {'ID': 'int',
 'QUARTER': 'int',
 'DAY_OF_MONTH': 'int',
 'DAY_OF_WEEK': 'int',
 # 'FL_DATE': 'string',
 # 'OP_UNIQUE_CARRIER': 'string',
 'OP_CARRIER_AIRLINE_ID': 'int',
 # 'OP_CARRIER': 'string',
 # 'TAIL_NUM': 'string',
 'OP_CARRIER_FL_NUM': 'int',
 'ORIGIN_AIRPORT_ID': 'int',
 'ORIGIN_AIRPORT_SEQ_ID': 'int',
 'ORIGIN_CITY_MARKET_ID': 'int',
 # 'ORIGIN': 'string',
 # 'ORIGIN_CITY_NAME': 'string',
 # 'ORIGIN_STATE_ABR': 'string',
 'ORIGIN_STATE_FIPS': 'float',
 # 'ORIGIN_STATE_NM': 'string',
 'ORIGIN_WAC': 'int',
 'DEST_AIRPORT_ID': 'int',
 'DEST_AIRPORT_SEQ_ID': 'int',
 'DEST_CITY_MARKET_ID': 'int',
 # 'DEST': 'string',
 # 'DEST_CITY_NAME': 'string',
 # 'DEST_STATE_ABR': 'string',
 'DEST_STATE_FIPS': 'float',
 # 'DEST_STATE_NM': 'string',
 'DEST_WAC': 'int',
 # 'CRS_DEP_TIME': 'string',
 # 'DEP_TIME': 'string',
 'DEP_DELAY': 'float',
 'DEP_DELAY_NEW': 'float',
 'DEP_DEL15': 'float',
 'DEP_DELAY_GROUP': 'float',
 # 'DEP_TIME_BLK': 'string',
 'TAXI_OUT': 'float',
 # 'WHEELS_OFF': 'string',
 # 'WHEELS_ON': 'string',
 'TAXI_IN': 'float',
 # 'CRS_ARR_TIME': 'string',
 # 'ARR_TIME': 'string',
 'ARR_DELAY': 'float',
 'ARR_DELAY_NEW': 'float',
 'ARR_DEL15': 'float',
 'ARR_DELAY_GROUP': 'float',
 # 'ARR_TIME_BLK': 'string',
 'CANCELLED': 'float',
 # 'CANCELLATION_CODE': 'string',
 'DIVERTED': 'float',
 'CRS_ELAPSED_TIME': 'float',
 'ACTUAL_ELAPSED_TIME': 'float',
 'AIR_TIME': 'float',
 'FLIGHTS': 'float',
 'DISTANCE': 'float',
 'DISTANCE_GROUP': 'float',
 'CARRIER_DELAY': 'float',
 'WEATHER_DELAY': 'float',
 'NAS_DELAY': 'float',
 'SECURITY_DELAY': 'float',
 'LATE_AIRCRAFT_DELAY': 'float',
 # 'FIRST_DEP_TIME': 'string',
 'TOTAL_ADD_GTIME': 'float',
 'LONGEST_ADD_GTIME': 'float',
 'DIV_AIRPORT_LANDINGS': 'float',
 'DIV_REACHED_DEST': 'float',
 'DIV_ACTUAL_ELAPSED_TIME': 'float',
 'DIV_ARR_DELAY': 'float',
 'DIV_DISTANCE': 'float',
 # 'DIV1_AIRPORT': 'string',
 'DIV1_AIRPORT_ID': 'int',
 'DIV1_AIRPORT_SEQ_ID': 'int',
 # 'DIV1_WHEELS_ON': 'string',
 'DIV1_TOTAL_GTIME': 'float',
 'DIV1_LONGEST_GTIME': 'float',
 # 'DIV1_WHEELS_OFF': 'string',
 # 'DIV1_TAIL_NUM': 'string',
 # 'DIV2_AIRPORT': 'string',
 'DIV2_AIRPORT_ID': 'int',
 'DIV2_AIRPORT_SEQ_ID': 'int',
 # 'DIV2_WHEELS_ON': 'string',
 'DIV2_TOTAL_GTIME': 'float',
 'DIV2_LONGEST_GTIME': 'float',
 # 'DIV2_WHEELS_OFF': 'string',
 # 'DIV2_TAIL_NUM': 'string',
 # 'DIV3_AIRPORT': 'string',
 'DIV3_AIRPORT_ID': 'int',
 'DIV3_AIRPORT_SEQ_ID': 'int',
 # 'DIV3_WHEELS_ON': 'string',
 'DIV3_TOTAL_GTIME': 'float',
 'DIV3_LONGEST_GTIME': 'float',
 # 'DIV3_WHEELS_OFF': 'string',
 # 'DIV3_TAIL_NUM': 'string',
 # 'DIV4_AIRPORT': 'string',
 'DIV4_AIRPORT_ID': 'int',
 'DIV4_AIRPORT_SEQ_ID': 'int',
 # 'DIV4_WHEELS_ON': 'string',
 'DIV4_TOTAL_GTIME': 'float',
 'DIV4_LONGEST_GTIME': 'float',
 # 'DIV4_WHEELS_OFF': 'string',
 # 'DIV4_TAIL_NUM': 'string',
 # 'DIV5_AIRPORT': 'string',
 'DIV5_AIRPORT_ID': 'int',
 'DIV5_AIRPORT_SEQ_ID': 'int',
 # 'DIV5_WHEELS_ON': 'string',
 'DIV5_TOTAL_GTIME': 'float',
 'DIV5_LONGEST_GTIME': 'float',
 # 'DIV5_WHEELS_OFF': 'string',
 # 'DIV5_TAIL_NUM': 'string',
 'YEAR': 'int',
 'MONTH': 'int'}


replace_na = {'ID': -1,
 'QUARTER': -1,
 'DAY_OF_MONTH': -1,
 'DAY_OF_WEEK': -1,
 'OP_CARRIER_AIRLINE_ID': -1,
 'OP_CARRIER_FL_NUM': -1,
 'ORIGIN_AIRPORT_ID': -1,
 'ORIGIN_AIRPORT_SEQ_ID': -1,
 'ORIGIN_CITY_MARKET_ID': -1,
 'ORIGIN_STATE_FIPS': -1,
 'ORIGIN_WAC': -1,
 'DEST_AIRPORT_ID': -1,
 'DEST_AIRPORT_SEQ_ID': -1,
 'DEST_CITY_MARKET_ID': -1,
 'DEST_STATE_FIPS': -1,
 'DEST_WAC': -1,
 'DEP_DELAY': 0,
 'DEP_DELAY_NEW': 0,
 'DEP_DEL15': 0,
 'DEP_DELAY_GROUP': -1,
 'TAXI_OUT': 0,
 'TAXI_IN': 0,
 'ARR_DELAY': 0,
 'ARR_DELAY_NEW': 0,
 'ARR_DEL15': 0,
 'ARR_DELAY_GROUP': -1,
 'CANCELLED': -1,
 'DIVERTED': -1,
 'CRS_ELAPSED_TIME': 0,
 'ACTUAL_ELAPSED_TIME': 0,
 'AIR_TIME': 0,
 'FLIGHTS': 0,
 'DISTANCE': 0,
 'DISTANCE_GROUP': 0,
 'CARRIER_DELAY': 0,
 'WEATHER_DELAY': 0,
 'NAS_DELAY': 0,
 'SECURITY_DELAY': 0,
 'LATE_AIRCRAFT_DELAY': 0,
 'TOTAL_ADD_GTIME': 0,
 'LONGEST_ADD_GTIME': 0,
 'DIV_AIRPORT_LANDINGS': 0,
 'DIV_REACHED_DEST': 0,
 'DIV_ACTUAL_ELAPSED_TIME': 0,
 'DIV_ARR_DELAY': 0,
 'DIV_DISTANCE': 0,
 'DIV1_AIRPORT_ID': -1,
 'DIV1_AIRPORT_SEQ_ID': -1,
 'DIV1_TOTAL_GTIME': 0,
 'DIV1_LONGEST_GTIME': 0,
 'DIV2_AIRPORT_ID': -1,
 'DIV2_AIRPORT_SEQ_ID': -1,
 'DIV2_TOTAL_GTIME': 0,
 'DIV2_LONGEST_GTIME': 0,
 'DIV3_AIRPORT_ID': -1,
 'DIV3_AIRPORT_SEQ_ID': -1,
 'DIV3_TOTAL_GTIME': 0,
 'DIV3_LONGEST_GTIME': 0,
 'DIV4_AIRPORT_ID': -1,
 'DIV4_AIRPORT_SEQ_ID': -1,
 'DIV4_TOTAL_GTIME': 0,
 'DIV4_LONGEST_GTIME': 0,
 'DIV5_AIRPORT_ID': -1,
 'DIV5_AIRPORT_SEQ_ID': -1,
 'DIV5_TOTAL_GTIME': 0,
 'DIV5_LONGEST_GTIME': 0}


df = df.fillna(replace_na)

for column in list(new_dtypes.keys()):
    df.withColumn(column, F.col(column).astype(new_dtypes[column]))

output_file = input_file + '_spark_casted'

df.write.format('parquet').save(output_file)

print("Archivo almacenado en: " + output_file)