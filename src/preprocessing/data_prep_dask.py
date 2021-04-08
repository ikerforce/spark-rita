import dask.dataframe as dd
import numpy as np
import pandas as pd
import json
from dask.diagnostics import ProgressBar
import math
import subprocess
import argparse # Utilizado para leer archivo de configuracion

parser = argparse.ArgumentParser()
parser.add_argument("--file", help="Los archivos parquet que se van a procesar.")
args = parser.parse_args()

input_file = args.file

def define_particiones(size, chunksize_mb):
    return math.ceil(int(size.replace('MB', '')) / chunksize_mb)

size = subprocess.check_output(['du','--max-depth=0', '--block-size=MB', input_file]).split()[0].decode('utf-8') # Obtencion del tamaño del directorio
n_partitions = define_particiones(size, chunksize_mb=100) # Obtencion del numero de particiones considerando particiones de al rededor de 100MB

print("Archivo: " + input_file)
print("Tamaño del archivo: " + size)
print("Número de particiones: " + str(n_partitions))

df = dd.read_parquet(input_file, engine='pyarrow', chunksize='100MB').repartition(n_partitions)

new_dtypes = {'ID': np.uint64,
 'QUARTER': np.uint32,
 'DAY_OF_MONTH': np.uint32,
 'DAY_OF_WEEK': np.uint32,
 # 'FL_DATE': str,
 # 'OP_UNIQUE_CARRIER': str,
 'OP_CARRIER_AIRLINE_ID': np.uint32,
 # 'OP_CARRIER': str,
 # 'TAIL_NUM': str,
 'OP_CARRIER_FL_NUM': np.uint32,
 'ORIGIN_AIRPORT_ID': np.uint32,
 'ORIGIN_AIRPORT_SEQ_ID': np.uint32,
 'ORIGIN_CITY_MARKET_ID': np.uint32,
 # 'ORIGIN': str,
 # 'ORIGIN_CITY_NAME': str,
 # 'ORIGIN_STATE_ABR': str,
 'ORIGIN_STATE_FIPS': np.float32,
 # 'ORIGIN_STATE_NM': str,
 'ORIGIN_WAC': np.uint32,
 'DEST_AIRPORT_ID': np.uint32,
 'DEST_AIRPORT_SEQ_ID': np.uint32,
 'DEST_CITY_MARKET_ID': np.uint32,
 # 'DEST': str,
 # 'DEST_CITY_NAME': str,
 # 'DEST_STATE_ABR': str,
 'DEST_STATE_FIPS': np.float32,
 # 'DEST_STATE_NM': str,
 'DEST_WAC': np.uint32,
 # 'CRS_DEP_TIME': str,
 # 'DEP_TIME': str,
 'DEP_DELAY': np.float32,
 'DEP_DELAY_NEW': np.float32,
 'DEP_DEL15': np.float32,
 'DEP_DELAY_GROUP': np.float32,
 # 'DEP_TIME_BLK': str,
 'TAXI_OUT': np.float32,
 # 'WHEELS_OFF': str,
 # 'WHEELS_ON': str,
 'TAXI_IN': np.float32,
 # 'CRS_ARR_TIME': str,
 # 'ARR_TIME': str,
 'ARR_DELAY': np.float32,
 'ARR_DELAY_NEW': np.float32,
 'ARR_DEL15': np.float32,
 'ARR_DELAY_GROUP': np.float32,
 # 'ARR_TIME_BLK': str,
 'CANCELLED': np.float32,
 # 'CANCELLATION_CODE': str,
 'DIVERTED': np.float32,
 'CRS_ELAPSED_TIME': np.float32,
 'ACTUAL_ELAPSED_TIME': np.float32,
 'AIR_TIME': np.float32,
 'FLIGHTS': np.float32,
 'DISTANCE': np.float32,
 'DISTANCE_GROUP': np.float32,
 'CARRIER_DELAY': np.float32,
 'WEATHER_DELAY': np.float32,
 'NAS_DELAY': np.float32,
 'SECURITY_DELAY': np.float32,
 'LATE_AIRCRAFT_DELAY': np.float32,
 # 'FIRST_DEP_TIME': str,
 'TOTAL_ADD_GTIME': np.float32,
 'LONGEST_ADD_GTIME': np.float32,
 'DIV_AIRPORT_LANDINGS': np.float32,
 'DIV_REACHED_DEST': np.float32,
 'DIV_ACTUAL_ELAPSED_TIME': np.float32,
 'DIV_ARR_DELAY': np.float32,
 'DIV_DISTANCE': np.float32,
 # 'DIV1_AIRPORT': str,
 'DIV1_AIRPORT_ID': np.uint32,
 'DIV1_AIRPORT_SEQ_ID': np.uint32,
 # 'DIV1_WHEELS_ON': str,
 'DIV1_TOTAL_GTIME': np.float32,
 'DIV1_LONGEST_GTIME': np.float32,
 # 'DIV1_WHEELS_OFF': str,
 # 'DIV1_TAIL_NUM': str,
 # 'DIV2_AIRPORT': str,
 'DIV2_AIRPORT_ID': np.uint32,
 'DIV2_AIRPORT_SEQ_ID': np.uint32,
 # 'DIV2_WHEELS_ON': str,
 'DIV2_TOTAL_GTIME': np.float32,
 'DIV2_LONGEST_GTIME': np.float32,
 # 'DIV2_WHEELS_OFF': str,
 # 'DIV2_TAIL_NUM': str,
 # 'DIV3_AIRPORT': str,
 'DIV3_AIRPORT_ID': np.uint32,
 'DIV3_AIRPORT_SEQ_ID': np.uint32,
 # 'DIV3_WHEELS_ON': str,
 'DIV3_TOTAL_GTIME': np.float32,
 'DIV3_LONGEST_GTIME': np.float32,
 # 'DIV3_WHEELS_OFF': str,
 # 'DIV3_TAIL_NUM': str,
 # 'DIV4_AIRPORT': str,
 'DIV4_AIRPORT_ID': np.uint32,
 'DIV4_AIRPORT_SEQ_ID': np.uint32,
 # 'DIV4_WHEELS_ON': str,
 'DIV4_TOTAL_GTIME': np.float32,
 'DIV4_LONGEST_GTIME': np.float32,
 # 'DIV4_WHEELS_OFF': str,
 # 'DIV4_TAIL_NUM': str,
 # 'DIV5_AIRPORT': str,
 'DIV5_AIRPORT_ID': np.uint32,
 'DIV5_AIRPORT_SEQ_ID': np.uint32,
 # 'DIV5_WHEELS_ON': str,
 'DIV5_TOTAL_GTIME': np.float32,
 'DIV5_LONGEST_GTIME': np.float32,
 # 'DIV5_WHEELS_OFF': str,
 # 'DIV5_TAIL_NUM': str,
 'YEAR': np.uint32,
 'MONTH': np.uint32}


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

for column in list(new_dtypes.keys()):
    if column in list(replace_na.keys()):
        df[column] = df[column].fillna(replace_na[column]).astype(new_dtypes[column])
    else:
        df[column] = df[column].astype(new_dtypes[column])

output_file = input_file + '_dask_casted'

from dask.diagnostics import ProgressBar
with ProgressBar():
    df.to_parquet(output_file, engine='pyarrow')

print("Archivo almacenado en: " + output_file)