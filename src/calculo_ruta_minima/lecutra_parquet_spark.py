from pyspark import SparkContext
sc = SparkContext()
import datetime
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion
import datetime

# ----------------------------------------------------------------------------
df = spark.read.format('parquet').load('data')

df.repartition(10).write.format('parquet').save('data_dask_10')