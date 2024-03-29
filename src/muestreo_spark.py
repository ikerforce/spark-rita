# Obtencion de muestras en spark

from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql import functions as F
spark = SparkSession(sc)
sqlContext = SQLContext(sc)

sample_size = 10000000
tag = '10M'

df_total = spark.read.format('parquet').load('data')

if sample_size > 1:

	total_registros = df_total.count()

	if sample_size / total_registros * 1.01 < 1:

		df_muestra = df_total.sample(False, float(sample_size) / (total_registros) * 1.1, 22102001)\
			.orderBy(F.rand(seed=22102001))\
			.limit(sample_size)

else:

	df_muestra = df_total.sample(False, float(sample_size), 22102001)

# print(df_muestra.count())

df_muestra.repartition('YEAR', 'MONTH').write.mode('overwrite').format('parquet').save('samples/data_' + tag)