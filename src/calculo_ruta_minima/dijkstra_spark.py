# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU

from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql import SparkSession, SQLContext
spark = SparkSession(sc)
sqlContext = SQLContext(sc)
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

schema = StructType([
  StructField('ORIGIN', StringType(), True),
  StructField('DEST', StringType(), True),
  StructField('W', IntegerType(), True),
  StructField('R_min', IntegerType(), True)])

def actualiza_peso(nodo_actual, nodo_destino, peso_acumulado, peso_arista, peso_actual):
	'''Esta funcion actualiza el peso de la arista cuando es necesario.
	nodo_actual- El nodo desde el que se visita.
	nodo_destino - El nodo que se visita.
	peso_acumulado - Peso acumulado hasta el nodo_actual
	peso_arista - Peso correspondiente a la arista (nodo_actual nodo_destino).
	peso_actual - El peso actual del nodo inicial hasta el nodo destino.'''
	if nodo_actual== nodo_destino:
		if int(peso_acumulado) + int(peso_arista) < int(peso_actual):
			return int(peso_acumulado) + int(peso_arista)
		else:
			return int(peso_actual)
	else:
		return int(peso_actual)

udf_actualiza_peso = F.udf(actualiza_peso)

# Creacion de red
# Cada elemento tiene la forma: [origen, destino, peso, horario]
nodo_actual = 'a'
peso_actual = 0
origenes = 'a,a,a,a,b,b,c,c,d,d,d,e,e,f,f,g,g,g,h,i'.split(',')
destinos = 'a,b,d,c,e,d,d,f,e,f,g,h,g,g,i,h,j,i,j,j'.split(',')
pesos = [0,4,8,5,12,3,1,11,9,4,10,10,6,5,11,3,15,5,14,8]
infinity = sum(pesos)
pesos_min = [0] + [infinity for i in range(len(pesos)-1)]

nodos = set(origenes + destinos)

aristas = [x for x in zip(origenes, destinos, pesos, pesos_min)]

df = sc.parallelize(aristas).toDF(schema=schema)
df_minimal = sc.parallelize([[nodo_actual, peso_actual]]).toDF(['NODO', 'R_min'])

df.show()

# Asignacion de peso

while df.count() > 0:
	# Actualizo el peso de las aristas al minimo posible
	df = df.withColumn('R_min', udf_actualiza_peso(F.lit(nodo_actual), F.col('ORIGIN'), F.lit(peso_actual), F.col('W'), F.col('R_min')))
	df.show()

	# Elimino los registros que llevan al nodo en el que estoy parado
	df = df.filter(F.col('DEST') != F.lit(nodo_actual))

	# Calculo el valor minimo de los pesos para obtener el siguiente nodo
	minimo = df.agg(F.min(F.expr('CAST(R_min AS INT)')).alias('MIN')).collect()[0][0]

	print(minimo)

	df.show()

	estado_actual = df.filter(F.col('R_min').cast(IntegerType()) == F.lit(minimo).cast(IntegerType()))
	estado_actual.show()
	estado_actual = estado_actual.select('DEST', 'R_min').collect()[0]
	nodo_actual = estado_actual[0]
	peso_actual = estado_actual[1]
	print(nodo_actual)
	print(peso_actual)
	df_minimal = df_minimal.union(sc.parallelize([[nodo_actual, peso_actual]]).toDF(['NODO', 'R_min']))

	df_minimal.show()

# actual.show()

