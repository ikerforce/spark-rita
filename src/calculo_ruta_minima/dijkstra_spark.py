# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--config", help="Ruta hacia archivo de configuracion")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos")
parser.add_argument("--origen", help="Clave del aeropuerto de origen.")
parser.add_argument("--dest", help="Clave del aeropuerto de destino.")
args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
with open(args.config) as json_file:
    config = json.load(json_file)
with open(args.creds) as json_file:
    creds = json.load(json_file)
# ----------------------------------------------------------------------------------------------------


t_inicio = time.time() # Inicia tiempo de ejecucion

# Lectura de datos de MySQL
df_rita = spark.read.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="(SELECT * FROM " + 'vuelos_conectados' + " LIMIT 10000) df_rita",
        user=creds["user"],
        password=creds["password"])\
    .load()

suma = df_rita.agg(F.sum('ACTUAL_ELAPSED_TIME')).collect()[0][0]

df_rita = df_rita.groupBy('ORIGIN', 'DEST').agg(F.avg('ACTUAL_ELAPSED_TIME').alias('ACTUAL_ELAPSED_TIME'))

print('\n')
print(df_rita.count())
print('\n')

df_rita.show()

df = df_rita.select('ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME')\
    .withColumnRenamed('ACTUAL_ELAPSED_TIME', 'W')\
    .withColumn('R_min', F.lit(suma))

df = df.union(sc.parallelize([[args.origen, args.origen, 0.0, 0.0]]).toDF(['ORIGIN', 'DEST', 'W', 'R_min']))

n_nodos = df.select('ORIGIN')\
        .union(df.select('DEST')).distinct().count()

df.show()

schema = StructType([
  StructField('ORIGIN', StringType(), True),
  StructField('DEST', StringType(), True),
  StructField('W', IntegerType(), True),
  StructField('R_min', IntegerType(), True)])

# Creacion de red
# Cada elemento tiene la forma: [origen, destino, peso, peso_minimo, horario]
# origenes = 'a,a,a,a,b,b,c,c,d,d,d,e,e,f,f,g,g,g,h,i'.split(',')
# destinos = 'a,b,d,c,e,d,d,f,e,f,g,h,g,g,i,h,j,i,j,j'.split(',')
# pesos = [0,4,8,5,12,3,1,11,9,4,10,10,6,5,11,3,15,5,14,8]
# infinity = sum(pesos)
# pesos_min = [0] + [infinity for i in range(len(pesos)-1)]

# origenes = 'A,A,A,B,B,C,C,D,D,E,E,F,G'.split(',')
# destinos = 'A,B,C,D,E,D,E,F,G,F,G,H,H'.split(',')
# pesos = [0,5,6,7,8,5,6,8,9,6,7,13,11]
# infinity = sum(pesos)
# pesos_min = [0] + [infinity for i in range(len(pesos)-1)]

# origenes = 'A,A,A,B,B,C,E'.split(',')
# destinos = 'A,B,C,D,E,E,F'.split(',')
# pesos = [0,5,7,2,3,8,4]
# infinity = sum(pesos)
# pesos_min = [0] + [infinity for i in range(len(pesos)-1)]


def actualiza_peso(nodo_actual, nodo_destino, peso_acumulado, peso_arista, peso_actual):
    '''Esta funcion actualiza el peso de la arista cuando es necesario.
    nodo_actual- El nodo desde el que se visita.
    nodo_destino - El nodo que se visita.
    peso_acumulado - Peso acumulado hasta el nodo_actual
    peso_arista - Peso correspondiente a la arista (nodo_actual nodo_destino).
    peso_actual - El peso actual del nodo inicial hasta el nodo destino.'''
    if nodo_actual== nodo_destino:
        if float(peso_acumulado) + float(peso_arista) < float(peso_actual):
            return float(peso_acumulado) + float(peso_arista)
        else:
            return float(peso_actual)
    else:
        return float(peso_actual)

udf_actualiza_peso = F.udf(actualiza_peso)

# nodos = set(origenes + destinos)
# aristas = [x for x in zip(origenes, destinos, pesos, pesos_min)]

# df = sc.parallelize(aristas).toDF(schema=schema)

ruta_optima = dict()

inicio = time.time()
print('\nInicio del loop.')

for i in range(n_nodos):
# while df.count() > 0:
    # Calculo el valor minimo de los pesos para obtener el siguiente nodo
    minimo = df.agg(F.min(F.expr('CAST(R_min AS INT)')).alias('MIN')).collect()[0][0] # Obtenemos el vertice con el minimo valor

    # Obtenemos el nuevo nodo a explorar y el nodo que lleva a el con el peso minimo
    estado_actual = df.filter(F.col('R_min').cast(IntegerType()) == F.lit(minimo).cast(IntegerType()))\
                        .select('ORIGIN', 'DEST', 'R_min')\
                        .collect()[0]
    nodo_anterior = estado_actual[0]
    nodo_actual = estado_actual[1]
    peso_actual = estado_actual[2]

    ruta_optima.update({nodo_anterior : peso_actual})

    print('\n\tNumero de ejecucion: {i}/{total}.\n\tNodo actual: {nodo_actual}.\n\tPeso actual: {peso_actual}'.format(i=i, total=n_nodos, nodo_actual=nodo_actual, peso_actual=peso_actual))

    # Actualizo el peso de las aristas al minimo posible
    df = df.withColumn('R_min', udf_actualiza_peso(F.lit(nodo_actual), F.col('ORIGIN'), F.lit(peso_actual), F.col('W'), F.col('R_min')))

    # Elimino los registros que llevan al nodo en el que estoy parado
    df = df.filter(F.col('DEST') != F.lit(nodo_actual))

peso_optimo = list(ruta_optima.values())[-1]
ruta_optima = list(ruta_optima.keys()) + [nodo_actual]

ruta_optima_str = ""
for v in ruta_optima:
    ruta_optima_str += v + " - "
ruta_optima_str = ruta_optima_str[:-3]

resultado = print("\nLa ruta_optima es {ruta_optima_str} y su peso es de {peso_optimo}.\n".format(peso_optimo=peso_optimo, ruta_optima_str=ruta_optima_str))

print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=time.time() - inicio))