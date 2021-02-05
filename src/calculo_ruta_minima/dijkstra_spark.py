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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

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


nodo_actual = args.origen # Empezamos a explorar en el nodo origen
# ----------------------------------------------------------------------------------------------------

# Lectura de datos de MySQL
# ----------------------------------------------------------------------------------------------------
df_rita = spark.read.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable="(SELECT * FROM " + 'vuelos_conectados' + " LIMIT 10000) df_rita",
        user=creds["user"],
        password=creds["password"])\
    .load()
# ----------------------------------------------------------------------------------------------------


# OBTENCION DE VALORES INICIALES
# ----------------------------------------------------------------------------------------------------
# Es el valor inicial de cada nodo. Es el valor mas alto posible.
infinity = df_rita.agg(F.sum('ACTUAL_ELAPSED_TIME')).collect()[0][0]

df_rita = df_rita.groupBy('ORIGIN', 'DEST')\
    .agg(F.avg('ACTUAL_ELAPSED_TIME').alias('ACTUAL_ELAPSED_TIME'))

df = df_rita.select('ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME')\
    .withColumnRenamed('ACTUAL_ELAPSED_TIME', 'W')\
    .withColumn('R_min', F.lit(infinity))

# Obtenemos el numero de nodos que hay en la red
n_nodos = df.select('ORIGIN')\
        .union(df.select('DEST')).distinct().count()

# Este es el esquema que tendra el df
schema = StructType([
  StructField('ORIGIN', StringType(), True),
  StructField('DEST', StringType(), True),
  StructField('W', FloatType(), True),
  StructField('R_min', FloatType(), True)])

# Este df almacena la informacion de los nodos cuyo peso ya se actualizo (no necesariamente es el peso minimo)
df_temp = sc.parallelize([[nodo_actual, nodo_actual, 0.0, 0.0]]).toDF(['ORIGIN', 'DEST', 'W', 'R_min'])

ruta_optima = dict()
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCION DE ACTUALIZACION DE PESO ACUMULADO
# ----------------------------------------------------------------------------------------------------
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
# ----------------------------------------------------------------------------------------------------


# CALCULO DE RUTAS MINIMAS
# ----------------------------------------------------------------------------------------------------
inicio = time.time()
print('\nInicio del loop.')

i = 0
minimo = 0
while i < n_nodos and nodo_actual != args.dest and minimo != infinity:
    i+=1
    # Agrego a los valores considerados los nodos conectados al nodo en el que estoy parado
    df_temp = df.filter(F.col('ORIGIN') == F.lit(nodo_actual)).union(df_temp)
    df_temp.cache()

    # Calculo el valor minimo de los pesos para obtener el siguiente nodo a explorar
    minimo = df_temp.agg(F.min(F.expr('CAST(R_min AS float)')).alias('MIN')).collect()[0][0] # Obtenemos el vertice con el minimo valor

    # Obtenemos el nuevo nodo a explorar y el nodo que lleva a el con el peso minimo
    estado_actual = df_temp.filter(F.col('R_min').cast(FloatType()) == F.lit(minimo).cast(FloatType()))\
                        .select('ORIGIN', 'DEST', 'R_min')\
                        .collect()[0]
    nodo_anterior = estado_actual[0]
    nodo_actual = estado_actual[1]
    peso_actual = estado_actual[2]

    # Añado el nodo actual a la ruta optima
    ruta_optima.update({nodo_anterior : peso_actual})

    print('\n\tNumero de ejecucion: {i}/{total}.\n\tNodo actual: {nodo_actual}.\n\tPeso actual: {peso_actual}.\n\tTiempo transcurrido: {tiempo}.\n\tMinimo: {minimo}\n.'.format(i=i, total=n_nodos, nodo_actual=nodo_actual, peso_actual=peso_actual, tiempo=time.time()-inicio, minimo=minimo))

    # Actualizo el peso de las aristas al minimo posible
    df_temp = df_temp.withColumn('R_min', udf_actualiza_peso(F.lit(nodo_actual), F.col('ORIGIN'), F.lit(peso_actual), F.col('W'), F.col('R_min')))

    # Elimino los registros que llevan al nodo en el que estoy parado de los nodos por explorar (ya tengo ruta optima a este nodo)
    df_temp = df_temp.filter(F.col('DEST') != F.lit(nodo_actual))

    # En el df de todos los vuelos tambien elimino las aristas que llevan al nodo actual
    df = df.filter(F.col('DEST') != F.lit(nodo_actual))
    df.cache()
# ----------------------------------------------------------------------------------------------------


# RESULTADOS
# ----------------------------------------------------------------------------------------------------
peso_optimo = list(ruta_optima.values())[-1]
ruta_optima = list(ruta_optima.keys()) + [nodo_actual]

ruta_optima_str = ""
for v in ruta_optima:
    ruta_optima_str += v + " - "
ruta_optima_str = ruta_optima_str[:-3]

resultado = print("\nLa ruta_optima es {ruta_optima_str} y su peso es de {peso_optimo}.\n".format(peso_optimo=peso_optimo, ruta_optima_str=ruta_optima_str))

print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=time.time() - inicio))
# ----------------------------------------------------------------------------------------------------