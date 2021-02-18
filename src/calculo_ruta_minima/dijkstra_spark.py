# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU

# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de PySpark
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

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--config", help="Ruta hacia archivo de configuracion")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos")
parser.add_argument("--origin", help="Clave del aeropuerto de origen.")
parser.add_argument("--dest", help="Clave del aeropuerto de destino.")
parser.add_argument("--dep_date", help="Fecha de vuelo deseada.")
args = parser.parse_args()

# Leemos las credenciales de la ruta especificada
with open(args.config) as json_file:
    config = json.load(json_file)
with open(args.creds) as json_file:
    creds = json.load(json_file)

t_inicio = time.time()

process = config['results_table']
nodo_actual = args.origin # Empezamos a explorar en el nodo origen
visitados = dict() # Diccionario en el que almaceno las rutas optimas entre los nodos
# ----------------------------------------------------------------------------------------------------


# Lectura de datos de MySQL
# ----------------------------------------------------------------------------------------------------
# df_rita = spark.read.format("jdbc")\
#     .options(
#         url=creds["db_url"] + creds["database"],
#         driver=creds["db_driver"],
#         dbtable="(SELECT ORIGIN, DEST, ACTUAL_ELAPSED_TIME, FL_DATE, DEP_TIME, ARR_TIME FROM " + config['input_table'] + " LIMIT 10000) df_rita",
#         user=creds["user"],
#         password=creds["password"])\
#     .load().na.drop('any')

date_time_obj = datetime.datetime.strptime(args.dep_date, '%Y-%m-%d')
max_arr_date = str(date_time_obj + datetime.timedelta(days=7))[0:10]

df_rita = spark.read.format('parquet').load('/home/ikerforce/Documents/Tesis/spark-rita/data')\
    .filter('''FL_DATE >= "{dep_date}" AND FL_DATE <= "{max_arr_date}"'''.format(dep_date=args.dep_date, max_arr_date=max_arr_date))\
    .na.drop(subset=['ORIGIN', 'DEST', 'FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ACTUAL_ELAPSED_TIME'])
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCION DE ACTUALIZACION DE PESO ACUMULADO
# ----------------------------------------------------------------------------------------------------
def actualiza_peso(nodo_actual, nodo_destino, peso_arista, peso_actual):
    '''Esta funcion actualiza el peso de la arista cuando es necesario.
    nodo_actual- El nodo desde el que se visita.
    nodo_destino - El nodo que se visita.
    peso_acumulado - Peso acumulado hasta el nodo_actual.
    peso_arista - Peso correspondiente a la arista (nodo_actual nodo_destino).
    peso_actual - El peso actual del nodo inicial hasta el nodo destino.'''
    if nodo_actual == nodo_destino:
        return float(peso_arista)
    else:
        return float(peso_actual)

udf_actualiza_peso = F.udf(actualiza_peso)


def convierte_timestamp_a_epoch(fl_date, dep_time):
    fl_date = str(fl_date).split('-')
    dt = datetime.datetime(int(fl_date[0]), int(fl_date[1]), int(fl_date[2]), int(dep_time[:2]) % 24, int(dep_time[2:4]))
    return time.mktime(dt.timetuple())

udf_convierte_timestamp_a_epoch = F.udf(convierte_timestamp_a_epoch)


def convierte_dict_en_lista(diccionario):
    '''Recibe un diccionario de la forma {'key1':{'subkey1': value1, 'subkey2', value2, 'subkey3':value3}, ... , 'keyN':{'subkey1':value1_1, 'subkey2', value2, 'subkey3':value3}}
    y regresa una lista de listas de la forma: [['key1', value1, value2, value3], ... , ['keyN', value1, value2, value3]]'''
    lista_dict = list(map(lambda x: [x] + [diccionario[x][k] for k in diccionario[x].keys()], diccionario.keys()))
    return lista_dict
# ----------------------------------------------------------------------------------------------------


# OBTENCION DE VALORES INICIALES
# ----------------------------------------------------------------------------------------------------
df = df_rita\
    .withColumn('dep_epoch', udf_convierte_timestamp_a_epoch(F.col('FL_DATE'), F.col('DEP_TIME')))\
    .withColumn('arr_epoch', udf_convierte_timestamp_a_epoch(F.col('FL_DATE'), F.col('ARR_TIME')))\
    .select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME')

# Obtenemos el numero de nodos que hay en la red
n_nodos = df.select('ORIGIN')\
        .union(df.select('DEST')).distinct().count()

encontro_ruta = True
early_arr = 0
# ----------------------------------------------------------------------------------------------------


# CALCULO DE RUTAS MINIMAS
# ----------------------------------------------------------------------------------------------------
# Primero busco si hay vuelo directo
frontera = df.filter(F.col('ORIGIN') == F.lit(args.origin)).filter(F.col('DEST') == F.lit(args.dest))\
                    .orderBy(F.asc('dep_epoch'))\
                    .limit(1)

if frontera.count() > 0:
    # Si hay vuelo directo lo regreso como ruta optima
    vuelo_elegido = frontera.select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME').collect()[0]
    nodo_anterior = vuelo_elegido[0] # Origen del vuelo directo
    nodo_actual = vuelo_elegido[1] # Destino del vuelo directo
    visitados[nodo_actual] = {'origen': nodo_anterior, 'salida': float(vuelo_elegido[2]), 'llegada': float(vuelo_elegido[3])}
    salida = float(vuelo_elegido[2])
    early_arr = float(vuelo_elegido[4]) # Duracion del trayecto
else:

    # En otro caso uso Dijkstra para encontrar la ruta optima
    # print('\nNo hay vuelo directo. Buscando ruta óptima.')
    i = 0
    while i < n_nodos and nodo_actual != args.dest:
        i += 1
        # - Elimino los vuelos en los que DEST == `nodo_actual`.
        df = df.filter(F.col('DEST') != F.lit(nodo_actual))

        df.write.format('parquet').mode('overwrite').save('temp_dir/df_vuelos_spark')
        df = spark.read.format('parquet').load('temp_dir/df_vuelos_spark').cache()

        # - Agrego los vuelos en los que ORIGEN == nodo_actual a la frontera.
        # t_conexion es el tiempo que transucrre entre que llega el avion y toma el siguiente vuelo
        # t_acumulado es el tiempo transcurrido hasta el momento mas el tiempo de conexion mas la duracion del vuelo actual
        frontera = df.filter(F.col('ORIGIN') == F.lit(nodo_actual))\
                    .withColumn('t_conexion', F.col('dep_epoch').cast('float') - F.lit(early_arr).cast('float'))\
                    .filter('t_conexion > 7200')\
                    .withColumn('t_acumulado', F.lit(early_arr) + F.col('t_conexion').cast('float') + F.col('ACTUAL_ELAPSED_TIME').cast('float'))\
                    .select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado')\
                    .union(frontera)

        frontera.write.format('parquet').mode('overwrite').save('temp_dir/frontera_spark')
        frontera = spark.read.format('parquet').load('temp_dir/frontera_spark').cache()
        
        # - Obtengo `V`, el vuelo de la frontera que llega más pronto a su destino (`MIN(dep_epoch + ELAPSED_TIME)`).
        try:
            vuelo_elegido = frontera.orderBy(F.asc('t_acumulado')).select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado').limit(1).collect()[0]
            # - Hago `nodo_actual` =  `V.DEST`.
            # Es el aeropuerto al que llego al tomar el vuelo elegido
            nodo_anterior = vuelo_elegido[0] # No es el nodo anterior explorado, sino el nodo del que se cumple la ruta minima
            nodo_actual = vuelo_elegido[1]
            visitados[nodo_actual] = {'origen':nodo_anterior, 'salida': float(vuelo_elegido[2]), 'llegada': float(vuelo_elegido[3])}
            early_arr = vuelo_elegido[4]
            # - Hago `next_dep_epoch` = `V.arr_epoch + 2 hrs`.
            # La hora mas pronta a la que puedo salir del siguiente aeropuerto considerando 2 horas de conexion
            min_dep_epoch = float(vuelo_elegido[3]) + 7200

            print('Iteracion: {i} / {n_nodos}\nNodo actual: {nodo_actual}\nTiempo: {tiempo}\nEARLY_ARR: {early_arr}'.format(i=i, n_nodos=n_nodos, nodo_actual=nodo_actual, tiempo=time.time()-t_inicio, early_arr=early_arr))

            # - Hago `t_acumulado` = `t_acumulado + V.ELAPSED_TIME + 2h`
            # - Elimino de la frontera los vuelos en los que V.DEST = nodo_actual y con `MIN(dep_epoch + ELAPSED_TIME)` > dep_epoch + ELAPSED_TIME.
            frontera = frontera.filter('DEST != "{nodo_actual}" OR t_acumulado < {early_arr}'.format(nodo_actual=nodo_actual, early_arr=early_arr))
            # - Elimino de los vuelos totales los vuelos con dep_epoch < min_dep_epoch y ORIGEN == nodo_actual.
            df = df.filter('dep_epoch > {min_dep_epoch} OR ORIGIN != "{nodo_actual}"'.format(nodo_actual=nodo_actual, min_dep_epoch=min_dep_epoch))
        except:
            print('\n\tNo hay ruta entre {origen} y {destino}.\n'.format(origen=args.origin, destino=args.dest))
            encontro_ruta = False
            break;
# # ----------------------------------------------------------------------------------------------------


# RESULTADOS
# ----------------------------------------------------------------------------------------------------
# Obtencion de la ruta a partir del diccinario
if encontro_ruta == True:
    # ruta_optima_str = '''
    #                 ORIGEN:  {origen}
    #                   Salida:  {salida}
    #                 DESTINO: {destino}
    #                   Llegada: {llegada}.\n'''.format(origen=visitados[args.dest]['origen']
    #                                                 , destino=args.dest
    #                                                 , salida=time.ctime(visitados[args.dest]['salida'])
    #                                                 , llegada=time.ctime(visitados[args.dest]['llegada'])
    #                                                 )

    solo_optimo = dict() # En este diccionario guardo solo los vuelos que me interesan
    solo_optimo[args.dest] = visitados[args.dest]
    x = visitados[args.dest]['origen']
    # early_arr = visitados[args.dest]['llegada']

    while x != args.origin:
        salida = visitados[x]['salida']
        solo_optimo[x] = visitados[x]
        # ruta_optima_str =  '''
        #             ORIGEN:  {origen}
        #               Salida:  {salida}
        #             DESTINO: {destino}
        #               Llegada: {llegada}\n'''.format(origen=visitados[x]['origen']
        #                                             , destino=x
        #                                             , salida=time.ctime(visitados[x]['salida'])
        #                                             , llegada=time.ctime(visitados[x]['llegada'])
        #                                             ) + ruta_optima_str
        x = visitados[x]['origen']

    df_resp = sc.parallelize(convierte_dict_en_lista(solo_optimo)).toDF(['DEST', 'ORIGIN', 'ARR_TIME', 'DEP_TIME']).select('ORIGIN', 'DEST', 'ARR_TIME', 'DEP_TIME')

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
    ruta_optima_str = float(visitados[args.dest]['llegada']) - float(salida)
    # print(early_arr - float(salida))
    # print(salida)
    t_final = time.time() # Tiempo de finalizacion de la ejecucion
    print("\n\tLa ruta óptima es:\n{ruta_optima_str}\n\tDuración del trayecto: {early_arr}.\n".format(early_arr=str(datetime.timedelta(seconds=float(early_arr)-salida)), ruta_optima_str=ruta_optima_str))

    # print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=t_final - t_inicio))
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