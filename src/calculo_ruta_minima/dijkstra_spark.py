# -*- coding: utf-8 -*-
# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU
import time # Utilizado para medir el timpo de ejecucion
t_inicio = time.time()

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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
spark.sparkContext.setCheckpointDir('temp_dir')

# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import datetime # Utilizado para el parseo de fecha de salida

# Definicion y lectura de los argumentos que se le pasan a la funcion
parser = argparse.ArgumentParser()
parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
parser.add_argument("--origin", help="Clave del aeropuerto de origen.")
parser.add_argument("--dest", help="Clave del aeropuerto de destino.")
parser.add_argument("--command_time", help="Hora a la que fue enviada la ejecución del proceso.")
parser.add_argument("--env", help="Puede ser local o cluster. Esto determina los recursos utilizados y los archivos de configuración que se utilizarán.")
args = parser.parse_args()

def lee_config_csv(path, sample_size, process):
    """Esta función lee un archivo de configuración y obtiene la información para un proceso y tamaño de muestra específico."""
    file = open(path, "r").read().splitlines()
    nombres = file[0]
    info = filter(lambda row: row.split("|")[0] == sample_size and row.split("|")[1] == process, file[1:])
    parametros = dict(zip(nombres.split('|'), list(info)[0].split('|')))
    return parametros

if args.env != 'cluster':
    config = lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
else:
    config = lee_config_csv(path="conf/base/configs_cluster.csv", sample_size=args.sample_size, process=args.process)

with open(args.creds) as json_file:
    creds = json.load(json_file)

command_time = float(args.command_time)

process = config['results_table']
nodo_actual = args.origin # Empezamos a explorar en el nodo origen
visitados = dict() # Diccionario en el que almaceno las rutas optimas entre los nodos
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
df_rita = spark.read.format('parquet').load(config['input_path'])\
    .select(*['YEAR', 'MONTH', 'DAY_OF_MONTH', 'ORIGIN', 'DEST', 'FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ACTUAL_ELAPSED_TIME'])\
    .na.drop(subset=['ORIGIN', 'DEST', 'FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ACTUAL_ELAPSED_TIME'])\
    .withColumn('ACTUAL_ELAPSED_TIME', F.col('ACTUAL_ELAPSED_TIME') * 60.0)
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCION DE ACTUALIZACION DE PESO ACUMULADO
# ----------------------------------------------------------------------------------------------------
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

df.cache()

# Obtenemos el numero de nodos que hay en la red
n_nodos = df.select('ORIGIN')\
        .union(df.select('DEST')).distinct().count()

encontro_ruta = True
early_arr = 0
t_acumulado = 0
# ----------------------------------------------------------------------------------------------------


# CALCULO DE RUTAS MINIMAS
# ----------------------------------------------------------------------------------------------------
# Primero compruebo que haya vuelos saliendo del aeropuerto elegido
if len(df.filter(F.col('ORIGIN') == F.lit(args.origin)).head(1)) > 0:

    # Luego busco si hay vuelo directo
    frontera = df.filter(F.col('ORIGIN') == F.lit(args.origin)).filter(F.col('DEST') == F.lit(args.dest))\
                        .orderBy(F.asc('dep_epoch'))\
                        .limit(1).select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME')

    if len(frontera.head(1)) > 0:

        # Si hay vuelo directo lo regreso como ruta optima
        vuelo_elegido = frontera.select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME').collect()[0]
        nodo_anterior = vuelo_elegido[0] # Origen del vuelo directo
        nodo_actual = vuelo_elegido[1] # Destino del vuelo directo
        visitados[nodo_actual] = {'origen': nodo_anterior, 'salida': float(vuelo_elegido[2]), 'llegada': float(vuelo_elegido[3])}
        salida = float(vuelo_elegido[2])
        t_acumulado = float(vuelo_elegido[4]) # Duracion del trayecto

    else:

        # Elimino los vuelos que regresan al nodo actual para eliminar ciclos
        df = df.filter(F.col('DEST') != F.lit(nodo_actual))

        df = df.checkpoint()

        # df.write.format('parquet').mode('overwrite').save('temp_dir/df_vuelos_spark')
        # df = spark.read.format('parquet').load('temp_dir/df_vuelos_spark').cache()

        # Agrego a la frontera los vuelos cuyo origen es el nodo actual y que tengan un tiempo de conexion mayor a 7200 minutos
        frontera_nueva = df.filter(F.col('ORIGIN') == F.lit(nodo_actual))\
                    .withColumn('t_acumulado', F.lit(t_acumulado) + F.col('ACTUAL_ELAPSED_TIME').cast('float'))\
                    .select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado')

        # Uno los nuevos vuelos de la frontera a los vuelos de la frontera anterior
        frontera = frontera_nueva.union(frontera)

        # En otro caso uso Dijkstra para encontrar la ruta optima
        i = 1
        while i < n_nodos and nodo_actual != args.dest:

            i += 1

            try:

                vuelo_elegido = frontera.orderBy(F.asc('t_acumulado')).select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado').limit(1).collect()[0]
                nodo_anterior = vuelo_elegido[0] # Origen del vuelo elegido
                nodo_actual = vuelo_elegido[1] # Destino del vuelo elegido
                visitados[nodo_actual] = {'origen' : nodo_anterior, 'salida' : float(vuelo_elegido[2]), 'llegada' : float(vuelo_elegido[3])}
                early_arr = vuelo_elegido[3]
                t_acumulado = vuelo_elegido[4]
                min_dep_epoch = float(vuelo_elegido[3]) + 7200

                # print('''\nIteration {i} / {n_nodos}
                #             Nodo actual = {nodo_actual}
                #             Weight = {w}
                #             Transcurrido = {transcurrido}'''.format(i = i
                #                                 , n_nodos = n_nodos
                #                                 , nodo_actual = nodo_actual
                #                                 , w = t_acumulado
                #                                 , transcurrido=time.time()-t_inicio))

                frontera = frontera.filter('DEST != "{nodo_actual}" OR t_acumulado < {t_acumulado}'.format(nodo_actual=nodo_actual, t_acumulado=t_acumulado))

                df = df.filter('dep_epoch > {min_dep_epoch} OR ORIGIN != "{nodo_actual}"'.format(nodo_actual=nodo_actual, min_dep_epoch=min_dep_epoch))

            except Exception as e:

                print('\n\tNo hay ruta entre {origen} y {destino}.\n'.format(origen=args.origin, destino=args.dest))
                encontro_ruta = False
                print(e)
                break;

            df = df.filter(F.col('DEST') != F.lit(nodo_actual))

            # df.write.format('parquet').mode('overwrite').save('temp_dir/df_vuelos_spark')
            # df = spark.read.format('parquet').load('temp_dir/df_vuelos_spark').cache()

            # df.unpersist()

            df = df.checkpoint()

            frontera_nueva = df.filter(F.col('ORIGIN') == F.lit(nodo_actual))\
                        .withColumn('t_conexion', F.col('dep_epoch').cast('float') - F.lit(early_arr).cast('float'))\
                        .filter('t_conexion > 7200')\
                        .withColumn('t_acumulado', F.lit(t_acumulado).cast('float') + F.col('t_conexion').cast('float') + F.col('ACTUAL_ELAPSED_TIME').cast('float'))\
                        .select('ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado')\
                        
            frontera = frontera_nueva.union(frontera)

            # frontera.write.format('parquet').mode('overwrite').save('temp_dir/frontera_spark')
            # frontera = spark.read.format('parquet').load('temp_dir/frontera_spark').cache()

            frontera = frontera.checkpoint()

else:

    print('\n\tNo hay vuelos saliendo de {origen}..\n'.format(origen=args.origin))
    encontro_ruta = False

# ----------------------------------------------------------------------------------------------------


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

    print('\n\tSe encontro ruta entre {origen} y {destino}.\n'.format(origen=args.origin, destino=args.dest))

    solo_optimo = dict() # En este diccionario guardo solo los vuelos que me interesan
    solo_optimo[args.dest] = visitados[args.dest]
    x = visitados[args.dest]['origen']
    early_arr = visitados[args.dest]['llegada']

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

    df_resp.cache()

    print(df_resp.head(1))

    write_time = time.time()

    df_resp.write.format("jdbc")\
        .options(
            url=creds["db_url"] + creds["database"],
            driver=creds["db_driver"],
            dbtable=process,
            user=creds["user"],
            rewriteBatchedStatements=True,
            password=creds["password"],
            numPartitions=config["db_numPartitions"])\
        .mode(config["results_table_mode"])\
        .save()

t_final = time.time() # Tiempo de finalizacion de la ejecucion

    # print("\n\tLa ruta óptima es:\n{ruta_optima_str}\n\tDuración del trayecto: {early_arr}.\n".format(early_arr=str(datetime.timedelta(seconds=float(t_acumulado))), ruta_optima_str=ruta_optima_str))

print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=t_final - t_inicio))
# ----------------------------------------------------------------------------------------------------



# REGISTRO DE TIEMPO
# ----------------------------------------------------------------------------------------------------
rdd_time = sc.parallelize([[process + '_command_time', command_time, t_inicio, t_inicio - command_time, config["description"], config["resources"], args.sample_size, args.env],
                            [process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], args.sample_size, args.env],
                            [process + '_write_time', write_time, t_final, t_final - write_time, config["description"], config["resources"], args.sample_size, args.env]])
df_time = rdd_time.toDF(['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size', 'env'])\
    .withColumn("insertion_ts", F.current_timestamp())

df_time.write.format("jdbc")\
    .options(
        url=creds["db_url"] + creds["database"],
        driver=creds["db_driver"],
        dbtable=config['time_table'],
        user=creds["user"],
        rewriteBatchedStatements=True,
        password=creds["password"])\
    .mode(config["time_table_mode"])\
    .save()

print('\tTiempo ejecución: {t}'.format(t = t_final - t_inicio))
print('\tFIN DE LA EJECUCIÓN')
# ----------------------------------------------------------------------------------------------------