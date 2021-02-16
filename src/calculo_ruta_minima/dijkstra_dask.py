# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU

# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion
import dask.dataframe as dd # Utilizado para el procesamiento de los datos
import pandas as pd # Utilizado para crear dataframe que escribe la informacion del tiempo en MySQL
from sqlalchemy import create_engine
import datetime
import sys # Ayuda a agregar archivos al path
from os import getcwdb # Nos permite conocer el directorio actual
curr_path = getcwdb().decode() # Obtenemos el directorio actual
sys.path.insert(0, curr_path) # Agregamos el directioro en el que se encuentra el directorio src
from src import utils # Estas son las funciones definidas por mi

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

t_inicio = time.time()

uri = 'mysql+pymysql://{0}:{1}@localhost:{2}/{3}'.format(creds["user"], creds["password"], "3306", creds["database"])

process = config['results_table']
nodo_actual = args.origen # Empezamos a explorar en el nodo origen
visitados = dict() # Diccionario en el que almaceno las rutas optimas entre los nodos
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
t_inicio = time.time() # Inicia tiempo de ejecucion

df = dd.read_sql_table(config["input_table"], uri=uri, index_col=config["partition_column"])\
    .dropna(subset=['FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME'])
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

def convierte_timestamp_a_epoch(fl_date, dep_time):
        fl_date = str(fl_date).split('-')
        dt = datetime.datetime(int(fl_date[0]), int(fl_date[1]), int(fl_date[2]), int(dep_time[:2]) % 24, int(dep_time[2:4]))
        return time.mktime(dt.timetuple())


def convierte_dict_en_lista(diccionario):
    '''Recibe un diccionario de la forma {'key1':{'subkey1': value1, 'subkey2', value2, 'subkey3':value3}, ... , 'keyN':{'subkey1':value1_1, 'subkey2', value2, 'subkey3':value3}}
    y regresa una lista de listas de la forma: [['key1', value1, value2, value3], ... , ['keyN', value1, value2, value3]]'''
    lista_dict = list(map(lambda x: [x] + [diccionario[x][k] for k in diccionario[x].keys()], diccionario.keys()))
    return lista_dict
# ----------------------------------------------------------------------------------------------------


# OBTENCION DE VALORES INICIALES
# ----------------------------------------------------------------------------------------------------
df = df[['FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME']]
df['dep_epoch'] = df.apply(lambda row: convierte_timestamp_a_epoch(row['FL_DATE'], row['DEP_TIME']), axis=1, meta='float')
df['arr_epoch'] = df.apply(lambda row: convierte_timestamp_a_epoch(row['FL_DATE'], row['ARR_TIME']), axis=1, meta='float')
df = df[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME']]
df['t_acumulado'] = 0

# Obtenemos el numero de nodos que hay en la red
n_nodos = dd.concat([df['DEST'], df['ORIGIN']], axis=0).drop_duplicates().count().compute()
encontro_ruta = True
early_arr = 0
# ----------------------------------------------------------------------------------------------------


# CALCULO DE RUTAS MINIMAS
# ----------------------------------------------------------------------------------------------------
# Primero busco si hay vuelo directo
frontera = df[(df['ORIGIN'] == args.origen) & (df['DEST'] == args.dest)]\
            .nsmallest(1, columns=['dep_epoch'])
frontera['t_conexion'] = 0

if frontera['ORIGIN'].count().compute() > 0:
    # Si hay vuelo directo lo regreso como ruta optima
    vuelo_elegido = frontera[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME']].reset_index().compute()
    nodo_anterior = vuelo_elegido['ORIGIN'].values[0] # Origen del vuelo directo
    nodo_actual = vuelo_elegido['DEST'].values[0] # Destino del vuelo directo
    visitados[nodo_actual] = {'origen': nodo_anterior, 'salida': float(vuelo_elegido['dep_epoch']), 'llegada': float(vuelo_elegido['arr_epoch'])}
    salida = float(vuelo_elegido['dep_epoch'])
    early_arr = salida + float(vuelo_elegido['ACTUAL_ELAPSED_TIME']) # Duracion del trayecto
else:
    # En otro caso uso Dijkstra para encontrar la ruta optima
    # print('\nNo hay vuelo directo. Buscando ruta óptima.')
    i = 0
    while i < n_nodos and nodo_actual != args.dest:
        i += 1

        # Elimino los vuelos que regresan al nodo actual para eliminar ciclos
        df = df[(df['DEST'] != nodo_actual)]

        df.to_parquet('temp_dir/df_vuelos_dask')
        df = dd.read_parquet('temp_dir/df_vuelos_dask')

        # print(frontera.compute().head())

        # Agrego a la frontera los vuelos cuyo origen es el nodo actual y que tengan un tipo de conexion mayor a 7200 minutos
        # frontera_nueva = df[(df['ORIGIN'] == nodo_actual) & (df['dep_epoch'] - early_arr > 7200)]
        frontera_nueva = df[(df['ORIGIN'] == nodo_actual)]
        frontera_nueva['t_conexion'] = frontera_nueva['dep_epoch'] - early_arr
        frontera_nueva = frontera_nueva[frontera_nueva['t_conexion'] > 7200]
        # Uno los nuevos vuelos de la frontera a los vuelos de la frontera anterior
        frontera = dd.concat([frontera, frontera_nueva], axis=0)
        # Agrego al tiempo acumulado el tiempo de conexion y la duracion de cada vuelo
        frontera['t_acumulado'] = frontera['t_acumulado'] + frontera['t_conexion'] + frontera['ACTUAL_ELAPSED_TIME']
        # print('\n\tFRONTERA NUEVA\n')
        # print(frontera.compute().head())

        frontera.to_parquet('temp_dir/frontera_dask')
        frontera = dd.read_parquet('temp_dir/frontera_dask')        

        try:

            vuelo_elegido = frontera[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado']].nsmallest(1, columns=['t_acumulado']).reset_index().compute()
            nodo_anterior = vuelo_elegido['ORIGIN'].values[0] # Origen del vuelo elegido
            nodo_actual = vuelo_elegido['DEST'].values[0] # Destino del vuelo elegido
            visitados[nodo_actual] = {'origen': nodo_anterior, 'salida': float(vuelo_elegido['dep_epoch']), 'llegada': float(vuelo_elegido['arr_epoch'])}
            early_arr = float(vuelo_elegido['t_acumulado']) # Duracion del trayecto
            min_dep_epoch = float(vuelo_elegido['arr_epoch']) + 7200
            
            # print(''' Iteracion {i} / {n_nodos}
            #         Número de registros en frontera = {n_frontera}.
            #         Número de registros en df = {n_df}.
            #         Nodo actual = {nodo_actual}
            #         Early arr = {early_arr}'''.format(i = i
            #                                                     , n_nodos = n_nodos
            #                                                     , n_frontera = int(frontera['t_acumulado'].count().compute())
            #                                                     , n_df = int(df['t_acumulado'].count().compute())
            #                                                     , nodo_actual = nodo_actual
            #                                                     , early_arr = early_arr))

            frontera = frontera[(frontera['DEST'] != nodo_actual) | (frontera['t_acumulado'] < early_arr)]

            df = df[(df['dep_epoch'] > min_dep_epoch) | (df['ORIGIN'] != nodo_actual)]

        except Exception as e:
            print('\n\tNo hay ruta entre {origen} y {destino}.\n'.format(origen=args.origen, destino=args.dest))
            encontro_ruta = False
            print(e)
            break;


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
    early_arr = visitados[args.dest]['llegada']

    while x != args.origen:
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

    df_resp = pd.DataFrame(data=convierte_dict_en_lista(solo_optimo), columns=['DEST', 'ORIGIN', 'ARR_TIME', 'DEP_TIME'])[['ORIGIN', 'DEST', 'ARR_TIME', 'DEP_TIME']]

    df_resp.to_sql(process, uri, if_exists=config["results_table_mode"], index=False)

    # df_resp.write.format("jdbc")\
    #     .options(
    #         url=creds["db_url"] + creds["database"],
    #         driver=creds["db_driver"],
    #         dbtable=process,
    #         user=creds["user"],
    #         password=creds["password"],
    #         numPartitions=config["db_numPartitions"])\
    #     .mode(config["results_table_mode"])\
    #     .save()

    t_final = time.time() # Tiempo de finalizacion de la ejecucion
    # print("\n\tLa ruta óptima es:\n{ruta_optima_str}\n\tDuración del trayecto: {early_arr}.\n".format(early_arr=str(datetime.timedelta(seconds=float(early_arr)-salida)), ruta_optima_str=ruta_optima_str))

    # print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=t_final - t_inicio))
# ----------------------------------------------------------------------------------------------------



# # REGISTRO DE TIEMPO
# # ----------------------------------------------------------------------------------------------------
info_tiempo = [[process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], time.strftime('%Y-%m-%d %H:%M:%S')]]
df_tiempo = pd.DataFrame(data=info_tiempo, columns=['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'insertion_ts'])
df_tiempo.to_sql("registro_de_tiempo_dask", uri, if_exists=config["time_table_mode"], index=False)

print('\n\n\tFIN DE LA EJECUCIÓN\n\n')
# # ----------------------------------------------------------------------------------------------------