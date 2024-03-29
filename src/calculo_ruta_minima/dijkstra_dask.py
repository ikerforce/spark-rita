# -*- coding: utf-8 -*-
# Algoritmo para calcular la ruta minima entre un par de aeropuertos dentro de EU
import time # Utilizado para medir el timpo de ejecucion
t_inicio = time.time()

# PREPARACION DE AMBIENTE
# ----------------------------------------------------------------------------------------------------
# Inicio de cliente dask distributed
from dask.distributed import Client
# Importaciones de Python
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time # Utilizado para medir el timpo de ejecucion
import pandas as pd # Utilizado para crear dataframe que escribe la informacion del tiempo en MySQL y para el dataframe frontera
from sqlalchemy import create_engine
import datetime # Utilizado para el parseo de fecha de salida
import sys # Ayuda a agregar archivos al path
import os # Nos permite conocer el directorio actual
curr_path = os.getcwdb().decode() # Obtenemos el directorio actual
sys.path.insert(0, curr_path) # Agregamos el directioro en el que se encuentra el directorio src
from src import utils # Estas son las funciones definidas por mi
import dask.dataframe as dd # Utilizado para el procesamiento de los datos

# Definicion y lectura de los argumentos que se le pasan a la funcion
parser = argparse.ArgumentParser()
parser.add_argument("--sample_size", help="Tamaño de la muestra de datos que se utilizará.")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
parser.add_argument("--process", help="Nombre del proceso que se va a ejecutar.")
parser.add_argument("--origin", help="Clave del aeropuerto de origen.")
parser.add_argument("--dest", help="Clave del aeropuerto de destino.")
parser.add_argument("--scheduler", help="Direccion IP y puerto del scheduler.")
parser.add_argument("--command_time", help="Hora a la que fue enviada la ejecución del proceso.")
parser.add_argument("--env", help="Puede ser local o cluster. Esto determina los recursos utilizados y los archivos de configuración que se utilizarán.")
args = parser.parse_args()
# Leemos las credenciales de la ruta especificada
if args.env != 'cluster':
    config = utils.lee_config_csv(path="conf/base/configs.csv", sample_size=args.sample_size, process=args.process)
else:
    config = utils.lee_config_csv(path="conf/base/configs_cluster.csv", sample_size=args.sample_size, process=args.process)
with open(args.creds) as json_file:
    creds = json.load(json_file)

command_time = float(args.command_time)

# Cadena de conexion a base de datos (para escrbir los resultados)
uri = 'mysql+pymysql://{0}:{1}@{2}:{3}/{4}'.format(creds["user"], creds["password"], creds["host"], "3306", creds["database"])

t_inicio = time.time()

process = config['results_table']
nodo_actual = args.origin # Empezamos a explorar en el nodo origen
visitados = dict() # Diccionario en el que almaceno las rutas optimas entre los nodos

if __name__ == '__main__':

    if args.scheduler != None:
        
        client = Client(args.scheduler)
    
    else:
        
        client = Client()
# ----------------------------------------------------------------------------------------------------


# LECTURA DE DATOS
# ----------------------------------------------------------------------------------------------------
    df = dd.read_parquet(config['input_path']
            , infer_divisions=False
            , engine='pyarrow'
            , columns=['YEAR', 'MONTH', 'DAY_OF_MONTH', 'FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME']
            , index=False
            , dtype={'ACTUAL_ELAPSED_TIME' : float}
        )\
        .dropna(subset=['FL_DATE', 'DEP_TIME', 'ARR_TIME', 'ORIGIN', 'DEST', 'ACTUAL_ELAPSED_TIME'])
    df['ACTUAL_ELAPSED_TIME'] = df['ACTUAL_ELAPSED_TIME'].astype(float) * 60.0
# ----------------------------------------------------------------------------------------------------


# DEFINICION DE FUNCION DE ACTUALIZACION DE PESO ACUMULADO
# ----------------------------------------------------------------------------------------------------
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
    df['dep_epoch'] = df.apply(lambda row: convierte_timestamp_a_epoch(row['FL_DATE'], row['DEP_TIME']), axis=1, meta='float')
    df['arr_epoch'] = df.apply(lambda row: convierte_timestamp_a_epoch(row['FL_DATE'], row['ARR_TIME']), axis=1, meta='float')
    df = df[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME']]
    
    df = client.persist(df)
    
    # Obtenemos el numero de nodos que hay en la red
    n_nodos = dd.concat([df['DEST'], df['ORIGIN']], axis=0).drop_duplicates().count().compute()

    encontro_ruta = True
    early_arr = 0
    t_acumulado = 0
# ----------------------------------------------------------------------------------------------------


# CALCULO DE RUTAS MINIMAS
# ----------------------------------------------------------------------------------------------------
# Primero compruebo que haya vuelos saliendo del aeropuerto elegido
    if len(df[df['ORIGIN'] == args.origin].index) > 0:

        # Primero busco si hay vuelo directo
        frontera = df[(df['ORIGIN'] == args.origin) & (df['DEST'] == args.dest)]\
                    .nsmallest(1, columns=['dep_epoch'])[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME']].compute()
        
        if frontera['ORIGIN'].shape[0] > 0:

            # Si hay vuelo directo lo regreso como ruta optima
            vuelo_elegido = frontera[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 'ACTUAL_ELAPSED_TIME']].reset_index()
            nodo_anterior = vuelo_elegido['ORIGIN'].values[0] # Origen del vuelo directo
            nodo_actual = vuelo_elegido['DEST'].values[0] # Destino del vuelo directo
            visitados[nodo_actual] = {'origen': nodo_anterior, 'salida': float(vuelo_elegido['dep_epoch']), 'llegada': float(vuelo_elegido['arr_epoch'])}
            salida = float(vuelo_elegido['dep_epoch'])
            t_acumulado = float(vuelo_elegido['ACTUAL_ELAPSED_TIME']) # Duracion del trayecto

        else:

            # Elimino los vuelos que regresan al nodo actual para eliminar ciclos
            df = df[(df['DEST'] != nodo_actual)]

            df = client.persist(df)

            # Agrego a la frontera los vuelos cuyo origen es el nodo actual y que tengan un tiempo de conexion mayor a 7200 minutos
            frontera_nueva = df[(df['ORIGIN'] == nodo_actual)]
            frontera_nueva['t_acumulado'] = t_acumulado + frontera_nueva['ACTUAL_ELAPSED_TIME']
            frontera_nueva = frontera_nueva[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado']].compute()
            
            # Uno los nuevos vuelos de la frontera a los vuelos de la frontera anterior
            frontera = pd.concat([frontera, frontera_nueva], axis=0)

            # En otro caso uso Dijkstra para encontrar la ruta optima
            i = 1
            while i < n_nodos and nodo_actual != args.dest:

                i += 1 

                try:

                    vuelo_elegido = frontera[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado']].nsmallest(1, columns=['t_acumulado']).reset_index()
                    nodo_anterior = vuelo_elegido['ORIGIN'].values[0] # Origen del vuelo elegido
                    nodo_actual = vuelo_elegido['DEST'].values[0] # Destino del vuelo elegido
                    visitados[nodo_actual] = {'origen' : nodo_anterior, 'salida' : float(vuelo_elegido['dep_epoch']), 'llegada' : float(vuelo_elegido['arr_epoch'])}
                    early_arr = float(vuelo_elegido['arr_epoch']) # Duracion del trayecto
                    t_acumulado = float(vuelo_elegido['t_acumulado'])
                    min_dep_epoch = float(vuelo_elegido['arr_epoch']) + 7200
                    
                    # print('''\nIteration {i} / {n_nodos}
                    #         Nodo actual = {nodo_actual}
                    #         Weight = {w}
                    #         Transcurrido = {transcurrido}'''.format(i = i
                    #                             , n_nodos = n_nodos
                    #                             , nodo_actual = nodo_actual
                    #                             , w = t_acumulado
                    #                             , transcurrido=time.time()-t_inicio))

                    frontera = frontera[(frontera['DEST'] != nodo_actual) | (frontera['t_acumulado'] < t_acumulado)]

                    df = df[(df['dep_epoch'] > min_dep_epoch) | (df['ORIGIN'] != nodo_actual)]

                except Exception as e:

                    print('\n\tNo hay ruta entre {origen} y {destino}.\n'.format(origen=args.origin, destino=args.dest))
                    encontro_ruta = False
                    print(e)
                    break;

                df = df[(df['DEST'] != nodo_actual)]

                df = client.persist(df)

                frontera_nueva = df[(df['ORIGIN'] == nodo_actual)]
                frontera_nueva['t_conexion'] = frontera_nueva['dep_epoch'] - early_arr
                frontera_nueva = frontera_nueva[frontera_nueva['t_conexion'] > 7200]
                frontera_nueva['t_acumulado'] = t_acumulado + frontera_nueva['t_conexion'] + frontera_nueva['ACTUAL_ELAPSED_TIME'] # .astype(float)
                frontera_nueva = frontera_nueva[['ORIGIN', 'DEST', 'dep_epoch', 'arr_epoch', 't_acumulado']].compute()
                
                frontera = pd.concat([frontera, frontera_nueva], axis=0)

    else:

        print('\n\tNo hay vuelos saliendo de {origen}.\n'.format(origen=args.origin))
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

        print('\tSe encontro ruta entre {origen} y {destino}.'.format(origen=args.origin, destino=args.dest))

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

        df_resp = pd.DataFrame(data=convierte_dict_en_lista(solo_optimo), columns=['DEST', 'ORIGIN', 'ARR_TIME', 'DEP_TIME'])[['ORIGIN', 'DEST', 'ARR_TIME', 'DEP_TIME']]

        write_time = time.time()
        
        df_resp.to_sql(process, uri, if_exists=config["results_table_mode"], index=False)

    t_final = time.time() # Tiempo de finalizacion de la ejecucion

        # print("\n\tLa ruta óptima es:\n{ruta_optima_str}\n\tDuración del trayecto: {early_arr}.\n".format(early_arr=str(datetime.timedelta(seconds=float(t_acumulado))), ruta_optima_str=ruta_optima_str))

    print('\n\tTiempo de ejecucion: {tiempo}.\n'.format(tiempo=t_final - t_inicio))
    # ----------------------------------------------------------------------------------------------------



    # # REGISTRO DE TIEMPO
    # # ----------------------------------------------------------------------------------------------------
    info_tiempo = [[process + '_command_time', command_time, t_inicio, t_inicio - command_time, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')],
                    [process, t_inicio, t_final, t_final - t_inicio, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')],
                    [process + '_write_time', write_time, t_final, t_final - write_time, config["description"], config["resources"], args.sample_size, args.env, time.strftime('%Y-%m-%d %H:%M:%S')]]
    df_tiempo = pd.DataFrame(data=info_tiempo, columns=['process', 'start_ts', 'end_ts', 'duration', 'description', 'resources', 'sample_size', 'env', 'insertion_ts'])
    df_tiempo.to_sql(config['time_table'], uri, if_exists=config["time_table_mode"], index=False)

    print('\tTiempo ejecución: {t}'.format(t = t_final - t_inicio))
    print('\tFIN DE LA EJECUCIÓN')
    # # ----------------------------------------------------------------------------------------------------