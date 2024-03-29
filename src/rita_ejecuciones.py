# -*- coding: utf-8 -*-
# Script para ejecutar
import os
import random
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time
import datetime

hora_ejecucion = time.strftime("%d_%m_%Y_%H_%M_%S") # 

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
parser.add_argument("--ejecs", help="Determina el numero_de_ejecuciones que se haran.")
parser.add_argument("--blacklist", help="Son los procesos que no se ejecutarán.")
parser.add_argument("--sample_size", help="Determina la muestra con la que se ejecutarán los procesos.")
parser.add_argument("--env", help="Puede ser local o cluster. Esto determina los recursos utilizados y los archivos de configuración que se utilizarán.")
parser.add_argument("--scheduler", help="Direccion IP y puerto del scheduler.")
args = parser.parse_args()

if args.env == None:
    print('\n\tPor favor especifica el parámetro env.\n')
    len(args.env)

if args.blacklist == None:
    blacklist = []
else:
    blacklist = args.blacklist.split(',')

def convierte_en_dict(l):
    return {l[0]: {'dask':l[1], 'spark':l[2].replace('\n', '')}}


def orden_pruebas(numero_de_ejecuciones):
    '''Esta funcion regresa una lista de tamano 2 * numero_de_ejecuciones de ceros y unos.'''
    pruebas = [1 for i in range(numero_de_ejecuciones)] + [0 for i in range(numero_de_ejecuciones)]
    random.shuffle(pruebas)
    return pruebas


def lee_config_csv(path, sample_size, process):
    """Esta función lee un archivo de configuración y obtiene la información para un proceso y tamaño de muestra específico."""
    file = open(path, "r").read().splitlines()
    nombres = file[0]
    info = filter(lambda row: row.split("|")[0] == sample_size and row.split("|")[1] == process, file[1:])
    parametros = dict(zip(nombres.split('|'), list(info)[0].split('|')))
    return parametros


def obten_procesos(path, sample_size):
    """Esta función lee un archivo de configuración y obtiene la información para un proceso y tamaño de muestra específico."""
    file = open(path, "r").read().splitlines()
    file_sample_only = filter(lambda row: row.split('|')[0] == sample_size, file[1:])
    procesos = set(x.replace('_dask', '').replace('_spark','') for x in map(lambda row: row.split('|')[1], file_sample_only))
    return procesos


def selecciona_aeropuertos(lista):
    origen = random.choice(lista)
    destino = random.choice(lista)
    while origen == destino:
        destino = random.choice(lista)

    return [origen, destino]


if args.env != 'cluster':

    procesos = obten_procesos(path='conf/base/configs.csv', sample_size=args.sample_size)

else:

    procesos = obten_procesos(path='conf/base/configs_cluster.csv', sample_size=args.sample_size)


# 0 es dask
# 1 es spark
numero_de_ejecuciones = int(args.ejecs)
procesos.remove('dijkstra')
# procesos = ['demoras_ruta_aeropuerto', 'demoras_ruta_mktid']
pruebas_totales = dict(zip(procesos, [orden_pruebas(numero_de_ejecuciones) for i in range(len(procesos))]))
n_errores = 0
conteo_proceso = 0
total_p = len(procesos) + 1
n_total_ejecuciones = total_p * numero_de_ejecuciones * 2
for proceso in procesos:
    for x in pruebas_totales[proceso]:
        conteo_proceso += 1
        print('\tAvance {cp}/{n_ejecs}'.format(cp=conteo_proceso, n_ejecs=n_total_ejecuciones))
        if x == 1:
            try:
                print('+----------------------------------+')
                print('\t' + proceso + ' spark')
                if args.env != 'cluster':
                    spark_cmd = """spark-submit \
                                    --driver-memory 11G \
                                    --jars sql/mysql-connector-java-8.0.23.jar \
                                    src/rita_master_spark.py \
                                    --env {env} \
                                    --creds {creds} \
                                    --process {proceso}_spark \
                                    --command_time {c_time} \
                                    --sample_size {sample_size}""".format(creds=args.creds, proceso=proceso, sample_size=args.sample_size, env=args.env, c_time=time.time())
                    if proceso + '_spark' in blacklist:
                        print('El proceso: {p} será omitido.'.format(p=proceso))
                    else:
                        os.system(spark_cmd)
                else:
                    spark_cmd = """spark-submit \
                                    --driver-memory 2G \
                                    --driver-cores 2 \
                                    --num-executors 6 \
                                    --executor-cores 3 \
                                    --executor-memory 8G \
                                    --jars sql/mysql-connector-java-8.0.23.jar \
                                    src/rita_master_spark.py \
                                    --env cluster \
                                    --creds {creds} \
                                    --process {proceso}_spark \
                                    --command_time {c_time} \
                                    --sample_size {sample_size}""".format(creds=args.creds, proceso=proceso, sample_size=args.sample_size, c_time=time.time())
                    if proceso + '_spark' in blacklist:
                        print('El proceso: {p} será omitido.'.format(p=proceso))
                    else:
                        os.system(spark_cmd)
                print('+----------------------------------+')
            except Exception as e:
                n_errores += 1
                with open("rita_ejecuciones{fecha}.err".format(fecha=hora_ejecucion), "a") as myfile:
                    myfile.write('\n\t' + proceso + ' spark\n' + e)
        else:
            try:
                print('+----------------------------------+')
                print('\t' + proceso + ' dask')
                if args.scheduler == None:
                    dask_cmd = """python \
                                    src/rita_master_dask.py \
                                    --creds {creds} \
                                    --process {proceso}_dask \
                                    --env {env} \
                                    --command_time {c_time} \
                                    --sample_size {sample_size}""".format(creds=args.creds, proceso=proceso, sample_size=args.sample_size, c_time=time.time(), env=args.env)
                    if proceso + '_dask' in blacklist:
                        print('El proceso: {p} será omitido.'.format(p=proceso))
                    else:
                        os.system(dask_cmd)
                else:
                    dask_cmd = """~/miniconda/envs/dask_yarn/bin/python3 \
                                    src/rita_master_dask.py \
                                    --env cluster \
                                    --creds {creds} \
                                    --process {proceso}_dask \
                                    --command_time {c_time} \
                                    --sample_size {sample_size} \
                                    --scheduler {scheduler}""".format(creds=args.creds, proceso=proceso, sample_size=args.sample_size, scheduler=args.scheduler, c_time=time.time())
                    if proceso + '_dask' in blacklist:
                        print('El proceso: {p} será omitido.'.format(p=proceso))
                    else:
                        os.system(dask_cmd)
                print('+----------------------------------+')
            except Exception as e:
                n_errores += 1
                with open("rita_ejecuciones{fecha}.err".format(fecha=hora_ejecucion), "a") as myfile:
                    myfile.write('\n\t' + proceso + ' dask\n' + e)

pruebas_rutas = orden_pruebas(numero_de_ejecuciones)
random.shuffle(pruebas_rutas)
aeropuertos = ['DFW','LAX','MEM','DTW','ORD','IAH','JFK','SFO','MIA','EWR','HNL','LGA','CLT','PHX','BOS','IAD','OGG','AUS','DEN','DCA','ATL','MCO','GSP','ABE','TUL','EGE','FLL','CLE','SBN','SJU','BNA','LAS','BGR','MSP','AVP','SEA','SAN','TUS','MSY','BDL','SNA','CID','CMH','STL','RDU','HPN','PBI','PIT','CHS','FWA']
# aeropuertos = ['BGM', 'DLG', 'INL', 'PSE', 'MSY', 'PPG', 'GEG', 'DRT', 'SNA', 'BUR', 'GRB', 'GTF', 'IFP', 'IDA', 'GRR', 'LWB', 'JLN', 'PVU', 'EUG', 'PSG', 'ATY', 'PVD', 'MYR', 'GSO', 'OAK', 'EAR', 'FAR', 'MSN', 'BTM', 'FSM', 'COD', 'MQT', 'SCC', 'ESC', 'FNL', 'DCA', 'RFD', 'MLU', 'CID', 'SWO', 'GTR', 'LWS', 'PIB', 'UTM', 'WRG', 'HLN', 'CIU', 'IAG', 'DDC', 'RDM', 'LEX', 'FLO', 'JMS', 'ORF', 'SCE', 'KTN', 'EVV', 'ENV', 'CRW', 'CWA', 'OGS', 'VCT', 'SAV', 'GCK', 'TRI', 'CDV', 'CMH', 'MOD', 'SPN', 'YNG', 'LBF', 'MWH', 'CAK', 'TYR', 'ADK', 'CDB', 'CHO', 'MOB', 'PNS', 'MCN', 'DIK', 'CEC', 'LIH', 'IAH', 'HNL', 'SLN', 'SHV', 'ERI', 'GST', 'CVG', 'SJC', 'TOL', 'LGA', 'BUF', 'TLH', 'CDC', 'ACT', 'HPN', 'RDD', 'AUS', 'MLI', 'GCC', 'SJU', 'DHN', 'ATW', 'LGB', 'GJT', 'AVL', 'LYH', 'CNY', 'GFK', 'BFL', 'RIW', 'RNO', 'SRQ', 'EYW', 'SBN', 'BJI', 'TTN', 'JAC', 'RST', 'CHS', 'HGR', 'RSW', 'TUL', 'HRL', 'IPL', 'ISP', 'AMA', 'BOS', 'MAF', 'MLB', 'TUP', 'EWR', 'LAS', 'BIS', 'OGD', 'JAN', 'FAI', 'ITO', 'IMT', 'UIN', 'XNA', 'HHH', 'DLH', 'HYA', 'DEN', 'EWN', 'RHI', 'SGU', 'ALS', 'ALB', 'CPR', 'LNK', 'OME', 'GRI', 'PSP', 'SBA', 'BOI', 'IAD', 'IYK', 'MEI', 'LAR', 'HOB', 'DRO', 'BRO', 'BRD', 'BMI', 'RKS', 'SEA', 'LAN', 'CMI', 'VEL', 'LRD', 'PBG', 'HYS', 'VLD', 'PSM', 'MCI', 'PIR', 'FLG', 'GRK', 'CLT', 'TVC', 'BNA', 'CLL', 'CGI', 'PAE', 'UST', 'PSC', 'BLI', 'CIC', 'ORH', 'ABQ', 'PBI', 'PIE', 'SDF', 'ART', 'ACV', 'LAW', 'SCK', 'SLE', 'BDL', 'DAL', 'MRY', 'ITH', 'DBQ', 'USA', 'CLE', 'PDX', 'ACK', 'MIA', 'MFR', 'TWF', 'ILG', 'TPA', 'BWI', 'BKG', 'APN', 'CMX', 'PRC', 'OKC', 'ROA', 'SMF', 'SPI', 'BRW', 'OTH', 'SFB', 'BFF', 'ABI', 'OXR', 'MBS', 'ELM', 'PHX', 'ABR', 'DVL', 'FCA', 'STL', 'ABY', 'PWM', 'BET', 'DFW', 'MHT', 'ABE', 'TXK', 'GSP', 'LSE', 'BFM', 'MMH', 'FAY', 'STX', 'HDN', 'EFD', 'GUC', 'LMT', 'LBB', 'EKO', 'CRP', 'HVN', 'EGE', 'FSD', 'SWF', 'BQK', 'SUN', 'CSG', 'SFO', 'MEM', 'SAF', 'ELP', 'BHM', 'ATL', 'FLL', 'FNT', 'PIH', 'YKM', 'AZA', 'DEC', 'RIC', 'AKN', 'LCK', 'DAY', 'PHF', 'OMA', 'SJT', 'LCH', 'STC', 'TEX', 'VPS', 'BPT', 'MHK', 'TKI', 'LIT', 'MVY', 'FAT', 'ICT', 'CAE', 'ECP', 'PFN', 'ORD', 'AVP', 'LBL', 'LBE', 'BTV', 'COU', 'MKG', 'IPT', 'BIL', 'AEX', 'SPS', 'ILM', 'SMX', 'PUB', 'PIA', 'GUM', 'RDU', 'BQN', 'PGV', 'MFE', 'HIB', 'MKE', 'XWA', 'SYR', 'BLV', 'ISN', 'HSV', 'LFT', 'PIT', 'MTJ', 'TUS', 'ROW', 'ACY', 'MDW', 'AZO', 'PLN', 'COS', 'CKB', 'OAJ', 'JNU', 'IND', 'ALO', 'KOA', 'EAU', 'GPT', 'MGM', 'OWB', 'DTW', 'TYS', 'HOU', 'CHA', 'YUM', 'ADQ', 'MDT', 'ONT', 'FWA', 'JAX', 'STS', 'LAX', 'MSP', 'MOT', 'HTS', 'BTR', 'BGR', 'SIT', 'MCO', 'SGF', 'AGS', 'ROC', 'OTZ', 'WYS', 'SAN', 'BZN', 'GGG', 'SHD', 'YAK', 'JFK', 'DAB', 'ANC', 'PAH', 'SUX', 'MSO', 'GNV', 'CYS', 'PHL', 'OGG', 'PGD', 'DSM', 'FOE', 'SAT', 'SLC', 'PMD', 'SHR', 'STT', 'SBP', 'RAP', 'ASE', 'CLD']
rutas_spark = [selecciona_aeropuertos(aeropuertos) for i in range(numero_de_ejecuciones)]
rutas_dask = rutas_spark[:]


for i in pruebas_rutas:
    if i == 1:
        ruta = rutas_spark.pop()
        print('+----------------------------------+')
        print('\tdijkstra - spark')
        if args.env != 'cluster':
            spark_cmd = '''spark-submit \
                            --driver-memory 11G \
                            --jars sql/mysql-connector-java-8.0.23.jar \
                            src/calculo_ruta_minima/dijkstra_spark.py \
                            --env {env} \
                            --sample_size {sample_size} \
                            --process {process} \
                            --creds {creds} \
                            --command_time {c_time} \
                            --origin {origin} \
                            --dest {dest}'''.format(origin=ruta[0], dest=ruta[1], sample_size=args.sample_size, process='dijkstra_spark', creds=args.creds, env=args.env, c_time=time.time())
            if 'dijkstra_spark' in blacklist:
                print('El proceso: dijkstra_spark será omitido.')
            else:
                os.system(spark_cmd)
                os.system('rm -r temp_dir/*')
        else:
            spark_cmd = '''spark-submit \
                            --driver-memory 2G \
                            --driver-cores 2 \
                            --num-executors 6 \
                            --executor-cores 3 \
                            --executor-memory 8G \
                            --jars sql/mysql-connector-java-8.0.23.jar \
                            src/calculo_ruta_minima/dijkstra_spark.py \
                            --env cluster \
                            --sample_size {sample_size} \
                            --process {process} \
                            --creds {creds} \
                            --command_time {c_time} \
                            --origin {origin} \
                            --dest {dest}'''.format(origin=ruta[0], dest=ruta[1], sample_size=args.sample_size, process='dijkstra_spark', creds=args.creds, c_time=time.time())
            if 'dijkstra_spark' in blacklist:
                print('El proceso: dijkstra_spark será omitido.')
            else:
                os.system(spark_cmd)
        print('+----------------------------------+')
    else:
        ruta = rutas_dask.pop()
        print('+----------------------------------+')
        print('\tdijkstra - dask')
        if args.scheduler == None:
            dask_cmd = '''python \
                            src/calculo_ruta_minima/dijkstra_dask.py \
                            --sample_size {sample_size} \
                            --process {process} \
                            --creds {creds} \
                            --command_time {c_time} \
                            --env {env} \
                            --origin {origin} \
                            --dest {dest}'''.format(origin=ruta[0], dest=ruta[1], sample_size=args.sample_size, process='dijkstra_dask', creds=args.creds, c_time=time.time(), env=args.env)
            if 'dijkstra_dask' in blacklist:
                print('El proceso: dijkstra_dask será omitido.')
            else:
                os.system(dask_cmd)
        else:
            dask_cmd = '''~/miniconda/envs/dask_yarn/bin/python3 \
                            src/calculo_ruta_minima/dijkstra_dask.py \
                            --sample_size {sample_size} \
                            --process {process} \
                            --creds {creds} \
                            --env cluster \
                            --scheduler {scheduler} \
                            --command_time {c_time} \
                            --origin {origin} \
                            --dest {dest}'''.format(origin=ruta[0], dest=ruta[1], sample_size=args.sample_size, process='dijkstra_dask', creds=args.creds, scheduler=args.scheduler, c_time=time.time())
            if 'dijkstra_dask' in blacklist:
                print('El proceso: dijkstra_dask será omitido.')
            else:
                os.system(dask_cmd)
        print('+----------------------------------+')

if n_errores > 0:
    print('\n\tSe registraron {n_errores} errores.\n'.format(n_errores=n_errores))
else:
    print('\tNo se registraron errores.')
