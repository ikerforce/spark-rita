# Script para ejecutar
import os
import random
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion
import time

hora_ejecucion = time.strftime("%d_%m_%Y_%H_%M_%S") # 

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos.")
parser.add_argument("--ejecs", help="Determina el numero_de_ejecuciones que se haran.")
args = parser.parse_args()

from collections import ChainMap

def lee_config(path):
    f = open(path, "r").readlines()
    dicts = list(map(lambda x: convierte_en_dict(x.split("|")), f))
    paths = dict(ChainMap(*dicts))
    return paths

def convierte_en_dict(l):
    return {l[0]: {'dask':l[1], 'spark':l[2].replace('\n', '')}}


def orden_pruebas(numero_de_ejecuciones):
    '''Esta funcion regresa una lista de tamano 2 * numero_de_ejecuciones de ceros y unos.'''
    pruebas = [1 for i in range(numero_de_ejecuciones)] + [0 for i in range(numero_de_ejecuciones)]
    random.shuffle(pruebas)
    return pruebas


rutas = lee_config("conf/base/paths_to_file.csv")

# 0 es dask
# 1 es spark
numero_de_ejecuciones = int(args.ejecs)
procesos = list(rutas.keys())
pruebas_totales = dict(zip(list(rutas.keys()), [orden_pruebas(numero_de_ejecuciones) for i in range(len(rutas.keys()))]))
n_errores = 0
for proceso in procesos:
    for x in pruebas_totales[proceso]:
        if x == 1:
            try:
                print('\n\t' + proceso + ' spark\n')
                os.system('spark-submit src/rita_master_spark.py --creds ' + args.creds + ' --conf ' + rutas[proceso]['spark'])
            except Exception as e:
                n_errores += 1
                with open("rita_ejecuciones{fecha}.err".format(fecha=hora_ejecucion), "a") as myfile:
                    myfile.write('\n\t' + proceso + ' spark\n' + e)
        else:
            try:
                print('\n\t' + proceso + ' dask\n')
                os.system('python src/rita_master_dask.py --creds ' + args.creds + ' --conf ' + rutas[proceso]['dask'])
            except Exception as e:
                n_errores += 1
                with open("rita_ejecuciones{fecha}.err".format(fecha=hora_ejecucion), "a") as myfile:
                    myfile.write('\n\t' + proceso + ' dask\n' + e)

if n_errores > 0:
    print('\n\tSe registraron {n_errores} errores.\n'.format(n_errores=n_errores))
else:
    print('\n\tNo se registraron errores.')