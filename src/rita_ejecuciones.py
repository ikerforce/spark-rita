# Script para ejecutar
import os
import random
import argparse # Utilizado para leer archivo de configuracion
import json # Utilizado para leer archivo de configuracion

# Al ejecutar el archivo se debe de pasar el argumento --config /ruta/a/archivo/de/crecenciales.json
parser = argparse.ArgumentParser()
parser.add_argument("--config_spark", help="Ruta hacia archivo de configuracion")
parser.add_argument("--config_dask", help="Ruta hacia archivo de configuracion")
parser.add_argument("--creds", help="Ruta hacia archivo con credenciales de la base de datos")
args = parser.parse_args()

# 0 es dask
# 1 es spark
numero_de_ejecuciones = 3
pruebas = [1 for i in range(numero_de_ejecuciones)] + [0 for i in range(numero_de_ejecuciones)]
random.shuffle(pruebas)
while pruebas != []:
	x = pruebas.pop()
	if x == 1:
		print('\n\tspark\n')
		os.system('/opt/spark/bin/spark-submit src/rita_master_spark.py --creds ' + args.creds + ' --conf ' + args.config_spark)
	else:
		print('\n\tdask\n')
		os.system('python src/rita_master_dask.py --creds ' + args.creds + ' --conf ' + args.config_dask)
