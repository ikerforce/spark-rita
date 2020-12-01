# spark-rita
Repositorio de desarrollo de arquitectura de datos para el dataset RITA de TranStat en Spark.

## Cálculo de demoras en rutas

El archivo `rita_spark_demoras_en_rutas.py` calcula la duración y tiempo de demora promedio de cada una de las rutas registradas en en el periodo considerado. Se debe utilizar junto con un archivo de configuración cuyo esquema se ejemplifica en el archivo `config_dummy.json`.

Para ejecutarlo se requeren los datos disponibles en `https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236`. Los campos utilizados para la ejecucución del código se detallan en al archivo `glosario_campos.md`. 

El periodo considerado es de Enero de 2008 a Diciembre de 2020.

### Requermientos

Para ejecutar el código se utizaron los siguientes componentes de software:

- MySQL 8.0.21-0ubuntu0.20.04.4 for Linux on x86_64 con usuario y contraseña con permisos de lectura y escritura.
- Apache Spark 2.4.4.
- Python 2.7.18.

Además, ese necesario ingestar los datos de `https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236` en una tabla y base de datos de MySQL en la que también escribiremos el resultado del código y datos del tiempo de ejecución. 

### Ejecución

La ejecución del archivo se hizo en modo local usando 4GB de memoria y 8 _cores_. 

Actualmente el proceso consiste de 5 procesos en Spark y 5 en Dask. Cada proceso en Spark tiene un proceso equivalente en Dask para poder comparar el rendimiento de cada uno de ellos.

El siguiente comando es utilizado para ejecutar cada proceso de Spark:
```
spark-submit src/rita_master_spark.py --creds /ruta/a/credenciales.json --conf ruta/a/archivos/de/configuracion.json
```

Dependiendo del archivo de configuración se ejecutará un proceso distinto.

__demoras_ruta_spark__ Este proceso calcula las retrasos de los aviones de acuerdo a la ruta que recorrió el vuelo.

__demoras_aeropuerto_origen_spark__ Este proceso calcula las retrasos de los aviones respecto a su aeropuerto de origen y al market id de origen.

__demoras_aeropuerto_destino_spark__ Este proceso calcula las retrasos de los aviones respecto a su aeropuerto de destino y al market id de destino.

__demoras_aerolinea_spark__ Este proceso calcula las retrasos de los aviones respecto a la aerolínea a la que pertenece.

__flota_spark__ Este proceso calcula el tamaño de la flota de una aerolínea.
