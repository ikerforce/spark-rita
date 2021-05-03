# spark-rita

El volumen de datos y las capacidades de almacenamiento han aumentado de forma considerable en las últimas décadas. Esto ha incrementado la necesidad de desarrollar _software_ de procesamiento de datos que utilice cómputo en paralelo y distribuido para poder procesar los conjuntos masivos de datos creados diariamente. Dos de las herramientas de código abierto más populares para el desarrollo de aplicaciones de ciencia de datos y de ejecución de algoritmos distribuidos son _Dask_ y _Spark_. El objetivo de esta investigación es comparar su desempeño al realizar operaciones como obtención de estadísticas y ejecución de algoritmos sobre un conjunto de datos grande (43 GB aproximadamente), correspondiente a vuelos dentro de Estados Unidos. Los diferentes procesos se realizarán por ambos _softwares_, en un orden aleatorio y con un número específico de ejecuciones, en un ambiente local y otro en la nube. Además, en ambos algoritmos se implementará una versión paralelizada y distribuida de un algoritmo de ruta mínima. Los resultados serán comparados para determinar bajo qué circunstancias un _software_ es superior e identificar las características que permiten ese desempeño.

Este proceso contempla los siguientes 8 procesos para comparar el desempeño:

- Proceso 1: Cálculo del tamaño de la flota por aerolínea
- Proceso 2: Demoras por aerolínea
- Proceso 3: Demora máxima por aeropuerto de origen
- Proceso 4: Demora mínima por aeropuerto de destino
- Proceso 5: Demora promedio por ruta entre aeropuertos
- Proceso 6: Desviación estándar de la demora por ruta entre _market ids_
- Proceso 7: Eliminación de nulos
- Proceso 8: Cálculo de la menor ruta en tiempo utilizando el algoritmo de ruta mínima _Dijkstra_


### Requerimientos

Para ejecutar el código de manera local se utilizaron los siguientes componentes de software:

- `MySQL 8.0.21-0ubuntu0.20.04.4` for Linux on x86_64 con credenciales para lectura y escritura.
- `Apache Spark 2.4.4`.
- `Python 2.7.18`.
- Los paquetes contenidos en el archivo `conf/conda_envs/dask_yarn.yml` de este repositorio.

Además, se necesitan los siguientes datos que fueron preprocesados con los scripts `src/preprocessing/data_prep_dask.py` y `src/preprocessing/data_prep_spark.py` y muestreados con el script `src/muestreo_spark.py`:

- Los datos de `https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236` en formato parquet. Una muestra de 10K registros de estos datos se puede encontrar en la ruta `samples/` de este repositorio. Estos archivos tienen una versión con los _data types_ adeacuados para _PySpark_ y otra para _Dask_.

También se necesitan los siguientes archivos de configuración:

- Un archivo de configuración como el de la ruta `conf/base/configs.csv` para determinar la forma en la que se escriben y leen los datos y detalles del ambiente acompañados de la ruta en la que están ubicados.
- Un archivo con las credenciales de acceso a la base de datos como el que está ubicado en la ruta: `conf/mysql_creds_dummy.json`.

### Ejecución

El siguiente comando es utilizado para ejecutar los procesos en ambos _softwares_:
```
python rita_ejecuciones.py \
	--ejecs <número_de_ejecuciones> \
	--creds <ruta_a_archivo_de_credenciales> \
	--sample_size <tamaño_de_la_muestra>
```

- Los valores de `sample_size` pueden ser 10K, 100K, 1M, 10M y TOTAL siempre y cuando estén disponibles los datos de esos tamaños. En este repositorio sólo se incluye el de 10K.
- El parámetro `ejecs` determina el número de veces que se ejecutará cada uno de los procesos.