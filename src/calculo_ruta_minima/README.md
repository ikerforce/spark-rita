# Algoritmo ruta minima

El objetivo de este algoritmo es encontrar la ruta más rápida entre dos aeropuertos.

## Pseudocódigo

### Variables iniciales:
- `t_acumulado = 0` (Tiempo acumulado hasta el momento).
- `nodo_actual = origen` (Último nodo visitado).
- `frontera = []` (Lista de nodos por visitar a los que existe un vuelo directo desde los vuelos visitados).
- `visitados = {}` (Diccionario vacío en la que almaceno la ruta óptima).
- `df` (Tabla con información de vuelos posibles).
- `encontro_ruta = True` (Variable que determina si una ruta fue encontrada).

### Pasos del algoritmo

- Defino la `frontera` como el primer vuelo directo disponible entre ambos aeropuertos.
- Si la `frontera` no está vacía (hay vuelo directo) entonces obtengo los datos del vuelo y termina la ejecución.
- En otro caso, inicio un ciclo que aplica Dijkstra para encontrar la ruta más rápida entre ambos aeropuertos:
- Ciclo: Mientras la frontera no esté vacía y el nodo actual sea diferente del nodo destino:
	- Elimino los vuelos que tengan como destino el nodo actual de los vuelos totales.
	- Agrego a la frontera los vuelos que tengan como origen el nodo actual y que tenga horario de salida al menos dos horas mayor al horario de llegada al nodo actual.
	- Actualizo el valor `t_acumulado` para cada vuelo, sumando el la duración del vuelo y el tiempo entre la llegada del vuelo anterior y la salida del vuelo actual con el `t_acumulado`.
	- Si la frontera está vacía salgo del ciclo y actualizo el valor de la variable `encontro_ruta` a `False`.
	- De la frontera, obtengo el vuelo que más pronto alcanza su destino y actualizo el `nodo actual` (destino del vuelo elegido), el `nodo_anterior` (origen del vuelo elegido), `early_arr` (Tiempo de llegada más pronto posible).
	- Actualizo el valor de la variable `min_dep_epoch` (La hora mínima a partir de la cual puede salir el siguiente vuelo) a la hora de llegada del vuelo elegido más 2 horas de conexión.
	- Elimino de la frontera los vuelos que llegan al nodo actual y que tardan más tiempo en llegar (`t_acumulado > early_arr`).
	- Elimino de los vuelos totales los vuelos que salen antes de `min_dep_epoch` (No puedo tomar vuelos en el pasado o con menos de dos horas de tiempo de conexión) y tienen como origen el nodo actual.
- Una vez terminado el círculo, si `encontro_ruta=True` recorro el dicicionario visitados para devolver la ruta. En otro caso se avisa que no hay ruta entre ambos aeropuertos.

## Ejecución en spark

El algoritmo se ejecuta con el siguiente comando y utiliza el archivo de configuración: `src/`.

```
spark-submit \
	--driver-memory=8g \
	src/calculo_ruta_minima/dijkstra_spark.py \
	--conf src/transtat_config_spark_dijkstra.json \
	--creds conf/mysql_creds.json \
	--origin <codigo_del_aeropuerto_origen> \
	--dest <codigo_del_aeropuerto_destino> \
	--dep_date <AAAA-MM-DD>
```

Ejemplo:

```
spark-submit \
	--driver-memory=8g \
	src/calculo_ruta_minima/dijkstra_spark.py \
	--conf src/transtat_config_spark_dijkstra.json \
	--creds conf/mysql_creds.json \
	--origin SFO \
	--dest SJC \
	--dep_date 2008-10-10
```


## Ejecución en Dask

El algoritmo se ejecuta con el siguiente comando y utiliza el archivo de configuración: `src/`.
```
python src/calculo_ruta_minima/dijkstra_dask.py \
	--config src/transtat_config_dask_dijkstra.json \
	--creds conf/mysql_creds.json \
	--origin <codigo_del_aeropuerto_origen> \
	--dest <codigo_del_aeropuerto_destino> \
	--dep_date <AAAA-MM-DD>
```

Ejemplo: 

```
python src/calculo_ruta_minima/dijkstra_dask.py \
	--config src/transtat_config_dask_dijkstra.json \
	--creds conf/mysql_creds.json \
	--origin SFO \
	--dest SJC \
	--dep_date 2008-10-10
```