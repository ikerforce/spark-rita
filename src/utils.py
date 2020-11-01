import dask.dataframe as dd
import time

nunique = dd.Aggregation('nunique', lambda s: s.nunique(), lambda s0: s0.sum())

def tiempo_ejecucion(t_inicial):
    tiempo_segundos = time.time() - t_inicial
    tiempo = {}
    tiempo['horas'] = int(tiempo_segundos // 3600)
    tiempo['minutos'] = int(tiempo_segundos % 3600 // 60)
    tiempo['segundos'] = tiempo_segundos % 3600 % 60
    return tiempo

def conjuntos_rollup(columnas): # Hay que ver la forma de que se haga la agregacion totalgit 
    conjuntos = list(map(lambda x: columnas[0:x+1], range(len(columnas))))
    return conjuntos

def group_by_rollup(df, columnas_agregacion, columnas_totales):
    columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
    resultado = df.groupby(columnas_agregacion).agg(nunique).reset_index().compute()
    for columna in columnas_nulas:
        resultado[columna] = None
    return resultado

def rollup(df, columnas, agregaciones):
    df = df[list(set(columnas + list(agregaciones.keys())))]
    conjuntos_columnas = conjuntos_rollup(columnas)
    dataframes = list(map(lambda X: group_by_rollup(df, X, columnas), conjuntos_columnas))
    return dataframes