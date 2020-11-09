# Importaciones necesarias
import dask.dataframe as dd
import time

nunique = dd.Aggregation('nunique', lambda s: s.nunique(), lambda s0: s0.sum()) # Definimos como hacer la agregacion para contar elementos únicos

def tiempo_ejecucion(t_inicial):
    """Esta función mide el tiempo transcurrido entre t_inicial y el momento en el que se llama la función.
    El resultado es un JSON con los campos: horas, minutos y segundos."""
    tiempo_segundos = time.time() - t_inicial
    tiempo = {}
    tiempo['horas'] = int(tiempo_segundos // 3600)
    tiempo['minutos'] = int(tiempo_segundos % 3600 // 60)
    tiempo['segundos'] = tiempo_segundos % 3600 % 60
    return tiempo

def conjuntos_rollup(columnas): # Hay que ver la forma de que se haga la agregación total
    conjuntos = [['X']] + list(map(lambda x: columnas[0:x+1], range(len(columnas))))
    return conjuntos

def group_by_rollup(df, columnas_agregacion, columnas_totales, agregaciones):
    columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
    try:
        resultado = df.groupby(columnas_agregacion).agg(agregaciones).reset_index().compute()
    except:
        if 'nunique' in list(agregaciones.values()):
            resultado = df.groupby(columnas_agregacion).agg(nunique).reset_index().compute()
        else:
            print("\nHubo un error en la agregación.")
    for columna in columnas_nulas:
        resultado[columna] = None
    return resultado.drop(columns=['X'])

def rollup(df, columnas, agregaciones):
    df = df[list(set(columnas + list(agregaciones.keys())))]
    df['X'] = 1 # Esta es una columna temporal para hacer la agregación total
    conjuntos_columnas = conjuntos_rollup(columnas)
    dataframes = list(map(lambda X: group_by_rollup(df, X, columnas, agregaciones), conjuntos_columnas))
    return dataframes