# Importaciones necesarias
import dask.dataframe as dd
import time
import pandas as pd

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

def unir_columnas(df, col1, col2, col_resultante, meta=pd.DataFrame):
    """Esta función recibe como argumento un dataframe y dos columnas (col1, col2) cuyos valores concatenará en una columna nueva cuyo resultado
    corresponde al cuarto argumento. (col resultante)"""
    df[col_resultante] = df.apply(lambda row: str(row[col1]) + '-' + str(row[col2]), axis=1, meta='str')
    return df.drop(col1, axis=1).drop(col2, axis=1)

def conjuntos_rollup(columnas): # Hay que ver la forma de que se haga la agregación total
    conjuntos = list(map(lambda x: columnas[0:x+1], range(len(columnas))))
    return conjuntos

def group_by_rollup(df, columnas_agregacion, columnas_totales, agregaciones, meta=pd.DataFrame):
    columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
    try:
        resultado = df.groupby(columnas_agregacion).agg(agregaciones).reset_index().compute()
    except Exception as e:
        if 'nunique' in list(agregaciones.values()):
            resultado = df.groupby(columnas_agregacion).agg(nunique).reset_index().compute()
        else:
            print("\nHubo un error en la agregación.\n")
            print(e)
            print('\n')
    for columna in columnas_nulas:
        resultado[columna] = None
    return resultado.drop(columns=['X'])

def rollup(df, columnas, agregaciones, meta=list):
    df = df[list(set(columnas + list(agregaciones.keys())))]
    df['X'] = 1 # Esta es una columna temporal para hacer la agregación total
    columnas_totales = ["X"] + columnas  # Esta es una columna temporal que se utiliza para la agregacion total.
    conjuntos_columnas = conjuntos_rollup(columnas_totales)
    dataframes = list(map(lambda X: group_by_rollup(df, X, columnas_totales, agregaciones), conjuntos_columnas))
    return dataframes