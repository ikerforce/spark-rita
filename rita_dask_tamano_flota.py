# Lectura de datos
import time
import dask.dataframe as dd
df = dd.read_csv('RITA_Datos/625622795_T_ONTIME_REPORTING.csv', dtype={'DIV2_AIRPORT': 'object',
       'DIV2_TAIL_NUM': 'object',
       'DIV_AIRPORT_LANDINGS': 'float64'})

def tiempo_ejecucion(t_inicial):
    tiempo_segundos = time.time() - t_inicial
    tiempo = {}
    tiempo['horas'] = int(tiempo_segundos // 3600)
    tiempo['minutos'] = int(tiempo_segundos % 3600 // 60)
    tiempo['segundos'] = tiempo_segundos % 3600 % 60
    return tiempo

t_inicio = time.time()

def conjuntos_rollup(columnas):
    conjuntos = list(map(lambda x: columnas[0:x+1], range(len(columnas))))
    return conjuntos

def group_by_rollup(df, columnas_agregacion, columnas_totales):
    print(columnas_agregacion)
    columnas_nulas = [item for item in columnas_totales if item not in columnas_agregacion]
    resultado = df.groupby(columnas_agregacion).agg(agregaciones).reset_index().compute()
    for columna in columnas_nulas:
        resultado[columna] = -1
    return resultado

def rollup(df, columnas, agregaciones):
    df = df[list(set(columnas + list(agregaciones.keys())))]
    conjuntos_columnas = conjuntos_rollup(columnas)
    dataframes = list(map(lambda X: group_by_rollup(df, X, columnas), conjuntos_columnas[1:]))
    return dataframes

conjuntos_rollup(['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'])

agregaciones = {'TAIL_NUM' : 'count'}

lista_df = rollup(df, ['OP_UNIQUE_CARRIER', 'YEAR', 'QUARTER', 'MONTH', 'DAY_OF_MONTH'], agregaciones)

print(lista_df[1].head())

print(tiempo_ejecucion(t_inicio))