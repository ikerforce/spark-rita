# Conociendo a los datos

En esta sección el objetivo es conocer el conjunto de datos, entender las columnas que contiene y conocer qué análisis podemos hacer a través de cada una de ellas. El dataset consiste en datos sobre vuelos que sucedieron dentro de Estados Unidos. Cada renglón del dataset corresponde a un vuelo.

## Conociendo las columnas

El primer paso será entender qué datos contiene cada una de las columnas, por lo que las listaremos y daremos una breve explicación de qué información contiene cada una de ellas y el tipo de cada uno de los datos. Esta información está disponible en https://www.transtats.bts.gov/Fields.asp

1. YEAR: (Entero) Año en el que sucedió el vuelo.
2. QUARTER: (Entero entre 1 y 4) Trimestre en el que sucedió el vuelo.
3. MONTH: (Entero entre 1 y 12) Mes en el que sucedió el vuelo.
4. DAY_OF_MONTH: (Entero entre 1 y 31) Día en el que sucedió el vuelo.
5. DAY_OF_WEEK: (Entero entre 1 y 7) Día de la semana en en el que sucedió el vuelo. El 1 corresponde al Lunes.
6. FL_DATE: (String en formato yyyymmdd) Fecha en la que sucedió el vuelo
7. OP_UNIQUE_CARRIER: (String) Código único de cada aerolínea. El catálogo está en https://www.transtats.bts.gov/Oneway.asp?Field_Desc=Unique%20Carrier%20Code.%20When%20the%20same%20code%20has%20been%20used%20by%20multiple%20carriers%2C%20a%20numeric%20suffix%20is%20used%20for%20earlier%20users%2C%20for%20example%2C%20PA%2C%20PA%281%29%2C%20PA%282%29.%20Use%20this%20field%20for%20analysis%20across%20a%20range%20of%20years.&Field_Type=Char&Sel_Cat=OP_UNIQUE_CARRIER&Lookup_Table=L_UNIQUE_CARRIERS&Sel_Var=PCT_ONTIME_ARR&Sel_Stat=N/A&Data_Type=CAT&Percent_Flag=0&Display_Flag=0
8. OP_CARRIER_AIRLINE_ID: (Int) Un número asignado por US DOT para identificar una aerolínea de manera única.
9. OP_CARRIER: (String) Es un código para cada aerolínea pero puede no ser único ya que estos códigos cambian con el tiempo.
10. TAIL_NUM: (String) La matrícula que identifica de manera única a un avión.
11. OP_CARRIER_FL_NUM: (String) Número de vuelo por aerolínea.
12. ORIGIN_AIRPORT_ID: (Int) Número que identifica de forma única al aeropuerto de origen. 
13. ORIGIN_AIRPORT_SEQ_ID: (Int) Número que identifica de forma única a un aeropuerto de origen en un tiempo determinado. Este código se puede reutilizar a través del tiempo o puede cambiar para un aeropuerto determinado.
14. ORIGIN_CITY_MARKET_ID: (Int) Número que identifica de forma única la ciudad mercado de un aeropuerto de origen. Sirve para unir aeropuertos que sirven al mismo mercado de ciudades.
15. ORIGIN: (String) Código de 3 lestras que identifica a cada aeropuerto de orgien.
16. ORIGIN_CITY_NAME: (String) Ciudad de origen del vuelo.
17. ORIGIN_STATE_ABR: (String) Código en letras del estado origen del vuelo.
18. ORIGIN_STATE_FIPS: (String) Código en número del estado de origen del vuelo.
19. ORIGIN_STATE_NM: (String) Nombre del estado de origen del vuelo.
20. ORIGIN_WAC: (Int) World Area Code de origen del vuelo.
21. DEST_AIRPORT_ID: (Int) Número que identifica de forma única al aeropuerto de destino.
22. DEST_AIRPORT_SEQ_ID: (Int) Número que identifica de forma única a un aeropuerto de destino en un tiempo determinado. Este código se puede reutilizar a través del tiempo o puede cambiar para un aeropuerto determinado.
23. DEST_CITY_MARKET_ID: (Int) Número que identifica de forma única la ciudad mercado de un aeropuerto de destino. Sirve para unir aeropuertos que sirven al mismo mercado de ciudades.
24. DEST: (String) Código de 3 letras que identifica a cada aeropuerto destino.
25. DEST_CITY_NAME: (String) Ciudad de destino del vuelo.
26. DEST_STATE_ABR: (String) Código en letras del estado destino del vuelo.
27. DEST_STATE_FIPS: (String) Código en número del estado de destino del vuelo.
28. DEST_STATE_NM: (String) Nombre del estado de destino del vuelo.
29. DEST_WAC: (Int) World Area Code de destino del vuelo.
30. CRS_DEP_TIME: (String hhmm) Hora local de salida programada. 
31. DEP_TIME: (String hhmm) Hora local de salida real.
32. DEP_DELAY: (Int) Diferencia en minutos entre la hora de salida programada y la hora de salida real.
33. DEP_DELAY_NEW: (Int) Diferencia en minutos entre la hora de salida programada y la hora de salida real. Las salidas anticipadas se registran como 0.
34. DEP_DEL15: (Booleano 1=Si): Indica si el retraso fue mayor a 15 minutos. 
35. DEP_DELAY_GROUP: (Int): Grupo de retraso del vuelo en intervalos de 15 minutos. Si el vuelo salió antes de 15 minutos de lo anticipado el grupo es -2. Si salió entre 15 y 1 minutos antes de lo anticipado es -1. Si salió entre 0 y 14 minutos después de lo porgramado es 1 y así sucesivamente.
36. DEP_TIME_BLK: (String hhmm-hhmm) Intervalo de tiempo en el que estaba programado para salir el vuelo.
37. TAXI_OUT: (Float) Tiempo en minutos entre que el avión deja la plataforma y el despegue.
38. WHEELS_OFF: (Int) Hora local de despegue del avión.
39. WHEELS_ON: (Int) Hora local de llegada del avión.
40. TAXI_IN: (Int) Tiempo entre el aterrizaje y la llegada del avión a la plataforma de desembarco.
41. CRS_ARR_TIME: (String hhmm) Hora local de llegada programada. 
42. ARR_TIME: (String hhmm) Hora local de llegada real.
43. ARR_DELAY: (Int) Diferencia en minutos entre la hora de salida programada y la hora de salida real.
44. ARR_DELAY_NEW: (Int) Diferencia en minutos entre la hora de llegada programada y la hora de llegada real. Las llegadas anticipadas se registran como 0.
45. ARR_DEL15: (Booleano 1=Si): Indica si el retraso fue mayor a 15 minutos. 
46. ARR_DELAY_GROUP: (Int): Grupo de retraso del vuelo en intervalos de 15 minutos. Si el vuelo llegó antes de 15 minutos de lo anticipado el grupo es -2. Si llegó entre 15 y 1 minutos antes de lo anticipado es -1. Si llegó entre 0 y 14 minutos después de lo porgramado es 1 y así sucesivamente.
47. ARR_TIME_BLK: (String hhmm-hhmm) Intervalo de tiempo en el que estaba programado para llegar el vuelo.
48. CANCELLED: (Booleano 1=Si): Indica si el vuelo fue cancelado.
49. CANCELLATION_CODE: (String) Especifica la causa de la cancelación.
50. DIVERTED: (Booleano 1=Si): Indica si el vuelo fue desviado.
51. CRS_ELAPSED_TIME: (Float) Duración del vuelo programada en minutos.
52. ACTUAL_ELAPSED_TIME: (Float) Duración real del vuelo.
53. AIR_TIME: (Float) Tiempo en el aire (Duración sin contar TAXI IN, TAXI OUT).
54. FLIGHTS: Número de vuelos.
55. DISTANCE: (Float) Distancia entre dos aeropuertos (En millas).
56. DISTANCE_GROUP: (Int Categorica) Categoria de distancia del vuelo.
57. CARRIER_DELAY: (Float) Retraso provocado por la aerolínea en minutos.
58. WEATHER_DELAY: (Float) Retraso provocado por el clima en minutos.
59. NAS_DELAY: (Float) Retraso provocado por el National Air System en minutos.
60. SECURITY_DELAY: (Float) Retraso provocado por seguridad en minutos.
61. LATE_AIRCRAFT_DELAY: (Float) Retraso provocado por la llegada tardía del avión.
62. FIRST_DEP_TIME: (Int hhmm) Primera hora de salida de la puerta en el aeropuerto de origen para vuelos que fueron retrasados o cancelados.
63. TOTAL_ADD_GTIME: (Float) Tiempo que estuvo fuera de la puerta o que tardó en regresar a la puerta para un vuelo retrasado o cancelado.
64. LONGEST_ADD_GTIME: (Float) Tiempo que estuvo fuera de la puerta o que tardó en regresar a la puerta para un vuelo retrasado o cancelado.
65. DIV_AIRPORT_LANDINGS: (Int) Número de aterrizajes que hizo el avión en aeropuertos a los que fue desviado. 9 es que el avión regresó al aeropuerto de origen después del despegue.
66. DIV_REACHED_DEST: (Booleano 1=Si) El avión alcanzó el destino programado después de ser desviado.
67. DIV_ACTUAL_ELAPSED_TIME: (Float) Tiempo de duración entre salida y llegada al destino programado tomando en cuenta desviaciones. Cuando un vuelo es desviado se deja vacía la columna Elapsed Time
68. DIV_ARR_DELAY: (Float) Diferencia en minutos entre la salida del vuelo y la llegada en el destino programado. La columna ARR_DELAY permanece vacía para los vuelos desviados.
69. DIV_DISTANCE: Distancia en millas entre aeropuerto de llegada y el aeropuerto de llegada programado. Es cero cuando el vuelo llega al destino programado.
70. DIV1_AIRPORT: (String): Código en letras del primer aeropuerto al que el vuelo fue desviado.
71. DIV1_AIRPORT_ID: (Int) Número que identifica de forma única al primer aeropuerto al que el vuelo fue desviado.
72. DIV1_AIRPORT_SEQ_ID: (Int) Número que identifica de forma única al primer aeropuerto al que el vuelo fue desviado en un tiempo determinado. Este código se puede reutilizar a través del tiempo o puede cambiar para un aeropuerto determinado.
73. DIV1_WHEELS_ON: (Int hhmm) Hora local de aterrizaje en el primer aeropuerto al que el vuelo fue desviado.
74. 'DIV1_TOTAL_GTIME: (Float) Tiempo en minutos que el vuelo estuvo fuera de la puerta en el primer aeropuerto al que fue desviado.
75. DIV1_LONGEST_GTIME: (Float) Máximo tiempo fuera de la puerta en el que el avión fue desviado.
76. DIV1_WHEELS_OFF: (Int hhmm) Hora de despegue en el primer aeropuerto en el que el vuelo fue desviado.
77. DIV1_TAIL_NUM: (String) Número de cola del avión en el primer aeropuerto en el que fue desviado.

Las siguientes columnas son equivalentes a las 8 anteriores pero con información correspondiente al segundo, tercero, cuarto y quinto aeropuerto al que fue desviado el vuelo. Cuando el vuelo no fue desviado estos campos se mantienen como nulos.

78. 'DIV2_AIRPORT',
79. 'DIV2_AIRPORT_ID',
80. 'DIV2_AIRPORT_SEQ_ID',
81. 'DIV2_WHEELS_ON',
82. 'DIV2_TOTAL_GTIME',
83. 'DIV2_LONGEST_GTIME',
84. 'DIV2_WHEELS_OFF',
85. 'DIV2_TAIL_NUM',
86. 'DIV3_AIRPORT',
87. 'DIV3_AIRPORT_ID',
88. 'DIV3_AIRPORT_SEQ_ID',
89. 'DIV3_WHEELS_ON',
90. 'DIV3_TOTAL_GTIME',
91. 'DIV3_LONGEST_GTIME',
92. 'DIV3_WHEELS_OFF',
93. 'DIV3_TAIL_NUM',
94. 'DIV4_AIRPORT',
95. 'DIV4_AIRPORT_ID',
96. 'DIV4_AIRPORT_SEQ_ID',
97. 'DIV4_WHEELS_ON',
98. 'DIV4_TOTAL_GTIME',
99. 'DIV4_LONGEST_GTIME',
100. 'DIV4_WHEELS_OFF',
101. 'DIV4_TAIL_NUM',
102. 'DIV5_AIRPORT',
103. 'DIV5_AIRPORT_ID',
104. 'DIV5_AIRPORT_SEQ_ID',
105. 'DIV5_WHEELS_ON',
106. 'DIV5_TOTAL_GTIME',
107. 'DIV5_LONGEST_GTIME',
108. 'DIV5_WHEELS_OFF',
109. 'DIV5_TAIL_NUM'


```python

```
