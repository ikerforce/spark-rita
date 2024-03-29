-- Cracion de tabla con muestra de 10000 regitstos
CREATE TABLE `RITA_20` AS
SELECT CAST(`YEAR` AS UNSIGNED) AS YEAR
  , CAST(`QUARTER` AS UNSIGNED) AS QUARTER
  , CAST(`MONTH` AS UNSIGNED) AS MONTH
  , CAST(`DAY_OF_MONTH` AS UNSIGNED) AS DAY_OF_MONTH
  , CAST(`DAY_OF_WEEK` AS UNSIGNED) AS DAY_OF_WEEK
  , CAST(`FL_DATE` AS DATE) AS FL_DATE
  , CAST(`OP_UNIQUE_CARRIER` AS CHAR(2)) AS OP_UNIQUE_CARRIER
  , CAST(`OP_CARRIER_AIRLINE_ID` AS UNSIGNED) AS OP_CARRIER_AIRLINE_ID
  , CAST(`OP_CARRIER` AS CHAR(2)) AS OP_CARRIER
  , CAST(`TAIL_NUM` AS CHAR(6)) AS TAIL_NUM
  , CAST(`OP_CARRIER_FL_NUM` AS UNSIGNED) AS OP_CARRIER_FL_NUM
  , CAST(`ORIGIN_AIRPORT_ID` AS UNSIGNED) AS ORIGIN_AIRPORT_ID
  , CAST(`ORIGIN_AIRPORT_SEQ_ID` AS UNSIGNED) AS ORIGIN_AIRPORT_SEQ_ID
  , CAST(`ORIGIN_CITY_MARKET_ID` AS UNSIGNED) AS ORIGIN_CITY_MARKET_ID
  , CAST(`ORIGIN` AS CHAR(3)) AS ORIGIN
  , CAST(`ORIGIN_CITY_NAME` AS CHAR(50)) AS ORIGIN_CITY_NAME
  , CAST(`ORIGIN_STATE_ABR` AS CHAR(3)) AS ORIGIN_STATE_ABR
  , CAST(`ORIGIN_STATE_FIPS` AS DECIMAL(10,3)) AS ORIGIN_STATE_FIPS
  , CAST(`ORIGIN_STATE_NM` AS CHAR(30)) AS ORIGIN_STATE_NM
  , CAST(`ORIGIN_WAC` AS UNSIGNED) AS ORIGIN_WAC
  , CAST(`DEST_AIRPORT_ID` AS UNSIGNED) AS DEST_AIRPORT_ID
  , CAST(`DEST_AIRPORT_SEQ_ID` AS UNSIGNED) AS DEST_AIRPORT_SEQ_ID
  , CAST(`DEST_CITY_MARKET_ID` AS UNSIGNED) AS DEST_CITY_MARKET_ID
  , CAST(`DEST` AS CHAR(3)) AS DEST
  , CAST(`DEST_CITY_NAME` AS CHAR(50)) AS DEST_CITY_NAME
  , CAST(`DEST_STATE_ABR` AS CHAR(3)) AS DEST_STATE_ABR
  , CAST(`DEST_STATE_FIPS` AS DECIMAL(10,3)) AS DEST_STATE_FIPS
  , CAST(`DEST_STATE_NM` AS CHAR(30)) AS DEST_STATE_NM
  , CAST(`DEST_WAC` AS UNSIGNED) AS DEST_WAC
  , CAST(`CRS_DEP_TIME` AS CHAR(4)) AS CRS_DEP_TIME
  , CAST(`DEP_TIME` AS CHAR(4)) AS DEP_TIME
  , CAST(`DEP_DELAY` AS DECIMAL(10,3)) AS DEP_DELAY
  , CAST(`DEP_DELAY_NEW` AS DECIMAL(10,3)) AS DEP_DELAY_NEW
  , CAST(`DEP_DEL15` AS DECIMAL(10,3)) AS DEP_DEL15
  , CAST(`DEP_DELAY_GROUP` AS DECIMAL(10,3)) AS DEP_DELAY_GROUP
  , CAST(`DEP_TIME_BLK` AS CHAR(9)) AS DEP_TIME_BLK
  , CAST(`TAXI_OUT` AS DECIMAL(10,3)) AS TAXI_OUT
  , CAST(`WHEELS_OFF` AS CHAR(4)) AS WHEELS_OFF
  , CAST(`WHEELS_ON` AS CHAR(4)) AS WHEELS_ON
  , CAST(`TAXI_IN` AS DECIMAL(10,3)) AS TAXI_IN
  , CAST(`CRS_ARR_TIME` AS CHAR(4)) AS CRS_ARR_TIME
  , CAST(`ARR_TIME` AS CHAR(4)) AS ARR_TIME
  , CAST(`ARR_DELAY` AS DECIMAL(10,3)) AS ARR_DELAY
  , CAST(`ARR_DELAY_NEW` AS DECIMAL(10,3)) AS ARR_DELAY_NEW
  , CAST(`ARR_DEL15` AS DECIMAL(10,3)) AS ARR_DEL15
  , CAST(`ARR_DELAY_GROUP` AS DECIMAL(10,3)) AS ARR_DELAY_GROUP
  , CAST(`ARR_TIME_BLK` AS CHAR(9)) AS ARR_TIME_BLK
  , CAST(`CANCELLED` AS DECIMAL(10,3)) AS CANCELLED
  , CAST(`CANCELLATION_CODE` AS CHAR(1)) AS CANCELLATION_CODE 
  , CAST(`DIVERTED` AS DECIMAL(10,3)) AS DIVERTED
  , CAST(`CRS_ELAPSED_TIME` AS DECIMAL(10,3)) AS CRS_ELAPSED_TIME
  , CAST(`ACTUAL_ELAPSED_TIME` AS DECIMAL(10,3)) AS ACTUAL_ELAPSED_TIME
  , CAST(`AIR_TIME` AS DECIMAL(10,3)) AS AIR_TIME
  , CAST(`FLIGHTS` AS DECIMAL(10,3)) AS FLIGHTS
  , CAST(`DISTANCE` AS DECIMAL(10,3)) AS DISTANCE
  , CAST(`DISTANCE_GROUP` AS DECIMAL(10,3)) AS DISTANCE_GROUP
  , CAST(`CARRIER_DELAY` AS DECIMAL(10,3)) AS CARRIER_DELAY
  , CAST(`WEATHER_DELAY` AS DECIMAL(10,3)) AS WEATHER_DELAY
  , CAST(`NAS_DELAY` AS DECIMAL(10,3)) AS NAS_DELAY
  , CAST(`SECURITY_DELAY` AS DECIMAL(10,3)) AS SECURITY_DELAY
  , CAST(`LATE_AIRCRAFT_DELAY` AS DECIMAL(10,3)) AS LATE_AIRCRAFT_DELAY
  , CAST(`FIRST_DEP_TIME` AS CHAR(4)) AS FIRST_DEP_TIME
  , CAST(`TOTAL_ADD_GTIME` AS DECIMAL(10,3)) AS TOTAL_ADD_GTIME
  , CAST(`LONGEST_ADD_GTIME` AS DECIMAL(10,3)) AS LONGEST_ADD_GTIME
  , CAST(`DIV_AIRPORT_LANDINGS` AS DECIMAL(10,3)) AS DIV_AIRPORT_LANDINGS
  , CAST(`DIV_REACHED_DEST` AS DECIMAL(10,3)) AS DIV_REACHED_DEST
  , CAST(`DIV_ACTUAL_ELAPSED_TIME` AS DECIMAL(10,3)) AS DIV_ACTUAL_ELAPSED_TIME
  , CAST(`DIV_ARR_DELAY` AS DECIMAL(10,3)) AS DIV_ARR_DELAY
  , CAST(`DIV_DISTANCE` AS DECIMAL(10,3)) AS DIV_DISTANCE
  , CAST(`DIV1_AIRPORT` AS CHAR(3)) AS DIV1_AIRPORT
  , CAST(`DIV1_AIRPORT_ID` AS UNSIGNED) AS DIV1_AIRPORT_ID
  , CAST(`DIV1_AIRPORT_SEQ_ID` AS UNSIGNED) AS DIV1_AIRPORT_SEQ_ID
  , CAST(`DIV1_WHEELS_ON` AS CHAR(4)) AS DIV1_WHEELS_ON
  , CAST(`DIV1_TOTAL_GTIME` AS DECIMAL(10,3)) AS DIV1_TOTAL_GTIME
  , CAST(`DIV1_LONGEST_GTIME` AS DECIMAL(10,3)) AS DIV1_LONGEST_GTIME
  , CAST(`DIV1_WHEELS_OFF` AS CHAR(4)) AS DIV1_WHEELS_OFF
  , CAST(`DIV1_TAIL_NUM` AS CHAR(6)) AS DIV1_TAIL_NUM
  , CAST(`DIV2_AIRPORT` AS CHAR(3)) AS DIV2_AIRPORT
  , CAST(`DIV2_AIRPORT_ID` AS UNSIGNED) AS DIV2_AIRPORT_ID
  , CAST(`DIV2_AIRPORT_SEQ_ID` AS UNSIGNED) AS DIV2_AIRPORT_SEQ_ID
  , CAST(`DIV2_WHEELS_ON` AS CHAR(4)) AS DIV2_WHEELS_ON
  , CAST(`DIV2_TOTAL_GTIME` AS DECIMAL(10,3)) AS DIV2_TOTAL_GTIME
  , CAST(`DIV2_LONGEST_GTIME` AS DECIMAL(10,3)) AS DIV2_LONGEST_GTIME
  , CAST(`DIV2_WHEELS_OFF` AS CHAR(4)) AS DIV2_WHEELS_OFF
  , CAST(`DIV2_TAIL_NUM` AS CHAR(6)) AS DIV2_TAIL_NUM
  , CAST(`DIV3_AIRPORT` AS CHAR(3)) AS DIV3_AIRPORT
  , CAST(`DIV3_AIRPORT_ID` AS UNSIGNED) AS DIV3_AIRPORT_ID
  , CAST(`DIV3_AIRPORT_SEQ_ID` AS UNSIGNED) AS DIV3_AIRPORT_SEQ_ID
  , CAST(`DIV3_WHEELS_ON` AS CHAR(4)) AS DIV3_WHEELS_ON
  , CAST(`DIV3_TOTAL_GTIME` AS DECIMAL(10,3)) AS DIV3_TOTAL_GTIME
  , CAST(`DIV3_LONGEST_GTIME` AS DECIMAL(10,3)) AS DIV3_LONGEST_GTIME
  , CAST(`DIV3_WHEELS_OFF` AS CHAR(4)) AS DIV3_WHEELS_OFF
  , CAST(`DIV3_TAIL_NUM` AS CHAR(6)) AS DIV3_TAIL_NUM
  , CAST(`DIV4_AIRPORT` AS CHAR(3)) AS DIV4_AIRPORT
  , CAST(`DIV4_AIRPORT_ID` AS UNSIGNED) AS DIV4_AIRPORT_ID
  , CAST(`DIV4_AIRPORT_SEQ_ID` AS UNSIGNED) AS DIV4_AIRPORT_SEQ_ID
  , CAST(`DIV4_WHEELS_ON` AS CHAR(4)) AS DIV4_WHEELS_ON
  , CAST(`DIV4_TOTAL_GTIME` AS DECIMAL(10,3)) AS DIV4_TOTAL_GTIME
  , CAST(`DIV4_LONGEST_GTIME` AS DECIMAL(10,3)) AS DIV4_LONGEST_GTIME
  , CAST(`DIV4_WHEELS_OFF` AS CHAR(4)) AS DIV4_WHEELS_OFF
  , CAST(`DIV4_TAIL_NUM` AS CHAR(6)) AS DIV4_TAIL_NUM
  , CAST(`DIV5_AIRPORT` AS CHAR(3)) AS DIV5_AIRPORT
  , CAST(`DIV5_AIRPORT_ID` AS UNSIGNED) AS DIV5_AIRPORT_ID
  , CAST(`DIV5_AIRPORT_SEQ_ID` AS UNSIGNED) AS DIV5_AIRPORT_SEQ_ID
  , CAST(`DIV5_WHEELS_ON` AS CHAR(4)) AS DIV5_WHEELS_ON
  , CAST(`DIV5_TOTAL_GTIME` AS DECIMAL(10,3)) AS DIV5_TOTAL_GTIME
  , CAST(`DIV5_LONGEST_GTIME` AS DECIMAL(10,3)) AS DIV5_LONGEST_GTIME
  , CAST(`DIV5_WHEELS_OFF` AS CHAR(4)) AS DIV5_WHEELS_OFF
  , CAST(`DIV5_TAIL_NUM` AS CHAR(6)) AS DIV5_TAIL_NUM
  , `ID`
FROM RITA_10;