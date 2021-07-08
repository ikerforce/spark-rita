CREATE VIEW resumen AS
SELECT *
FROM
(
    SELECT process
        , sample_size
        , avg(duration) AS avg_duration
        , stddev(duration) AS stddev_duration
        , count(*) AS process_count
        , description
    FROM registro_de_tiempo_spark_rankeado
    GROUP BY process
        , sample_size
    UNION
    SELECT process
        , sample_size
        , avg(duration) AS avg_duration
        , stddev(duration) AS stddev_duration
        , count(*) AS process_count
        , description
    FROM registro_de_tiempo_dask_rankeado
    GROUP BY process
        , sample_size
) as resumen
ORDER BY resumen.process
    , resumen.sample_size
    , resumen.avg_duration
    ;


CREATE VIEW resumen_sin_rank AS
SELECT *
FROM
(
    SELECT process
        , sample_size
        , avg(duration) AS avg_duration
        , stddev(duration) AS stddev_duration
        , count(*) AS process_count
        , description
    FROM registro_de_tiempo_spark
    GROUP BY process
        , sample_size
        , description
    UNION
    SELECT process
        , sample_size
        , avg(duration) AS avg_duration
        , stddev(duration) AS stddev_duration
        , count(*) AS process_count
        , description
    FROM registro_de_tiempo_dask
    GROUP BY process
        , sample_size
        , description
) as resumen
ORDER BY resumen.process
    , resumen.sample_size
    , resumen.avg_duration
    ;


-- Cambio de tiempo de procesamiento de acuerdo al tamaño de la muestra
SELECT process
    , sample_size
    , avg(duration) AS avg_duration
    , stddev(duration) AS stddev_duration
    , count(*) AS count
FROM registro_de_tiempo_spark_test
WHERE process LIKE '%demoras_aerolinea%'
GROUP BY sample_size, process

SELECT process 
    , sample_size
    , avg(duration) AS avg_duration
    , stddev(duration) AS stddev_duration
    , count(*) AS count
FROM registro_de_tiempo_dask_test
WHERE process LIKE 'demoras_aerolinea_dask'
GROUP BY sample_size, process


SELECT * FROM resumen AS


UPDATE registro_de_tiempo_dask
SET 
    sample_size = '1M'
WHERE
    sample_size = '10M';


UPDATE registro_de_tiempo_spark
SET 
    sample_size = '1M'
WHERE
    sample_size = '10M';


INSERT INTO registro_de_tiempo_dask (process, start_ts, end_ts, duration, description, resources, sample_size, env, insertion_ts)
SELECT process
    , start_ts
    , end_ts
    , duration
    , description
    , resources
    , sample_size
    , env
    , insertion_ts
FROM registro_de_tiempo_dask_test
WHERE sample_size = '10M'


INSERT INTO registro_de_tiempo_spark (process, start_ts, end_ts, duration, description, resources, sample_size, env, insertion_ts)
SELECT process
    , start_ts
    , end_ts
    , duration
    , description
    , resources
    , sample_size
    , env
    , insertion_ts
FROM registro_de_tiempo_spark_test
WHERE sample_size = '10M'


DELETE FROM registro_de_tiempo_dask_test
WHERE sample_size = '10M'
AND process LIKE 'demoras_aeropuerto_origen_dask%'
AND insertion_ts < '2021-05-31 17:28:58';


-- Resumen general



-- Vista conjunta de tiempo
-- Esta vista une el registro de tiempo de spark con el de dask
CREATE VIEW registro_de_tiempo AS
SELECT process
    , start_ts
    , end_ts
    , duration
    , description
    , resources
    , sample_size
    , env
    , insertion_ts
    , 'spark' AS framework
FROM registro_de_tiempo_spark
UNION
SELECT process
    , start_ts
    , end_ts
    , duration
    , description
    , resources
    , sample_size
    , env
    , insertion_ts
    , 'dask' AS framework
FROM registro_de_tiempo_dask


-- Resumen general
-- Esta vista calcula estadísticas generales agrupadas por muestra de datos sin distinguir entre procesos
-- CREATE VIEW resumen_general AS
SELECT IF(process LIKE '%_command_time', 'command_time', IF(process LIKE '%_write_time', 'write_time', 'total_time')) AS general_process
    , sample_size
    , framework
    , count(1) as executions
    , AVG(duration) AS avg_duration
    , STDDEV(duration) AS stddev_duration
FROM registro_de_tiempo_rankeado
GROUP BY sample_size, framework, general_process
ORDER BY sample_size, framework, general_process


SELECT IF(process LIKE '%_command_time', 'command_time', IF(process LIKE '%_write_time', 'write_time', 'total_time')) AS process
FROM registro_de_tiempo LIMIT 20;


-- Resumen general de dask
CREATE VIEW resumen_general_dask AS
SELECT REPLACE(general.process, '_dask', '') AS process
    , 'dask' AS framework
    , general.sample_size
    , general.process_count
    , general.avg_duration
    , ROUND(general.avg_duration - IFNULL(write_table.avg_write_time, 0), 3) AS others_time
    , general.stddev_duration
    , general.coeff_variation
    , write_table.avg_write_time
    , write_table.stddev_write_time
    , command_table.avg_command_time
    , command_table.stddev_command_time
FROM
(
    SELECT process
        , sample_size
        , ROUND(avg_duration, 3) as avg_duration
        , ROUND(stddev_duration, 3) as stddev_duration
        , ROUND(stddev_duration / avg_duration, 3) as coeff_variation
        , process_count
    FROM resumen
    WHERE process NOT LIKE '%time%'
) general
LEFT OUTER JOIN
(
    SELECT REPLACE(process, '_write_time', '') AS process
        , sample_size
        , ROUND(avg_duration, 3) AS avg_write_time
        , ROUND(stddev_duration, 3) AS stddev_write_time
        , process_count
    FROM resumen
    WHERE process LIKE '%write%'
) write_table
ON general.process = write_table.process
AND general.sample_size = write_table.sample_size
LEFT OUTER JOIN
(
    SELECT REPLACE(process, '_command_time', '') AS process
        , sample_size
        , ROUND(avg_duration, 3) AS avg_command_time
        , ROUND(stddev_duration, 3) AS stddev_command_time
    FROM resumen
    WHERE process LIKE '%command_time%'
) command_table
ON general.process = command_table.process
AND general.sample_size = command_table.sample_size
WHERE general.process LIKE '%dask%'
ORDER BY sample_size, process;


-- Resumen general de spark
CREATE VIEW resumen_general_spark AS
SELECT REPLACE(general.process, '_spark', '') AS process
    , 'spark' AS framework
    , general.sample_size
    , general.avg_duration
    , general.process_count
    , ROUND(general.avg_duration - IFNULL(write_table.avg_write_time, 0), 3) AS others_time
    , general.stddev_duration
    , general.coeff_variation
    , write_table.avg_write_time
    , write_table.stddev_write_time
    , command_table.avg_command_time
    , command_table.stddev_command_time
FROM
(
    SELECT process
        , sample_size
        , ROUND(avg_duration, 3) as avg_duration
        , ROUND(stddev_duration, 3) as stddev_duration
        , ROUND(stddev_duration / avg_duration, 3) as coeff_variation
        , process_count
    FROM resumen
    WHERE process NOT LIKE '%time%'
) general
LEFT OUTER JOIN
(
    SELECT REPLACE(process, '_write_time', '') AS process
        , sample_size
        , ROUND(avg_duration, 3) AS avg_write_time
        , ROUND(stddev_duration, 3) AS stddev_write_time
        , process_count
    FROM resumen
    WHERE process LIKE '%write%'
) write_table
ON general.process = write_table.process
AND general.sample_size = write_table.sample_size
LEFT OUTER JOIN
(
    SELECT REPLACE(process, '_command_time', '') AS process
        , sample_size
        , ROUND(avg_duration, 3) AS avg_command_time
        , ROUND(stddev_duration, 3) AS stddev_command_time
    FROM resumen
    WHERE process LIKE '%command_time%'
) command_table
ON general.process = command_table.process
AND general.sample_size = command_table.sample_size
WHERE general.process LIKE '%spark%'
ORDER BY sample_size, process;


CREATE VIEW ratios_duracion AS
SELECT r_dask.process
    , r_dask.sample_size
    , r_dask.avg_duration / r_spark.avg_duration AS t_ratio_dask_spark
    , r_spark.avg_duration / r_dask.avg_duration AS t_ratio_spark_dask
    , r_dask.others_time / r_spark.others_time AS others_r_dask_spark
    , r_spark.others_time / r_dask.others_time AS others_r_spark_dask
    , r_dask.avg_write_time / r_spark.avg_write_time AS write_r_dask_spark
    , r_spark.avg_write_time / r_dask.avg_write_time AS write_r_spark_dask
FROM
(
    SELECT process
    , avg_duration
    , sample_size
    , others_time
    , avg_write_time
    FROM resumen_general_dask
) r_dask
LEFT JOIN
(
    SELECT process
    , avg_duration
    , sample_size
    , others_time
    , avg_write_time
    FROM resumen_general_spark
) r_spark
ON r_dask.process = r_spark.process
AND r_dask.sample_size = r_spark.sample_size
ORDER BY sample_size, process;


CREATE VIEW resumen_ratios AS
SELECT resumen_general.process
    , IF(
        resumen_general.process REGEXP('demoras_aerolinea|demoras_aeropuerto_destino|demoras_ruta_aeropuerto')
        , 'parquet'
        , IF(
            resumen_general.process REGEXP('demoras_aeropuerto_origen|demoras_ruta_mktid|flota|dijkstra')
            , 'mysql'
            , 'sin escritura'
            )
        ) AS destino_escritura
    , resumen_general.framework
    , resumen_general.sample_size
    , resumen_general.process_count
    , resumen_general.avg_duration
    , resumen_general.stddev_duration
    , ROUND(ratios_duracion.t_ratio_dask_spark, 3) AS t_ratio_dask_spark
    , ROUND(ratios_duracion.t_ratio_spark_dask, 3) AS t_ratio_spark_dask
    , ROUND(ratios_duracion.others_r_dask_spark, 3) AS others_r_dask_spark
    , ROUND(ratios_duracion.others_r_spark_dask, 3) AS others_r_spark_dask
    , ROUND(ratios_duracion.write_r_dask_spark, 3) AS write_r_dask_spark
    , ROUND(ratios_duracion.write_r_spark_dask, 3) AS write_r_spark_dask
    , resumen_general.coeff_variation
    , resumen_general.avg_write_time
    , resumen_general.stddev_write_time
    , resumen_general.avg_command_time
    , resumen_general.stddev_command_time
    , ROUND(resumen_general.avg_duration - IFNULL(resumen_general.avg_write_time, 0.0), 3) AS others_time
FROM
(
    SELECT process
        , framework
        , sample_size
        , avg_duration
        , stddev_duration
        , coeff_variation
        , avg_write_time
        , stddev_write_time
        , avg_command_time
        , stddev_command_time
        , process_count
    FROM resumen_general_spark
    UNION
    SELECT process
        , framework
        , sample_size
        , avg_duration
        , stddev_duration
        , coeff_variation
        , avg_write_time
        , stddev_write_time
        , avg_command_time
        , stddev_command_time
        , process_count
    FROM resumen_general_dask
) resumen_general
LEFT OUTER JOIN ratios_duracion
ON resumen_general.process = ratios_duracion.process
AND resumen_general.sample_size = ratios_duracion.sample_size
ORDER BY sample_size
    , process
    , framework;


SELECT REPLACE(process, '_dask', '') AS process_general
    , 'dask' AS framework
    , avg_duration
FROM resumen
WHERE process LIKE '%dask%';


SELECT REPLACE(process, '_', '') AS process_general
    , 'dask' AS framework
    , avg_duration
FROM resumen
WHERE process LIKE '%dask%';


SELECT process,
    IF(
        ratios_duracion.process REGEXP('demoras_aerolinea|demoras_aeropuerto_destino|demoras_ruta_aeropuerto')
        , 'parquet'
        , IF(
            ratios_duracion.process REGEXP('demoras_aeropuerto_origen|demoras_ruta_mktid|flota|dijkstra')
            , 'mysql'
            , 'sin escritura'
            )
        ) AS destino_escritura
FROM ratios_duracion;


-- Tablas de duracion
SELECT process, framework, process_count, avg_duration, stddev_duration, coeff_variation, t_ratio_dask_spark, t_ratio_spark_dask FROM resumen_ratios WHERE sample_size LIKE '10M';
SELECT process, framework, avg_command_time, others_time, others_r_spark_dask, others_r_dask_spark FROM resumen_ratios WHERE sample_size LIKE '10M';
SELECT process, framework, destino_escritura, avg_write_time, n_rows, n_columns FROM resumen_write WHERE sample_size LIKE '10M';

SELECT process, framework, process_count, avg_duration, stddev_duration, coeff_variation, t_ratio_dask_spark, t_ratio_spark_dask FROM resumen_ratios WHERE sample_size LIKE '1M';
SELECT process, framework, avg_command_time, others_time, others_r_spark_dask, others_r_dask_spark FROM resumen_ratios WHERE sample_size LIKE '1M';
SELECT process, framework, destino_escritura, avg_write_time, n_rows, n_columns FROM resumen_write WHERE sample_size LIKE '1M';

SELECT process, framework, process_count, avg_duration, stddev_duration, coeff_variation, t_ratio_dask_spark, t_ratio_spark_dask FROM resumen_ratios WHERE sample_size LIKE '100K';
SELECT process, framework, avg_command_time, others_time, others_r_spark_dask, others_r_dask_spark FROM resumen_ratios WHERE sample_size LIKE '100K';
SELECT process, framework, destino_escritura, avg_write_time, n_rows, n_columns FROM resumen_write WHERE sample_size LIKE '100K';

SELECT process, framework, process_count, avg_duration, stddev_duration, coeff_variation, t_ratio_dask_spark, t_ratio_spark_dask FROM resumen_ratios WHERE sample_size LIKE '10K';
SELECT process, framework, avg_command_time, others_time, others_r_spark_dask, others_r_dask_spark FROM resumen_ratios WHERE sample_size LIKE '10K';
SELECT process, framework, destino_escritura, avg_write_time, n_rows, n_columns FROM resumen_write WHERE sample_size LIKE '10K';


SELECT process, sample_size, destino_escritura, CONCAT(n_rows, ' ', avg_write_time, ' ', stddev_write_time), framework FROM resumen_write WHERE process LIKE 'demoras_aeropuerto_origen' ORDER BY framework, avg_write_time;
SELECT process, sample_size, CONCAT(avg_duration, ' ', stddev_duration), framework FROM resumen_ratios WHERE process LIKE 'demoras_aeropuerto_origen' ORDER BY framework, avg_duration;

-- Tabla con los tamaños de los dataframes resultantes
CREATE TABLE result_sizes
(
     process    varchar(300),
     sample_size    varchar(300),
     n_rows     varchar(300),
     n_columns  varchar(255)
);

INSERT INTO result_sizes ( process , sample_size , n_rows , n_columns )
VALUES
    ('demoras_aerolinea_spark', '10K', '12135', '11'),
    ('demoras_aerolinea_dask', '10K', '12135', '11'),
    ('demoras_aeropuerto_destino_spark', '10K', '22144', '11'),
    ('demoras_aeropuerto_destino_dask', '10K', '22144', '11'),
    ('demoras_aeropuerto_origen_spark', '10K', '22086', '11'),
    ('demoras_aeropuerto_origen_dask', '10K', '22086', '11'),
    ('demoras_ruta_aeropuerto_spark', '10K', '41147', '11'),
    ('demoras_ruta_aeropuerto_dask', '10K', '41147', '11'),
    ('dijkstra_spark', '10K', '', ''),
    ('dijkstra_dask', '10K', '', ''),
    ('flota_spark', '10K', '12135', '6'),
    ('flota_dask', '10K', '12135', '6'),
    ('demoras_ruta_mktid_spark', '10K', '38909', '11'),
    ('demoras_ruta_mktid_dask', '10K', '38909', '11'),
    ('demoras_aerolinea_spark', '100K', '49439', '11'),
    ('demoras_aerolinea_dask', '100K', '49439', '11'),
    ('demoras_aeropuerto_destino_spark', '100K', '116746', '11'),
    ('demoras_aeropuerto_destino_dask', '100K', '116746', '11'),
    ('demoras_aeropuerto_origen_spark', '100K', '116656', '11'),
    ('demoras_aeropuerto_origen_dask', '100K', '116656', '11'),
    ('demoras_ruta_aeropuerto_spark', '100K', '297814', '11'),
    ('demoras_ruta_aeropuerto_dask', '100K', '297814', '11'),
    ('dijkstra_spark', '100K', '', ''),
    ('dijkstra_dask', '100K', '', ''),
    ('flota_spark', '100K', '49439', '6'),
    ('flota_dask', '100K', '49439', '6'),
    ('demoras_ruta_mktid_spark', '100K', '267505', '11'),
    ('demoras_ruta_mktid_dask', '100K', '267505', '11'),
    ('demoras_aerolinea_spark', '1M', '76717', '11'),
    ('demoras_aerolinea_dask', '1M', '76717', '11'),
    ('demoras_aeropuerto_destino_spark', '1M', '436411', '11'),
    ('demoras_aeropuerto_destino_dask', '1M', '436411', '11'),
    ('demoras_aeropuerto_origen_spark', '1M', '436681', '11'),
    ('demoras_aeropuerto_origen_dask', '1M', '436681', '11'),
    ('demoras_ruta_aeropuerto_spark', '1M', '1619826', '11'),
    ('demoras_ruta_aeropuerto_dask', '1M', '1619826', '11'),
    ('dijkstra_spark', '1M', '', ''),
    ('dijkstra_dask', '1M', '', ''),
    ('flota_spark', '1M', '76717', '6'),
    ('flota_dask', '1M', '76717', '6'),
    ('demoras_ruta_mktid_spark', '1M', '1430282', '11'),
    ('demoras_ruta_mktid_dask', '1M', '1430282', '11'),
    ('demoras_aerolinea_spark', '10M', '78113', '11'),
    ('demoras_aerolinea_dask', '10M', '78113', '11'),
    ('demoras_aeropuerto_destino_spark', '10M', '1042066', '11'),
    ('demoras_aeropuerto_destino_dask', '10M', '1042066', '11'),
    ('demoras_aeropuerto_origen_spark', '10M', '1041665', '11'),
    ('demoras_aeropuerto_origen_dask', '10M', '1041665', '11'),
    ('demoras_ruta_aeropuerto_spark', '10M', '8361958', '11'),
    ('demoras_ruta_aeropuerto_dask', '10M', '8361958', '11'),
    ('dijkstra_spark', '10M', '', ''),
    ('dijkstra_dask', '10M', '', ''),
    ('flota_spark', '10M', '78113', '6'),
    ('flota_dask', '10M', '78113', '6'),
    ('demoras_ruta_mktid_spark', '10M', '6824834', '11'),
    ('demoras_ruta_mktid_dask', '10M', '6824834', '11'),
    ('demoras_aerolinea_spark', 'TOTAL', '78133', '11'),
    ('demoras_aerolinea_dask', 'TOTAL', '78133', '11'),
    ('demoras_aeropuerto_destino_spark', 'TOTAL', '1490152', '11'),
    ('demoras_aeropuerto_destino_dask', 'TOTAL', '1490152', '11'),
    ('demoras_aeropuerto_origen_spark', 'TOTAL', '1490410', '11'),
    ('demoras_aeropuerto_origen_dask', 'TOTAL', '1490410', '11'),
    ('demoras_ruta_aeropuerto_spark', 'TOTAL', '19897358', '11'),
    ('demoras_ruta_aeropuerto_dask', 'TOTAL', '19897358', '11'),
    ('dijkstra_spark', 'TOTAL', '', ''),
    ('dijkstra_dask', 'TOTAL', '', ''),
    ('flota_spark', 'TOTAL', '78133', '6'),
    ('flota_dask', 'TOTAL', '78133', '6'),
    ('demoras_ruta_mktid_spark', 'TOTAL', '15378534', '11'),
    ('demoras_ruta_mktid_dask', 'TOTAL', '15378534', '11');


-- Resumen write
CREATE VIEW resumen_write AS
SELECT rr.process
    , rr.framework
    , rr.sample_size
    , rr.destino_escritura
    , rr.avg_write_time
    , rr.stddev_write_time
    , ROUND(rr.avg_write_time / rr.avg_duration, 3) as w_pct_duration
    , 1 - ROUND(rr.avg_write_time / rr.avg_duration, 3) as others_pct_duration
    , rr.write_r_spark_dask
    , rr.write_r_dask_spark
    , rs.n_rows
    , rs.n_columns
FROM
(
    SELECT process
        , sample_size
        , destino_escritura
        , framework
        , avg_duration
        , avg_write_time
        , stddev_write_time
        , write_r_spark_dask
        , write_r_dask_spark
    FROM resumen_ratios
) rr
LEFT OUTER JOIN
(
    SELECT *
    FROM result_sizes
) rs
ON CONCAT(rr.process, CONCAT('_', rr.framework)) = rs.process
AND rr.sample_size = rs.sample_size
ORDER BY sample_size
    , process


-- Query para reestablecer desde el backup
INSERT INTO TRANSTAT.registro_de_tiempo_dask ( 
      process
      , start_ts
      , end_ts
      , duration
      , description
      , resources
      , sample_size
      , env
      , insertion_ts ) 
SELECT process
      , start_ts
      , end_ts
      , duration
      , description
      , resources
      , sample_size
      , env
      , insertion_ts 
FROM TRANSTAT_OLD.registro_de_tiempo_dask
WHERE process LIKE '%command_time%'
AND process LIKE '%elimina%'