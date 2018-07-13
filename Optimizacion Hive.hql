--SELECT * FROM hivesampletable where country='United States';
CREATE DATABASE TEST;
USE TEST;

DROP TABLE IF EXISTS TEST.porpartes;

CREATE TABLE TEST.porpartes (clienteid STRING, quertyime STRING,market string,deviceplatform string,
    devicemake string, devicemodel string,state string, querydwelltime double, sessionid bigint, sessionpagevieworder bigint )
PARTITIONED BY (country STRING)
STORED AS ORC;

-- Particionado estático set hive.mapred.mode=strict
-- Util cuando tenemos ficheros que nos llegan por la clave de partición y los cargamos directamente en diferentes particiones
-- Podemos hacerlo también con el LOAD DATA cargando los datos de cada fichero en cada partición

INSERT INTO TABLE TEST.porpartes
PARTITION (country='Unites States') -- Le estamos dando nombre a la partición y valor al campo en este caso hemos puesto mal Unites y ese es el valor asignado
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder
FROM default.hivesampletable where country = 'United States';

INSERT INTO TABLE TEST.porpartes
PARTITION (country='Unites States') 
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder
FROM default.hivesampletable where country = 'United States';

INSERT INTO TABLE TEST.porpartes
PARTITION (country='United Kingdom')
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder
 FROM default.hivesampletable where country = 'United Kingdom';

select * from TEST.porpartes where country='United States';
select disctinct country from TEST.porpartes;

select * from test.porpartes;
-- Particionado dinámico
SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT OVERWRITE TABLE TEST.porpartes
PARTITION (country)
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder,country
 FROM default.hivesampletable;

-- Bucketing
-- Particionado es ok cuando tenemos un número similar de filas para cada partición
-- Si no es así podemos utilizar buckting para dividir grandes conjuntos de datos aplicando una función de hash
-- Podemos utilizarlo en conjunto con particionado o no
-- No podemos cargar las tablas con LOAD
DROP TABLE test.porpartesclus;
CREATE TABLE test.porpartesclus (clienteid STRING, quertyime STRING,market string,deviceplatform string,
    devicemake string, devicemodel string,state string, querydwelltime double, sessionid bigint, sessionpagevieworder bigint )
PARTITIONED BY (country STRING) CLUSTERED BY (deviceplatform) SORTED BY (deviceplatform) INTO 32 BUCKETS
STORED AS ORC;

INSERT OVERWRITE TABLE test.porpartesclus
PARTITION (country)
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder,country
 FROM default.hivesampletable;

select distinct deviceplatform from default.hivesampletable where country = 'Ukraine';

select count(*),country,deviceplatform from default.hivesampletable where country='Sweden' group by country,deviceplatform;
 
 select distinct(market) from default.hivesampletable;
 -- Separar los más habituales
 
 DROP TABLE IF EXISTS test.ejemploskew;

CREATE TABLE test.ejemploskew (clienteid STRING, quertyime STRING,market string,deviceplatform string,
    devicemake string, devicemodel string,state string, querydwelltime double, sessionid bigint, sessionpagevieworder bigint,country string )
SKEWED BY (country) ON ("Spain","Sweden")
STORED AS DIRECTORIES;

INSERT INTO TABLE test.ejemploskew
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder,country
 FROM default.hivesampletable;

INSERT OVERWRITE TABLE  test.ejemploskew
SELECT clientid,querytime,market,deviceplatform,devicemake,
    devicemodel,state,querydwelltime,sessionid,sessionpagevieworder,country
 FROM default.hivesampletable
 DISTRIBUTE BY market;

INSERT OVERWRITE DIRECTORY '/datos/export'
SELECT * FROM test.porpartesclus;