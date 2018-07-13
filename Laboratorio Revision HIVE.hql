CREATE EXTERNAL TABLE samplelog(
  fecha date,
  time varchar(200),
  sitename varchar(200),
  method varchar(200),
  uristem varchar(200),
  uriquery varchar(200),
  port varchar(200),
  username varchar(200),
  ip varchar(200),
  UserAgent varchar(200),
  Cookie varchar(200),
  Referer varchar(200),
  host varchar(200),
  status varchar(200),
  substatus varchar(200),
  win32substatus varchar(200),
  scbytes int,
  csbytes int,
  timetaken int)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
STORED AS TEXTFILE LOCATION '/HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog'
tblproperties ("skip.header.line.count"="2");


 SELECT * FROM samplelog; 
 SELECT COUNT(*) from samplelog; --y anotamos el número de filas
-- Copiamos el fichero de origen con el comando hdfs dfs -cp /HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2b.log /HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2c.log
SELECT COUNT(*) from samplelog; --y obtendremos el doble de filas
--Si volvemos a consultar el contenido de los directorios, tanto del almacen de hive, como el origen, nada ha cambiado
--Eliminamos el fichero que hemos copiado /HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2c.log
 SELECT COUNT(*) from samplelog; --y ha desaparecido el contenido de ese fichero

DROP TABLE samplelog;

-- Tablas Administradas
CREATE TABLE samplelogadmin(
  fecha date,
  time varchar(200),
  sitename varchar(200),
  method varchar(200),
  uristem varchar(200),
  uriquery varchar(200),
  port varchar(200),
  username varchar(200),
  ip varchar(200),
  UserAgent varchar(200),
  Cookie varchar(200),
  Referer varchar(200),
  host varchar(200),
  status varchar(200),
  substatus varchar(200),
  win32substatus varchar(200),
  scbytes int,
  csbytes int,
  timetaken int)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ' '
tblproperties ("skip.header.line.count"="2");

--Consultamos el contenido de la tabla y vemos que está vacía
SELECT * FROM samplelogadmin;

LOAD DATA INPATH '/HdiSamples/HdiSamples/WebsiteLogSampleData/SampleLog/909f2b.log' INTO TABLE samplelogadmin;
-- Consultamos los directorios de origen y fichero y de almacén de Hive (hive/warehouse) ¿Qué ha ocurrido?

DROP TABLE IF EXISTS hvac;

--crear la tabla externa hvac sobre el csv
CREATE EXTERNAL TABLE hvac(dates STRING, time STRING, targettemp BIGINT,
			actualtemp BIGINT, system BIGINT, systemage BIGINT, buildingid BIGINT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/HdiSamples/HdiSamples/SensorSampleData/hvac/';

DROP TABLE IF EXISTS building;

--crear la tabla externa building sobre el csv
CREATE EXTERNAL TABLE building(buildingid BIGINT, buildingmgr STRING, 
			buildingage BIGINT, hvacproduct STRING, country STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE LOCATION '/HdiSamples/HdiSamples/SensorSampleData/building/';

DROP TABLE IF EXISTS hvac_temperatures;

--crear la tabla administrada hvac_temperatures desde la tabla hvac. Mostrar donde se almacena esa tabla
CREATE TABLE hvac_temperatures AS
SELECT *, targettemp - actualtemp AS temp_diff, 
	IF((targettemp - actualtemp) > 5, 'COLD',
	IF((targettemp - actualtemp) < -5, 'HOT', 'NORMAL')) AS temprange, 
	IF((targettemp - actualtemp) > 5, '1', IF((targettemp - actualtemp) < -5, '1', 0)) AS extremetemp
FROM hvac;

DROP TABLE IF EXISTS hvac_building;

-- crear la tabla hvac_building haciendo join entre las tablas de building y hvac_temperatures
CREATE TABLE hvac_building AS
SELECT h.*, b.country, b.hvacproduct, b.buildingage, b.buildingmgr
FROM building b JOIN hvac_temperatures h ON b.buildingid = h.buildingid;

--Comprueba que se han cargado datos en las dos últimas tablas a partir del resultado de las consultas SELECT. ¿Dónde se han almacenado?

--Limpiamos el entorno:

DROP TABLE hvac;
DROP TABLE building;
DROP TABLE hvac_temperaturas;
DROP TABLE hvac_Building;


