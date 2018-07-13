-- Debemos de subir el fichero weblogs.csv al directorio /data/weblogs

DROP DATABASE IF EXISTS HDILABDB CASCADE;
CREATE DATABASE HDILABDB;
Use HDILABDB;
CREATE EXTERNAL TABLE IF NOT EXISTS weblogs(
	TransactionDate varchar(50) ,
	CustomerId varchar(50) ,
	BookId varchar(50) ,
	PurchaseType varchar(50) ,
	TransactionId varchar(50) ,
	OrderId varchar(50) ,
	BookName varchar(50) ,
	CategoryName varchar(50) ,
	Quantity varchar(50) ,
	ShippingAmount varchar(50) ,
	InvoiceNumber varchar(50) ,
	InvoiceStatus varchar(50) ,
	PaymentAmount varchar(50) 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED by ',' lines TERMINATED by '\n' 
STORED AS TEXTFILE LOCATION "/data/weblogs";

--eliminamos la tabla

DROP TABLE weblogs;

-- Creamos la tabla como Administrada

CREATE TABLE IF NOT EXISTS weblogs(
	TransactionDate varchar(50) ,
	CustomerId varchar(50) ,
	BookId varchar(50) ,
	PurchaseType varchar(50) ,
	TransactionId varchar(50) ,
	OrderId varchar(50) ,
	BookName varchar(50) ,
	CategoryName varchar(50) ,
	Quantity varchar(50) ,
	ShippingAmount varchar(50) ,
	InvoiceNumber varchar(50) ,
	InvoiceStatus varchar(50) ,
	PaymentAmount varchar(50) 
) 
ROW FORMAT DELIMITED FIELDS TERMINATED by ',' lines TERMINATED by '\n' ;

--cargamos los datos del fichero
LOAD DATA INPATH '/data/weblogs/weblogs.csv' INTO TABLE HDILABDB.weblogs;

--comprobamos a través de HDFS que se han cargado los datos en el Warehouse de HIVE

-- Algunas consultas

SELECT COUNT(*) FROM HDILABDB.weblogs;
SELECT * FROM HDILABDB.weblogs LIMIT 5;

SELECT * FROM HDILABDB.weblogs WHERE orderid="107";

-- Crea un nuevo Worksheet y ejecuta:

SELECT DISTINCT bookname FROM HDILABDB.weblogs WHERE orderid="107";

SELECT bookname,COUNT(*) FROM HDILABDB.weblogs GROUP BY bookname; 
