DROP DATABASE TEMPERATURES_LAKEHOUSE
GO

CREATE DATABASE TEMPERATURES_LAKEHOUSE
GO

-- Create master key in databases with some password (one-off per database)
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Ganesh20022002!'
GO

DROP EXTERNAL TABLE locations_external
GO 

DROP EXTERNAL TABLE temperatures_external
GO

DROP EXTERNAL DATA SOURCE locations_ds
GO

DROP  DATABASE SCOPED CREDENTIAL SasCredential
GO 

CREATE EXTERNAL DATA SOURCE locations_ds
WITH ( LOCATION = 'https://venkydatalake1001.dfs.core.windows.net/files/' )
GO

CREATE EXTERNAL FILE FORMAT parquet_format
WITH
(  
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
)
GO

CREATE EXTERNAL TABLE locations_external
(
    latitude float,
    longitude float,
    elevation float
)  
WITH (
    LOCATION = '/location_master/**',
    DATA_SOURCE = locations_ds,  
    FILE_FORMAT = parquet_format
)
GO

SELECT * FROM locations_external
GO

CREATE EXTERNAL TABLE temperatures_external
(
    latitude float,
    longitude float,
    time varchar(4000),
    temperature_2m float
)  
WITH (
    LOCATION = '/spring_tx_temps_formatted/**',
    DATA_SOURCE = locations_ds,  
    FILE_FORMAT = parquet_format
)
GO


SELECT 
latitude
,longitude
,max(temperature_2m) as maxtemp
,min(temperature_2m) as mintemp
from temperatures_external
group BY
latitude,
longitude
GO

