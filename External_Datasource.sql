-- This will demonstrate how we can create a logical database inside the serverless synapse pool itself
-- This can be very useful because it can simplify how we write queries inside the data lake without
-- having to specify the full path to the ADLS files.
CREATE DATABASE venkytestdb1001;
GO

use venkytestdb1001;

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'files_venkydatalake1001_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [files_venkydatalake101_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://files@venkydatalake1001.dfs.core.windows.net' 
	)
GO

DROP EXTERNAL TABLE dbo.raleigh_airport_weather;

CREATE EXTERNAL TABLE dbo.raleigh_airport_weather (
	[awnd] float,
	[date] VARCHAR(10),
	[prcp] float,
	[snow] float,
	[snwd] float,
	[tmax] bigint,
	[tmin] bigint
	)
	WITH (
	LOCATION = 'raleigh_weather_parquet/**',
	DATA_SOURCE = [files_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM dbo.raleigh_airport_weather
GO

SELECT 
min(cast(date as DATE)) as mindate, 
max(cast(date as DATE)) as maxdate 
FROM dbo.raleigh_airport_weather
GO

SELECT min(tmin) as lowesttemp, 
max(tmax) as greatesttemp 
FROM dbo.raleigh_airport_weather
GO

--Find max and min temp by year and month

select tempyear,
tempmonth,
max(tmax) as maxtemp,
min(tmin) as mintemp
FROM ( 
SELECT 
YEAR(cast(date as DATE)) as tempyear,
MONTH(cast(date as DATE)) as tempmonth,
tmin, 
tmax 
FROM dbo.raleigh_airport_weather
) X
GROUP BY X.tempyear, X.tempmonth
ORDER BY X.tempyear, X.tempmonth
