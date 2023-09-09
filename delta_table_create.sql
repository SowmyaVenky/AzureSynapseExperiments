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

CREATE EXTERNAL TABLE dbo.raleigh_airport_weather_delta (
	[awnd] float,
	[date] nvarchar(10),
	[prcp] float,
	[snow] float,
	[snwd] float,
	[tmax] bigint,
	[tmin] bigint
	)
	WITH (
	LOCATION = 'raleigh_weather_delta/**',
	DATA_SOURCE = [files_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM dbo.raleigh_airport_weather_delta
GO

SELECT distinct(year(cast(date as DATE))) as tempyear FROM dbo.raleigh_airport_weather_delta
order by tempyear
GO

-- Now we will go to the notebook and remove 2017 data and observe in the external table.
-- Even vaccum is not removing rows as expected. Need to research.
