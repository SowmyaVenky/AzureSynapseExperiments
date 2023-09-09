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

CREATE EXTERNAL TABLE dbo.spring_tx_temperatures (
	[elevation] float,
	[latitude] float,
	[longitude] float,
	[time] nvarchar(50),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'spring_tx_temps_formatted/**',
	DATA_SOURCE = [files_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM dbo.spring_tx_temperatures
GO