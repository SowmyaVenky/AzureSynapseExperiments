IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseDeltaFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseDeltaFormat] 
	WITH ( FORMAT_TYPE = DELTA)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'datalake_venkydatalake1001_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [datalake_venkydatalake1001_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://datalake@venkydatalake1001.dfs.core.windows.net' 
	)
GO

-- Make sure that the database picked is the temperaturesdb. 

CREATE EXTERNAL TABLE [dbo].[temperatures_ext_delta] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures_delta/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseDeltaFormat]
	)
GO

