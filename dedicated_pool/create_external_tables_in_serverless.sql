IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'datalake_venkydatalake1001_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [datalake_venkydatalake1001_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://datalake@venkydatalake1001.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2023] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_0/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2022] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_1/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2021] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_2/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2020] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_3/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2019] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_4/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

CREATE EXTERNAL TABLE [dbo].[temperatures_2018] (
	[latitude] float,
	[longitude] float,
	[time] nvarchar(4000),
	[temperature_2m] float
	)
	WITH (
	LOCATION = 'temperatures/AirQualityIndexWithTemperatures_5/**',
	DATA_SOURCE = [datalake_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

SELECT substring([time], 1, 4) as [yyyy], count(*) as [rowcnt]
FROM ( 
SELECT * FROM [dbo].[temperatures_2023] UNION
SELECT * FROM [dbo].[temperatures_2022] UNION
SELECT * FROM [dbo].[temperatures_2021] UNION
SELECT * FROM [dbo].[temperatures_2020] UNION
SELECT * FROM [dbo].[temperatures_2019] UNION
SELECT * FROM [dbo].[temperatures_2018]
) XX
GROUP BY substring([time], 1, 4) 
ORDER BY [yyyy]
GO