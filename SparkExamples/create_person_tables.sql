CREATE DATABASE PERSON_DATABASE;

IF NOT EXISTS (SELECT * FROM sys.external_file_formats WHERE name = 'SynapseParquetFormat') 
	CREATE EXTERNAL FILE FORMAT [SynapseParquetFormat] 
	WITH ( FORMAT_TYPE = PARQUET)
GO

IF NOT EXISTS (SELECT * FROM sys.external_data_sources WHERE name = 'files_venkydatalake1001_dfs_core_windows_net') 
	CREATE EXTERNAL DATA SOURCE [files_venkydatalake1001_dfs_core_windows_net] 
	WITH (
		LOCATION = 'abfss://files@venkydatalake1001.dfs.core.windows.net' 
	)
GO

CREATE EXTERNAL TABLE dbo.person (
	[accountNumber] nvarchar(4000),
	[city] nvarchar(4000),
	[creditCard] nvarchar(4000),
	[expDate] nvarchar(4000),
	[firstName] nvarchar(4000),
	[lastName] nvarchar(4000),
	[phoneNumber] nvarchar(4000),
	[state] nvarchar(4000),
	[streetName] nvarchar(4000),
	[streetNumber] nvarchar(4000),
	[zip] nvarchar(4000),
	[firstName_H] nvarchar(4000),
	[lastName_H] nvarchar(4000),
	[streetName_H] nvarchar(4000),
	[phoneNumber_H] nvarchar(4000),
	[creditCard_H] nvarchar(4000),
	[accountNumber_H] nvarchar(4000)
	)
	WITH (
	LOCATION = 'person/**',
	DATA_SOURCE = [files_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO


SELECT TOP 100 * FROM dbo.person
GO

CREATE EXTERNAL TABLE dbo.person_hashed (
	[city] nvarchar(4000),
	[expDate] nvarchar(4000),
	[state] nvarchar(4000),
	[streetNumber] nvarchar(4000),
	[zip] nvarchar(4000),
	[firstName_H] nvarchar(4000),
	[lastName_H] nvarchar(4000),
	[streetName_H] nvarchar(4000),
	[phoneNumber_H] nvarchar(4000),
	[creditCard_H] nvarchar(4000),
	[accountNumber_H] nvarchar(4000)
	)
	WITH (
	LOCATION = 'person/**',
	DATA_SOURCE = [files_venkydatalake1001_dfs_core_windows_net],
	FILE_FORMAT = [SynapseParquetFormat]
	)
GO

Select top 10 
*
from dbo.person_hashed
GO