IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'users' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE [dbo].[users]
	(
	 [user_id] int,
	 [firstName] nvarchar(4000),
	 [lastName] nvarchar(4000),
	 [streetName] nvarchar(4000),
	 [number] nvarchar(4000),
	 [city] nvarchar(4000),
	 [state] nvarchar(4000),
	 [zip] nvarchar(4000),
	 [phone] nvarchar(4000),
	 [creditCard] nvarchar(4000),
	 [expDate] nvarchar(4000),
	 [accountNumber] nvarchar(4000),
	 [emailAddr] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_users
AS
BEGIN
COPY INTO [dbo].[users]
(user_id 1, firstName 2, lastName 3, streetName 4, number 5, city 6, state 7, zip 8, phone 9, creditCard 10, expDate 11, accountNumber 12, emailAddr 13)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/users'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'ratings' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE [dbo].[ratings]
	(
	 [user_id] int,
	 [movie_id] int,
	 [rating] float
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_ratings
AS
BEGIN
COPY INTO [dbo].[ratings]
(user_id 1, movie_id 2, rating 3)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/ratings'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'cast' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.cast
	(
	 [movie_id] int,
	 [cast_id] bigint,
	 [character_name] nvarchar(4000),
	 [credit_id] nvarchar(4000),
	 [gender] bigint,
	 [movie_cast_id] bigint,
	 [cast_name] nvarchar(4000),
	 [cast_order] bigint,
	 [profile_path] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_cast
AS
BEGIN
COPY INTO dbo.cast
(movie_id 1, cast_id 2, character_name 3, credit_id 4, gender 5, movie_cast_id 6, cast_name 7, cast_order 8, profile_path 9)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/cast'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'collections' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.collections
	(
	 [collection_id] int,
	 [collection_name] nvarchar(4000),
	 [collection_poster_path] nvarchar(4000),
	 [collection_backdrop_path] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_collections
AS
BEGIN
COPY INTO dbo.collections
(collection_id 1, collection_name 2, collection_poster_path 3, collection_backdrop_path 4)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/collections'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'crew' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.crew
	(
	 [movie_id] int,
	 [credit_id] nvarchar(4000),
	 [department] nvarchar(4000),
	 [gender] bigint,
	 [crew_id] bigint,
	 [crew_job] nvarchar(4000),
	 [crew_name] nvarchar(4000),
	 [profile_path] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_crew
AS
BEGIN
COPY INTO dbo.crew
(movie_id 1, credit_id 2, department 3, gender 4, crew_id 5, crew_job 6, crew_name 7, profile_path 8)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/crew'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'keywords' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.keywords
	(
	 [keyword_id] int,
	 [keyword] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_keywords
AS
BEGIN
COPY INTO dbo.keywords
(keyword_id 1, keyword 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/keywords'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_collection' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movie_collection
	(
	 [movie_id] int,
	 [collection_id] int
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_movie_collection
AS
BEGIN
COPY INTO dbo.movie_collection
(movie_id 1, collection_id 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/movie_collection'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_keywords' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movie_keywords
	(
	 [movie_id] int,
	 [keyword_id] int
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_movie_keywords
AS
BEGIN
COPY INTO dbo.movie_keywords
(movie_id 1, keyword_id 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/movie_keywords'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_production_company' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movie_production_company
	(
	 [movie_id] int,
	 [production_company_id] int
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_movie_production_company
AS
BEGIN
COPY INTO dbo.movie_production_company
(movie_id 1, production_company_id 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/movie_production_company'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_production_country' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movie_production_country
	(
	 [movie_id] int,
	 [production_country_id] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_movie_production_country
AS
BEGIN
COPY INTO dbo.movie_production_country
(movie_id 1, production_country_id 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/movie_production_country'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_spoken_lang' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movie_spoken_lang
	(
	 [movie_id] int,
	 [spoken_language_id] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

--Uncomment the 4 lines below to create a stored procedure for data pipeline orchestration​
CREATE PROC bulk_load_movie_spoken_lang
AS
BEGIN
COPY INTO dbo.movie_spoken_lang
(movie_id 1, spoken_language_id 2)
FROM 'https://venkydatalake1001.dfs.core.windows.net/files/bronze/movielens/movie_spoken_lang'
WITH
(
	FILE_TYPE = 'PARQUET'
	,MAXERRORS = 0
)
END
GO

SELECT TOP 100 * FROM dbo.movie_spoken_lang
GO

SELECT TOP 100 * FROM dbo.movie_production_country
GO

SELECT TOP 100 * FROM dbo.movie_production_company
GO

SELECT TOP 100 * FROM dbo.movie_keywords
GO

SELECT TOP 100 * FROM dbo.movie_collection
GO

SELECT TOP 100 * FROM dbo.keywords
GO

SELECT TOP 100 * FROM dbo.crew
GO

SELECT TOP 100 * FROM dbo.collections
GO

SELECT TOP 100 * FROM dbo.cast
GO

SELECT count(*) FROM [dbo].[ratings]
GO

SELECT count(*) FROM [dbo].[users]
GO