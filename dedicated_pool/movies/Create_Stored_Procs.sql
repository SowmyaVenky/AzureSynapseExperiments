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
