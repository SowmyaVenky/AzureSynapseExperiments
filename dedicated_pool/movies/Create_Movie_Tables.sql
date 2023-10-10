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

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movies' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.movies
	(
	 [movie_id] int,
	 [is_adult] nvarchar(4000),
	 [budget] bigint,
	 [homepage] nvarchar(4000),
	 [imdb_id] nvarchar(4000),
	 [video] bit,
	 [poster_path] nvarchar(4000),
	 [original_language] nvarchar(4000),
	 [original_title] nvarchar(4000),
	 [overview] nvarchar(4000),
	 [title] nvarchar(4000),
	 [popularity] float,
	 [release_date] date,
	 [revenue] bigint,
	 [runtime] real,
	 [status] nvarchar(4000),
	 [tagline] nvarchar(4000),
	 [vote_count] int,
	 [vote_average] real
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'production_company' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.production_company
	(
	 [production_company_id] int,
	 [production_company_name] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'production_country' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.production_country
	(
	 [production_country_id] nvarchar(4000),
	 [production_country_name] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'spoken_language' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.spoken_language
	(
	 [spoken_language_id] nvarchar(4000),
	 [spoken_language_name] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'genre' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.genre
	(
	 [genre_id] bigint,
	 [genre_name] nvarchar(4000)
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO

IF NOT EXISTS (SELECT * FROM sys.objects O JOIN sys.schemas S ON O.schema_id = S.schema_id WHERE O.NAME = 'movie_genre' AND O.TYPE = 'U' AND S.NAME = 'dbo')
CREATE TABLE dbo.bulk_load_movie_genre
	(
	 [genre_id] bigint,
	 [movie_id] bigint
	)
WITH
	(
	DISTRIBUTION = ROUND_ROBIN,
	 CLUSTERED COLUMNSTORE INDEX
	 -- HEAP
	)
GO
