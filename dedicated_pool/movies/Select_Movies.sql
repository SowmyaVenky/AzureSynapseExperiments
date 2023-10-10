EXECUTE [dbo].[bulk_load_cast]
GO
EXECUTE [dbo].[bulk_load_collections]
GO
EXECUTE [dbo].[bulk_load_crew]
GO
EXECUTE [dbo].[bulk_load_keywords]
GO
EXECUTE [dbo].[bulk_load_movie_collection]
GO
EXECUTE [dbo].[bulk_load_movie_keywords]
GO
EXECUTE [dbo].[bulk_load_movie_production_company]
GO
EXECUTE [dbo].[bulk_load_movie_production_country]
GO
EXECUTE [dbo].[bulk_load_movie_spoken_lang]
GO
EXECUTE [dbo].[bulk_load_ratings]
GO
EXECUTE [dbo].[bulk_load_users]
GO

SELECT 'cast', count(*)  FROM dbo.cast
UNION 
SELECT 'movie_production_country', count(*)  FROM dbo.movie_production_country
UNION
SELECT 'movie_production_company', count(*)  FROM dbo.movie_production_company
UNION
SELECT 'movie_keywords', count(*)  FROM dbo.movie_keywords
UNION
SELECT 'movie_collection', count(*)  FROM dbo.movie_collection
UNION
SELECT 'keywords', count(*)  FROM dbo.keywords
UNION
SELECT 'crew', count(*)  FROM dbo.crew
UNION
SELECT 'collections', count(*)  FROM dbo.collections
UNION
SELECT 'spoken_lang', count(*)  FROM dbo.movie_spoken_lang
UNION
SELECT 'ratings', count(*) FROM [dbo].[ratings]
UNION
SELECT 'users', count(*) FROM [dbo].[users]