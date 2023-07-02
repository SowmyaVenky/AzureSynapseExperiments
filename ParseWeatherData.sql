-- This demonstrates how we can flatten the json array structure and select the fiels we want.
-- This is done inside the build in pool or the serverless pool. Ad-hoc analysis can be performed 
-- using this technique. 

SELECT 
    [key] as rowindex,    
    JSON_VALUE( [value], '$.date' ) datetaken,
    JSON_VALUE( [value], '$.tmin' ) tmin,
    JSON_VALUE( [value], '$.tmax' ) tmax,
    JSON_VALUE( [value], '$.prcp' ) prcp,
    JSON_VALUE( [value], '$.snow' ) snow,
    JSON_VALUE( [value], '$.snwd' ) snwd,
    JSON_VALUE( [value], '$.awnd' ) awnd
FROM
    OPENROWSET(
        BULK 'https://venkydatalake101.dfs.core.windows.net/files/rdu-weather-history.json',
        FORMAT = 'CSV',
        FIELDQUOTE = '0x0b',
        FIELDTERMINATOR ='0x0b',
        ROWTERMINATOR = '0x0b'
    ) 
    WITH (
        jsonContent varchar(MAX)
    ) AS [result]
    CROSS APPLY OPENJSON( jsonContent)
