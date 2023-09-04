-- This is auto-generated code
SELECT
     latitude, longitude, max(temperature_2m) as maxtemp
FROM
    OPENROWSET(
        BULK 'https://venkydatalake1001.dfs.core.windows.net/files/spring_tx_temps_formatted/**',
        FORMAT = 'PARQUET'
    ) AS [result]
GROUP BY latitude, longitude
