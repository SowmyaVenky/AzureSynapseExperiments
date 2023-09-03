-- This is auto-generated code
SELECT
    TOP 100 time, temperature_2m
FROM
    OPENROWSET(
        BULK 'https://venkydatalake101.dfs.core.windows.net/files/spring_tx_temps_formatted/**',
        FORMAT = 'PARQUET'
    ) AS [result]
