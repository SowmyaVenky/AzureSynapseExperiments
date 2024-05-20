-- This is auto-generated code
SELECT
    TOP 100 *
FROM
    OPENROWSET(
        BULK 'https://venkydatalake1001.dfs.core.windows.net/files/part-00000-66ea8956-0b0f-454e-8fd1-1b779e39b4a6-c000.snappy.parquet',
        FORMAT = 'PARQUET'
    ) AS [result]
