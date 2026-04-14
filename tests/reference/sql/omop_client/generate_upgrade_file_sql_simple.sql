
        COPY (
            SELECT * FROM table
            FROM read_parquet('gs://bucket/2025-01-01/artifacts/converted_files/person.parquet')
        ) TO 'gs://bucket/2025-01-01/artifacts/converted_files/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        