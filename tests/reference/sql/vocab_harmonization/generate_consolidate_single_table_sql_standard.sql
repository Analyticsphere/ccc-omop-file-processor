
            COPY (
                SELECT * FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/parts/*.parquet')
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        