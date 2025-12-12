
            COPY (
                SELECT * FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_non_dup_abc123.parquet')
                UNION ALL
                SELECT * FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_dup_fixed_abc123.parquet')
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
