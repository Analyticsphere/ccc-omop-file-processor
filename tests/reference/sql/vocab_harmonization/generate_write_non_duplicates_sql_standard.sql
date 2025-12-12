
            COPY (
                SELECT *
                FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')
                WHERE condition_occurrence_id NOT IN (SELECT condition_occurrence_id FROM duplicate_keys)
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_non_dup_abc123.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
