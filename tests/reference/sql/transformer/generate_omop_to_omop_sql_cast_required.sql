
            COPY (
                
        SELECT
            CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id
        FROM read_parquet('gs://bucket/2025-01-01/harmonized/*.parquet')
        
                WHERE target_table = 'observation'
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/observation/parts/observation_from_condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        