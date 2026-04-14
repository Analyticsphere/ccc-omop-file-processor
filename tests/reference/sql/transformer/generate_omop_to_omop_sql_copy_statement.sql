
            COPY (
                
        SELECT
            CAST(COALESCE(hash(CONCAT(CAST(condition_occurrence_id AS VARCHAR),'test_site')) % 9223372036854775807, '-1') AS BIGINT) AS observation_id
        FROM read_parquet('gs://bucket/2025-01-01/harmonized/*.parquet')
        
                WHERE target_table = 'observation'
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/observation/parts/observation_from_condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        