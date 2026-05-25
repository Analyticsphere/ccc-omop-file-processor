COPY (
            SELECT
                * EXCLUDE (visit_occurrence_id, visit_detail_id, provider_id),
                
            CASE WHEN visit_occurrence_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(visit_occurrence_id AS VARCHAR), 'site_beta')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS visit_occurrence_id
        ,
                
            CASE WHEN visit_detail_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(visit_detail_id AS VARCHAR), 'site_beta')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS visit_detail_id
        ,
                
            CASE WHEN provider_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(provider_id AS VARCHAR), 'site_beta')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS provider_id
        
            FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet')
        ) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
