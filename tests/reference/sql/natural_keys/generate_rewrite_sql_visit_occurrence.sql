COPY (
            SELECT * REPLACE (
                
            CASE WHEN visit_occurrence_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(visit_occurrence_id AS VARCHAR), 'site_alpha')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS visit_occurrence_id
        ,
                
            CASE WHEN preceding_visit_occurrence_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(preceding_visit_occurrence_id AS VARCHAR), 'site_alpha')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS preceding_visit_occurrence_id
        ,
                
            CASE WHEN provider_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(provider_id AS VARCHAR), 'site_alpha')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS provider_id
        ,
                
            CASE WHEN care_site_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(care_site_id AS VARCHAR), 'site_alpha')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS care_site_id
        
            )
            FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/visit_occurrence.parquet')
        ) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/visit_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
