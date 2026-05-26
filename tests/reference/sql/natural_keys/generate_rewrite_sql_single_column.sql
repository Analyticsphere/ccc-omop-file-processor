COPY (
            SELECT * REPLACE (
                
            CASE WHEN location_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(location_id AS VARCHAR), 'site_gamma')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS location_id
        
            )
            FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/care_site.parquet')
        ) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/care_site.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
