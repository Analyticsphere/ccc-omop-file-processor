
            COPY (
                
        SELECT
            TRY_CAST(value_as_number AS DOUBLE) AS value_as_number
        FROM read_parquet('gs://bucket/2025-01-01/harmonized/*.parquet')
        
                WHERE target_table = 'measurement'
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/measurement/parts/measurement_from_observation.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        