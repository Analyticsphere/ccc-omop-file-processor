
        COPY (
            SELECT 
                "measurement_id" AS measurement_id
            , 
                "person_id" AS person_id
            , 
                "measurement_concept_id" AS measurement_concept_id
            , 
                "measurement_date" AS measurement_date
            
            FROM read_csv('gs://synthea53/2025-01-01/measurement.csv',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False ,store_rejects=True, ignore_errors=True, parallel=False)
        ) TO 'gs://synthea53/2025-01-01/artifacts/converted_files/measurement.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    
