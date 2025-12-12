
        COPY (
            
        SELECT
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            birth_datetime
    
            FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/person.parquet')
        ) TO 'gs://synthea53/2025-01-01/artifacts/converted_files/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    