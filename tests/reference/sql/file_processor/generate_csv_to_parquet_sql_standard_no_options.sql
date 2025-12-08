
        COPY (
            SELECT 
                "person_id" AS person_id
            , 
                "gender_concept_id" AS gender_concept_id
            , 
                "year_of_birth" AS year_of_birth
            , 
                "birth_datetime" AS birth_datetime
            
            FROM read_csv('gs://synthea53/2025-01-01/person.csv',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False )
        ) TO 'gs://synthea53/2025-01-01/artifacts/converted_files/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    
