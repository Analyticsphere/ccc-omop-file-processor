
        COPY (
            SELECT 
                "person_id" AS person_id
                , 
                "gender_concept_id" AS gender_concept_id
                
            FROM read_csv('gs://bucket/2025-01-01/person.csv',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False ,store_rejects=True, ignore_errors=True)
        ) TO 'gs://bucket/2025-01-01/artifacts/converted_files/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        