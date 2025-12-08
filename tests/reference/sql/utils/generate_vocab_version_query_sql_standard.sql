
        SELECT vocabulary_version
        FROM read_parquet('gs://vocab-bucket/synthea53/2025-01-01/artifacts/converted_files/vocabulary.parquet')
        WHERE vocabulary_id = 'None'
