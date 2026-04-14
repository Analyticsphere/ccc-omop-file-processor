
        SELECT vocabulary_version
        FROM read_parquet('gs://vocab-bucket/vocab/v5.0/optimized_vocab/vocabulary.parquet')
        WHERE vocabulary_id = 'None'
    