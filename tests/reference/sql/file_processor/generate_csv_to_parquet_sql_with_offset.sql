
        COPY (
            SELECT 
                "note_nlp_id" AS note_nlp_id
            , 
                "note_id" AS note_id
            , "offset" AS "offset", 
                "lexical_variant" AS lexical_variant
            
            FROM read_csv('gs://synthea53/2025-01-01/note_nlp.csv',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False )
        ) TO 'gs://synthea53/2025-01-01/artifacts/converted_files/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    
