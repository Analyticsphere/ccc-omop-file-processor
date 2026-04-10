
        COPY (
            SELECT NULLIF(NULLIF("note_nlp_id", 'NULL'), 'null') AS "note_nlp_id", NULLIF(NULLIF("offset", 'NULL'), 'null') AS "offset", NULLIF(NULLIF("lexical_variant", 'NULL'), 'null') AS "lexical_variant"
            FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/note_nlp.parquet')
        )
        TO 'gs://synthea53/2025-01-01/artifacts/converted_files/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    
