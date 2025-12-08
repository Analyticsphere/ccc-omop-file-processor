
        COPY (
            SELECT CAST(note_nlp_id AS VARCHAR) AS note_nlp_id, CAST(note_id AS VARCHAR) AS note_id, CAST("offset" AS VARCHAR) AS "offset", CAST(lexical_variant AS VARCHAR) AS lexical_variant
            FROM read_parquet('gs://synthea53/2025-01-01/note_nlp.parquet')
        )
        TO 'gs://synthea53/2025-01-01/artifacts/converted_files/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    