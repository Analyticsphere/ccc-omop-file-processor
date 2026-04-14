
        COPY (
            SELECT CAST(note_nlp_id AS VARCHAR) AS note_nlp_id, CAST("offset" AS VARCHAR) AS "offset", CAST(snippet AS VARCHAR) AS snippet
            FROM read_parquet('gs://bucket/2025-01-01/note_nlp.parquet')
        )
        TO 'gs://bucket/2025-01-01/artifacts/converted_files/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        