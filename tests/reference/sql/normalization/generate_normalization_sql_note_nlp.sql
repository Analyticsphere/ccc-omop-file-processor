CREATE OR REPLACE TABLE row_check AS
            SELECT
                TRY_CAST(COALESCE(note_nlp_id, '-1') AS BIGINT) AS note_nlp_id,
                TRY_CAST(COALESCE(note_id, '-1') AS BIGINT) AS note_id,
                TRY_CAST(COALESCE(section_concept_id, '0') AS BIGINT) AS section_concept_id,
                TRY_CAST(snippet AS VARCHAR) AS snippet,
                TRY_CAST("offset" AS VARCHAR) AS "offset",
                TRY_CAST(COALESCE(lexical_variant, '') AS VARCHAR) AS lexical_variant,
                TRY_CAST(COALESCE(note_nlp_concept_id, '0') AS BIGINT) AS note_nlp_concept_id,
                TRY_CAST(COALESCE(note_nlp_source_concept_id, '0') AS BIGINT) AS note_nlp_source_concept_id,
                TRY_CAST(nlp_system AS VARCHAR) AS nlp_system,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(nlp_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(nlp_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS nlp_date,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(nlp_datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATETIME),
                        TRY_CAST(nlp_datetime AS DATETIME),
                        CAST(NULL AS DATETIME)
                    ) AS nlp_datetime,
                TRY_CAST(term_exists AS VARCHAR) AS term_exists,
                TRY_CAST(term_temporal AS VARCHAR) AS term_temporal,
                TRY_CAST(term_modifiers AS VARCHAR) AS term_modifiers,
                CASE
                    WHEN NOT ((CAST(TRY_CAST(COALESCE(note_nlp_id, '-1') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(note_id, '-1') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(lexical_variant, '') AS VARCHAR) AS VARCHAR)) IS NOT NULL AND (CAST(COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(nlp_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(nlp_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS VARCHAR)) IS NOT NULL) THEN CAST((CAST(hash(CONCAT(COALESCE(CAST(note_nlp_id AS VARCHAR), ''), COALESCE(CAST(note_id AS VARCHAR), ''), COALESCE(CAST(section_concept_id AS VARCHAR), ''), COALESCE(CAST(snippet AS VARCHAR), ''), COALESCE(CAST("offset" AS VARCHAR), ''), COALESCE(CAST(lexical_variant AS VARCHAR), ''), COALESCE(CAST(note_nlp_concept_id AS VARCHAR), ''), COALESCE(CAST(note_nlp_source_concept_id AS VARCHAR), ''), COALESCE(CAST(nlp_system AS VARCHAR), ''), COALESCE(CAST(nlp_date AS VARCHAR), ''), COALESCE(CAST(nlp_datetime AS VARCHAR), ''), COALESCE(CAST(term_exists AS VARCHAR), ''), COALESCE(CAST(term_temporal AS VARCHAR), ''), COALESCE(CAST(term_modifiers AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://test-bucket/2025-01-01/note_nlp.parquet')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://test-bucket/2025-01-01/note_nlp.parquet')
            WHERE CAST((CAST(hash(CONCAT(COALESCE(CAST(note_nlp_id AS VARCHAR), ''), COALESCE(CAST(note_id AS VARCHAR), ''), COALESCE(CAST(section_concept_id AS VARCHAR), ''), COALESCE(CAST(snippet AS VARCHAR), ''), COALESCE(CAST("offset" AS VARCHAR), ''), COALESCE(CAST(lexical_variant AS VARCHAR), ''), COALESCE(CAST(note_nlp_concept_id AS VARCHAR), ''), COALESCE(CAST(note_nlp_source_concept_id AS VARCHAR), ''), COALESCE(CAST(nlp_system AS VARCHAR), ''), COALESCE(CAST(nlp_date AS VARCHAR), ''), COALESCE(CAST(nlp_datetime AS VARCHAR), ''), COALESCE(CAST(term_exists AS VARCHAR), ''), COALESCE(CAST(term_temporal AS VARCHAR), ''), COALESCE(CAST(term_modifiers AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://test-bucket/2025-01-01/artifacts/invalid_rows/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) 
            REPLACE(CAST((CAST(hash(CONCAT(COALESCE(CAST(note_id AS VARCHAR), ''), COALESCE(CAST(section_concept_id AS VARCHAR), ''), COALESCE(CAST(snippet AS VARCHAR), ''), COALESCE(CAST("offset" AS VARCHAR), ''), COALESCE(CAST(lexical_variant AS VARCHAR), ''), COALESCE(CAST(note_nlp_concept_id AS VARCHAR), ''), COALESCE(CAST(note_nlp_source_concept_id AS VARCHAR), ''), COALESCE(CAST(nlp_system AS VARCHAR), ''), COALESCE(CAST(nlp_date AS VARCHAR), ''), COALESCE(CAST(nlp_datetime AS VARCHAR), ''), COALESCE(CAST(term_exists AS VARCHAR), ''), COALESCE(CAST(term_temporal AS VARCHAR), ''), COALESCE(CAST(term_modifiers AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS note_nlp_id)
        
            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://test-bucket/2025-01-01/note_nlp.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;