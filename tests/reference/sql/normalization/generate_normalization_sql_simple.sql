CREATE OR REPLACE TABLE row_check AS
            SELECT
                TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id,
                TRY_CAST(COALESCE(gender_concept_id, '0') AS BIGINT) AS gender_concept_id,
                CASE
                    WHEN NOT ((CAST(TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(gender_concept_id, '0') AS BIGINT) AS VARCHAR)) IS NOT NULL) THEN CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(gender_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://bucket/2025-01-01/person.parquet')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://bucket/2025-01-01/person.parquet')
            WHERE CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(gender_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://bucket/2025-01-01/invalid_person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) 
            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://bucket/2025-01-01/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;