CREATE OR REPLACE TABLE row_check AS
            SELECT
                TRY_CAST(COALESCE(condition_occurrence_id, '-1') AS BIGINT) AS condition_occurrence_id,
                TRY_CAST(COALESCE(connect_id, '-1') AS BIGINT) AS person_id,
                TRY_CAST(COALESCE(condition_concept_id, '0') AS BIGINT) AS condition_concept_id,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(condition_start_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(condition_start_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS condition_start_date,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(condition_start_datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATETIME),
                        TRY_CAST(condition_start_datetime AS DATETIME),
                        CAST(NULL AS DATETIME)
                    ) AS condition_start_datetime,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(condition_end_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(condition_end_date AS DATE),
                        CAST(NULL AS DATE)
                    ) AS condition_end_date,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(condition_end_datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATETIME),
                        TRY_CAST(condition_end_datetime AS DATETIME),
                        CAST(NULL AS DATETIME)
                    ) AS condition_end_datetime,
                TRY_CAST(COALESCE(condition_type_concept_id, '0') AS BIGINT) AS condition_type_concept_id,
                TRY_CAST(COALESCE(condition_status_concept_id, '0') AS BIGINT) AS condition_status_concept_id,
                TRY_CAST(stop_reason AS VARCHAR) AS stop_reason,
                TRY_CAST(provider_id AS BIGINT) AS provider_id,
                TRY_CAST(visit_occurrence_id AS BIGINT) AS visit_occurrence_id,
                TRY_CAST(visit_detail_id AS BIGINT) AS visit_detail_id,
                TRY_CAST(condition_source_value AS VARCHAR) AS condition_source_value,
                TRY_CAST(COALESCE(condition_source_concept_id, '0') AS BIGINT) AS condition_source_concept_id,
                TRY_CAST(condition_status_source_value AS VARCHAR) AS condition_status_source_value,
                CASE
                    WHEN COALESCE(CAST(TRY_CAST(COALESCE(condition_occurrence_id, '-1') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(connect_id, '-1') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(condition_concept_id, '0') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(condition_start_date, '1970-01-01') AS DATE) AS VARCHAR), CAST(TRY_CAST(COALESCE(condition_type_concept_id, '0') AS BIGINT) AS VARCHAR)) IS NULL THEN CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(connect_id AS VARCHAR), ''), COALESCE(CAST(condition_occurrence_id AS VARCHAR), ''), COALESCE(CAST(condition_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_start_date AS VARCHAR), ''), COALESCE(CAST(condition_start_datetime AS VARCHAR), ''), COALESCE(CAST(condition_end_date AS VARCHAR), ''), COALESCE(CAST(condition_end_datetime AS VARCHAR), ''), COALESCE(CAST(condition_type_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_concept_id AS VARCHAR), ''), COALESCE(CAST(stop_reason AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(condition_source_value AS VARCHAR), ''), COALESCE(CAST(condition_source_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_source_value AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://test-bucket/2025-01-01/condition_occurrence.parquet')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://test-bucket/2025-01-01/condition_occurrence.parquet')
            WHERE CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(connect_id AS VARCHAR), ''), COALESCE(CAST(condition_occurrence_id AS VARCHAR), ''), COALESCE(CAST(condition_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_start_date AS VARCHAR), ''), COALESCE(CAST(condition_start_datetime AS VARCHAR), ''), COALESCE(CAST(condition_end_date AS VARCHAR), ''), COALESCE(CAST(condition_end_datetime AS VARCHAR), ''), COALESCE(CAST(condition_type_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_concept_id AS VARCHAR), ''), COALESCE(CAST(stop_reason AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(condition_source_value AS VARCHAR), ''), COALESCE(CAST(condition_source_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_source_value AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://test-bucket/2025-01-01/artifacts/invalid_rows/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;

        COPY (
            SELECT * EXCLUDE (row_hash)
            REPLACE(CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(condition_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_start_date AS VARCHAR), ''), COALESCE(CAST(condition_start_datetime AS VARCHAR), ''), COALESCE(CAST(condition_end_date AS VARCHAR), ''), COALESCE(CAST(condition_end_datetime AS VARCHAR), ''), COALESCE(CAST(condition_type_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_concept_id AS VARCHAR), ''), COALESCE(CAST(stop_reason AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(condition_source_value AS VARCHAR), ''), COALESCE(CAST(condition_source_concept_id AS VARCHAR), ''), COALESCE(CAST(condition_status_source_value AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS condition_occurrence_id)

            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://test-bucket/2025-01-01/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;
