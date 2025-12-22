CREATE OR REPLACE TABLE row_check AS
            SELECT
                TRY_CAST(COALESCE(measurement_id, '-1') AS BIGINT) AS measurement_id,
                TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id,
                TRY_CAST(COALESCE(measurement_concept_id, '0') AS BIGINT) AS measurement_concept_id,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(measurement_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(measurement_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS measurement_date,
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(measurement_datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATETIME),
                        TRY_CAST(measurement_datetime AS DATETIME),
                        CAST(NULL AS DATETIME)
                    ) AS measurement_datetime,
                TRY_CAST(measurement_time AS VARCHAR) AS measurement_time,
                TRY_CAST(COALESCE(measurement_type_concept_id, '0') AS BIGINT) AS measurement_type_concept_id,
                TRY_CAST(COALESCE(operator_concept_id, '0') AS BIGINT) AS operator_concept_id,
                TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,
                TRY_CAST(COALESCE(value_as_concept_id, '0') AS BIGINT) AS value_as_concept_id,
                TRY_CAST(COALESCE(unit_concept_id, '0') AS BIGINT) AS unit_concept_id,
                TRY_CAST(range_low AS DOUBLE) AS range_low,
                TRY_CAST(range_high AS DOUBLE) AS range_high,
                TRY_CAST(provider_id AS BIGINT) AS provider_id,
                TRY_CAST(visit_occurrence_id AS BIGINT) AS visit_occurrence_id,
                TRY_CAST(visit_detail_id AS BIGINT) AS visit_detail_id,
                TRY_CAST(measurement_source_value AS VARCHAR) AS measurement_source_value,
                TRY_CAST(COALESCE(measurement_source_concept_id, '0') AS BIGINT) AS measurement_source_concept_id,
                TRY_CAST(unit_source_value AS VARCHAR) AS unit_source_value,
                TRY_CAST(COALESCE(unit_source_concept_id, '0') AS BIGINT) AS unit_source_concept_id,
                TRY_CAST(value_source_value AS VARCHAR) AS value_source_value,
                TRY_CAST(measurement_event_id AS BIGINT) AS measurement_event_id,
                TRY_CAST(COALESCE(meas_event_field_concept_id, '0') AS BIGINT) AS meas_event_field_concept_id,
                CASE
                    WHEN NOT ((CAST(TRY_CAST(COALESCE(measurement_id, '-1') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(measurement_concept_id, '0') AS BIGINT) AS VARCHAR)) IS NOT NULL AND (CAST(COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(measurement_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(measurement_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS VARCHAR)) IS NOT NULL AND (CAST(TRY_CAST(COALESCE(measurement_type_concept_id, '0') AS BIGINT) AS VARCHAR)) IS NOT NULL) THEN CAST((CAST(hash(CONCAT(COALESCE(CAST(measurement_id AS VARCHAR), ''), COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(measurement_concept_id AS VARCHAR), ''), COALESCE(CAST(measurement_date AS VARCHAR), ''), COALESCE(CAST(measurement_datetime AS VARCHAR), ''), COALESCE(CAST(measurement_time AS VARCHAR), ''), COALESCE(CAST(measurement_type_concept_id AS VARCHAR), ''), COALESCE(CAST(operator_concept_id AS VARCHAR), ''), COALESCE(CAST(value_as_number AS VARCHAR), ''), COALESCE(CAST(value_as_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_concept_id AS VARCHAR), ''), COALESCE(CAST(range_low AS VARCHAR), ''), COALESCE(CAST(range_high AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(measurement_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_source_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_source_value AS VARCHAR), ''), COALESCE(CAST(unit_source_concept_id AS VARCHAR), ''), COALESCE(CAST(value_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_event_id AS VARCHAR), ''), COALESCE(CAST(meas_event_field_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://test-bucket/2025-01-01/measurement.parquet')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://test-bucket/2025-01-01/measurement.parquet')
            WHERE CAST((CAST(hash(CONCAT(COALESCE(CAST(measurement_id AS VARCHAR), ''), COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(measurement_concept_id AS VARCHAR), ''), COALESCE(CAST(measurement_date AS VARCHAR), ''), COALESCE(CAST(measurement_datetime AS VARCHAR), ''), COALESCE(CAST(measurement_time AS VARCHAR), ''), COALESCE(CAST(measurement_type_concept_id AS VARCHAR), ''), COALESCE(CAST(operator_concept_id AS VARCHAR), ''), COALESCE(CAST(value_as_number AS VARCHAR), ''), COALESCE(CAST(value_as_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_concept_id AS VARCHAR), ''), COALESCE(CAST(range_low AS VARCHAR), ''), COALESCE(CAST(range_high AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(measurement_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_source_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_source_value AS VARCHAR), ''), COALESCE(CAST(unit_source_concept_id AS VARCHAR), ''), COALESCE(CAST(value_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_event_id AS VARCHAR), ''), COALESCE(CAST(meas_event_field_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://test-bucket/2025-01-01/artifacts/invalid_rows/measurement.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) 
            REPLACE(CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(measurement_concept_id AS VARCHAR), ''), COALESCE(CAST(measurement_date AS VARCHAR), ''), COALESCE(CAST(measurement_datetime AS VARCHAR), ''), COALESCE(CAST(measurement_time AS VARCHAR), ''), COALESCE(CAST(measurement_type_concept_id AS VARCHAR), ''), COALESCE(CAST(operator_concept_id AS VARCHAR), ''), COALESCE(CAST(value_as_number AS VARCHAR), ''), COALESCE(CAST(value_as_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_concept_id AS VARCHAR), ''), COALESCE(CAST(range_low AS VARCHAR), ''), COALESCE(CAST(range_high AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(visit_occurrence_id AS VARCHAR), ''), COALESCE(CAST(visit_detail_id AS VARCHAR), ''), COALESCE(CAST(measurement_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_source_concept_id AS VARCHAR), ''), COALESCE(CAST(unit_source_value AS VARCHAR), ''), COALESCE(CAST(unit_source_concept_id AS VARCHAR), ''), COALESCE(CAST(value_source_value AS VARCHAR), ''), COALESCE(CAST(measurement_event_id AS VARCHAR), ''), COALESCE(CAST(meas_event_field_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS measurement_id)
        
            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://test-bucket/2025-01-01/measurement.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;