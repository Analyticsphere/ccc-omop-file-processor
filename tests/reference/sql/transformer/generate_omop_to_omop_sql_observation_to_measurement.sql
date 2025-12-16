
            COPY (
                SELECT
 CAST(COALESCE(hash(CONCAT(CAST(observation_id AS VARCHAR),CAST(person_id AS VARCHAR),CAST(observation_concept_id AS VARCHAR),CAST(observation_date AS VARCHAR),CAST(observation_datetime AS VARCHAR),CAST(observation_datetime AS VARCHAR),CAST(observation_type_concept_id AS VARCHAR),CAST(0 AS VARCHAR),CAST(value_as_number AS VARCHAR),CAST(COALESCE(vh_value_as_concept_id, value_as_concept_id, 0) AS VARCHAR),CAST(unit_concept_id AS VARCHAR),CAST(NULL AS VARCHAR),CAST(NULL AS VARCHAR),CAST(provider_id AS VARCHAR),CAST(visit_occurrence_id AS VARCHAR),CAST(visit_detail_id AS VARCHAR),CAST(observation_source_value AS VARCHAR),CAST(observation_source_concept_id AS VARCHAR),CAST(unit_source_value AS VARCHAR),CAST(0 AS VARCHAR),CAST(value_source_value AS VARCHAR),CAST(observation_event_id AS VARCHAR),CAST(obs_event_field_concept_id AS VARCHAR),'test_site')) % 9223372036854775807, '-1') AS BIGINT) AS measurement_id,
 CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id,
 CAST(COALESCE(observation_concept_id, '0') AS BIGINT) AS measurement_concept_id,
 CAST(COALESCE(observation_date, '1970-01-01') AS DATE) AS measurement_date,
 TRY_CAST(observation_datetime AS DATETIME) AS measurement_datetime,
 TRY_CAST(observation_datetime AS VARCHAR) AS measurement_time,
 CAST(COALESCE(observation_type_concept_id, '0') AS BIGINT) AS measurement_type_concept_id,
 TRY_CAST(COALESCE(0, '0') AS BIGINT) AS operator_concept_id,
 TRY_CAST(value_as_number AS DOUBLE) AS value_as_number,
 TRY_CAST(COALESCE(COALESCE(vh_value_as_concept_id, value_as_concept_id, 0), '0') AS BIGINT) AS value_as_concept_id,
 TRY_CAST(COALESCE(unit_concept_id, '0') AS BIGINT) AS unit_concept_id,
 TRY_CAST(NULL AS DOUBLE) AS range_low,
 TRY_CAST(NULL AS DOUBLE) AS range_high,
 TRY_CAST(provider_id AS BIGINT) AS provider_id,
 TRY_CAST(visit_occurrence_id AS BIGINT) AS visit_occurrence_id,
 TRY_CAST(visit_detail_id AS BIGINT) AS visit_detail_id,
 TRY_CAST(observation_source_value AS VARCHAR) AS measurement_source_value,
 TRY_CAST(COALESCE(observation_source_concept_id, '0') AS BIGINT) AS measurement_source_concept_id,
 TRY_CAST(unit_source_value AS VARCHAR) AS unit_source_value,
 TRY_CAST(COALESCE(0, '0') AS BIGINT) AS unit_source_concept_id,
 TRY_CAST(value_source_value AS VARCHAR) AS value_source_value,
 TRY_CAST(observation_event_id AS BIGINT) AS measurement_event_id,
 TRY_CAST(COALESCE(obs_event_field_concept_id, '0') AS BIGINT) AS meas_event_field_concept_id
FROM read_parquet('gs://synthea53/2025-01-01/harmonized/*.parquet')
                WHERE target_table = 'measurement'
            ) TO 'gs://synthea53/2025-01-01/artifacts/omop_etl/measurement/parts/measurement_from_observation.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        