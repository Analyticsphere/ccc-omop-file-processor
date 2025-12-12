
            COPY (
                SELECT
 CAST(COALESCE(condition_occurrence_id, '-1') AS BIGINT) AS visit_occurrence_id,
 CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id,
 CAST(COALESCE(condition_concept_id, '0') AS BIGINT) AS visit_concept_id,
 CAST(COALESCE(condition_start_date, '1970-01-01') AS DATE) AS visit_start_date,
 TRY_CAST(condition_start_datetime AS DATETIME) AS visit_start_datetime,
 CAST(COALESCE(COALESCE(condition_end_date, condition_start_date), '1970-01-01') AS DATE) AS visit_end_date,
 TRY_CAST(condition_end_datetime AS DATETIME) AS visit_end_datetime,
 CAST(COALESCE(condition_type_concept_id, '0') AS BIGINT) AS visit_type_concept_id,
 TRY_CAST(provider_id AS BIGINT) AS provider_id,
 TRY_CAST(NULL AS BIGINT) AS care_site_id,
 TRY_CAST(condition_source_value AS VARCHAR) AS visit_source_value,
 TRY_CAST(condition_source_concept_id AS BIGINT) AS visit_source_concept_id,
 TRY_CAST(0 AS BIGINT) AS admitted_from_concept_id,
 TRY_CAST(NULL AS VARCHAR) AS admitted_from_source_value,
 TRY_CAST(0 AS BIGINT) AS discharged_to_concept_id,
 TRY_CAST(NULL AS VARCHAR) AS discharged_to_source_value,
 TRY_CAST(NULL AS BIGINT) AS preceding_visit_occurrence_id
FROM read_parquet('gs://synthea53/2025-01-01/harmonized/*.parquet')
                WHERE target_table = 'visit_occurrence'
            ) TO 'gs://synthea53/2025-01-01/artifacts/omop_etl/visit_occurrence/parts/visit_occurrence_from_condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        