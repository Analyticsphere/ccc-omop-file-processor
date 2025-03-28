SELECT
	condition_occurrence_id AS condition_occurrence_id,
	person_id AS person_id,
	condition_concept_id AS condition_concept_id,
	condition_start_date AS condition_start_date,
	condition_start_datetime AS condition_start_datetime,
	condition_end_date AS condition_end_date,
	condition_end_datetime AS condition_end_datetime,
	condition_type_concept_id AS condition_type_concept_id,
	condition_status_concept_id AS condition_status_concept_id,
	stop_reason AS stop_reason,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	condition_source_value AS condition_source_value,
	condition_source_concept_id AS condition_source_concept_id,
	condition_status_source_value AS condition_status_source_value
FROM read_parquet('@CONDITION_OCCURRENCE')