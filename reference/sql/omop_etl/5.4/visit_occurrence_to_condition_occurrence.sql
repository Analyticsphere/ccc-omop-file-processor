SELECT
	visit_occurrence_id AS condition_occurrence_id,
	person_id AS person_id,
	visit_concept_id AS condition_concept_id,
	visit_start_date AS condition_start_date,
	visit_start_datetime AS condition_start_datetime,
	visit_end_date AS condition_end_date,
	visit_end_datetime AS condition_end_datetime,
	visit_type_concept_id AS condition_type_concept_id,
	0 AS condition_status_concept_id,
	NULL AS stop_reason,
	provider_id AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	visit_source_value AS condition_source_value,
	visit_source_concept_id AS condition_source_concept_id,
	NULL AS condition_status_source_value
FROM read_parquet('@VISIT_OCCURRENCE')