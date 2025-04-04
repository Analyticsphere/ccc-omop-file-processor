SELECT
	procedure_occurrence_id AS condition_occurrence_id,
	person_id AS person_id,
	procedure_concept_id AS condition_concept_id,
	procedure_date AS condition_start_date,
	procedure_datetime AS condition_start_datetime,
	procedure_end_date AS condition_end_date,
	procedure_end_datetime AS condition_end_datetime,
	procedure_type_concept_id AS condition_type_concept_id,
	0 AS condition_status_concept_id,
	NULL AS stop_reason,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	procedure_source_value AS condition_source_value,
	procedure_source_concept_id AS condition_source_concept_id,
	NULL AS condition_status_source_value
FROM read_parquet('@PROCEDURE_OCCURRENCE')