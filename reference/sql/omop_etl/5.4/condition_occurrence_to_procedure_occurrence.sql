SELECT
	condition_occurrence_id AS procedure_occurrence_id,
	person_id AS person_id,
	condition_concept_id AS procedure_concept_id,
	condition_start_date AS procedure_date,
	condition_start_datetime AS procedure_datetime,
	condition_end_date AS procedure_end_date,
	condition_end_datetime AS procedure_end_datetime,
	condition_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	NULL AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	condition_source_value AS procedure_source_value,
	condition_source_concept_id AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@CONDITION_OCCURRENCE')