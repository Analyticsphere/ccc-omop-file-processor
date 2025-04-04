SELECT
	visit_occurrence_id AS procedure_occurrence_id,
	person_id AS person_id,
	visit_concept_id AS procedure_concept_id,
	visit_start_date AS procedure_date,
	visit_start_datetime AS procedure_datetime,
	visit_end_date AS procedure_end_date,
	visit_end_datetime AS procedure_end_datetime,
	visit_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	NULL AS quantity,
	provider_id AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	visit_source_value AS procedure_source_value,
	visit_source_concept_id AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@VISIT_OCCURRENCE')