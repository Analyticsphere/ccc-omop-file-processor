SELECT
	measurement_id AS procedure_occurrence_id,
	person_id AS person_id,
	measurement_concept_id AS procedure_concept_id,
	measurement_date AS procedure_date,
	measurement_datetime AS procedure_datetime,
	NULL AS procedure_end_date,
	NULL AS procedure_end_datetime,
	measurement_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	NULL AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	measurement_source_value AS procedure_source_value,
	measurement_source_concept_id AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@MEASUREMENT')