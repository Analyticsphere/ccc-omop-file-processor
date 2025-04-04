SELECT
	note_id AS procedure_occurrence_id,
	person_id AS person_id,
	note_class_concept_id AS procedure_concept_id,
	note_date AS procedure_date,
	note_datetime AS procedure_datetime,
	NULL AS procedure_end_date,
	NULL AS procedure_end_datetime,
	note_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	NULL AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	note_source_value AS procedure_source_value,
	0 AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@NOTE')