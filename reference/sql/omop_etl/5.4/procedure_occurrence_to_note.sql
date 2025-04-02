SELECT
	procedure_occurrence_id AS note_id,
	person_id AS person_id,
	procedure_date AS note_date,
	procedure_datetime AS note_datetime,
	procedure_type_concept_id AS note_type_concept_id,
	procedure_concept_id AS note_class_concept_id,
	NULL AS note_title,
	'' AS note_text,
	0 AS encoding_concept_id,
	0 AS language_concept_id,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	procedure_source_value AS note_source_value,
	NULL AS note_event_id,
	0 AS note_event_field_concept_id
FROM read_parquet('@PROCEDURE_OCCURRENCE')