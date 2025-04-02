SELECT
	visit_occurrence_id AS note_id,
	person_id AS person_id,
	visit_start_date AS note_date,
	visit_start_datetime AS note_datetime,
	visit_type_concept_id AS note_type_concept_id,
	visit_concept_id AS note_class_concept_id,
	NULL AS note_title,
	'' AS note_text,
	0 AS encoding_concept_id,
	0 AS language_concept_id,
	provider_id AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	visit_source_value AS note_source_value,
	NULL AS note_event_id,
	0 AS note_event_field_concept_id
FROM read_parquet('@VISIT_OCCURRENCE')