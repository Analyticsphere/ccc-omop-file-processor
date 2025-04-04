SELECT
	specimen_id AS note_id,
	person_id AS person_id,
	specimen_date AS note_date,
	specimen_datetime AS note_datetime,
	specimen_type_concept_id AS note_type_concept_id,
	specimen_concept_id AS note_class_concept_id,
	NULL AS note_title,
	'' AS note_text,
	0 AS encoding_concept_id,
	0 AS language_concept_id,
	NULL AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	specimen_source_value AS note_source_value,
	NULL AS note_event_id,
	0 AS note_event_field_concept_id
FROM read_parquet('@SPECIMEN')