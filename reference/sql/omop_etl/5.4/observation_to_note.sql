SELECT
	observation_id AS note_id,
	person_id AS person_id,
	observation_date AS note_date,
	observation_datetime AS note_datetime,
	observation_type_concept_id AS note_type_concept_id,
	observation_concept_id AS note_class_concept_id,
	NULL AS note_title,
	'' AS note_text,
	0 AS encoding_concept_id,
	0 AS language_concept_id,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	observation_source_value AS note_source_value,
	observation_event_id AS note_event_id,
	obs_event_field_concept_id AS note_event_field_concept_id
FROM read_parquet('@OBSERVATION')