SELECT
	device_exposure_id AS note_id,
	person_id AS person_id,
	device_exposure_start_date AS note_date,
	device_exposure_start_datetime AS note_datetime,
	device_type_concept_id AS note_type_concept_id,
	device_concept_id AS note_class_concept_id,
	NULL AS note_title,
	'' AS note_text,
	0 AS encoding_concept_id,
	0 AS language_concept_id,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	device_source_value AS note_source_value,
	NULL AS note_event_id,
	0 AS note_event_field_concept_id
FROM read_parquet('@DEVICE_EXPOSURE')