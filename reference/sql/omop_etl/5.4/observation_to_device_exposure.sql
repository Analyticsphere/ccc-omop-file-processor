SELECT
	observation_id AS device_exposure_id,
	person_id AS person_id,
	observation_concept_id AS device_concept_id,
	observation_date AS device_exposure_start_date,
	observation_datetime AS device_exposure_start_datetime,
	NULL AS device_exposure_end_date,
	NULL AS device_exposure_end_datetime,
	observation_type_concept_id AS device_type_concept_id,
	NULL AS unique_device_id,
	NULL AS production_id,
	NULL AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	observation_source_value AS device_source_value,
	observation_source_concept_id AS device_source_concept_id,
	unit_concept_id AS unit_concept_id,
	unit_source_value AS unit_source_value,
	0 AS unit_source_concept_id
FROM read_parquet('@OBSERVATION')