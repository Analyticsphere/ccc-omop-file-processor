SELECT
	visit_occurrence_id AS device_exposure_id,
	person_id AS person_id,
	visit_concept_id AS device_concept_id,
	visit_start_date AS device_exposure_start_date,
	visit_start_datetime AS device_exposure_start_datetime,
	visit_end_date AS device_exposure_end_date,
	visit_end_datetime AS device_exposure_end_datetime,
	visit_type_concept_id AS device_type_concept_id,
	NULL AS unique_device_id,
	NULL AS production_id,
	NULL AS quantity,
	provider_id AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	visit_source_value AS device_source_value,
	visit_source_concept_id AS device_source_concept_id,
	0 AS unit_concept_id,
	NULL AS unit_source_value,
	0 AS unit_source_concept_id
FROM read_parquet('@VISIT_OCCURRENCE')