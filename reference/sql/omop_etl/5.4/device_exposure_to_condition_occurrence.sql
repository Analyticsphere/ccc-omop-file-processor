SELECT
	device_exposure_id AS condition_occurrence_id,
	person_id AS person_id,
	device_concept_id AS condition_concept_id,
	device_exposure_start_date AS condition_start_date,
	device_exposure_start_datetime AS condition_start_datetime,
	device_exposure_end_date AS condition_end_date,
	device_exposure_end_datetime AS condition_end_datetime,
	device_type_concept_id AS condition_type_concept_id,
	0 AS condition_status_concept_id,
	NULL AS stop_reason,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	device_source_value AS condition_source_value,
	device_source_concept_id AS condition_source_concept_id,
	NULL AS condition_status_source_value
FROM read_parquet('@DEVICE_EXPOSURE')