SELECT
	device_exposure_id AS device_exposure_id,
	person_id AS person_id,
	device_concept_id AS device_concept_id,
	device_exposure_start_date AS device_exposure_start_date,
	device_exposure_start_datetime AS device_exposure_start_datetime,
	device_exposure_end_date AS device_exposure_end_date,
	device_exposure_end_datetime AS device_exposure_end_datetime,
	device_type_concept_id AS device_type_concept_id,
	unique_device_id AS unique_device_id,
	production_id AS production_id,
	quantity AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	device_source_value AS device_source_value,
	device_source_concept_id AS device_source_concept_id,
	unit_concept_id AS unit_concept_id,
	unit_source_value AS unit_source_value,
	unit_source_concept_id AS unit_source_concept_id
FROM read_parquet('@DEVICE_EXPOSURE')