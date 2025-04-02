SELECT
	device_exposure_id AS specimen_id,
	person_id AS person_id,
	device_concept_id AS specimen_concept_id,
	device_type_concept_id AS specimen_type_concept_id,
	device_exposure_start_date AS specimen_date,
	device_exposure_start_datetime AS specimen_datetime,
	quantity AS quantity,
	unit_concept_id AS unit_concept_id,
	0 AS anatomic_site_concept_id,
	0 AS disease_status_concept_id,
	NULL AS specimen_source_id,
	device_source_value AS specimen_source_value,
	unit_source_value AS unit_source_value,
	NULL AS anatomic_site_source_value,
	NULL AS disease_status_source_value
FROM read_parquet('@DEVICE_EXPOSURE')