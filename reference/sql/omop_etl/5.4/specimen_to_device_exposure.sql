SELECT
	specimen_id AS device_exposure_id,
	person_id AS person_id,
	specimen_concept_id AS device_concept_id,
	specimen_date AS device_exposure_start_date,
	specimen_datetime AS device_exposure_start_datetime,
	NULL AS device_exposure_end_date,
	NULL AS device_exposure_end_datetime,
	specimen_type_concept_id AS device_type_concept_id,
	NULL AS unique_device_id,
	NULL AS production_id,
	quantity AS quantity,
	NULL AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	specimen_source_value AS device_source_value,
	0 AS device_source_concept_id,
	unit_concept_id AS unit_concept_id,
	unit_source_value AS unit_source_value,
	0 AS unit_source_concept_id
FROM read_parquet('@SPECIMEN')