SELECT
	observation_id AS specimen_id,
	person_id AS person_id,
	observation_concept_id AS specimen_concept_id,
	observation_type_concept_id AS specimen_type_concept_id,
	observation_date AS specimen_date,
	observation_datetime AS specimen_datetime,
	NULL AS quantity,
	unit_concept_id AS unit_concept_id,
	0 AS anatomic_site_concept_id,
	0 AS disease_status_concept_id,
	NULL AS specimen_source_id,
	observation_source_value AS specimen_source_value,
	unit_source_value AS unit_source_value,
	NULL AS anatomic_site_source_value,
	NULL AS disease_status_source_value
FROM read_parquet('@OBSERVATION')