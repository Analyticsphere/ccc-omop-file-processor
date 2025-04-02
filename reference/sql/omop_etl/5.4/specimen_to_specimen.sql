SELECT
	specimen_id AS specimen_id,
	person_id AS person_id,
	specimen_concept_id AS specimen_concept_id,
	specimen_type_concept_id AS specimen_type_concept_id,
	specimen_date AS specimen_date,
	specimen_datetime AS specimen_datetime,
	quantity AS quantity,
	unit_concept_id AS unit_concept_id,
	anatomic_site_concept_id AS anatomic_site_concept_id,
	disease_status_concept_id AS disease_status_concept_id,
	specimen_source_id AS specimen_source_id,
	specimen_source_value AS specimen_source_value,
	unit_source_value AS unit_source_value,
	anatomic_site_source_value AS anatomic_site_source_value,
	disease_status_source_value AS disease_status_source_value
FROM read_parquet('@SPECIMEN')