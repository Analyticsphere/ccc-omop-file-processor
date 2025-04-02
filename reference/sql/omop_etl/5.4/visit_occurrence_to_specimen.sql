SELECT
	visit_occurrence_id AS specimen_id,
	person_id AS person_id,
	visit_concept_id AS specimen_concept_id,
	visit_type_concept_id AS specimen_type_concept_id,
	visit_start_date AS specimen_date,
	visit_start_datetime AS specimen_datetime,
	NULL AS quantity,
	0 AS unit_concept_id,
	0 AS anatomic_site_concept_id,
	0 AS disease_status_concept_id,
	NULL AS specimen_source_id,
	visit_source_value AS specimen_source_value,
	NULL AS unit_source_value,
	NULL AS anatomic_site_source_value,
	NULL AS disease_status_source_value
FROM read_parquet('@VISIT_OCCURRENCE')