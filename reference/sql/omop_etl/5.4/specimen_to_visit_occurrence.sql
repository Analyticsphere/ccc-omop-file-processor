SELECT
	specimen_id AS visit_occurrence_id,
	person_id AS person_id,
	specimen_concept_id AS visit_concept_id,
	specimen_date AS visit_start_date,
	specimen_datetime AS visit_start_datetime,
	specimen_date AS visit_end_date,
	NULL AS visit_end_datetime,
	specimen_type_concept_id AS visit_type_concept_id,
	NULL AS provider_id,
	NULL AS care_site_id,
	specimen_source_value AS visit_source_value,
	0 AS visit_source_concept_id,
	0 AS admitted_from_concept_id,
	NULL AS admitted_from_source_value,
	0 AS discharged_to_concept_id,
	NULL AS discharged_to_source_value,
	NULL AS preceding_visit_occurrence_id
FROM read_parquet('@SPECIMEN')