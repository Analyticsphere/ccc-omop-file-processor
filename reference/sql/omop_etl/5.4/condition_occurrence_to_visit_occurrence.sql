SELECT
	condition_occurrence_id AS visit_occurrence_id,
	person_id AS person_id,
	condition_concept_id AS visit_concept_id,
	condition_start_date AS visit_start_date,
	condition_start_datetime AS visit_start_datetime,
	COALESCE(condition_end_date, condition_start_date) AS visit_end_date,
	condition_end_datetime AS visit_end_datetime,
	condition_type_concept_id AS visit_type_concept_id,
	provider_id AS provider_id,
	NULL AS care_site_id,
	condition_source_value AS visit_source_value,
	condition_source_concept_id AS visit_source_concept_id,
	0 AS admitted_from_concept_id,
	NULL AS admitted_from_source_value,
	0 AS discharged_to_concept_id,
	NULL AS discharged_to_source_value,
	NULL AS preceding_visit_occurrence_id
FROM read_parquet('@CONDITION_OCCURRENCE')