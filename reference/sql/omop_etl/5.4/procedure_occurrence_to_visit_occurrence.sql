SELECT
	procedure_occurrence_id AS visit_occurrence_id,
	person_id AS person_id,
	procedure_concept_id AS visit_concept_id,
	procedure_date AS visit_start_date,
	procedure_datetime AS visit_start_datetime,
	COALESCE(procedure_end_date, procedure_date) AS visit_end_date,
	procedure_end_datetime AS visit_end_datetime,
	procedure_type_concept_id AS visit_type_concept_id,
	provider_id AS provider_id,
	NULL AS care_site_id,
	procedure_source_value AS visit_source_value,
	procedure_source_concept_id AS visit_source_concept_id,
	0 AS admitted_from_concept_id,
	NULL AS admitted_from_source_value,
	0 AS discharged_to_concept_id,
	NULL AS discharged_to_source_value,
	NULL AS preceding_visit_occurrence_id
FROM read_parquet('@PROCEDURE_OCCURRENCE')