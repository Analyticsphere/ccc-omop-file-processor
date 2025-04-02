SELECT
	observation_id AS visit_occurrence_id,
	person_id AS person_id,
	observation_concept_id AS visit_concept_id,
	observation_date AS visit_start_date,
	observation_datetime AS visit_start_datetime,
	observation_date AS visit_end_date,
	NULL AS visit_end_datetime,
	observation_type_concept_id AS visit_type_concept_id,
	provider_id AS provider_id,
	NULL AS care_site_id,
	observation_source_value AS visit_source_value,
	observation_source_concept_id AS visit_source_concept_id,
	0 AS admitted_from_concept_id,
	NULL AS admitted_from_source_value,
	0 AS discharged_to_concept_id,
	NULL AS discharged_to_source_value,
	NULL AS preceding_visit_occurrence_id
FROM read_parquet('@OBSERVATION')