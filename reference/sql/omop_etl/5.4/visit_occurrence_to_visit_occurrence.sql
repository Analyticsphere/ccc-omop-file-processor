SELECT
	visit_occurrence_id AS visit_occurrence_id,
	person_id AS person_id,
	visit_concept_id AS visit_concept_id,
	visit_start_date AS visit_start_date,
	visit_start_datetime AS visit_start_datetime,
	visit_end_date AS visit_end_date,
	visit_end_datetime AS visit_end_datetime,
	visit_type_concept_id AS visit_type_concept_id,
	provider_id AS provider_id,
	care_site_id AS care_site_id,
	visit_source_value AS visit_source_value,
	visit_source_concept_id AS visit_source_concept_id,
	admitted_from_concept_id AS admitted_from_concept_id,
	admitted_from_source_value AS admitted_from_source_value,
	discharged_to_concept_id AS discharged_to_concept_id,
	discharged_to_source_value AS discharged_to_source_value,
	preceding_visit_occurrence_id AS preceding_visit_occurrence_id
FROM read_parquet('@VISIT_OCCURRENCE')