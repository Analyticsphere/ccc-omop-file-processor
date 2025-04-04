SELECT
	observation_id AS condition_occurrence_id,
	person_id AS person_id,
	observation_concept_id AS condition_concept_id,
	observation_date AS condition_start_date,
	observation_datetime AS condition_start_datetime,
	NULL AS condition_end_date,
	NULL AS condition_end_datetime,
	observation_type_concept_id AS condition_type_concept_id,
	0 AS condition_status_concept_id,
	NULL AS stop_reason,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	observation_source_value AS condition_source_value,
	observation_source_concept_id AS condition_source_concept_id,
	NULL AS condition_status_source_value
FROM read_parquet('@OBSERVATION')