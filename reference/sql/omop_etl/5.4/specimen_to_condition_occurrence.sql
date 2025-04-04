SELECT
	specimen_id AS condition_occurrence_id,
	person_id AS person_id,
	specimen_concept_id AS condition_concept_id,
	specimen_date AS condition_start_date,
	specimen_datetime AS condition_start_datetime,
	NULL AS condition_end_date,
	NULL AS condition_end_datetime,
	specimen_type_concept_id AS condition_type_concept_id,
	disease_status_concept_id AS condition_status_concept_id,
	NULL AS stop_reason,
	NULL AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	specimen_source_value AS condition_source_value,
	0 AS condition_source_concept_id,
	disease_status_source_value AS condition_status_source_value
FROM read_parquet('@SPECIMEN')