SELECT
	specimen_id AS procedure_occurrence_id,
	person_id AS person_id,
	specimen_concept_id AS procedure_concept_id,
	specimen_date AS procedure_date,
	specimen_datetime AS procedure_datetime,
	NULL AS procedure_end_date,
	NULL AS procedure_end_datetime,
	specimen_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	quantity AS quantity,
	NULL AS provider_id,
	NULL AS visit_occurrence_id,
	NULL AS visit_detail_id,
	specimen_source_value AS procedure_source_value,
	0 AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@SPECIMEN')