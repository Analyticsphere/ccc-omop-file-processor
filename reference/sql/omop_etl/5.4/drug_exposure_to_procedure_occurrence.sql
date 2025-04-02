SELECT
	drug_exposure_id AS procedure_occurrence_id,
	person_id AS person_id,
	drug_concept_id AS procedure_concept_id,
	drug_exposure_start_date AS procedure_date,
	drug_exposure_start_datetime AS procedure_datetime,
	drug_exposure_end_date AS procedure_end_date,
	drug_exposure_end_datetime AS procedure_end_datetime,
	drug_type_concept_id AS procedure_type_concept_id,
	0 AS modifier_concept_id,
	quantity AS quantity,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	drug_source_value AS procedure_source_value,
	drug_source_concept_id AS procedure_source_concept_id,
	NULL AS modifier_source_value
FROM read_parquet('@DRUG_EXPOSURE')