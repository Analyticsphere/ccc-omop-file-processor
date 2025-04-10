SELECT
	procedure_occurrence_id AS drug_exposure_id,
	person_id AS person_id,
	procedure_concept_id AS drug_concept_id,
	procedure_date AS drug_exposure_start_date,
	procedure_datetime AS drug_exposure_start_datetime,
	COALESCE(procedure_end_date, procedure_date) AS drug_exposure_end_date,
	procedure_end_datetime AS drug_exposure_end_datetime,
	NULL AS verbatim_end_date,
	procedure_type_concept_id AS drug_type_concept_id,
	NULL AS stop_reason,
	NULL AS refills,
	quantity AS quantity,
	NULL AS days_supply,
	NULL AS sig,
	0 AS route_concept_id,
	NULL AS lot_number,
	provider_id AS provider_id,
	visit_occurrence_id AS visit_occurrence_id,
	visit_detail_id AS visit_detail_id,
	procedure_source_value AS drug_source_value,
	procedure_source_concept_id AS drug_source_concept_id,
	NULL AS route_source_value,
	NULL AS dose_unit_source_value
FROM read_parquet('@PROCEDURE_OCCURRENCE')