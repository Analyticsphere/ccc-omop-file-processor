SELECT
	condition_occurrence_id AS measurement_id
	,person_id AS person_id
	,condition_concept_id AS measurement_concept_id
	,condition_start_date AS measurement_date
	,condition_start_datetime AS measurement_datetime
	,condition_start_datetime AS measurement_time
	,condition_type_concept_id AS measurement_type_concept_id
	,NULL AS operator_concept_id
	,NULL AS value_as_number
	,value_as_concept_id AS value_as_concept_id
	,NULL AS unit_concept_id
	,NULL AS range_low
	,NULL AS range_high
	,provider_id AS provider_id
	,visit_occurrence_id AS visit_occurrence_id
	,visit_detail_id AS visit_detail_id
	,condition_source_value AS measurement_source_value
	,condition_source_concept_id AS measurement_source_concept_id
	,NULL AS unit_source_value
	,NULL AS unit_source_concept_id
	,NULL AS value_source_value
	,NULL AS measurement_event_id
	,NULL AS meas_event_field_concept_id
FROM @CONDITION_OCCURRENCE