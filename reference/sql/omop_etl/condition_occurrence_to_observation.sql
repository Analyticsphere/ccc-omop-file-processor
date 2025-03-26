SELECT
	condition_occurrence_id AS observation_id
	,person_id AS person_id
	,condition_concept_id AS observation_concept_id
	,condition_start_date AS observation_date
	,condition_start_datetime AS observation_datetime
	,condition_type_concept_id AS observation_type_concept_id
	,NULL AS value_as_number
	,NULL AS value_as_string
	,value_as_concept_id AS value_as_concept_id
	,NULL AS qualifier_concept_id
	,NULL AS unit_concept_id
	,provider_id AS provider_id
	,visit_occurrence_id AS visit_occurrence_id
	,visit_detail_id AS visit_detail_id
	,condition_source_value AS observation_source_value
	,condition_source_concept_id AS observation_source_concept_id
	,NULL AS unit_source_value
	,NULL AS qualifier_source_value
	,NULL AS value_source_value
	,NULL AS observation_event_id
	,NULL AS obs_event_field_concept_id
FROM @CONDITION_OCCURRENCE