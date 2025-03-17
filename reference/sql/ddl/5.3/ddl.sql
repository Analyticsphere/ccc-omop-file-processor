--BigQuery CDM DDL Specification for OMOP Common Data Model 5.3

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.person` (
			person_id INT64,
			gender_concept_id INT64,
			year_of_birth INT64,
			month_of_birth INT64,
			day_of_birth INT64,
			birth_datetime TIMESTAMP,
			race_concept_id INT64,
			ethnicity_concept_id INT64,
			location_id INT64,
			provider_id INT64,
			care_site_id INT64,
			person_source_value STRING,
			gender_source_value STRING,
			gender_source_concept_id INT64,
			race_source_value STRING,
			race_source_concept_id INT64,
			ethnicity_source_value STRING,
			ethnicity_source_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.observation_period` (
			observation_period_id INT64,
			person_id INT64,
			observation_period_start_date TIMESTAMP,
			observation_period_end_date TIMESTAMP,
			period_type_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.visit_occurrence` (
			visit_occurrence_id INT64,
			person_id INT64,
			visit_concept_id INT64,
			visit_start_date TIMESTAMP,
			visit_start_datetime TIMESTAMP,
			visit_end_date TIMESTAMP,
			visit_end_datetime TIMESTAMP,
			visit_type_concept_id INT64,
			provider_id INT64,
			care_site_id INT64,
			visit_source_value STRING,
			visit_source_concept_id INT64,
			admitting_source_concept_id INT64,
			admitting_source_value STRING,
			discharge_to_concept_id INT64,
			discharge_to_source_value STRING,
			preceding_visit_occurrence_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.visit_detail` (
			visit_detail_id INT64,
			person_id INT64,
			visit_detail_concept_id INT64,
			visit_detail_start_date TIMESTAMP,
			visit_detail_start_datetime TIMESTAMP,
			visit_detail_end_date TIMESTAMP,
			visit_detail_end_datetime TIMESTAMP,
			visit_detail_type_concept_id INT64,
			provider_id INT64,
			care_site_id INT64,
			visit_detail_source_value STRING,
			visit_detail_source_concept_id INT64,
			admitting_source_value STRING,
			admitting_source_concept_id INT64,
			discharge_to_source_value STRING,
			discharge_to_concept_id INT64,
			preceding_visit_detail_id INT64,
			visit_detail_parent_id INT64,
			visit_occurrence_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.condition_occurrence` (
			condition_occurrence_id INT64,
			person_id INT64,
			condition_concept_id INT64,
			condition_start_date TIMESTAMP,
			condition_start_datetime TIMESTAMP,
			condition_end_date TIMESTAMP,
			condition_end_datetime TIMESTAMP,
			condition_type_concept_id INT64,
			condition_status_concept_id INT64,
			stop_reason STRING,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			condition_source_value STRING,
			condition_source_concept_id INT64,
			condition_status_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.drug_exposure` (
			drug_exposure_id INT64,
			person_id INT64,
			drug_concept_id INT64,
			drug_exposure_start_date TIMESTAMP,
			drug_exposure_start_datetime TIMESTAMP,
			drug_exposure_end_date TIMESTAMP,
			drug_exposure_end_datetime TIMESTAMP,
			verbatim_end_date TIMESTAMP,
			drug_type_concept_id INT64,
			stop_reason STRING,
			refills INT64,
			quantity FLOAT64,
			days_supply INT64,
			sig STRING,
			route_concept_id INT64,
			lot_number STRING,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			drug_source_value STRING,
			drug_source_concept_id INT64,
			route_source_value STRING,
			dose_unit_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.procedure_occurrence` (
			procedure_occurrence_id INT64,
			person_id INT64,
			procedure_concept_id INT64,
			procedure_date TIMESTAMP,
			procedure_datetime TIMESTAMP,
			procedure_type_concept_id INT64,
			modifier_concept_id INT64,
			quantity INT64,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			procedure_source_value STRING,
			procedure_source_concept_id INT64,
			modifier_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.device_exposure` (
			device_exposure_id INT64,
			person_id INT64,
			device_concept_id INT64,
			device_exposure_start_date TIMESTAMP,
			device_exposure_start_datetime TIMESTAMP,
			device_exposure_end_date TIMESTAMP,
			device_exposure_end_datetime TIMESTAMP,
			device_type_concept_id INT64,
			unique_device_id STRING,
			quantity INT64,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			device_source_value STRING,
			device_source_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.measurement` (
			measurement_id INT64,
			person_id INT64,
			measurement_concept_id INT64,
			measurement_date TIMESTAMP,
			measurement_datetime TIMESTAMP,
			measurement_time STRING,
			measurement_type_concept_id INT64,
			operator_concept_id INT64,
			value_as_number FLOAT64,
			value_as_concept_id INT64,
			unit_concept_id INT64,
			range_low FLOAT64,
			range_high FLOAT64,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			measurement_source_value STRING,
			measurement_source_concept_id INT64,
			unit_source_value STRING,
			value_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.observation` (
			observation_id INT64,
			person_id INT64,
			observation_concept_id INT64,
			observation_date TIMESTAMP,
			observation_datetime TIMESTAMP,
			observation_type_concept_id INT64,
			value_as_number FLOAT64,
			value_as_string STRING,
			value_as_concept_id INT64,
			qualifier_concept_id INT64,
			unit_concept_id INT64,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			observation_source_value STRING,
			observation_source_concept_id INT64,
			unit_source_value STRING,
			qualifier_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.death` (
			person_id INT64,
			death_date TIMESTAMP,
			death_datetime TIMESTAMP,
			death_type_concept_id INT64,
			cause_concept_id INT64,
			cause_source_value STRING,
			cause_source_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.note` (
			note_id INT64,
			person_id INT64,
			note_date TIMESTAMP,
			note_datetime TIMESTAMP,
			note_type_concept_id INT64,
			note_class_concept_id INT64,
			note_title STRING,
			note_text STRING,
			encoding_concept_id INT64,
			language_concept_id INT64,
			provider_id INT64,
			visit_occurrence_id INT64,
			visit_detail_id INT64,
			note_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.note_nlp` (
			note_nlp_id INT64,
			note_id INT64,
			section_concept_id INT64,
			snippet STRING,
			`offset` STRING,
			lexical_variant STRING,
			note_nlp_concept_id INT64,
			note_nlp_source_concept_id INT64,
			nlp_system STRING,
			nlp_date TIMESTAMP,
			nlp_datetime TIMESTAMP,
			term_exists STRING,
			term_temporal STRING,
			term_modifiers STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.specimen` (
			specimen_id INT64,
			person_id INT64,
			specimen_concept_id INT64,
			specimen_type_concept_id INT64,
			specimen_date TIMESTAMP,
			specimen_datetime TIMESTAMP,
			quantity FLOAT64,
			unit_concept_id INT64,
			anatomic_site_concept_id INT64,
			disease_status_concept_id INT64,
			specimen_source_id STRING,
			specimen_source_value STRING,
			unit_source_value STRING,
			anatomic_site_source_value STRING,
			disease_status_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.fact_relationship` (
			domain_concept_id_1 INT64,
			fact_id_1 INT64,
			domain_concept_id_2 INT64,
			fact_id_2 INT64,
			relationship_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.location` (
			location_id INT64,
			address_1 STRING,
			address_2 STRING,
			city STRING,
			state STRING,
			zip STRING,
			county STRING,
			location_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.care_site` (
			care_site_id INT64,
			care_site_name STRING,
			place_of_service_concept_id INT64,
			location_id INT64,
			care_site_source_value STRING,
			place_of_service_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.provider` (
			provider_id INT64,
			provider_name STRING,
			npi STRING,
			dea STRING,
			specialty_concept_id INT64,
			care_site_id INT64,
			year_of_birth INT64,
			gender_concept_id INT64,
			provider_source_value STRING,
			specialty_source_value STRING,
			specialty_source_concept_id INT64,
			gender_source_value STRING,
			gender_source_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.payer_plan_period` (
			payer_plan_period_id INT64,
			person_id INT64,
			payer_plan_period_start_date TIMESTAMP,
			payer_plan_period_end_date TIMESTAMP,
			payer_concept_id INT64,
			payer_source_value STRING,
			payer_source_concept_id INT64,
			plan_concept_id INT64,
			plan_source_value STRING,
			plan_source_concept_id INT64,
			sponsor_concept_id INT64,
			sponsor_source_value STRING,
			sponsor_source_concept_id INT64,
			family_source_value STRING,
			stop_reason_concept_id INT64,
			stop_reason_source_value STRING,
			stop_reason_source_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.cost` (
			cost_id INT64,
			cost_event_id INT64,
			cost_domain_id STRING,
			cost_type_concept_id INT64,
			currency_concept_id INT64,
			total_charge FLOAT64,
			total_cost FLOAT64,
			total_paid FLOAT64,
			paid_by_payer FLOAT64,
			paid_by_patient FLOAT64,
			paid_patient_copay FLOAT64,
			paid_patient_coinsurance FLOAT64,
			paid_patient_deductible FLOAT64,
			paid_by_primary FLOAT64,
			paid_ingredient_cost FLOAT64,
			paid_dispensing_fee FLOAT64,
			payer_plan_period_id INT64,
			amount_allowed FLOAT64,
			revenue_code_concept_id INT64,
			revenue_code_source_value STRING,
			drg_concept_id INT64,
			drg_source_value STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.drug_era` (
			drug_era_id INT64,
			person_id INT64,
			drug_concept_id INT64,
			drug_era_start_date TIMESTAMP,
			drug_era_end_date TIMESTAMP,
			drug_exposure_count INT64,
			gap_days INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.dose_era` (
			dose_era_id INT64,
			person_id INT64,
			drug_concept_id INT64,
			unit_concept_id INT64,
			dose_value FLOAT64,
			dose_era_start_date TIMESTAMP,
			dose_era_end_date date );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.condition_era` (
			condition_era_id INT64,
			person_id INT64,
			condition_concept_id INT64,
			condition_era_start_date TIMESTAMP,
			condition_era_end_date TIMESTAMP,
			condition_occurrence_count INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.metadata` (
			metadata_concept_id INT64,
			metadata_type_concept_id INT64,
			name STRING,
			value_as_string STRING,
			value_as_concept_id INT64,
			metadata_date TIMESTAMP,
			metadata_datetime DATETIME );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.cdm_source` (
			cdm_source_name STRING,
			cdm_source_abbreviation STRING,
			cdm_holder STRING,
			source_description STRING,
			source_documentation_reference STRING,
			cdm_etl_reference STRING,
			source_release_date TIMESTAMP,
			cdm_release_date TIMESTAMP,
			cdm_version STRING,
			vocabulary_version STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept` (
			concept_id INT64,
			concept_name STRING,
			domain_id STRING,
			vocabulary_id STRING,
			concept_class_id STRING,
			standard_concept STRING,
			concept_code STRING,
			valid_start_date TIMESTAMP,
			valid_end_date TIMESTAMP,
			invalid_reason STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.vocabulary` (
			vocabulary_id STRING,
			vocabulary_name STRING,
			vocabulary_reference STRING,
			vocabulary_version STRING,
			vocabulary_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.domain` (
			domain_id STRING,
			domain_name STRING,
			domain_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_class` (
			concept_class_id STRING,
			concept_class_name STRING,
			concept_class_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_relationship` (
			concept_id_1 INT64,
			concept_id_2 INT64,
			relationship_id STRING,
			valid_start_date TIMESTAMP,
			valid_end_date TIMESTAMP,
			invalid_reason STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.relationship` (
			relationship_id STRING,
			relationship_name STRING,
			is_hierarchical STRING,
			defines_ancestry STRING,
			reverse_relationship_id STRING,
			relationship_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_synonym` (
			concept_id INT64,
			concept_synonym_name STRING,
			language_concept_id INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_ancestor` (
			ancestor_concept_id INT64,
			descendant_concept_id INT64,
			min_levels_of_separation INT64,
			max_levels_of_separation INT64 );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.source_to_concept_map` (
			source_code STRING,
			source_concept_id INT64,
			source_vocabulary_id STRING,
			source_code_description STRING,
			target_concept_id INT64,
			target_vocabulary_id STRING,
			valid_start_date TIMESTAMP,
			valid_end_date TIMESTAMP,
			invalid_reason STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.drug_strength` (
			drug_concept_id INT64,
			ingredient_concept_id INT64,
			amount_value FLOAT64,
			amount_unit_concept_id INT64,
			numerator_value FLOAT64,
			numerator_unit_concept_id INT64,
			denominator_value FLOAT64,
			denominator_unit_concept_id INT64,
			box_size INT64,
			valid_start_date TIMESTAMP,
			valid_end_date TIMESTAMP,
			invalid_reason STRING );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.cohort_definition` (
			cohort_definition_id INT64,
			cohort_definition_name STRING,
			cohort_definition_description STRING,
			definition_type_concept_id INT64,
			cohort_definition_syntax STRING,
			subject_concept_id INT64,
			cohort_initiation_date DATE );

CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.attribute_definition` (
			attribute_definition_id INT64,
			attribute_name STRING,
			attribute_description STRING,
			attribute_type_concept_id INT64,
			attribute_syntax STRING );