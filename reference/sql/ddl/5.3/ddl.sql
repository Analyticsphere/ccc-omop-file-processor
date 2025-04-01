-- DDL script for OMOP CDM v5.3 with DATETIME casting
-- For each table, checks if it exists and casts DATETIME fields if needed

-- Person table
BEGIN
  -- Check if the table exists
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'person') THEN
    -- Replace the table, only casting DATETIME fields
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.person` AS
    SELECT
      person_id,
      gender_concept_id,
      year_of_birth,
      month_of_birth,
      day_of_birth,
      CAST(birth_datetime AS DATETIME) AS birth_datetime,
      race_concept_id,
      ethnicity_concept_id,
      location_id,
      provider_id,
      care_site_id,
      person_source_value,
      gender_source_value,
      gender_source_concept_id,
      race_source_value,
      race_source_concept_id,
      ethnicity_source_value,
      ethnicity_source_concept_id
    FROM `@cdmDatabaseSchema.person`;
  ELSE
    -- Create new table
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.person` (
      person_id INT64,
      gender_concept_id INT64,
      year_of_birth INT64,
      month_of_birth INT64,
      day_of_birth INT64,
      birth_datetime DATETIME,
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
      ethnicity_source_concept_id INT64
    );
  END IF;
END;

-- Observation period table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'observation_period') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.observation_period` AS
    SELECT
      observation_period_id,
      person_id,
      CAST(observation_period_start_date AS DATETIME) AS observation_period_start_date,
      CAST(observation_period_end_date AS DATETIME) AS observation_period_end_date,
      period_type_concept_id
    FROM `@cdmDatabaseSchema.observation_period`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.observation_period` (
      observation_period_id INT64,
      person_id INT64,
      observation_period_start_date DATETIME,
      observation_period_end_date DATETIME,
      period_type_concept_id INT64
    );
  END IF;
END;

-- Visit occurrence table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'visit_occurrence') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.visit_occurrence` AS
    SELECT
      visit_occurrence_id,
      person_id,
      visit_concept_id,
      CAST(visit_start_date AS DATETIME) AS visit_start_date,
      CAST(visit_start_datetime AS DATETIME) AS visit_start_datetime,
      CAST(visit_end_date AS DATETIME) AS visit_end_date,
      CAST(visit_end_datetime AS DATETIME) AS visit_end_datetime,
      visit_type_concept_id,
      provider_id,
      care_site_id,
      visit_source_value,
      visit_source_concept_id,
      admitting_source_concept_id,
      admitting_source_value,
      discharge_to_concept_id,
      discharge_to_source_value,
      preceding_visit_occurrence_id
    FROM `@cdmDatabaseSchema.visit_occurrence`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.visit_occurrence` (
      visit_occurrence_id INT64,
      person_id INT64,
      visit_concept_id INT64,
      visit_start_date DATETIME,
      visit_start_datetime DATETIME,
      visit_end_date DATETIME,
      visit_end_datetime DATETIME,
      visit_type_concept_id INT64,
      provider_id INT64,
      care_site_id INT64,
      visit_source_value STRING,
      visit_source_concept_id INT64,
      admitting_source_concept_id INT64,
      admitting_source_value STRING,
      discharge_to_concept_id INT64,
      discharge_to_source_value STRING,
      preceding_visit_occurrence_id INT64
    );
  END IF;
END;

-- Visit detail table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'visit_detail') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.visit_detail` AS
    SELECT
      visit_detail_id,
      person_id,
      visit_detail_concept_id,
      CAST(visit_detail_start_date AS DATETIME) AS visit_detail_start_date,
      CAST(visit_detail_start_datetime AS DATETIME) AS visit_detail_start_datetime,
      CAST(visit_detail_end_date AS DATETIME) AS visit_detail_end_date,
      CAST(visit_detail_end_datetime AS DATETIME) AS visit_detail_end_datetime,
      visit_detail_type_concept_id,
      provider_id,
      care_site_id,
      visit_detail_source_value,
      visit_detail_source_concept_id,
      admitting_source_value,
      admitting_source_concept_id,
      discharge_to_source_value,
      discharge_to_concept_id,
      preceding_visit_detail_id,
      visit_detail_parent_id,
      visit_occurrence_id
    FROM `@cdmDatabaseSchema.visit_detail`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.visit_detail` (
      visit_detail_id INT64,
      person_id INT64,
      visit_detail_concept_id INT64,
      visit_detail_start_date DATETIME,
      visit_detail_start_datetime DATETIME,
      visit_detail_end_date DATETIME,
      visit_detail_end_datetime DATETIME,
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
      visit_occurrence_id INT64
    );
  END IF;
END;

-- Condition occurrence table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'condition_occurrence') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.condition_occurrence` AS
    SELECT
      condition_occurrence_id,
      person_id,
      condition_concept_id,
      CAST(condition_start_date AS DATETIME) AS condition_start_date,
      CAST(condition_start_datetime AS DATETIME) AS condition_start_datetime,
      CAST(condition_end_date AS DATETIME) AS condition_end_date,
      CAST(condition_end_datetime AS DATETIME) AS condition_end_datetime,
      condition_type_concept_id,
      condition_status_concept_id,
      stop_reason,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      condition_source_value,
      condition_source_concept_id,
      condition_status_source_value
    FROM `@cdmDatabaseSchema.condition_occurrence`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.condition_occurrence` (
      condition_occurrence_id INT64,
      person_id INT64,
      condition_concept_id INT64,
      condition_start_date DATETIME,
      condition_start_datetime DATETIME,
      condition_end_date DATETIME,
      condition_end_datetime DATETIME,
      condition_type_concept_id INT64,
      condition_status_concept_id INT64,
      stop_reason STRING,
      provider_id INT64,
      visit_occurrence_id INT64,
      visit_detail_id INT64,
      condition_source_value STRING,
      condition_source_concept_id INT64,
      condition_status_source_value STRING
    );
  END IF;
END;

-- Drug exposure table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'drug_exposure') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.drug_exposure` AS
    SELECT
      drug_exposure_id,
      person_id,
      drug_concept_id,
      CAST(drug_exposure_start_date AS DATETIME) AS drug_exposure_start_date,
      CAST(drug_exposure_start_datetime AS DATETIME) AS drug_exposure_start_datetime,
      CAST(drug_exposure_end_date AS DATETIME) AS drug_exposure_end_date,
      CAST(drug_exposure_end_datetime AS DATETIME) AS drug_exposure_end_datetime,
      CAST(verbatim_end_date AS DATETIME) AS verbatim_end_date,
      drug_type_concept_id,
      stop_reason,
      refills,
      quantity,
      days_supply,
      sig,
      route_concept_id,
      lot_number,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      drug_source_value,
      drug_source_concept_id,
      route_source_value,
      dose_unit_source_value
    FROM `@cdmDatabaseSchema.drug_exposure`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.drug_exposure` (
      drug_exposure_id INT64,
      person_id INT64,
      drug_concept_id INT64,
      drug_exposure_start_date DATETIME,
      drug_exposure_start_datetime DATETIME,
      drug_exposure_end_date DATETIME,
      drug_exposure_end_datetime DATETIME,
      verbatim_end_date DATETIME,
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
      dose_unit_source_value STRING
    );
  END IF;
END;

-- Procedure occurrence table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'procedure_occurrence') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.procedure_occurrence` AS
    SELECT
      procedure_occurrence_id,
      person_id,
      procedure_concept_id,
      CAST(procedure_date AS DATETIME) AS procedure_date,
      CAST(procedure_datetime AS DATETIME) AS procedure_datetime,
      procedure_type_concept_id,
      modifier_concept_id,
      quantity,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      procedure_source_value,
      procedure_source_concept_id,
      modifier_source_value
    FROM `@cdmDatabaseSchema.procedure_occurrence`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.procedure_occurrence` (
      procedure_occurrence_id INT64,
      person_id INT64,
      procedure_concept_id INT64,
      procedure_date DATETIME,
      procedure_datetime DATETIME,
      procedure_type_concept_id INT64,
      modifier_concept_id INT64,
      quantity INT64,
      provider_id INT64,
      visit_occurrence_id INT64,
      visit_detail_id INT64,
      procedure_source_value STRING,
      procedure_source_concept_id INT64,
      modifier_source_value STRING
    );
  END IF;
END;

-- Device exposure table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'device_exposure') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.device_exposure` AS
    SELECT
      device_exposure_id,
      person_id,
      device_concept_id,
      CAST(device_exposure_start_date AS DATETIME) AS device_exposure_start_date,
      CAST(device_exposure_start_datetime AS DATETIME) AS device_exposure_start_datetime,
      CAST(device_exposure_end_date AS DATETIME) AS device_exposure_end_date,
      CAST(device_exposure_end_datetime AS DATETIME) AS device_exposure_end_datetime,
      device_type_concept_id,
      unique_device_id,
      quantity,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      device_source_value,
      device_source_concept_id
    FROM `@cdmDatabaseSchema.device_exposure`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.device_exposure` (
      device_exposure_id INT64,
      person_id INT64,
      device_concept_id INT64,
      device_exposure_start_date DATETIME,
      device_exposure_start_datetime DATETIME,
      device_exposure_end_date DATETIME,
      device_exposure_end_datetime DATETIME,
      device_type_concept_id INT64,
      unique_device_id STRING,
      quantity INT64,
      provider_id INT64,
      visit_occurrence_id INT64,
      visit_detail_id INT64,
      device_source_value STRING,
      device_source_concept_id INT64
    );
  END IF;
END;

-- Measurement table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'measurement') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.measurement` AS
    SELECT
      measurement_id,
      person_id,
      measurement_concept_id,
      CAST(measurement_date AS DATETIME) AS measurement_date,
      CAST(measurement_datetime AS DATETIME) AS measurement_datetime,
      measurement_time,
      measurement_type_concept_id,
      operator_concept_id,
      value_as_number,
      value_as_concept_id,
      unit_concept_id,
      range_low,
      range_high,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      measurement_source_value,
      measurement_source_concept_id,
      unit_source_value,
      value_source_value
    FROM `@cdmDatabaseSchema.measurement`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.measurement` (
      measurement_id INT64,
      person_id INT64,
      measurement_concept_id INT64,
      measurement_date DATETIME,
      measurement_datetime DATETIME,
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
      value_source_value STRING
    );
  END IF;
END;

-- Observation table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'observation') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.observation` AS
    SELECT
      observation_id,
      person_id,
      observation_concept_id,
      CAST(observation_date AS DATETIME) AS observation_date,
      CAST(observation_datetime AS DATETIME) AS observation_datetime,
      observation_type_concept_id,
      value_as_number,
      value_as_string,
      value_as_concept_id,
      qualifier_concept_id,
      unit_concept_id,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      observation_source_value,
      observation_source_concept_id,
      unit_source_value,
      qualifier_source_value
    FROM `@cdmDatabaseSchema.observation`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.observation` (
      observation_id INT64,
      person_id INT64,
      observation_concept_id INT64,
      observation_date DATETIME,
      observation_datetime DATETIME,
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
      qualifier_source_value STRING
    );
  END IF;
END;

-- Death table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'death') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.death` AS
    SELECT
      person_id,
      CAST(death_date AS DATETIME) AS death_date,
      CAST(death_datetime AS DATETIME) AS death_datetime,
      death_type_concept_id,
      cause_concept_id,
      cause_source_value,
      cause_source_concept_id
    FROM `@cdmDatabaseSchema.death`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.death` (
      person_id INT64,
      death_date DATETIME,
      death_datetime DATETIME,
      death_type_concept_id INT64,
      cause_concept_id INT64,
      cause_source_value STRING,
      cause_source_concept_id INT64
    );
  END IF;
END;

-- Note table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'note') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.note` AS
    SELECT
      note_id,
      person_id,
      CAST(note_date AS DATETIME) AS note_date,
      CAST(note_datetime AS DATETIME) AS note_datetime,
      note_type_concept_id,
      note_class_concept_id,
      note_title,
      note_text,
      encoding_concept_id,
      language_concept_id,
      provider_id,
      visit_occurrence_id,
      visit_detail_id,
      note_source_value
    FROM `@cdmDatabaseSchema.note`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.note` (
      note_id INT64,
      person_id INT64,
      note_date DATETIME,
      note_datetime DATETIME,
      note_type_concept_id INT64,
      note_class_concept_id INT64,
      note_title STRING,
      note_text STRING,
      encoding_concept_id INT64,
      language_concept_id INT64,
      provider_id INT64,
      visit_occurrence_id INT64,
      visit_detail_id INT64,
      note_source_value STRING
    );
  END IF;
END;

-- Note NLP table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'note_nlp') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.note_nlp` AS
    SELECT
      note_nlp_id,
      note_id,
      section_concept_id,
      snippet,
      offset,
      lexical_variant,
      note_nlp_concept_id,
      note_nlp_source_concept_id,
      nlp_system,
      CAST(nlp_date AS DATETIME) AS nlp_date,
      CAST(nlp_datetime AS DATETIME) AS nlp_datetime,
      term_exists,
      term_temporal,
      term_modifiers
    FROM `@cdmDatabaseSchema.note_nlp`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.note_nlp` (
      note_nlp_id INT64,
      note_id INT64,
      section_concept_id INT64,
      snippet STRING,
      offset STRING,
      lexical_variant STRING,
      note_nlp_concept_id INT64,
      note_nlp_source_concept_id INT64,
      nlp_system STRING,
      nlp_date DATETIME,
      nlp_datetime DATETIME,
      term_exists STRING,
      term_temporal STRING,
      term_modifiers STRING
    );
  END IF;
END;

-- Specimen table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'specimen') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.specimen` AS
    SELECT
      specimen_id,
      person_id,
      specimen_concept_id,
      specimen_type_concept_id,
      CAST(specimen_date AS DATETIME) AS specimen_date,
      CAST(specimen_datetime AS DATETIME) AS specimen_datetime,
      quantity,
      unit_concept_id,
      anatomic_site_concept_id,
      disease_status_concept_id,
      specimen_source_id,
      specimen_source_value,
      unit_source_value,
      anatomic_site_source_value,
      disease_status_source_value
    FROM `@cdmDatabaseSchema.specimen`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.specimen` (
      specimen_id INT64,
      person_id INT64,
      specimen_concept_id INT64,
      specimen_type_concept_id INT64,
      specimen_date DATETIME,
      specimen_datetime DATETIME,
      quantity FLOAT64,
      unit_concept_id INT64,
      anatomic_site_concept_id INT64,
      disease_status_concept_id INT64,
      specimen_source_id STRING,
      specimen_source_value STRING,
      unit_source_value STRING,
      anatomic_site_source_value STRING,
      disease_status_source_value STRING
    );
  END IF;
END;

-- Fact relationship table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'fact_relationship') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.fact_relationship` (
      domain_concept_id_1 INT64,
      fact_id_1 INT64,
      domain_concept_id_2 INT64,
      fact_id_2 INT64,
      relationship_concept_id INT64
    );
  END IF;
END;

-- Location table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'location') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.location` (
      location_id INT64,
      address_1 STRING,
      address_2 STRING,
      city STRING,
      state STRING,
      zip STRING,
      county STRING,
      location_source_value STRING
    );
  END IF;
END;

-- Care site table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'care_site') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.care_site` (
      care_site_id INT64,
      care_site_name STRING,
      place_of_service_concept_id INT64,
      location_id INT64,
      care_site_source_value STRING,
      place_of_service_source_value STRING
    );
  END IF;
END;

-- Provider table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'provider') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
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
      gender_source_concept_id INT64
    );
  END IF;
END;

-- Payer plan period table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'payer_plan_period') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.payer_plan_period` AS
    SELECT
      payer_plan_period_id,
      person_id,
      CAST(payer_plan_period_start_date AS DATETIME) AS payer_plan_period_start_date,
      CAST(payer_plan_period_end_date AS DATETIME) AS payer_plan_period_end_date,
      payer_concept_id,
      payer_source_value,
      payer_source_concept_id,
      plan_concept_id,
      plan_source_value,
      plan_source_concept_id,
      sponsor_concept_id,
      sponsor_source_value,
      sponsor_source_concept_id,
      family_source_value,
      stop_reason_concept_id,
      stop_reason_source_value,
      stop_reason_source_concept_id
    FROM `@cdmDatabaseSchema.payer_plan_period`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.payer_plan_period` (
      payer_plan_period_id INT64,
      person_id INT64,
      payer_plan_period_start_date DATETIME,
      payer_plan_period_end_date DATETIME,
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
      stop_reason_source_concept_id INT64
    );
  END IF;
END;

-- Cost table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'cost') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
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
      drg_source_value STRING
    );
  END IF;
END;

-- Drug era table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'drug_era') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.drug_era` AS
    SELECT
      drug_era_id,
      person_id,
      drug_concept_id,
      CAST(drug_era_start_date AS DATETIME) AS drug_era_start_date,
      CAST(drug_era_end_date AS DATETIME) AS drug_era_end_date,
      drug_exposure_count,
      gap_days
    FROM `@cdmDatabaseSchema.drug_era`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.drug_era` (
      drug_era_id INT64,
      person_id INT64,
      drug_concept_id INT64,
      drug_era_start_date DATETIME,
      drug_era_end_date DATETIME,
      drug_exposure_count INT64,
      gap_days INT64
    );
  END IF;
END;

-- Dose era table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'dose_era') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.dose_era` AS
    SELECT
      dose_era_id,
      person_id,
      drug_concept_id,
      unit_concept_id,
      dose_value,
      CAST(dose_era_start_date AS DATETIME) AS dose_era_start_date,
      CAST(dose_era_end_date AS DATETIME) AS dose_era_end_date
    FROM `@cdmDatabaseSchema.dose_era`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.dose_era` (
      dose_era_id INT64,
      person_id INT64,
      drug_concept_id INT64,
      unit_concept_id INT64,
      dose_value FLOAT64,
      dose_era_start_date DATETIME,
      dose_era_end_date DATETIME
    );
  END IF;
END;

-- Condition era table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'condition_era') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.condition_era` AS
    SELECT
      condition_era_id,
      person_id,
      condition_concept_id,
      CAST(condition_era_start_date AS DATETIME) AS condition_era_start_date,
      CAST(condition_era_end_date AS DATETIME) AS condition_era_end_date,
      condition_occurrence_count
    FROM `@cdmDatabaseSchema.condition_era`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.condition_era` (
      condition_era_id INT64,
      person_id INT64,
      condition_concept_id INT64,
      condition_era_start_date DATETIME,
      condition_era_end_date DATETIME,
      condition_occurrence_count INT64
    );
  END IF;
END;

-- Metadata table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'metadata') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.metadata` AS
    SELECT
      metadata_concept_id,
      metadata_type_concept_id,
      name,
      value_as_string,
      value_as_concept_id,
      CAST(metadata_date AS DATETIME) AS metadata_date,
      CAST(metadata_datetime AS DATETIME) AS metadata_datetime
    FROM `@cdmDatabaseSchema.metadata`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.metadata` (
      metadata_concept_id INT64,
      metadata_type_concept_id INT64,
      name STRING,
      value_as_string STRING,
      value_as_concept_id INT64,
      metadata_date DATETIME,
      metadata_datetime DATETIME
    );
  END IF;
END;

-- CDM source table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'cdm_source') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.cdm_source` AS
    SELECT
      cdm_source_name,
      cdm_source_abbreviation,
      cdm_holder,
      source_description,
      source_documentation_reference,
      cdm_etl_reference,
      CAST(source_release_date AS DATETIME) AS source_release_date,
      CAST(cdm_release_date AS DATETIME) AS cdm_release_date,
      cdm_version,
      vocabulary_version
    FROM `@cdmDatabaseSchema.cdm_source`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.cdm_source` (
      cdm_source_name STRING,
      cdm_source_abbreviation STRING,
      cdm_holder STRING,
      source_description STRING,
      source_documentation_reference STRING,
      cdm_etl_reference STRING,
      source_release_date DATETIME,
      cdm_release_date DATETIME,
      cdm_version STRING,
      vocabulary_version STRING
    );
  END IF;
END;

-- Concept table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'concept') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.concept` AS
    SELECT
      concept_id,
      concept_name,
      domain_id,
      vocabulary_id,
      concept_class_id,
      standard_concept,
      concept_code,
      CAST(valid_start_date AS DATETIME) AS valid_start_date,
      CAST(valid_end_date AS DATETIME) AS valid_end_date,
      invalid_reason
    FROM `@cdmDatabaseSchema.concept`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept` (
      concept_id INT64,
      concept_name STRING,
      domain_id STRING,
      vocabulary_id STRING,
      concept_class_id STRING,
      standard_concept STRING,
      concept_code STRING,
      valid_start_date DATETIME,
      valid_end_date DATETIME,
      invalid_reason STRING
    );
  END IF;
END;

-- Vocabulary table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'vocabulary') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.vocabulary` (
      vocabulary_id STRING,
      vocabulary_name STRING,
      vocabulary_reference STRING,
      vocabulary_version STRING,
      vocabulary_concept_id INT64
    );
  END IF;
END;

-- Domain table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'domain') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.domain` (
      domain_id STRING,
      domain_name STRING,
      domain_concept_id INT64
    );
  END IF;
END;

-- Concept class table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'concept_class') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_class` (
      concept_class_id STRING,
      concept_class_name STRING,
      concept_class_concept_id INT64
    );
  END IF;
END;

-- Concept relationship table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'concept_relationship') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.concept_relationship` AS
    SELECT
      concept_id_1,
      concept_id_2,
      relationship_id,
      CAST(valid_start_date AS DATETIME) AS valid_start_date,
      CAST(valid_end_date AS DATETIME) AS valid_end_date,
      invalid_reason
    FROM `@cdmDatabaseSchema.concept_relationship`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_relationship` (
      concept_id_1 INT64,
      concept_id_2 INT64,
      relationship_id STRING,
      valid_start_date DATETIME,
      valid_end_date DATETIME,
      invalid_reason STRING
    );
  END IF;
END;

-- Relationship table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'relationship') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.relationship` (
      relationship_id STRING,
      relationship_name STRING,
      is_hierarchical STRING,
      defines_ancestry STRING,
      reverse_relationship_id STRING,
      relationship_concept_id INT64
    );
  END IF;
END;

-- Concept synonym table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'concept_synonym') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_synonym` (
      concept_id INT64,
      concept_synonym_name STRING,
      language_concept_id INT64
    );
  END IF;
END;

-- Concept ancestor table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'concept_ancestor') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.concept_ancestor` (
      ancestor_concept_id INT64,
      descendant_concept_id INT64,
      min_levels_of_separation INT64,
      max_levels_of_separation INT64
    );
  END IF;
END;

-- Source to concept map table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'source_to_concept_map') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.source_to_concept_map` AS
    SELECT
      source_code,
      source_concept_id,
      source_vocabulary_id,
      source_code_description,
      target_concept_id,
      target_vocabulary_id,
      CAST(valid_start_date AS DATETIME) AS valid_start_date,
      CAST(valid_end_date AS DATETIME) AS valid_end_date,
      invalid_reason
    FROM `@cdmDatabaseSchema.source_to_concept_map`;
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.source_to_concept_map` (
      source_code STRING,
      source_concept_id INT64,
      source_vocabulary_id STRING,
      source_code_description STRING,
      target_concept_id INT64,
      target_vocabulary_id STRING,
      valid_start_date DATETIME,
      valid_end_date DATETIME,
      invalid_reason STRING
    );
  END IF;
END;

-- Drug strength table
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'drug_strength') THEN
    CREATE OR REPLACE TABLE `@cdmDatabaseSchema.drug_strength` AS
    SELECT
      drug_concept_id,
      ingredient_concept_id,
      amount_value,
      amount_unit_concept_id,
      numerator_value,
      numerator_unit_concept_id,
      denominator_value,
      denominator_unit_concept_id,
      box_size,
      CAST(valid_start_date AS DATETIME) AS valid_start_date,
      CAST(valid_end_date AS DATETIME) AS valid_end_date,
      invalid_reason
    FROM `@cdmDatabaseSchema.drug_strength`;
  ELSE
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
      valid_start_date DATETIME,
      valid_end_date DATETIME,
      invalid_reason STRING
    );
  END IF;
END;

-- Cohort definition table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'cohort_definition') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.cohort_definition` (
      cohort_definition_id INT64,
      cohort_definition_name STRING,
      cohort_definition_description STRING,
      definition_type_concept_id INT64,
      cohort_definition_syntax STRING,
      subject_concept_id INT64,
      cohort_initiation_date DATE
    );
  END IF;
END;

-- Attribute definition table (no DATETIME fields)
BEGIN
  IF EXISTS (SELECT 1 FROM `@cdmDatabaseSchema.__TABLES__` WHERE table_id = 'attribute_definition') THEN
    -- No action needed, as there are no DATETIME fields to cast
  ELSE
    CREATE TABLE IF NOT EXISTS `@cdmDatabaseSchema.attribute_definition` (
      attribute_definition_id INT64,
      attribute_name STRING,
      attribute_description STRING,
      attribute_type_concept_id INT64,
      attribute_syntax STRING
    );
  END IF;
END;