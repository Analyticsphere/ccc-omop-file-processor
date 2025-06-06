-- Custom SQL to generate observation_period records that follow OHDSI recommendations/convensions:
    -- https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html
-- Generating deterministic hash composite primary key
-- Used in the event that visit_occurrence table is not deliviered

SELECT DISTINCT
    hash(CONCAT('@SITE', person_id, '1970-01-01', '@CURRENT_DATE')) % 9223372036854775807 AS observation_period_id,
    person_id,
    CAST('1970-01-01' AS DATE) AS observation_period_start_date,
    CAST('@CURRENT_DATE' AS DATE) AS observation_period_end_date,
    32882 AS period_type_concept_id
FROM read_parquet('@PERSON')