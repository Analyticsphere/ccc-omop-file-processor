-- Optimized Drug Era generation script for DuckDB
-- Eliminates window functions while maintaining correct drug era logic
-- Allows for 30-day gaps between exposures

-- Step 1: Create the initial drug target data with normalized end dates
CREATE OR REPLACE TABLE ctePreDrugTarget AS
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date AS drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            CASE WHEN d.drug_exposure_end_date IS NOT NULL THEN d.drug_exposure_end_date END,
            CASE WHEN d.days_supply IS NOT NULL AND d.drug_exposure_start_date IS NOT NULL 
                 THEN d.drug_exposure_start_date + d.days_supply * INTERVAL '1' DAY END,
            d.drug_exposure_start_date + INTERVAL '1' DAY
        ) AS drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE') d
    JOIN read_parquet('@CONCEPT_ANCESTOR') ca ON ca.descendant_concept_id = d.drug_concept_id
    JOIN read_parquet('@CONCEPT') c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
    AND c.concept_class_id = 'Ingredient'
    AND d.drug_concept_id != 0
    AND IFNULL(d.days_supply, 0) >= 0;

CREATE OR REPLACE TABLE cteSubExposureEndDates AS 
    SELECT person_id, ingredient_concept_id, event_date AS end_date
    FROM
    (
        SELECT person_id, ingredient_concept_id, event_date, event_type,
        MAX(start_ordinal) OVER (PARTITION BY person_id, ingredient_concept_id
            ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                ORDER BY event_date, event_type) AS overall_ord
        FROM (
            SELECT person_id, ingredient_concept_id, drug_exposure_start_date AS event_date,
            -1 AS event_type,
            ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id
                ORDER BY drug_exposure_start_date) AS start_ordinal
            FROM ctePreDrugTarget

            UNION ALL

            SELECT person_id, ingredient_concept_id, drug_exposure_end_date, 1 AS event_type, NULL
            FROM ctePreDrugTarget
        ) RAWDATA
    ) e
    WHERE (2 * e.start_ordinal) - e.overall_ord = 0
;

