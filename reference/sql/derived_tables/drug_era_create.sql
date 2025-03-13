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
            CASE WHEN d.drug_exposure_end_date IS NOT NULL THEN CAST(d.drug_exposure_end_date AS DATE) END,
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
    --AND IFNULL(TRY_CAST(d.days_supply AS INTEGER), 0) >= 0;
