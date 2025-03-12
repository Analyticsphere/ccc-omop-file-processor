-- https://ohdsi.github.io/CommonDataModel/sqlScripts.html#Drug_Eras
-- Modified the OHDSI provided SQL script so it runs in DuckDB
-- Generating deterministic hash composite primary key using custom UDF generate_id()

-- Optimized version of the drug era generation script
-- Eliminates window functions to reduce memory usage

-- Step 1: Create the initial drug target data (no window functions here)
CREATE OR REPLACE TABLE ctePreDrugTarget AS
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        CAST(d.drug_exposure_start_date AS DATE) AS drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            CASE 
                WHEN d.drug_exposure_end_date IS NOT NULL 
                THEN CAST(d.drug_exposure_end_date AS DATE) 
            END,
            CASE 
                WHEN TRY_CAST(d.days_supply AS INTEGER) IS NOT NULL AND d.drug_exposure_start_date IS NOT NULL 
                THEN CAST(d.drug_exposure_start_date AS DATE) + CAST(d.days_supply AS INTEGER) * INTERVAL '1' DAY 
            END,
            CAST(d.drug_exposure_start_date AS DATE) + INTERVAL '1' DAY
        ) AS drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE') d
    JOIN read_parquet('@CONCEPT_ANCESTOR') ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    JOIN read_parquet('@CONCEPT') c 
        ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
    AND c.concept_class_id = 'Ingredient'
    AND d.drug_concept_id != 0
    AND IFNULL(TRY_CAST(d.person_id AS BIGINT), -1) != -1;

-- Step 2: Create start dates and end dates separately
CREATE OR REPLACE TABLE cteExposureStartDates AS
    SELECT 
        person_id, 
        ingredient_concept_id, 
        drug_exposure_start_date AS event_date,
        drug_exposure_id
    FROM ctePreDrugTarget
    ORDER BY person_id, ingredient_concept_id, drug_exposure_start_date;

CREATE OR REPLACE TABLE cteExposureEndDates AS
    SELECT 
        person_id, 
        ingredient_concept_id, 
        drug_exposure_end_date AS event_date,
        drug_exposure_id
    FROM ctePreDrugTarget
    ORDER BY person_id, ingredient_concept_id, drug_exposure_end_date;

-- Step 3: Alternative approach to finding sub-exposure end dates
-- Instead of window functions, use a self-join approach
CREATE OR REPLACE TABLE cteSubExposureEndDates AS
    SELECT 
        s.person_id, 
        s.ingredient_concept_id, 
        MIN(e.event_date) AS end_date
    FROM cteExposureStartDates s
    JOIN cteExposureEndDates e ON 
        s.person_id = e.person_id AND 
        s.ingredient_concept_id = e.ingredient_concept_id AND
        e.event_date >= s.event_date AND
        NOT EXISTS (
            SELECT 1 FROM cteExposureStartDates s2
            WHERE s2.person_id = s.person_id AND
                  s2.ingredient_concept_id = s.ingredient_concept_id AND
                  s2.event_date > s.event_date AND
                  s2.event_date <= e.event_date
        )
    GROUP BY s.person_id, s.ingredient_concept_id, s.event_date, s.drug_exposure_id;

-- Step 4: Determine drug exposure ends
CREATE OR REPLACE TABLE cteDrugExposureEnds AS
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget dt
    JOIN cteSubExposureEndDates e ON 
        dt.person_id = e.person_id AND 
        dt.ingredient_concept_id = e.ingredient_concept_id AND 
        e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.drug_exposure_id,
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date;

-- Step 5: Group into sub-exposures without window function
CREATE OR REPLACE TABLE cteSubExposures AS
    SELECT 
        person_id, 
        ingredient_concept_id AS drug_concept_id, 
        MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date, 
        drug_sub_exposure_end_date, 
        COUNT(*) AS drug_exposure_count
    FROM cteDrugExposureEnds
    GROUP BY person_id, ingredient_concept_id, drug_sub_exposure_end_date;

-- Step 6: Create final target without ROW_NUMBER
CREATE OR REPLACE TABLE cteFinalTarget AS
    SELECT 
        person_id, 
        drug_concept_id, 
        drug_sub_exposure_start_date, 
        drug_sub_exposure_end_date, 
        drug_exposure_count,
        DATEDIFF('day', drug_sub_exposure_start_date, drug_sub_exposure_end_date) AS days_exposed
    FROM cteSubExposures;

-- Step 7: Add persistence window for eras
-- Create start and end dates with padding separately

CREATE OR REPLACE TABLE cteEraPaddedEndDates AS
    SELECT 
        person_id, 
        drug_concept_id, 
        drug_sub_exposure_end_date + INTERVAL '30' DAY AS padded_end_date,
        drug_sub_exposure_end_date AS original_end_date
    FROM cteFinalTarget;

-- Step 8: Find era end dates through self-join
CREATE OR REPLACE TABLE cteEraEndDates AS
    SELECT 
        t1.person_id,
        t1.drug_concept_id,
        t1.drug_sub_exposure_start_date,
        MIN(
            CASE WHEN t2.drug_sub_exposure_start_date IS NULL 
                THEN t1.padded_end_date - INTERVAL '30' DAY
                ELSE t2.padded_end_date - INTERVAL '30' DAY
            END
        ) AS era_end_date
    FROM 
        (SELECT ft.*, pe.padded_end_date 
         FROM cteFinalTarget ft
         JOIN cteEraPaddedEndDates pe ON 
             ft.person_id = pe.person_id AND 
             ft.drug_concept_id = pe.drug_concept_id AND
             ft.drug_sub_exposure_end_date = pe.original_end_date) t1
    LEFT JOIN 
        (SELECT ft.*, pe.padded_end_date 
         FROM cteFinalTarget ft
         JOIN cteEraPaddedEndDates pe ON 
             ft.person_id = pe.person_id AND 
             ft.drug_concept_id = pe.drug_concept_id AND
             ft.drug_sub_exposure_end_date = pe.original_end_date) t2
    ON 
        t1.person_id = t2.person_id AND
        t1.drug_concept_id = t2.drug_concept_id AND
        t1.drug_sub_exposure_start_date < t2.drug_sub_exposure_start_date AND
        t1.padded_end_date >= t2.drug_sub_exposure_start_date
    GROUP BY 
        t1.person_id,
        t1.drug_concept_id,
        t1.drug_sub_exposure_start_date;

-- Step 9: Final results
CREATE OR REPLACE TABLE final_select AS
    SELECT
        person_id,
        drug_concept_id,
        MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
        MAX(era_end_date) AS drug_era_end_date,
        SUM(drug_exposure_count) AS drug_exposure_count,
        DATEDIFF('day', MIN(drug_sub_exposure_start_date), MAX(era_end_date)) - SUM(days_exposed) AS gap_days
    FROM
        (SELECT 
            t.person_id,
            t.drug_concept_id,
            t.drug_sub_exposure_start_date,
            e.era_end_date,
            t.drug_exposure_count,
            t.days_exposed
         FROM cteFinalTarget t
         JOIN cteEraEndDates e ON
            t.person_id = e.person_id AND
            t.drug_concept_id = e.drug_concept_id AND
            t.drug_sub_exposure_start_date = e.drug_sub_exposure_start_date
        ) AS joined_data
    GROUP BY 
        person_id, 
        drug_concept_id,
        -- Group by a derived column that identifies connected periods
        (SELECT COUNT(*) 
         FROM cteFinalTarget t2 
         WHERE t2.person_id = joined_data.person_id AND
               t2.drug_concept_id = joined_data.drug_concept_id AND
               t2.drug_sub_exposure_end_date + INTERVAL '30' DAY < joined_data.drug_sub_exposure_start_date);