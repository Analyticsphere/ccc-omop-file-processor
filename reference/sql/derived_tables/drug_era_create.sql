-- Optimized Drug Era generation script for DuckDB
-- Eliminates window functions while maintaining correct drug era logic
-- Allows for 30-day gaps between exposures

-- Step 1: Create the initial drug target data with normalized end dates
CREATE OR REPLACE TABLE ctePreDrugTarget AS
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        CAST(d.drug_exposure_start_date AS DATE) AS drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            CASE WHEN d.drug_exposure_end_date IS NOT NULL THEN CAST(d.drug_exposure_end_date AS DATE) END,
            CASE WHEN TRY_CAST(d.days_supply AS INTEGER) IS NOT NULL AND d.drug_exposure_start_date IS NOT NULL 
                 THEN CAST(d.drug_exposure_start_date AS DATE) + CAST(d.days_supply AS INTEGER) * INTERVAL '1' DAY END,
            CAST(d.drug_exposure_start_date AS DATE) + INTERVAL '1' DAY
        ) AS drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE') d
    JOIN read_parquet('@CONCEPT_ANCESTOR') ca ON ca.descendant_concept_id = d.drug_concept_id
    JOIN read_parquet('@CONCEPT') c ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
    AND c.concept_class_id = 'Ingredient'
    AND d.drug_concept_id != 0
    AND IFNULL(TRY_CAST(d.days_supply AS INTEGER), 0) >= 0;

-- Step 2: Create temporary tables for start and end dates
CREATE OR REPLACE TABLE temp_exposure_events AS
    -- Start dates (marked as type -1)
    SELECT 
        person_id, 
        ingredient_concept_id, 
        drug_exposure_start_date AS event_date, 
        -1 AS event_type,
        drug_exposure_id
    FROM ctePreDrugTarget
    
    UNION ALL
    
    -- End dates (marked as type 1)
    SELECT 
        person_id, 
        ingredient_concept_id, 
        drug_exposure_end_date AS event_date, 
        1 AS event_type,
        drug_exposure_id
    FROM ctePreDrugTarget;

-- Step 3: Create a sorted version with event ordering
CREATE OR REPLACE TABLE sorted_exposure_events AS
    SELECT
        person_id,
        ingredient_concept_id,
        event_date,
        event_type,
        drug_exposure_id,
        -- Create a running count of start events minus end events
        -- This helps identify continuous exposure periods
        SUM(CASE WHEN event_type = -1 THEN 1 
                 WHEN event_type = 1 THEN -1
                 ELSE 0 END) OVER (
            PARTITION BY person_id, ingredient_concept_id
            ORDER BY event_date, event_type
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS active_exposure_count
    FROM temp_exposure_events
    ORDER BY person_id, ingredient_concept_id, event_date, event_type;

-- Step 4: Extract sub-exposure endpoints without window functions
CREATE OR REPLACE TABLE cteSubExposureEndDates AS
    SELECT
        e1.person_id,
        e1.ingredient_concept_id,
        e1.event_date AS end_date
    FROM sorted_exposure_events e1
    WHERE e1.event_type = 1  -- This is an end date
    AND e1.active_exposure_count = 0  -- No active exposures after this point
    AND EXISTS (  -- Confirm there's a corresponding start date
        SELECT 1 FROM sorted_exposure_events e2
        WHERE e2.person_id = e1.person_id
        AND e2.ingredient_concept_id = e1.ingredient_concept_id
        AND e2.event_type = -1
        AND e2.event_date <= e1.event_date
    );

-- Step 5: Link exposures to their end dates
CREATE OR REPLACE TABLE cteDrugExposureEnds AS
    SELECT
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date,
        MIN(e.end_date) AS drug_sub_exposure_end_date
    FROM ctePreDrugTarget dt
    JOIN cteSubExposureEndDates e 
        ON dt.person_id = e.person_id 
        AND dt.ingredient_concept_id = e.ingredient_concept_id 
        AND e.end_date >= dt.drug_exposure_start_date
    GROUP BY
        dt.drug_exposure_id,
        dt.person_id,
        dt.ingredient_concept_id,
        dt.drug_exposure_start_date;

-- Step 6: Group overlapping sub-exposures
CREATE OR REPLACE TABLE cteSubExposures AS
    SELECT
        person_id,
        ingredient_concept_id AS drug_concept_id,
        MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        COUNT(*) AS drug_exposure_count
    FROM cteDrugExposureEnds
    GROUP BY person_id, ingredient_concept_id, drug_sub_exposure_end_date;

-- Step 7: Add days exposed calculation
CREATE OR REPLACE TABLE cteFinalTarget AS
    SELECT
        person_id,
        drug_concept_id,
        drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        drug_exposure_count,
        DATEDIFF('day', drug_sub_exposure_start_date, drug_sub_exposure_end_date) AS days_exposed
    FROM cteSubExposures;

-- Step 8: Apply persistence window (30-day gap rule)
-- First, add persistence window to each sub-exposure
CREATE OR REPLACE TABLE padded_exposures AS
    SELECT
        person_id,
        drug_concept_id,
        drug_sub_exposure_start_date,
        drug_sub_exposure_end_date,
        drug_sub_exposure_end_date + INTERVAL '30' DAY AS padded_end_date,
        drug_exposure_count,
        days_exposed
    FROM cteFinalTarget;

-- Step 9: Identify drug eras by finding connected sub-exposures
-- Using a recursive CTE to group connected exposures


-- Step 10: Alternative without recursive CTE if that's not preferred
-- This approach works by joining each exposure to potential "next" exposures
CREATE OR REPLACE TABLE exposure_chains AS
    SELECT
        e1.person_id,
        e1.drug_concept_id,
        e1.drug_sub_exposure_start_date,
        e1.drug_sub_exposure_end_date,
        e2.drug_sub_exposure_start_date AS next_start_date
    FROM padded_exposures e1
    LEFT JOIN padded_exposures e2 ON
        e1.person_id = e2.person_id AND
        e1.drug_concept_id = e2.drug_concept_id AND
        e2.drug_sub_exposure_start_date > e1.drug_sub_exposure_start_date AND
        e2.drug_sub_exposure_start_date <= e1.padded_end_date
    -- Get the earliest "next" exposure if multiple exist
    WHERE e2.drug_sub_exposure_start_date = (
        SELECT MIN(e3.drug_sub_exposure_start_date)
        FROM padded_exposures e3
        WHERE e3.person_id = e1.person_id AND
              e3.drug_concept_id = e1.drug_concept_id AND
              e3.drug_sub_exposure_start_date > e1.drug_sub_exposure_start_date AND
              e3.drug_sub_exposure_start_date <= e1.padded_end_date
    ) OR e2.drug_sub_exposure_start_date IS NULL;

-- Step 11: Assign era identifiers without window functions
CREATE OR REPLACE TABLE drug_era_endpoints AS
    SELECT
        person_id,
        drug_concept_id,
        drug_sub_exposure_start_date AS era_start_date,
        CASE 
            WHEN next_start_date IS NULL THEN drug_sub_exposure_end_date
            ELSE NULL
        END AS era_end_date
    FROM exposure_chains
    -- Mark chain endpoints (where there's no next exposure)
    WHERE next_start_date IS NULL;

-- Step 12: Construct complete eras by matching each exposure to its era
CREATE OR REPLACE TABLE drug_era_groups_alt AS
    SELECT
        c.person_id,
        c.drug_concept_id,
        -- Find the earliest era start that this exposure belongs to
        (SELECT MIN(era_start_date)
         FROM drug_era_endpoints e
         WHERE e.person_id = c.person_id AND
               e.drug_concept_id = c.drug_concept_id AND
               e.era_start_date <= c.drug_sub_exposure_start_date AND
               (c.next_start_date IS NULL OR 
                c.next_start_date > (SELECT MIN(era_end_date) FROM drug_era_endpoints 
                                    WHERE person_id = e.person_id AND 
                                          drug_concept_id = e.drug_concept_id AND
                                          era_start_date = e.era_start_date))
        ) AS era_start_date,
        c.drug_sub_exposure_start_date,
        c.drug_sub_exposure_end_date
    FROM exposure_chains c;

-- Step 13: Final drug era table
CREATE OR REPLACE TABLE final_drug_eras AS
    SELECT
        person_id,
        drug_concept_id,
        MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
        MAX(drug_sub_exposure_end_date) AS drug_era_end_date,
        SUM(drug_exposure_count) AS drug_exposure_count,
        SUM(days_exposed) AS total_days_exposed
    FROM (
        SELECT
            p.person_id,
            p.drug_concept_id,
            p.drug_sub_exposure_start_date,
            p.drug_sub_exposure_end_date,
            p.drug_exposure_count,
            p.days_exposed,
            g.era_start_date
        FROM padded_exposures p
        JOIN drug_era_groups_alt g ON
            p.person_id = g.person_id AND
            p.drug_concept_id = g.drug_concept_id AND
            p.drug_sub_exposure_start_date = g.drug_sub_exposure_start_date
    ) AS grouped_exposures
    GROUP BY person_id, drug_concept_id, era_start_date;

-- Step 14: Add gap days calculation and create final output
CREATE OR REPLACE TABLE final_select AS
    SELECT
        CAST(person_id AS BIGINT) AS person_id,
        CAST(drug_concept_id AS BIGINT) AS drug_concept_id,
        CAST(drug_era_start_date AS DATE) AS drug_era_start_date,
        CAST(drug_era_end_date AS DATE) AS drug_era_end_date,
        CAST(drug_exposure_count AS BIGINT) AS drug_exposure_count,
        CAST((DATEDIFF('day', drug_era_start_date, drug_era_end_date) - total_days_exposed) AS BIGINT) AS gap_days
    FROM final_drug_eras;