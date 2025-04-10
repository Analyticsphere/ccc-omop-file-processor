CREATE OR REPLACE TEMP TABLE drug_exposure_optimized AS
    SELECT
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        days_supply,
        drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE')
    WHERE drug_concept_id != 0
    AND IFNULL(TRY_CAST(person_id AS BIGINT), -1) != -1
;


CREATE OR REPLACE TEMP TABLE concept_ancestor_optimized AS
    SELECT descendant_concept_id, ancestor_concept_id FROM read_parquet('@CONCEPT_ANCESTOR')
    WHERE descendant_concept_id IN (
        SELECT DISTINCT drug_concept_id FROM drug_exposure_optimized
    )
;

CREATE OR REPLACE TEMP TABLE concept_optimized AS
    SELECT concept_id, vocabulary_id, concept_class_id FROM read_parquet('@CONCEPT')
    WHERE concept_id IN (
        SELECT DISTINCT ancestor_concept_id FROM concept_ancestor_optimized
    )
;

CREATE OR REPLACE TABLE ctePreDrugTarget AS
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            d.drug_exposure_end_date,
            CASE 
                WHEN d.days_supply IS NOT NULL AND d.drug_exposure_start_date IS NOT NULL 
                THEN d.drug_exposure_start_date + d.days_supply * INTERVAL '1' DAY 
            END,
            d.drug_exposure_start_date + INTERVAL '1' DAY
        ) AS drug_exposure_end_date
    FROM drug_exposure_optimized d
    JOIN concept_ancestor_optimized ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    JOIN concept_optimized c 
        ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
    AND c.concept_class_id = 'Ingredient'
;

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

CREATE OR REPLACE TABLE cteDrugExposureEnds AS 
SELECT
    dt.person_id,
    dt.ingredient_concept_id,
    dt.drug_exposure_start_date,
    MIN(e.end_date) AS drug_sub_exposure_end_date
FROM ctePreDrugTarget dt
JOIN cteSubExposureEndDates e ON dt.person_id = e.person_id AND dt.ingredient_concept_id = e.ingredient_concept_id AND e.end_date >= dt.drug_exposure_start_date
GROUP BY
    dt.drug_exposure_id,
    dt.person_id,
    dt.ingredient_concept_id,
    dt.drug_exposure_start_date
;
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE cteSubExposures AS 
SELECT ROW_NUMBER() OVER (PARTITION BY person_id, ingredient_concept_id, drug_sub_exposure_end_date ORDER BY person_id) AS row_number,
    person_id, ingredient_concept_id AS drug_concept_id, MIN(drug_exposure_start_date) AS drug_sub_exposure_start_date, drug_sub_exposure_end_date, COUNT(*) AS drug_exposure_count
FROM cteDrugExposureEnds
GROUP BY person_id, ingredient_concept_id, drug_sub_exposure_end_date
;

--------------------------------------------------------------------------------------------------------------
/*Everything above grouped exposures into sub_exposures if there was overlap between exposures.
* So there was no persistence window. Now we can add the persistence window to calculate eras.
*/
--------------------------------------------------------------------------------------------------------------

CREATE OR REPLACE TABLE cteFinalTarget AS 
SELECT row_number, person_id, drug_concept_id, drug_sub_exposure_start_date, drug_sub_exposure_end_date, drug_exposure_count,
    DATEDIFF('day', drug_sub_exposure_start_date, drug_sub_exposure_end_date) AS days_exposed
FROM cteSubExposures
;
--------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE TABLE cteEndDates AS 
SELECT person_id, drug_concept_id, event_date - INTERVAL '30' DAY AS end_date -- unpad the end date
FROM
(
    SELECT person_id, drug_concept_id, event_date, event_type,
    MAX(start_ordinal) OVER (PARTITION BY person_id, drug_concept_id
        ORDER BY event_date, event_type ROWS UNBOUNDED PRECEDING) AS start_ordinal,
        ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id
            ORDER BY event_date, event_type) AS overall_ord
    FROM (
        SELECT person_id, drug_concept_id, drug_sub_exposure_start_date AS event_date,
        -1 AS event_type,
        ROW_NUMBER() OVER (PARTITION BY person_id, drug_concept_id
            ORDER BY drug_sub_exposure_start_date) AS start_ordinal
        FROM cteFinalTarget

        UNION ALL

        -- pad the end dates by 30 to allow a grace period for overlapping ranges.
        SELECT person_id, drug_concept_id, drug_sub_exposure_end_date + INTERVAL '30' DAY, 1 AS event_type, NULL
        FROM cteFinalTarget
    ) RAWDATA
) e
WHERE (2 * e.start_ordinal) - e.overall_ord = 0
;

CREATE OR REPLACE TABLE cteDrugEraEnds AS 
SELECT
    ft.person_id,
    ft.drug_concept_id,
    ft.drug_sub_exposure_start_date,
    MIN(e.end_date) AS era_end_date,
    drug_exposure_count,
    days_exposed
FROM cteFinalTarget ft
JOIN cteEndDates e ON ft.person_id = e.person_id AND ft.drug_concept_id = e.drug_concept_id AND e.end_date >= ft.drug_sub_exposure_start_date
GROUP BY
    ft.person_id,
    ft.drug_concept_id,
    ft.drug_sub_exposure_start_date,
    drug_exposure_count,
    days_exposed
;

CREATE OR REPLACE TABLE  final_select AS 
SELECT DISTINCT
    --ROW_NUMBER() OVER (ORDER BY person_id) AS drug_era_id,
    person_id,
    drug_concept_id,
    MIN(drug_sub_exposure_start_date) AS drug_era_start_date,
    era_end_date AS drug_era_end_date,
    SUM(drug_exposure_count) AS drug_exposure_count,
    DATEDIFF('day', MIN(drug_sub_exposure_start_date), drug_era_end_date) - SUM(days_exposed) AS gap_days
FROM cteDrugEraEnds dee
GROUP BY person_id, drug_concept_id, era_end_date
;