-- create base eras from the concepts found in condition_occurrence
-- Step 1: Create the initial condition target table
CREATE OR REPLACE TABLE cteConditionTarget AS
SELECT co.PERSON_ID,
    co.condition_concept_id,
    CAST(co.CONDITION_START_DATE AS DATE) AS CONDITION_START_DATE,
    COALESCE(TRY_CAST(co.CONDITION_END_DATE AS DATE), CAST(CONDITION_START_DATE AS DATE) + INTERVAL 1 DAY) AS CONDITION_END_DATE
FROM read_parquet('@CONDITION_OCCURRENCE_PATH') co
WHERE TRY_CAST(CONDITION_START_DATE AS DATE) IS NOT NULL
AND condition_concept_id != 0
AND CONDITION_START_DATE != default_date()
AND IFNULL(CONDITION_END_DATE, '') != default_date()
AND IFNULL(TRY_CAST(person_id AS BIGINT), -1) != -1
;

-- Step 2: Create the table for end dates
CREATE OR REPLACE TABLE cteCondEndDates AS 
SELECT PERSON_ID,
    CONDITION_CONCEPT_ID,
    EVENT_DATE - INTERVAL 30 DAY AS END_DATE -- unpad the end date
FROM (
    SELECT E1.PERSON_ID,
        E1.CONDITION_CONCEPT_ID,
        E1.EVENT_DATE,
        COALESCE(E1.START_ORDINAL, MAX(E2.START_ORDINAL)) START_ORDINAL,
        E1.OVERALL_ORD
    FROM (
        SELECT PERSON_ID,
            CONDITION_CONCEPT_ID,
            EVENT_DATE,
            EVENT_TYPE,
            START_ORDINAL,
            ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONDITION_CONCEPT_ID ORDER BY EVENT_DATE, EVENT_TYPE) AS OVERALL_ORD
        FROM (
            -- Step 3: Select start dates, assigning a row number to each
            SELECT PERSON_ID,
                CONDITION_CONCEPT_ID,
                CONDITION_START_DATE AS EVENT_DATE,
                -1 AS EVENT_TYPE,
                ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE) AS START_ORDINAL
            FROM cteConditionTarget
            UNION ALL
            -- Pad the end dates by 30 days
            SELECT PERSON_ID,
                CONDITION_CONCEPT_ID,
                CONDITION_END_DATE + INTERVAL 30 DAY AS EVENT_DATE,
                1 AS EVENT_TYPE,
                NULL
            FROM cteConditionTarget
        ) RAWDATA
    ) E1
    INNER JOIN (
        SELECT PERSON_ID,
            CONDITION_CONCEPT_ID,
            CONDITION_START_DATE AS EVENT_DATE,
            ROW_NUMBER() OVER (PARTITION BY PERSON_ID, CONDITION_CONCEPT_ID ORDER BY CONDITION_START_DATE) AS START_ORDINAL
        FROM cteConditionTarget
    ) E2 ON E1.PERSON_ID = E2.PERSON_ID
        AND E1.CONDITION_CONCEPT_ID = E2.CONDITION_CONCEPT_ID
        AND E2.EVENT_DATE <= E1.EVENT_DATE
    GROUP BY E1.PERSON_ID,
            E1.CONDITION_CONCEPT_ID,
            E1.EVENT_DATE,
            E1.START_ORDINAL,
            E1.OVERALL_ORD
) E
WHERE (2 * E.START_ORDINAL) - E.OVERALL_ORD = 0;

-- Step 4: Create the table for condition end dates
CREATE OR REPLACE TABLE cteConditionEnds AS
SELECT c.PERSON_ID,
    c.CONDITION_CONCEPT_ID,
    c.CONDITION_START_DATE,
    MIN(e.END_DATE) AS ERA_END_DATE
FROM cteConditionTarget c
INNER JOIN cteCondEndDates e ON c.PERSON_ID = e.PERSON_ID
                            AND c.CONDITION_CONCEPT_ID = e.CONDITION_CONCEPT_ID
                            AND e.END_DATE >= c.CONDITION_START_DATE
GROUP BY c.PERSON_ID,
        c.CONDITION_CONCEPT_ID,
        c.CONDITION_START_DATE;

-- Step 5: Select the final result into a new table
CREATE OR REPLACE TABLE finalConditionEra AS
SELECT row_number() OVER (ORDER BY person_id) AS condition_era_id,
    person_id,
    condition_concept_id,
    min(CONDITION_START_DATE) AS condition_era_start_date,
    ERA_END_DATE AS condition_era_end_date,
    COUNT(*) AS condition_occurrence_count
FROM cteConditionEnds
GROUP BY person_id,
        CONDITION_CONCEPT_ID,
        ERA_END_DATE;