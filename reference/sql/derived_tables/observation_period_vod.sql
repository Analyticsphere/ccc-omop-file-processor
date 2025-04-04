-- Custom SQL to generate observation_period records that follow OHDSI recommendations/convensions:
    -- https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html
-- Generating deterministic hash composite primary key
-- Uses visit_occurrence and death tables

WITH death_data AS (
    SELECT 
        person_id, 
        CAST(death_date AS DATE) AS death_date
    FROM (
        -- Try to read the death file, but return empty result if it doesn't exist
        SELECT * FROM (
            SELECT * 
            FROM read_parquet('@DEATH') 
            WHERE TRY_CAST(death_date AS DATE) IS NOT NULL
                AND IFNULL(TRY_CAST(person_id AS BIGINT), -1) != -1
        ) WHERE 1=1
        EXCEPT ALL
        SELECT * FROM (
            -- This will produce empty result if @DEATH doesn't exist (due to subtraction)
            SELECT * 
            FROM read_parquet('@DEATH')
            WHERE 0=1
        ) WHERE 0=1
    )
),
valid_visits AS (
    SELECT
        v.person_id, 
        CAST(visit_start_date AS DATE) AS visit_start_date, 
        CAST(visit_end_date AS DATE) AS visit_end_date,
        d.death_date
    FROM
        read_parquet('@VISIT_OCCURRENCE') v
    LEFT JOIN
        death_data d ON v.person_id = d.person_id
    WHERE
        v.visit_start_date <= '@CURRENT_DATE'
        AND v.visit_end_date <= '@CURRENT_DATE'
        AND TRY_CAST(visit_start_date AS DATE) IS NOT NULL
        AND TRY_CAST(visit_end_date AS DATE) IS NOT NULL
        AND IFNULL(TRY_CAST(v.person_id AS BIGINT), -1) != -1
),
adjusted_visits AS (
    SELECT
        *,
        -- Adjust visit_end_date considering death_date and CURRENT_DATE
        LEAST(
            CASE
                WHEN death_date IS NOT NULL AND death_date < visit_end_date THEN death_date
                ELSE visit_end_date
            END,
            '@CURRENT_DATE'
        ) AS adjusted_visit_end_date,
        -- Exclude visits starting after death_date or CURRENT_DATE
        CASE
            WHEN death_date IS NOT NULL AND death_date < visit_start_date THEN NULL
            WHEN visit_start_date > '@CURRENT_DATE' THEN NULL
            ELSE visit_start_date
        END AS adjusted_visit_start_date,
        -- Adjusted visit_end_date + 60 days, not exceeding death_date or CURRENT_DATE
        LEAST(
            CASE
                WHEN death_date IS NOT NULL AND death_date < visit_end_date THEN death_date
                ELSE visit_end_date
            END + INTERVAL '60 days',
            COALESCE(death_date, DATE '9999-12-31'),
            '@CURRENT_DATE'
        ) AS adjusted_visit_end_plus_60
    FROM
        valid_visits
),
filtered_visits AS (
    SELECT
        *
    FROM
        adjusted_visits
    WHERE
        adjusted_visit_start_date IS NOT NULL
),
ordered_visits AS (
    SELECT
        *,
        LAG(adjusted_visit_end_plus_60) OVER (
            PARTITION BY person_id
            ORDER BY adjusted_visit_start_date
        ) AS prev_adjusted_end_date
    FROM
        filtered_visits
),
visit_gaps AS (
    SELECT
        *,
        DATE_DIFF('day', prev_adjusted_end_date, adjusted_visit_start_date) AS gap_in_days
    FROM
        ordered_visits
),
island_flags AS (
    SELECT
        *,
        CASE
            WHEN gap_in_days > 548 OR prev_adjusted_end_date IS NULL THEN 1
            ELSE 0
        END AS new_island_flag
    FROM
        visit_gaps
),
island_groups AS (
    SELECT
        *,
        SUM(new_island_flag) OVER (
            PARTITION BY person_id
            ORDER BY adjusted_visit_start_date
            ROWS UNBOUNDED PRECEDING
        ) AS island_group
    FROM
        island_flags
), groupedby AS (
    SELECT
        person_id,
        island_group,
        MIN(adjusted_visit_start_date) AS island_start_date,
        MAX(adjusted_visit_end_plus_60) AS island_end_date
    FROM
        island_groups
    GROUP BY
        person_id,
        island_group
), missingpersons AS (
    SELECT DISTINCT
        hash(CONCAT('@SITE', person_id, '1970-01-01', '@CURRENT_DATE')) % 9223372036854775807 AS observation_period_id,
        person_id,
        CAST('1970-01-01' AS DATE) AS observation_period_start_date,
        CAST('@CURRENT_DATE' AS DATE) AS observation_period_end_date,
        32882 AS period_type_concept_id
    FROM read_parquet('@PERSON')
    WHERE person_id NOT IN (
        SELECT DISTINCT person_id FROM groupedby
    )
)
SELECT DISTINCT
    hash(CONCAT('@SITE', person_id, island_start_date, island_end_date)) % 9223372036854775807 AS observation_period_id,
    person_id,
    CAST(island_start_date AS DATE) AS observation_period_start_date,
    CAST(island_end_date AS DATE) AS observation_period_end_date,
    32882 AS period_type_concept_id
FROM groupedby
UNION
SELECT 
    observation_period_id,
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    period_type_concept_id
FROM missingpersons