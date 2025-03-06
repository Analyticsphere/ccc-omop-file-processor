-- Custom SQL to generate observation_period records that follow OHDSI recommendations/convensions:
    -- https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html
-- Generating deterministic hash composite primary key using custom UDF generate_id()
-- Uses visit_occurrence table

WITH valid_visits AS (
    SELECT
        person_id,
        CAST(visit_start_date AS DATE) AS visit_start_date,
        CAST(visit_end_date AS DATE) AS visit_end_date
    FROM read_parquet('@VISIT_OCCURRENCE')
    WHERE
        visit_start_date <= '@CURRENT_DATE'
        AND visit_end_date <= '@CURRENT_DATE'
        AND TRY_CAST(visit_start_date AS DATE) IS NOT NULL
        AND TRY_CAST(visit_end_date AS DATE) IS NOT NULL
        AND IFNULL(TRY_CAST(person_id AS BIGINT), -1) != -1
),
adjusted_visits AS (
    SELECT
        *,
        -- Adjust visit_end_date to not exceed CURRENT_DATE
        LEAST(visit_end_date, '@CURRENT_DATE') AS adjusted_visit_end_date,
        -- Exclude visits that start after CURRENT_DATE
        CASE
            WHEN visit_start_date > '@CURRENT_DATE' THEN NULL
            ELSE visit_start_date
        END AS adjusted_visit_start_date,
        -- Extend the visit end date by 60 days without exceeding CURRENT_DATE
        LEAST(visit_end_date + INTERVAL '60 days', '@CURRENT_DATE') AS adjusted_visit_end_plus_60
    FROM valid_visits
),
filtered_visits AS (
    SELECT *
    FROM adjusted_visits
    WHERE adjusted_visit_start_date IS NOT NULL
),
ordered_visits AS (
    SELECT
        *,
        LAG(adjusted_visit_end_plus_60) OVER (
            PARTITION BY person_id
            ORDER BY adjusted_visit_start_date
        ) AS prev_adjusted_end_date
    FROM filtered_visits
),
visit_gaps AS (
    SELECT
        *,
        DATE_DIFF('day', prev_adjusted_end_date, adjusted_visit_start_date) AS gap_in_days
    FROM ordered_visits
),
island_flags AS (
    SELECT
        *,
        CASE
            WHEN gap_in_days > 548 OR prev_adjusted_end_date IS NULL THEN 1
            ELSE 0
        END AS new_island_flag
    FROM visit_gaps
),
island_groups AS (
    SELECT
        *,
        SUM(new_island_flag) OVER (
            PARTITION BY person_id
            ORDER BY adjusted_visit_start_date
            ROWS UNBOUNDED PRECEDING
        ) AS island_group
    FROM island_flags
),
groupedby AS (
    SELECT
        person_id,
        island_group,
        MIN(adjusted_visit_start_date) AS island_start_date,
        MAX(adjusted_visit_end_plus_60) AS island_end_date
    FROM island_groups
    GROUP BY person_id, island_group
)
SELECT DISTINCT
    generate_id(CONCAT('@SITE', person_id, island_start_date, island_end_date)) AS observation_period_id,
    person_id,
    island_start_date AS observation_period_start_date,
    island_end_date AS observation_period_end_date,
    32882 AS period_type_concept_id
FROM groupedby