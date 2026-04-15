WITH table_ids AS (
    SELECT DISTINCT
        TRY_CAST(person_id AS BIGINT) AS person_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet')
    WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
      AND TRY_CAST(person_id AS BIGINT) != -1
),
connect_ids AS (
    SELECT DISTINCT
        TRY_CAST(Connect_ID AS BIGINT) AS connect_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet')
    WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
),
numeric_not_in_connect AS (
    SELECT CAST(t.person_id AS VARCHAR) AS unmatched_id
    FROM table_ids t
    LEFT JOIN connect_ids cd
        ON t.person_id = cd.connect_id
    WHERE cd.connect_id IS NULL
),
non_numeric_ids AS (
    SELECT DISTINCT CAST(person_id AS VARCHAR) AS unmatched_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/invalid_rows/condition_occurrence.parquet')
    WHERE person_id IS NOT NULL
      AND TRIM(CAST(person_id AS VARCHAR)) != ''
      AND TRY_CAST(person_id AS BIGINT) IS NULL
),
all_unmatched AS (
    SELECT unmatched_id FROM numeric_not_in_connect
    UNION
    SELECT unmatched_id FROM non_numeric_ids
)
SELECT
    COUNT(*) AS patient_count,
    COALESCE(STRING_AGG(unmatched_id, '|' ORDER BY unmatched_id), '') AS patient_ids
FROM all_unmatched
