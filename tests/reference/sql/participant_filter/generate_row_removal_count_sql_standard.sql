WITH source_rows AS (
    SELECT
        TRY_CAST(person_id AS BIGINT) AS normalized_person_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet')
),
known_connect_ids AS (
    SELECT DISTINCT
        TRY_CAST(Connect_ID AS BIGINT) AS connect_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet')
    WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
),
excluded_connect_ids AS (
    SELECT DISTINCT
        TRY_CAST(Connect_ID AS BIGINT) AS connect_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet')
    WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
      AND (
          TRY_CAST(verified_status_concept_id AS BIGINT) != 197316935
          OR TRY_CAST(consent_withdrawn_concept_id AS BIGINT) = 353358909
          OR TRY_CAST(hipaa_revoked_concept_id AS BIGINT) = 353358909
          OR TRY_CAST(data_destruction_requested_concept_id AS BIGINT) = 353358909
      )
),
classified_rows AS (
    SELECT
        CASE
            WHEN src.normalized_person_id IS NULL OR src.normalized_person_id = -1
                THEN 'unknown_identifier'
            WHEN known.connect_id IS NULL
                THEN 'identifier_not_in_connect'
            WHEN excluded.connect_id IS NOT NULL
                THEN 'connect_exclusion_rules'
            ELSE 'retained'
        END AS removal_bucket
    FROM source_rows src
    LEFT JOIN known_connect_ids known
        ON src.normalized_person_id = known.connect_id
    LEFT JOIN excluded_connect_ids excluded
        ON src.normalized_person_id = excluded.connect_id
)
SELECT
    COALESCE(SUM(CASE WHEN removal_bucket = 'unknown_identifier' THEN 1 ELSE 0 END), 0)
        AS unknown_identifier_count,
    COALESCE(SUM(CASE WHEN removal_bucket = 'identifier_not_in_connect' THEN 1 ELSE 0 END), 0)
        AS identifier_not_in_connect_count,
    COALESCE(SUM(CASE WHEN removal_bucket = 'connect_exclusion_rules' THEN 1 ELSE 0 END), 0)
        AS connect_exclusion_rules_count
FROM classified_rows
