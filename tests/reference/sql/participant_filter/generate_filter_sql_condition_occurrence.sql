COPY (
    WITH filtered_source AS (
        SELECT *
        FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet')
        WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
          AND TRY_CAST(person_id AS BIGINT) != -1
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
    )
    SELECT src.*
    FROM filtered_source src
    INNER JOIN known_connect_ids known
        ON TRY_CAST(src.person_id AS BIGINT) = known.connect_id
    LEFT JOIN excluded_connect_ids excluded
        ON TRY_CAST(src.person_id AS BIGINT) = excluded.connect_id
    WHERE excluded.connect_id IS NULL
) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
