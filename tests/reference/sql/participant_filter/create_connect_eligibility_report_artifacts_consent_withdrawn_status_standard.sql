WITH person_delivery AS (
    SELECT DISTINCT
        TRY_CAST(person_id AS BIGINT) AS person_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet')
    WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
),
connect_data AS (
    SELECT DISTINCT
        TRY_CAST(Connect_ID AS BIGINT) AS connect_id,
        COALESCE(NULLIF(TRIM(CAST(verified_status AS VARCHAR)), ''), 'UNKNOWN') AS verified_status,
        TRY_CAST(verified_status_concept_id AS BIGINT) AS verified_status_concept_id,
        COALESCE(NULLIF(TRIM(CAST(consent_withdrawn AS VARCHAR)), ''), 'UNKNOWN') AS consent_withdrawn,
        TRY_CAST(consent_withdrawn_concept_id AS BIGINT) AS consent_withdrawn_concept_id,
        COALESCE(NULLIF(TRIM(CAST(hipaa_revoked AS VARCHAR)), ''), 'UNKNOWN') AS hipaa_revoked,
        TRY_CAST(hipaa_revoked_concept_id AS BIGINT) AS hipaa_revoked_concept_id,
        COALESCE(NULLIF(TRIM(CAST(data_destruction_requested AS VARCHAR)), ''), 'UNKNOWN') AS data_destruction_requested,
        TRY_CAST(data_destruction_requested_concept_id AS BIGINT) AS data_destruction_requested_concept_id
    FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet')
    WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
),
matched_patients AS (
    SELECT DISTINCT
        p.person_id,
        cd.connect_id,
        cd.verified_status,
        cd.verified_status_concept_id,
        cd.consent_withdrawn,
        cd.consent_withdrawn_concept_id,
        cd.hipaa_revoked,
        cd.hipaa_revoked_concept_id,
        cd.data_destruction_requested,
        cd.data_destruction_requested_concept_id
    FROM person_delivery p
    INNER JOIN connect_data cd
        ON p.person_id = cd.connect_id
)
SELECT
    consent_withdrawn AS status_value,
    consent_withdrawn_concept_id AS status_concept_id,
    COUNT(DISTINCT person_id) AS patient_count,
    COALESCE(STRING_AGG(CAST(connect_id AS VARCHAR), '|' ORDER BY connect_id), '') AS patient_ids
FROM matched_patients
GROUP BY 1, 2
ORDER BY 1, 2
