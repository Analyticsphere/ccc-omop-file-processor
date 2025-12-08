CREATE OR REPLACE TABLE row_check AS
            SELECT
                TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS person_id,
                TRY_CAST(COALESCE(gender_concept_id, '0') AS BIGINT) AS gender_concept_id,
                TRY_CAST(COALESCE(year_of_birth, '-1') AS BIGINT) AS year_of_birth,
                TRY_CAST(month_of_birth AS BIGINT) AS month_of_birth,
                TRY_CAST(day_of_birth AS BIGINT) AS day_of_birth,
                COALESCE(
                TRY_CAST(TRY_STRPTIME(birth_datetime, '%Y-%m-%d %H:%M:%S') AS DATETIME),
                TRY_CAST(birth_datetime AS DATETIME),
                TRY_CAST(
                CONCAT(
                    LPAD(COALESCE(year_of_birth, '1900'), 4, '0'), '-',
                    LPAD(COALESCE(month_of_birth, '1'), 2, '0'), '-',
                    LPAD(COALESCE(day_of_birth, '1'), 2, '0'),
                    ' 00:00:00'
                ) AS DATETIME)
            ) AS birth_datetime,
                TRY_CAST(COALESCE(race_concept_id, '0') AS BIGINT) AS race_concept_id,
                TRY_CAST(COALESCE(ethnicity_concept_id, '0') AS BIGINT) AS ethnicity_concept_id,
                TRY_CAST(location_id AS BIGINT) AS location_id,
                TRY_CAST(provider_id AS BIGINT) AS provider_id,
                TRY_CAST(care_site_id AS BIGINT) AS care_site_id,
                TRY_CAST(person_source_value AS VARCHAR) AS person_source_value,
                TRY_CAST(gender_source_value AS VARCHAR) AS gender_source_value,
                TRY_CAST(COALESCE(gender_source_concept_id, '0') AS BIGINT) AS gender_source_concept_id,
                TRY_CAST(race_source_value AS VARCHAR) AS race_source_value,
                TRY_CAST(COALESCE(race_source_concept_id, '0') AS BIGINT) AS race_source_concept_id,
                TRY_CAST(ethnicity_source_value AS VARCHAR) AS ethnicity_source_value,
                TRY_CAST(COALESCE(ethnicity_source_concept_id, '0') AS BIGINT) AS ethnicity_source_concept_id,
                CASE
                    WHEN COALESCE(CAST(TRY_CAST(COALESCE(person_id, '-1') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(gender_concept_id, '0') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(year_of_birth, '-1') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(race_concept_id, '0') AS BIGINT) AS VARCHAR), CAST(TRY_CAST(COALESCE(ethnicity_concept_id, '0') AS BIGINT) AS VARCHAR)) IS NULL THEN CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(gender_concept_id AS VARCHAR), ''), COALESCE(CAST(year_of_birth AS VARCHAR), ''), COALESCE(CAST(month_of_birth AS VARCHAR), ''), COALESCE(CAST(day_of_birth AS VARCHAR), ''), COALESCE(CAST(birth_datetime AS VARCHAR), ''), COALESCE(CAST(race_concept_id AS VARCHAR), ''), COALESCE(CAST(ethnicity_concept_id AS VARCHAR), ''), COALESCE(CAST(location_id AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(care_site_id AS VARCHAR), ''), COALESCE(CAST(person_source_value AS VARCHAR), ''), COALESCE(CAST(gender_source_value AS VARCHAR), ''), COALESCE(CAST(gender_source_concept_id AS VARCHAR), ''), COALESCE(CAST(race_source_value AS VARCHAR), ''), COALESCE(CAST(race_source_concept_id AS VARCHAR), ''), COALESCE(CAST(ethnicity_source_value AS VARCHAR), ''), COALESCE(CAST(ethnicity_source_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://test-bucket/2025-01-01/person.parquet')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://test-bucket/2025-01-01/person.parquet')
            WHERE CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(gender_concept_id AS VARCHAR), ''), COALESCE(CAST(year_of_birth AS VARCHAR), ''), COALESCE(CAST(month_of_birth AS VARCHAR), ''), COALESCE(CAST(day_of_birth AS VARCHAR), ''), COALESCE(CAST(birth_datetime AS VARCHAR), ''), COALESCE(CAST(race_concept_id AS VARCHAR), ''), COALESCE(CAST(ethnicity_concept_id AS VARCHAR), ''), COALESCE(CAST(location_id AS VARCHAR), ''), COALESCE(CAST(provider_id AS VARCHAR), ''), COALESCE(CAST(care_site_id AS VARCHAR), ''), COALESCE(CAST(person_source_value AS VARCHAR), ''), COALESCE(CAST(gender_source_value AS VARCHAR), ''), COALESCE(CAST(gender_source_concept_id AS VARCHAR), ''), COALESCE(CAST(race_source_value AS VARCHAR), ''), COALESCE(CAST(race_source_concept_id AS VARCHAR), ''), COALESCE(CAST(ethnicity_source_value AS VARCHAR), ''), COALESCE(CAST(ethnicity_source_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://test-bucket/2025-01-01/artifacts/invalid_rows/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) 
            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://test-bucket/2025-01-01/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        ;