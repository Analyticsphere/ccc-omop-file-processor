SELECT
            (SELECT COUNT(*) FROM (
                SELECT hash(CONCAT(CAST(person_id AS VARCHAR), CAST(death_date AS VARCHAR), CAST(death_datetime AS VARCHAR), CAST(death_type_concept_id AS VARCHAR), CAST(cause_concept_id AS VARCHAR))) AS row_hash FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/death.parquet')
             ) c WHERE c.row_hash NOT IN (SELECT row_hash FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/death_pre.parquet'))) AS added,
            (SELECT COUNT(*) FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/death_pre.parquet') s
             WHERE s.row_hash NOT IN (
                SELECT hash(CONCAT(CAST(person_id AS VARCHAR), CAST(death_date AS VARCHAR), CAST(death_datetime AS VARCHAR), CAST(death_type_concept_id AS VARCHAR), CAST(cause_concept_id AS VARCHAR))) FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/death.parquet')
             )) AS removed
