SELECT COUNT(*)
        FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet') c
        WHERE c.person_id IS NOT NULL
          AND c.person_id IN (
              SELECT s.person_id
              FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/post_processing/remove_test_patients/tmp/person_pre.parquet') s
              WHERE s.person_id NOT IN (
                  SELECT person_id FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet')
              )
          )
