SELECT
            (SELECT COUNT(*) FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet') c
             WHERE c.person_id IS NOT NULL
               AND c.person_id NOT IN (SELECT person_id FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/person_pre.parquet'))) AS added,
            (SELECT COUNT(*) FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/person_pre.parquet') s
             WHERE s.person_id IS NOT NULL
               AND s.person_id NOT IN (SELECT person_id FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet'))) AS removed
