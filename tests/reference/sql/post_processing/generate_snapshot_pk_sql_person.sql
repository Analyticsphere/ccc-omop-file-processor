COPY (
            SELECT person_id
            FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet')
        ) TO 'gs://test-bucket/2025-01-15/artifacts/post_processing/remove_test_patients/tmp/person_pre.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
