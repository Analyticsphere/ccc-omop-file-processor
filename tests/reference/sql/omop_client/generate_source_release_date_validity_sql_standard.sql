
            SELECT
                (TRY_CAST(TRY_STRPTIME(CAST(source_release_date AS VARCHAR), '%Y-%m-%d') AS DATE) IS NOT NULL)
                OR (TRY_CAST(source_release_date AS DATE) IS NOT NULL) AS is_valid
            FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/cdm_source.parquet')
            LIMIT 1
