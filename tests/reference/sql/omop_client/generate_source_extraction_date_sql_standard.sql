
            SELECT CAST(
                COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(source_release_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(source_release_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS VARCHAR
            ) AS extraction_date
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/converted_files/cdm_source.parquet')
            LIMIT 1
