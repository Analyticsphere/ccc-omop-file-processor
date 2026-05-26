
            COPY (
                SELECT * REPLACE (
                    COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(source_release_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(source_release_date AS DATE),
                        CAST('2025-01-15' AS DATE)
                    ) AS source_release_date
                )
                FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/converted_files/cdm_source.parquet')
            ) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/cdm_source.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
