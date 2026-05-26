
            COPY (
                SELECT
                    'Test Site' AS cdm_source_name,
                    'test' AS cdm_source_abbreviation,
                    'NIH/NCI' AS cdm_holder,
                    'Test data' AS source_description,
                    '' AS source_documentation_reference,
                    '' AS cdm_etl_reference,
                    COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST('2024-12-31' AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST('2024-12-31' AS DATE),
                        CAST('2025-01-15' AS DATE)
                    ) AS source_release_date,
                    COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST('2024-12-15' AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST('2024-12-15' AS DATE),
                        CAST('2025-01-15' AS DATE)
                    ) AS cdm_release_date,
                    '5.4' AS cdm_version,
                    756265 AS cdm_version_concept_id,
                    'v5.0_24-JAN-25' AS vocabulary_version
            ) TO 'gs://test-bucket/cdm_source.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)

