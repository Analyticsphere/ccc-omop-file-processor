
COPY (
SELECT
'Test Site Medical Center' AS cdm_source_name,
'test_site' AS cdm_source_abbreviation,
'NIH/NCI Connect for Cancer Prevention Study' AS cdm_holder,
'Electronic Health Record (EHR) data from test_site' AS source_description,
'https://example.com/docs' AS source_documentation_reference,
'https://github.com/example/etl' AS cdm_etl_reference,
COALESCE(
TRY_CAST(TRY_STRPTIME(CAST('December 31, 2024' AS VARCHAR), '%Y-%m-%d') AS DATE),
TRY_CAST('December 31, 2024' AS DATE),
CAST('2025-01-15' AS DATE)
) AS source_release_date,
COALESCE(
TRY_CAST(TRY_STRPTIME(CAST('December 15, 2024' AS VARCHAR), '%Y-%m-%d') AS DATE),
TRY_CAST('December 15, 2024' AS DATE),
CAST('2025-01-15' AS DATE)
) AS cdm_release_date,
'5.4' AS cdm_version,
756265 AS cdm_version_concept_id,
'v5.0_24-JAN-25' AS vocabulary_version
) TO 'gs://test-bucket/2025-01-15/artifacts/converted_files/cdm_source.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
