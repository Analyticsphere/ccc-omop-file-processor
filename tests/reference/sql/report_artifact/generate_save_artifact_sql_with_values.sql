COPY (
    SELECT
        CAST('123456789' AS INT) AS metadata_id,
        TRY_CAST('1147330' AS INT) AS metadata_concept_id,
        32880 AS metadata_type_concept_id,
        'Final row count: measurement' AS name,
        'measurement' AS value_as_string,
        TRY_CAST('1147330' AS INT) AS value_as_concept_id,
        TRY_CAST('98159833.0' AS DOUBLE) AS value_as_number,
        TRY_CAST('2025-01-15' AS DATE) AS metadata_date,
        TRY_CAST('2025-01-15 12:34:56' AS DATETIME) AS metadata_datetime
) TO 'gs://test-bucket/2025-01-15/tmp/delivery_report_part_test-uuid.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
