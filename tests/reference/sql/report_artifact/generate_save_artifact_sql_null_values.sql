COPY (
    SELECT
        CAST('987654321' AS INT) AS metadata_id,
        TRY_CAST('0' AS INT) AS metadata_concept_id,
        32880 AS metadata_type_concept_id,
        'Invalid table name: foo' AS name,
        CAST(NULL AS VARCHAR) AS value_as_string,
        TRY_CAST('0' AS INT) AS value_as_concept_id,
        TRY_CAST(NULL AS DOUBLE) AS value_as_number,
        TRY_CAST('2025-01-15' AS DATE) AS metadata_date,
        TRY_CAST('2025-01-15 12:34:56' AS DATETIME) AS metadata_datetime
) TO 'gs://test-bucket/2025-01-15/tmp/delivery_report_part_test-uuid.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
