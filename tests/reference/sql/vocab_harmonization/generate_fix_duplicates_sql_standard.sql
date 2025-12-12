
            COPY (
                SELECT
                    CASE
                        WHEN row_num = 1 THEN condition_occurrence_id
                        ELSE CAST(hash(CONCAT(CAST(condition_occurrence_id AS VARCHAR), CAST(row_num AS VARCHAR))) % 9223372036854775807 AS BIGINT)
                    END AS condition_occurrence_id,
                    * EXCLUDE (condition_occurrence_id, row_num)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY condition_occurrence_id ORDER BY (SELECT 1)) as row_num
                    FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')
                    WHERE condition_occurrence_id IN (SELECT condition_occurrence_id FROM duplicate_keys)
                )
            ) TO 'gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_dup_fixed_abc123.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
