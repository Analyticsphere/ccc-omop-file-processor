
        COPY (
            SELECT "concept_id", "concept_name", "domain_id", "vocabulary_id", CAST(STRPTIME(CAST("valid_start_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_start_date", CAST(STRPTIME(CAST("valid_end_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_end_date", "invalid_reason"
            FROM read_csv('gs://vocab_root/v5.0_24-JAN-25/CONCEPT.csv', delim='	',strict_mode=False)
        ) TO 'gs://vocab_root/v5.0_24-JAN-25/optimized/concept.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
    