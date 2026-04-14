
        COPY (
            SELECT "concept_id", CAST(STRPTIME(CAST("valid_start_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_start_date", CAST(STRPTIME(CAST("valid_end_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_end_date"
            FROM read_csv('gs://vocab-bucket/vocab/v5.0/CONCEPT.csv', delim='	',strict_mode=False)
        ) TO 'gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
        