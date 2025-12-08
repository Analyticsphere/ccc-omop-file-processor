
        COPY (
            SELECT "concept_id_1", "concept_id_2", "relationship_id", CAST(STRPTIME(CAST("valid_start_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_start_date", CAST(STRPTIME(CAST("valid_end_date" AS VARCHAR), '%Y%m%d') AS DATE) AS "valid_end_date"
            FROM read_csv('gs://vocab_root/v5.0_24-JAN-25/CONCEPT_RELATIONSHIP.csv', delim='	',strict_mode=False)
        ) TO 'gs://vocab_root/v5.0_24-JAN-25/optimized/concept_relationship.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
    