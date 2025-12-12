
        COPY (
            SELECT "domain_id", "domain_name", "domain_concept_id"
            FROM read_csv('gs://vocab_root/v5.0_24-JAN-25/DOMAIN.csv', delim='	',strict_mode=False)
        ) TO 'gs://vocab_root/v5.0_24-JAN-25/optimized/domain.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
    