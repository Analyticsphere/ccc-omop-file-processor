
        COPY (
            SELECT "concept_id", "concept_name", "domain_id"
            FROM read_csv('gs://vocab-bucket/vocab/v5.0/CONCEPT.csv', delim='	',strict_mode=False)
        ) TO 'gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
        