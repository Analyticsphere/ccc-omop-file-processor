
                COPY (
                    SELECT DISTINCT
                        c1.concept_id AS concept_id, -- Every concept_id from concept table
                        c1.standard_concept AS concept_id_standard,
                        c1.domain_id AS concept_id_domain,
                        cr.relationship_id,
                        cr.concept_id_2 AS target_concept_id, -- targets to concept_id's
                        c2.standard_concept AS target_concept_id_standard,
                        c2.domain_id AS target_concept_id_domain
                    FROM read_parquet('gs://vocab-bucket/v5.0_24-JAN-25/optimized/concept.parquet') c1
                    LEFT JOIN (
                        SELECT * FROM read_parquet('gs://vocab-bucket/v5.0_24-JAN-25/optimized/concept_relationship.parquet')
                        WHERE relationship_id IN ('Maps to','Maps to value','Maps to unit','Concept replaced by','Concept was_a to','Concept poss_eq to','Concept same_as to','Concept alt_to to')
                    ) cr on c1.concept_id = cr.concept_id_1
                    LEFT JOIN read_parquet('gs://vocab-bucket/v5.0_24-JAN-25/optimized/concept.parquet') c2 on cr.concept_id_2 = c2.concept_id
                ) TO 'gs://vocab-bucket/v5.0_24-JAN-25/optimized/optimized_vocab_file.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)

