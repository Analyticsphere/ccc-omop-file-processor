
            SELECT COUNT(*) as invalid_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet') t
            LEFT JOIN read_parquet('gs://vocab-bucket/v5.0/optimized/concept.parquet') c
                ON t.visit_concept_id = c.concept_id
            WHERE t.visit_concept_id IS NOT NULL
              AND t.visit_concept_id != 0
              AND c.concept_id IS NULL
