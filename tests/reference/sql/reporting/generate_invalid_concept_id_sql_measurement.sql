
            SELECT COUNT(*) as invalid_count
            FROM read_parquet('gs://test-bucket/table.parquet') t
            LEFT JOIN read_parquet('gs://vocab-bucket/concept.parquet') c
                ON t.measurement_concept_id = c.concept_id
            WHERE t.measurement_concept_id IS NOT NULL
              AND t.measurement_concept_id != 0
              AND c.concept_id IS NULL
        