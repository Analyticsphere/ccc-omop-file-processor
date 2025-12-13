
            SELECT
                COALESCE(t.visit_type_concept_id, 0) as type_concept_id,
                COALESCE(c.concept_name, 'No matching concept') as concept_name,
                COUNT(*) as record_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet') t
            LEFT JOIN read_parquet('gs://vocab-bucket/v5.0/optimized/concept.parquet') c
                ON t.visit_type_concept_id = c.concept_id
            GROUP BY t.visit_type_concept_id, c.concept_name
            ORDER BY record_count DESC

