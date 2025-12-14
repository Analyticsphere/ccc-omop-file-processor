
            SELECT
                COALESCE(c.vocabulary_id, 'No matching concept') as vocabulary_id,
                COUNT(*) as record_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet') t
            LEFT JOIN read_parquet('gs://vocab-bucket/v5.0/optimized/concept.parquet') c
                ON t.condition_concept_id = c.concept_id
            GROUP BY c.vocabulary_id
            ORDER BY record_count DESC

