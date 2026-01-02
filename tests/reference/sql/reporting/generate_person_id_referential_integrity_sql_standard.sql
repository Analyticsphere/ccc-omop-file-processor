
            SELECT COUNT(*) as violation_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet') t
            LEFT JOIN read_parquet('gs://test-bucket/2025-01-01/artifacts/converted_files/person.parquet') p
                ON t.person_id = p.person_id
            WHERE t.person_id IS NOT NULL
              AND p.person_id IS NULL
