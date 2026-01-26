
            SELECT COUNT(*) as default_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet')
            WHERE visit_start_datetime = '1970-01-01 00:00:00'
