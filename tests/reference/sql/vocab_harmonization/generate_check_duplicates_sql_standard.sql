
            SELECT
                condition_occurrence_id,
                COUNT(*) as dup_count
            FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')
            GROUP BY condition_occurrence_id
            HAVING COUNT(*) > 1
            LIMIT 1
