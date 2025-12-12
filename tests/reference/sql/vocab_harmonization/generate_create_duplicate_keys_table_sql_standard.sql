
            CREATE TEMP TABLE duplicate_keys AS
            SELECT condition_occurrence_id
            FROM read_parquet('gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')
            GROUP BY condition_occurrence_id
            HAVING COUNT(*) > 1
