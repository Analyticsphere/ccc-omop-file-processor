
            WITH row_analysis AS (
                SELECT
                    measurement_id as primary_key,
                    COUNT(CASE WHEN target_table = 'measurement' THEN 1 END) as same_table_count,
                    COUNT(CASE WHEN target_table != 'measurement' THEN 1 END) as cross_table_count
                FROM read_parquet('gs://synthea53/2025-01-01/artifacts/harmonized/measurement/*.parquet')
                GROUP BY measurement_id
            )
            SELECT
                CASE
                    WHEN same_table_count > 0 AND cross_table_count = 0 THEN 'stayed only'
                    WHEN same_table_count > 0 AND cross_table_count > 0 THEN 'stayed and copied'
                    WHEN same_table_count = 0 AND cross_table_count > 0 THEN 'moved to other tables'
                END as disposition,
                COUNT(*) as row_count
            FROM row_analysis
            GROUP BY disposition
            ORDER BY disposition
        