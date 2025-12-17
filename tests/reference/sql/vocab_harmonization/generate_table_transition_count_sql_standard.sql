
            SELECT
                target_table,
                COUNT(*) as row_count
            FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet')
            GROUP BY target_table
            ORDER BY target_table
