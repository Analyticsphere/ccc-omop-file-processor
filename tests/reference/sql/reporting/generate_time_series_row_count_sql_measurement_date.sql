
            SELECT
                EXTRACT(YEAR FROM measurement_date) as year,
                COUNT(*) as row_count
            FROM read_parquet('gs://bucket/table.parquet')
            WHERE measurement_date IS NOT NULL
              AND measurement_date >= '1970-01-01'
              AND measurement_date <= '2025-01-01'
            GROUP BY EXTRACT(YEAR FROM measurement_date)
            ORDER BY year
        