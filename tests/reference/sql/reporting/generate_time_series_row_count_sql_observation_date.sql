
            SELECT
                EXTRACT(YEAR FROM observation_date) as year,
                COUNT(*) as row_count
            FROM read_parquet('gs://bucket/table.parquet')
            WHERE observation_date IS NOT NULL
              AND observation_date >= '1970-01-01'
              AND observation_date <= '2025-12-31'
            GROUP BY EXTRACT(YEAR FROM observation_date)
            ORDER BY year
        