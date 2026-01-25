
            SELECT
                EXTRACT(YEAR FROM visit_start_date) as year,
                COUNT(*) as row_count
            FROM read_parquet('gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet')
            WHERE visit_start_date IS NOT NULL
              AND visit_start_date >= '1970-01-01'
              AND visit_start_date <= '2025-01-01'
            GROUP BY EXTRACT(YEAR FROM visit_start_date)
            ORDER BY year
