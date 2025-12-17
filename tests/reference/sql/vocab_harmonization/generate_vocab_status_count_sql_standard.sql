
            SELECT
                vocab_harmonization_status,
                COUNT(*) as row_count
            FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet')
            GROUP BY vocab_harmonization_status
            ORDER BY vocab_harmonization_status
