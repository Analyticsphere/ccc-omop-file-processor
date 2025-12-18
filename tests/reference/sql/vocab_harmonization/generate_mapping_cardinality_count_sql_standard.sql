
            SELECT
                num_targets,
                COUNT(*) as num_mappings
            FROM (
                SELECT
                    previous_target_concept_id,
                    COUNT(DISTINCT target_concept_id) as num_targets
                FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                GROUP BY previous_target_concept_id
            )
            GROUP BY num_targets
            ORDER BY num_targets
