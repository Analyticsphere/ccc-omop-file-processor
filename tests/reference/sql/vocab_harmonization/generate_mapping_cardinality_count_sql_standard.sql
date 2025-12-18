
            SELECT
                cardinality_lookup.num_same_table_targets,
                COUNT(*) as num_source_rows
            FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet') as main_data
            INNER JOIN (
                SELECT
                    previous_target_concept_id,
                    COUNT(DISTINCT target_concept_id) as num_same_table_targets
                FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                WHERE target_table = 'measurement'
                GROUP BY previous_target_concept_id
            ) as cardinality_lookup
            ON main_data.previous_target_concept_id = cardinality_lookup.previous_target_concept_id
            WHERE main_data.target_table = 'measurement'
            GROUP BY cardinality_lookup.num_same_table_targets
            ORDER BY cardinality_lookup.num_same_table_targets
