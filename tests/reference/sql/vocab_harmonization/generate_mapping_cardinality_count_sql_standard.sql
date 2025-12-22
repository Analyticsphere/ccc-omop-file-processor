
            SELECT
                pk_cardinality.num_same_table_targets,
                SUM(pk_cardinality.pk_row_count) as num_source_rows
            FROM (
                SELECT
                    measurement_id as primary_key,
                    COUNT(DISTINCT target_concept_id) as num_same_table_targets,
                    COUNT(*) as pk_row_count
                FROM read_parquet('synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                WHERE target_table = 'measurement'
                GROUP BY measurement_id
            ) as pk_cardinality
            GROUP BY pk_cardinality.num_same_table_targets
            ORDER BY pk_cardinality.num_same_table_targets
