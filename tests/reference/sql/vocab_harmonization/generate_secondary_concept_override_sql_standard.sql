
            COPY (
                SELECT * REPLACE (
                    CASE
                        WHEN unit_concept_id = 0
                            AND unit_source_concept_id != 0
                            AND unit_source_concept_id IN (
                                SELECT DISTINCT concept_id FROM read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet')
                            )
                        THEN unit_source_concept_id
                        ELSE unit_concept_id
                    END AS unit_concept_id
                )
                FROM read_parquet('file:///data/synthea53/2025-01-01/artifacts/harmonized_files/measurement/*.parquet')
            ) TO 'synthea53/2025-01-01/artifacts/harmonized_files/measurement/measurement_secondary_concept_override.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
