
            COPY (
                
                    WITH vocab_measurement_source_concept_id AS (
                        SELECT DISTINCT concept_id
                        FROM read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet')
                    ),
                    vocab_unit_source_concept_id AS (
                        SELECT DISTINCT concept_id
                        FROM read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet')
                    ),
                    domain_vocab AS (
                        SELECT DISTINCT concept_id, concept_id_domain
                        FROM read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet')
                    )
                    SELECT
                        tbl.measurement_id,
                tbl.person_id,
                CASE
                            WHEN tbl.measurement_concept_id = 0
                                AND tbl.measurement_source_concept_id != 0
                                AND vocab_measurement_source_concept_id.concept_id IS NOT NULL
                            THEN tbl.measurement_source_concept_id
                            ELSE tbl.measurement_concept_id
                        END AS measurement_concept_id,
                tbl.measurement_date,
                tbl.measurement_datetime,
                tbl.measurement_time,
                tbl.measurement_type_concept_id,
                tbl.operator_concept_id,
                tbl.value_as_number,
                tbl.value_as_concept_id,
                CASE
                            WHEN tbl.unit_concept_id = 0
                                AND tbl.unit_source_concept_id != 0
                                AND vocab_unit_source_concept_id.concept_id IS NOT NULL
                            THEN tbl.unit_source_concept_id
                            ELSE tbl.unit_concept_id
                        END AS unit_concept_id,
                tbl.range_low,
                tbl.range_high,
                tbl.provider_id,
                tbl.visit_occurrence_id,
                tbl.visit_detail_id,
                tbl.measurement_source_value,
                tbl.measurement_source_concept_id,
                tbl.unit_source_value,
                tbl.unit_source_concept_id,
                tbl.value_source_value,
                tbl.measurement_event_id,
                tbl.meas_event_field_concept_id,
                COALESCE(domain_vocab.concept_id_domain, 'Unknown') AS target_domain,
                'source_concept_id override' AS vocab_harmonization_status,
                tbl.measurement_source_concept_id AS source_concept_id,
                tbl.measurement_concept_id AS previous_target_concept_id,
                tbl.measurement_source_concept_id AS target_concept_id,
                CAST(NULL AS BIGINT) AS vh_value_as_concept_id,
                CASE
                    WHEN domain_vocab.concept_id_domain = 'Visit' THEN 'visit_occurrence'
                    WHEN domain_vocab.concept_id_domain = 'Condition' THEN 'condition_occurrence'
                    WHEN domain_vocab.concept_id_domain = 'Drug' THEN 'drug_exposure'
                    WHEN domain_vocab.concept_id_domain = 'Procedure' THEN 'procedure_occurrence'
                    WHEN domain_vocab.concept_id_domain = 'Device' THEN 'device_exposure'
                    WHEN domain_vocab.concept_id_domain = 'Measurement' THEN 'measurement'
                    WHEN domain_vocab.concept_id_domain = 'Observation' THEN 'observation'
                    WHEN domain_vocab.concept_id_domain = 'Note' THEN 'note'
                    WHEN domain_vocab.concept_id_domain = 'Specimen' THEN 'specimen'
                    WHEN domain_vocab.concept_id_domain IS NULL THEN 'measurement'
                ELSE 'measurement' END AS target_table
                    FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/measurement.parquet') AS tbl
                    LEFT JOIN vocab_measurement_source_concept_id
                        ON tbl.measurement_source_concept_id = vocab_measurement_source_concept_id.concept_id
                    LEFT JOIN vocab_unit_source_concept_id
                        ON tbl.unit_source_concept_id = vocab_unit_source_concept_id.concept_id
                    LEFT JOIN domain_vocab
                        ON tbl.measurement_source_concept_id = domain_vocab.concept_id
                    WHERE (
                        (tbl.measurement_concept_id = 0 AND tbl.measurement_source_concept_id != 0 AND vocab_measurement_source_concept_id.concept_id IS NOT NULL) OR
                        (tbl.unit_concept_id = 0 AND tbl.unit_source_concept_id != 0 AND vocab_unit_source_concept_id.concept_id IS NOT NULL)
                    )
                
            ) TO 'synthea53/2025-01-01/artifacts/harmonized/measurement_source_concept_override.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        