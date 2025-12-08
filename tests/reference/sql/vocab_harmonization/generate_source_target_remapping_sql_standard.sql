
            COPY (
                
                WITH base AS (
                    SELECT
                        tbl.condition_occurrence_id,
                tbl.person_id,
                vocab.target_concept_id AS condition_concept_id,
                tbl.condition_start_date,
                tbl.condition_start_datetime,
                tbl.condition_end_date,
                tbl.condition_end_datetime,
                tbl.condition_type_concept_id,
                tbl.condition_status_concept_id,
                tbl.stop_reason,
                tbl.provider_id,
                tbl.visit_occurrence_id,
                tbl.visit_detail_id,
                tbl.condition_source_value,
                tbl.condition_source_concept_id,
                tbl.condition_status_source_value,
                vocab.target_concept_id_domain AS target_domain,
                'source_concept_id mapped to new target' AS vocab_harmonization_status,
                tbl.condition_source_concept_id AS source_concept_id,
                tbl.condition_concept_id AS previous_target_concept_id,
                vocab.target_concept_id AS target_concept_id
                    
                FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/condition_occurrence.parquet') AS tbl
                INNER JOIN read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet') AS vocab
                    ON tbl.condition_source_concept_id = vocab.concept_id
                WHERE tbl.condition_source_concept_id != 0
                AND tbl.condition_concept_id != vocab.target_concept_id
                AND vocab.relationship_id IN ('Maps to', 'Maps to value')
                AND vocab.target_concept_id_standard = 'S'
            
                ), meas_value AS (
                    
                -- Pivot so that Meas Value mappings get associated with target_concept_id_column
                SELECT
                    tbl.condition_occurrence_id,
                    MAX(vocab.target_concept_id) AS vh_value_as_concept_id
                FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/condition_occurrence.parquet') AS tbl
                INNER JOIN read_parquet('gs://vocabularies//v5.0_22-JAN-23/optimized/optimized_vocab_file.parquet') AS vocab
                    ON tbl.condition_source_concept_id = vocab.concept_id
                WHERE vocab.target_concept_id_domain = 'Meas Value'
                GROUP BY tbl.condition_occurrence_id
            
                )
                SELECT
                    tbl.condition_occurrence_id,
                tbl.person_id,
                tbl.condition_concept_id,
                tbl.condition_start_date,
                tbl.condition_start_datetime,
                tbl.condition_end_date,
                tbl.condition_end_datetime,
                tbl.condition_type_concept_id,
                tbl.condition_status_concept_id,
                tbl.stop_reason,
                tbl.provider_id,
                tbl.visit_occurrence_id,
                tbl.visit_detail_id,
                tbl.condition_source_value,
                tbl.condition_source_concept_id,
                tbl.condition_status_source_value,
                target_domain,
                vocab_harmonization_status,
                source_concept_id,
                previous_target_concept_id,
                target_concept_id,
                mv_cte.vh_value_as_concept_id,
                
                CASE
                    WHEN tbl.target_domain = 'Visit' THEN 'visit_occurrence'
                    WHEN tbl.target_domain = 'Condition' THEN 'condition_occurrence'
                    WHEN tbl.target_domain = 'Drug' THEN 'drug_exposure'
                    WHEN tbl.target_domain = 'Procedure' THEN 'procedure_occurrence'
                    WHEN tbl.target_domain = 'Device' THEN 'device_exposure'
                    WHEN tbl.target_domain = 'Measurement' THEN 'measurement'
                    WHEN tbl.target_domain = 'Observation' THEN 'observation'
                    WHEN tbl.target_domain = 'Note' THEN 'note'
                    WHEN tbl.target_domain = 'Specimen' THEN 'specimen'
                ELSE 'condition_occurrence' END AS target_table
            
                
                FROM base AS tbl
                LEFT JOIN meas_value AS mv_cte
                    ON tbl.condition_occurrence_id = mv_cte.condition_occurrence_id
                WHERE tbl.target_domain != 'Meas Value'
            
            
            ) TO 'synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_source_target_remap.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        