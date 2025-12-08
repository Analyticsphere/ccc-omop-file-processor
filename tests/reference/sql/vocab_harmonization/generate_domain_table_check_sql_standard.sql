
                WITH vocab AS (
                    SELECT DISTINCT
                        concept_id,
                        concept_id_domain
                    FROM read_parquet('@OPTIMIZED_VOCABULARY')
                )
                SELECT tbl.condition_occurrence_id,
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
                vocab.concept_id_domain AS target_domain,
                'domain check' AS vocab_harmonization_status,
                tbl.condition_source_concept_id AS source_concept_id,
                tbl.condition_concept_id AS previous_target_concept_id,
                tbl.condition_concept_id AS target_concept_id,
                CAST(NULL AS BIGINT) AS vh_value_as_concept_id,
                
            CASE
                WHEN vocab.concept_id_domain = 'Visit' THEN 'visit_occurrence'
                WHEN vocab.concept_id_domain = 'Condition' THEN 'condition_occurrence'
                WHEN vocab.concept_id_domain = 'Drug' THEN 'drug_exposure'
                WHEN vocab.concept_id_domain = 'Procedure' THEN 'procedure_occurrence'
                WHEN vocab.concept_id_domain = 'Device' THEN 'device_exposure'
                WHEN vocab.concept_id_domain = 'Measurement' THEN 'measurement'
                WHEN vocab.concept_id_domain = 'Observation' THEN 'observation'
                WHEN vocab.concept_id_domain = 'Note' THEN 'note'
                WHEN vocab.concept_id_domain = 'Specimen' THEN 'specimen'
            ELSE 'condition_occurrence' END AS target_table
        
                
            FROM read_parquet('@CONDITION_OCCURRENCE') AS tbl
            INNER JOIN vocab
                ON tbl.condition_concept_id = vocab.concept_id
        
                
        
