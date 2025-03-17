CREATE OR REPLACE TEMP TABLE drug_exposure_optimized AS
    SELECT
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        days_supply,
        drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE')
;


CREATE OR REPLACE TEMP TABLE concept_ancestor_optimized AS
    SELECT * FROM read_parquet('@CONCEPT_ANCESTOR')
    WHERE descendant_concept_id IN (
        SELECT DISTINCT drug_concept_id FROM drug_exposure_optimized
    )
;

CREATE OR REPLACE TEMP TABLE concept_optimized AS
    SELECT * FROM read_parquet('@CONCEPT')
    WHERE concept_id IN (
        SELECT DISTINCT ancestor_concept_id FROM concept_ancestor_optimized
    )
;

CREATE OR REPLACE TEMP TABLE ctePreDrugTarget AS
    SELECT
        d.*
    FROM drug_exposure_optimized d
    INNER JOIN concept_ancestor_optimized ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    INNER JOIN concept_optimized c 
        ON ca.ancestor_concept_id = c.concept_id
;