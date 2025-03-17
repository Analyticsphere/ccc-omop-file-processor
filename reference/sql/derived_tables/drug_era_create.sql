CREATE OR REPLACE TEMP TABLE drug_exposure_optimized AS
    SELECT
        drug_exposure_id,
        person_id,
        drug_concept_id,
        drug_exposure_start_date,
        days_supply,
        drug_exposure_end_date
    FROM read_parquet('@DRUG_EXPOSURE')
    WHERE drug_concept_id != 0
    AND IFNULL(TRY_CAST(person_id AS BIGINT), -1) != -1
;


CREATE OR REPLACE TEMP TABLE concept_ancestor_optimized AS
    SELECT descendant_concept_id, ancestor_concept_id FROM read_parquet('@CONCEPT_ANCESTOR')
    WHERE descendant_concept_id IN (
        SELECT DISTINCT drug_concept_id FROM drug_exposure_optimized
    )
;

CREATE OR REPLACE TEMP TABLE concept_optimized AS
    SELECT concept_id, vocabulary_id, concept_class_id FROM read_parquet('@CONCEPT')
    WHERE concept_id IN (
        SELECT DISTINCT ancestor_concept_id FROM concept_ancestor_optimized
    )
;

CREATE OR REPLACE TABLE ctePreDrugTarget AS
    SELECT
        d.drug_exposure_id,
        d.person_id,
        c.concept_id AS ingredient_concept_id,
        d.drug_exposure_start_date,
        d.days_supply AS days_supply,
        COALESCE(
            d.drug_exposure_end_date,
            CASE 
                WHEN d.days_supply IS NOT NULL AND d.drug_exposure_start_date IS NOT NULL 
                THEN d.drug_exposure_start_date + d.days_supply * INTERVAL '1' DAY 
            END,
            d.drug_exposure_start_date + INTERVAL '1' DAY
        ) AS drug_exposure_end_date
    FROM drug_exposure_optimized d
    JOIN concept_ancestor_optimized ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    JOIN concept_optimized c 
        ON ca.ancestor_concept_id = c.concept_id
    WHERE c.vocabulary_id = 'RxNorm'
    AND c.concept_class_id = 'Ingredient'
;
