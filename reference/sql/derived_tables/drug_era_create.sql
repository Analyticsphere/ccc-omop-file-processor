CREATE OR REPLACE TEMP TABLE ctePreDrugTarget AS
    SELECT
        d.*
    FROM read_parquet('@DRUG_EXPOSURE') d
    INNER JOIN read_parquet('@CONCEPT_ANCESTOR') ca 
        ON ca.descendant_concept_id = d.drug_concept_id
    INNER JOIN read_parquet('@CONCEPT') c 
        ON ca.ancestor_concept_id = c.concept_id
;