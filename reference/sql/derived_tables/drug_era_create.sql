CREATE OR REPLACE TEMP TABLE ctePreDrugTarget AS
    SELECT
        *
    FROM read_parquet('@DRUG_EXPOSURE')

