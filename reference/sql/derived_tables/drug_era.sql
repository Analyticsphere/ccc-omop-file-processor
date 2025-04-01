SELECT
    generate_id(
        CONCAT(
            '@SITE', CAST(person_id AS STRING), CAST(drug_concept_id AS STRING), CAST(drug_era_start_date AS STRING), 
            CAST(drug_era_end_date AS STRING), CAST(drug_exposure_count AS STRING), CAST(gap_days AS STRING)
        )
    ) AS drug_era_id,
    person_id,
    drug_concept_id,
    CAST(drug_era_start_date AS DATE) AS drug_era_start_date,
    CAST(drug_era_end_date AS DATE) AS drug_era_end_date,
    drug_exposure_count,
    gap_days
FROM final_select