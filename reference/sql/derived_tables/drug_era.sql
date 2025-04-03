SELECT
    hash(
        CONCAT(
            '@SITE', CAST(person_id AS STRING), CAST(drug_concept_id AS STRING), CAST(drug_era_start_date AS STRING), 
            CAST(drug_era_end_date AS STRING), CAST(drug_exposure_count AS STRING), CAST(gap_days AS STRING)
        )
    ) % 9223372036854775807 AS drug_era_id,
    person_id,
    drug_concept_id,
    drug_era_start_date,
    drug_era_end_date,
    drug_exposure_count,
    gap_days
FROM final_select