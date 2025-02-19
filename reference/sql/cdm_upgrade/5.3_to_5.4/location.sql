SELECT 
    location_id,
    address_1,
    address_2,
    city,
    state,
    zip,
    county,
    location_source_value,
    CAST(NULL AS BIGINT) AS country_concept_id,
    CAST(NULL AS VARCHAR) AS country_source_value,
    CAST(NULL AS DOUBLE) AS latitude,
    CAST(NULL AS DOUBLE) AS longitude