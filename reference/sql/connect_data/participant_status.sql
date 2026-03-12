WITH status_map AS (
  -- Connect concept_id's are stored as strings in source table, so using string literals here for the join
	SELECT '197316935' AS concept_id, 'Verified' AS concept_name UNION ALL
	SELECT '875007964' AS concept_id, 'Not Yet Verified' AS concept_name UNION ALL
	SELECT '290379732' AS concept_id, 'No Longer Enrolling' AS concept_name UNION ALL
	SELECT '160161595' AS concept_id, 'Outreach timed Out' AS concept_name UNION ALL
	SELECT '922622075' AS concept_id, 'Duplicate' AS concept_name UNION ALL
	SELECT '219863910' AS concept_id, 'Cannot be Verified' AS concept_name UNION ALL
	SELECT '104430631' AS concept_id, 'No' AS concept_name UNION ALL
	SELECT '353358909' AS concept_id, 'Yes' AS concept_name
), statuses AS (
  SELECT DISTINCT
    Connect_ID,
    d_821247024,
    d_747006172,
    d_773707518,
    d_831041022
  FROM `@PROJECT_ID.@DATASET_ID.participants`
  WHERE d_827220437 = '@SITE_CONNECT_ID' -- filter by site's connect_id
), cleaned_datapoints AS (
  SELECT DISTINCT
    Connect_ID,
    CASE WHEN smap_vs.concept_name IS NULL THEN 'UNKNOWN' ELSE smap_vs.concept_name END AS verified_status,
    d_821247024 AS verified_status_concept_id,
    CASE WHEN smap_cw.concept_name IS NULL THEN 'UNKNOWN' ELSE smap_cw.concept_name END AS consent_withdrawn,
    d_747006172 AS consent_withdrawn_concept_id,
    CASE WHEN smap_hr.concept_name IS NULL THEN 'UNKNOWN' ELSE smap_hr.concept_name END AS hipaa_revoked,
    d_773707518 AS hipaa_revoked_concept_id,
    CASE WHEN smap_dd.concept_name IS NULL THEN 'UNKNOWN' ELSE smap_dd.concept_name END AS data_destruction_requested,
    d_831041022 AS data_destruction_requested_concept_id
  FROM statuses s
  LEFT JOIN status_map smap_vs ON s.d_821247024 = smap_vs.concept_id
  LEFT JOIN status_map smap_cw ON s.d_747006172 = smap_cw.concept_id
  LEFT JOIN status_map smap_hr ON s.d_773707518 = smap_hr.concept_id
  LEFT JOIN status_map smap_dd ON s.d_831041022 = smap_dd.concept_id
  WHERE Connect_ID IS NOT NULL
)
SELECT * FROM cleaned_datapoints
;