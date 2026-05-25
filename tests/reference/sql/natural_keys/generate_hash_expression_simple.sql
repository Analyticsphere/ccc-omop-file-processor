
            CASE WHEN visit_occurrence_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(visit_occurrence_id AS VARCHAR), 'site_alpha')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS visit_occurrence_id
        
