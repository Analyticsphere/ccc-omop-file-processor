
            CASE WHEN provider_id IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST(provider_id AS VARCHAR), 'my-site')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS provider_id
        
