
            REPLACE(CAST((CAST(hash(CONCAT(COALESCE(CAST(person_id AS VARCHAR), ''), COALESCE(CAST(condition_concept_id AS VARCHAR), ''))) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS condition_occurrence_id)
