COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(birth_date AS VARCHAR), '%Y-%m-%d') AS DATE),
                        TRY_CAST(birth_date AS DATE),
                        CAST('1970-01-01' AS DATE)
                    ) AS birth_date
