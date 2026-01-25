COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST(visit_start_datetime AS VARCHAR), '%Y-%m-%d %H:%M:%S') AS DATETIME),
                        TRY_CAST(visit_start_datetime AS DATETIME),
                        CAST('1970-01-01 00:00:00' AS DATETIME)
                    ) AS visit_start_datetime
