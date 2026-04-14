COALESCE(
                TRY_CAST(TRY_STRPTIME(birth_datetime, '%Y-%m-%d %H:%M:%S') AS DATETIME),
                TRY_CAST(birth_datetime AS DATETIME),
                TRY_CAST(
                CONCAT(
                    LPAD(COALESCE(year_of_birth, '1900'), 4, '0'), '-',
                    LPAD(COALESCE(month_of_birth, '1'), 2, '0'), '-',
                    LPAD(COALESCE(day_of_birth, '1'), 2, '0'),
                    ' 00:00:00'
                ) AS DATETIME)
            ) AS birth_datetime