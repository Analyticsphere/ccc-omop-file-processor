
        COPY (
            SELECT CAST(person_id AS VARCHAR) AS person_id, CAST(gender_concept_id AS VARCHAR) AS gender_concept_id, CAST(year_of_birth AS VARCHAR) AS year_of_birth, CAST(birth_datetime AS VARCHAR) AS birth_datetime
            FROM read_parquet('gs://synthea53/2025-01-01/person.parquet')
        )
        TO 'gs://synthea53/2025-01-01/artifacts/converted_files/person.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    