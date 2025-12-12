
        COPY (
            
        SELECT DISTINCT
            m.measurement_id,
            m.person_id,
            m.measurement_concept_id,
            COALESCE(m.value_as_number, 0) as value_as_number
        WHERE m.measurement_concept_id IS NOT NULL
    
            FROM read_parquet('gs://synthea53/2025-01-01/artifacts/converted_files/measurement.parquet')
        ) TO 'gs://synthea53/2025-01-01/artifacts/converted_files/measurement.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
    