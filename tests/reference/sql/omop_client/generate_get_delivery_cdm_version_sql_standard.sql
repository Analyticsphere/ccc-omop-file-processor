
            SELECT cdm_version, vocabulary_version
            FROM read_parquet('gs://siteA/2025-01-01/artifacts/converted_files/cdm_source.parquet')
            LIMIT 1
        