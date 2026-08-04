
            COPY (
                SELECT * FROM read_parquet('gs://siteA/2025-01-01/artifacts/converted_files/measurement.parquet')
            ) TO 'gs://ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/measurement__siteA__2025-01-01.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
            