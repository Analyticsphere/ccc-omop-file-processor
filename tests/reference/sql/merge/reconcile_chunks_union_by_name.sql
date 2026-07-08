
        COPY (
            SELECT * FROM read_parquet('gs://ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/*.parquet', union_by_name=true)
        ) TO 'gs://ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)
        