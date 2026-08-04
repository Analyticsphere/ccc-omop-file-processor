SELECT COUNT(*) AS row_count
FROM read_parquet('gs://ehr_merged/2026-06-24/artifacts/merge_chunks/*/*__siteA__2025-01-01.parquet', union_by_name=true)
