
    SELECT * FROM read_parquet('gs://bucket/2025-01-01/harmonized/*.parquet')
    JOIN read_parquet('gs://bucket/2025-01-01/harmonized/*.parquet') ON x = y
