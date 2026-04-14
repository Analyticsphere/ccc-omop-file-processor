
            SET max_expression_depth TO 1000000;

            COPY (
                SELECT * FROM read_parquet('s3://test-bucket/2025-01-15/artifacts/delivery_report/tmp/file1.parquet') UNION ALL SELECT * FROM read_parquet('s3://test-bucket/2025-01-15/artifacts/delivery_report/tmp/file2.parquet') UNION ALL SELECT * FROM read_parquet('s3://test-bucket/2025-01-15/artifacts/delivery_report/tmp/file3.parquet')
            ) TO 's3://test-bucket/2025-01-15/artifacts/delivery_report/delivery_report_test_site_2025-01-15.csv' (HEADER, DELIMITER ',');
        