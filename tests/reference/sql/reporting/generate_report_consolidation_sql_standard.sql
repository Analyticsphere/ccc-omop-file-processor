
        SET max_expression_depth TO 1000000;

        COPY (
            SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file1.parquet') UNION ALL SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file2.parquet') UNION ALL SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file3.parquet')
        ) TO 'gs://bucket/2025-01-01/report/delivery_report_site1_2025-01-01.csv' (HEADER, DELIMITER ',');
