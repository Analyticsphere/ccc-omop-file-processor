-- Post-processing task: remove_test_patients
--
-- Removes person rows whose person_source_value starts with the literal
-- prefix 'TEST_'. All other tables are left as-is. Child rows that reference
-- these persons (visit_occurrence, condition_occurrence, ...) remain in the
-- delivery; the post-processing diff reports orphaned references per affected
-- child table.
--
-- This is a delete-only example. For tasks that insert rows into a
-- surrogate-key table, the user-authored SQL must construct the primary key
-- using hash(CONCAT(<all non-PK columns as VARCHAR>, '@SITE')) % 9223372036854775807.

COPY (
    SELECT *
    FROM read_parquet('@PERSON')
    WHERE person_source_value IS NULL
       OR NOT starts_with(person_source_value, 'TEST_')
) TO '@PERSON' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
