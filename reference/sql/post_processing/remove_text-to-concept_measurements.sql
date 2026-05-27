-- Post-processing task: remove_text-to-concept_measurements
--
-- Removes rows from the measurement table in which text-to-concept_id
-- mapping was performed *EXCEPT FOR* a curated list of mappings which 
-- have been reviewed and found to be accurate

COPY (
    SELECT *
    FROM read_parquet('@MEASUREMENT')
    WHERE NOT (
        measurement_concept_id != 0
        AND measurement_source_concept_id = 0
        AND LENGTH(IFNULL(measurement_source_value, '')) > 5
        AND measurement_concept_id NOT IN (
            4020553, 44816618, 3661712, 36674488, 21490527, 3020891, 4140731, 4137519
        )
    )
) TO '@MEASUREMENT' (FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1);
