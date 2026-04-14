"""
Utility script to generate golden SQL reference files for tests that currently
use string-containment checks. Run from project root directory.

This script calls the actual SQL generation functions and writes the output
to the reference SQL directories used by the golden file tests.
"""

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure we can import from the project
sys.path.insert(0, os.path.dirname(__file__))

from core.file_processor import FileProcessor
from core.normalization import Normalizer
from core.omop_client import OMOPClient
from core.vocab_manager import VocabularyManager

REFERENCE_BASE = Path(__file__).parent / "tests" / "reference" / "sql"


def write_sql(subdir: str, filename: str, sql: str):
    """Write SQL to a reference file."""
    directory = REFERENCE_BASE / subdir
    directory.mkdir(parents=True, exist_ok=True)
    filepath = directory / filename
    with open(filepath, 'w') as f:
        f.write(sql)
    print(f"  Created: {filepath.relative_to(Path(__file__).parent)}")


def generate_file_processor_golden_files():
    """Generate golden SQL files for test_file_processor.py string-in-SQL tests."""
    print("\n=== FileProcessor golden SQL files ===")

    # 1. generate_process_incoming_parquet_sql - standard columns
    sql = FileProcessor.generate_process_incoming_parquet_sql(
        file_path="bucket/2025-01-01/person.parquet",
        parquet_columns=["person_id", "gender_concept_id", "year_of_birth"]
    )
    write_sql("file_processor", "generate_process_incoming_parquet_sql_standard_columns_v2.sql", sql)

    # 2. generate_process_incoming_parquet_sql - offset column
    sql = FileProcessor.generate_process_incoming_parquet_sql(
        file_path="bucket/2025-01-01/note_nlp.parquet",
        parquet_columns=["note_nlp_id", "offset", "snippet"]
    )
    write_sql("file_processor", "generate_process_incoming_parquet_sql_offset_column.sql", sql)

    # 3. generate_csv_to_parquet_sql - no options
    sql = FileProcessor.generate_csv_to_parquet_sql(
        file_path="bucket/2025-01-01/person.csv",
        csv_column_names=["person_id", "gender_concept_id"],
        conversion_options=[]
    )
    write_sql("file_processor", "generate_csv_to_parquet_sql_no_options.sql", sql)

    # 4. generate_csv_to_parquet_sql - with options
    sql = FileProcessor.generate_csv_to_parquet_sql(
        file_path="bucket/2025-01-01/person.csv",
        csv_column_names=["person_id", "gender_concept_id"],
        conversion_options=['store_rejects=True', 'ignore_errors=True']
    )
    write_sql("file_processor", "generate_csv_to_parquet_sql_with_string_options.sql", sql)

    # 5. convert_parquet_string_nulls_to_null - standard columns (person_id, row_count)
    with patch('core.file_processor.utils.get_columns_from_file') as mock_get_columns, \
         patch('core.file_processor.utils.execute_duckdb_sql') as mock_execute:
        mock_get_columns.return_value = ['person_id', 'row_count']
        FileProcessor.convert_parquet_string_nulls_to_null(
            "bucket/2025-01-01/artifacts/converted_files/person.parquet"
        )
        sql = mock_execute.call_args[0][0]
        write_sql("file_processor", "convert_parquet_string_nulls_to_null_standard.sql", sql)

    # 6. convert_parquet_string_nulls_to_null - offset column
    with patch('core.file_processor.utils.get_columns_from_file') as mock_get_columns, \
         patch('core.file_processor.utils.execute_duckdb_sql') as mock_execute:
        mock_get_columns.return_value = ['note_nlp_id', 'offset', 'lexical_variant']
        FileProcessor.convert_parquet_string_nulls_to_null(
            "bucket/2025-01-01/artifacts/converted_files/note_nlp.parquet"
        )
        sql = mock_execute.call_args[0][0]
        write_sql("file_processor", "convert_parquet_string_nulls_to_null_offset_column.sql", sql)

    # 7. generate_csv_to_parquet_sql - with conversion_options=['parallel=False']
    # Note: _process_csv prepends encoding to options, so include it here
    sql = FileProcessor.generate_csv_to_parquet_sql(
        file_path="bucket/2025-01-01/person.csv",
        csv_column_names=["person_id", "gender_concept_id"],
        conversion_options=["encoding='utf-8'", 'parallel=False']
    )
    write_sql("file_processor", "generate_csv_to_parquet_sql_with_parallel_false.sql", sql)

    # 8. CSV retry SQL - the retry uses store_rejects=True, ignore_errors=True, parallel=False
    # as a single combined option string (see _process_csv method)
    sql = FileProcessor.generate_csv_to_parquet_sql(
        file_path="bucket/2025-01-01/person.csv",
        csv_column_names=["person_id", "gender_concept_id"],
        conversion_options=["encoding='utf-8'", "store_rejects=True, ignore_errors=True, parallel=False"]
    )
    write_sql("file_processor", "generate_csv_to_parquet_sql_retry.sql", sql)


def generate_normalization_golden_files():
    """Generate golden SQL files for test_normalization.py string-in-SQL tests."""
    print("\n=== Normalization golden SQL files ===")

    # 1. generate_normalization_sql - simple/basic (with mocks matching existing test)
    with patch('core.normalization.storage.get_uri') as mock_get_uri, \
         patch('core.normalization.utils.get_invalid_rows_path_from_path') as mock_invalid_path, \
         patch('core.normalization.utils.get_primary_key_column') as mock_get_pk:
        mock_get_pk.return_value = 'person_id'
        mock_invalid_path.return_value = 'bucket/2025-01-01/invalid_person.parquet'
        mock_get_uri.side_effect = lambda x: f"gs://{x}"

        schema = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'},
                    'gender_concept_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        actual_columns = ['person_id', 'gender_concept_id']

        sql = Normalizer.generate_normalization_sql(
            file_path="bucket/2025-01-01/person.parquet",
            table_name="person",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S",
            schema=schema,
            actual_columns=actual_columns
        )
        write_sql("normalization", "generate_normalization_sql_simple.sql", sql)

    # 2. generate_birth_datetime_sql_expression - with column
    sql = Normalizer.generate_birth_datetime_sql_expression(
        "%Y-%m-%d %H:%M:%S", column_exists_in_file=True
    )
    write_sql("normalization", "generate_birth_datetime_sql_expression_with_column.sql", sql)

    # 3. generate_birth_datetime_sql_expression - without column
    sql = Normalizer.generate_birth_datetime_sql_expression(
        "%Y-%m-%d %H:%M:%S", column_exists_in_file=False
    )
    write_sql("normalization", "generate_birth_datetime_sql_expression_without_column.sql", sql)

    # 4. generate_row_count_sql
    sql = Normalizer.generate_row_count_sql("gs://bucket/file.parquet")
    write_sql("normalization", "generate_row_count_sql_simple.sql", sql)


def generate_omop_client_golden_files():
    """Generate golden SQL files for test_omop_client.py string-in-SQL tests."""
    print("\n=== OMOPClient golden SQL files ===")

    # 1. generate_upgrade_file_sql
    sql = OMOPClient.generate_upgrade_file_sql(
        upgrade_script="SELECT * FROM table",
        normalized_file_path="bucket/2025-01-01/artifacts/converted_files/person.parquet"
    )
    write_sql("omop_client", "generate_upgrade_file_sql_simple.sql", sql)

    # 2. generate_populate_cdm_source_sql
    cdm_source_data = {
        "cdm_source_name": "Test Site",
        "cdm_source_abbreviation": "test",
        "cdm_holder": "NIH/NCI",
        "source_description": "Test data",
        "source_release_date": "2025-01-01",
        "cdm_release_date": "2025-01-15",
        "cdm_version": "5.4"
    }
    sql = OMOPClient.generate_populate_cdm_source_sql(
        cdm_source_data=cdm_source_data,
        vocab_version="v5.0_24-JAN-25",
        output_path="gs://test-bucket/cdm_source.parquet"
    )
    write_sql("omop_client", "generate_populate_cdm_source_sql_simple.sql", sql)


def generate_vocab_manager_golden_files():
    """Generate golden SQL files for test_vocab_manager.py string-in-SQL tests."""
    print("\n=== VocabularyManager golden SQL files ===")

    # 1. generate_vocab_version_query_sql
    sql = VocabularyManager.generate_vocab_version_query_sql(
        "gs://vocab-bucket/vocab/v5.0/optimized_vocab/vocabulary.parquet"
    )
    write_sql("vocab_manager", "generate_vocab_version_query_sql_standard.sql", sql)

    # 2. generate_convert_vocab_sql - standard columns
    sql = VocabularyManager.generate_convert_vocab_sql(
        csv_file_path="gs://vocab-bucket/vocab/v5.0/CONCEPT.csv",
        parquet_file_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
        csv_columns=["concept_id", "concept_name", "domain_id"]
    )
    write_sql("vocab_manager", "generate_convert_vocab_sql_standard_columns.sql", sql)

    # 3. generate_convert_vocab_sql - with date columns
    sql = VocabularyManager.generate_convert_vocab_sql(
        csv_file_path="gs://vocab-bucket/vocab/v5.0/CONCEPT.csv",
        parquet_file_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
        csv_columns=["concept_id", "valid_start_date", "valid_end_date"]
    )
    write_sql("vocab_manager", "generate_convert_vocab_sql_with_date_columns.sql", sql)

    # 4. generate_optimized_vocab_sql
    sql = VocabularyManager.generate_optimized_vocab_sql(
        concept_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
        concept_relationship_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept_relationship.parquet",
        output_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/optimized_vocab_file.parquet"
    )
    write_sql("vocab_manager", "generate_optimized_vocab_sql_standard.sql", sql)


def generate_transformer_golden_files():
    """Generate golden SQL files for test_transformer.py string-in-SQL tests."""
    print("\n=== Transformer golden SQL files ===")

    from io import StringIO
    from unittest.mock import mock_open

    from core.transformer import Transformer

    # Helper to generate SQL with mocked template file read but real file writes
    def generate_with_template(template_sql, schema, pk, site, file_path, source_table, target_table, etl_path):
        original_open = open

        def patched_open(*args, **kwargs):
            # If opening the SQL template file, return mock
            if len(args) > 0 and isinstance(args[0], str) and args[0].endswith('.sql'):
                return StringIO(template_sql)
            return original_open(*args, **kwargs)

        with patch('core.transformer.utils.get_primary_key_column', return_value=pk), \
             patch('core.transformer.utils.get_table_schema', return_value=schema), \
             patch('builtins.open', side_effect=patched_open):
            transformer = Transformer(
                site=site,
                file_path=file_path,
                cdm_version="5.4",
                source_table=source_table,
                target_table=target_table,
                etl_artifact_path=etl_path
            )
            return transformer.generate_omop_to_omop_sql()

    # 1. test_generate_sql_reads_template_file & test_generate_sql_wraps_in_copy_statement
    # These two tests use the same template with 2 columns - the first verifies
    # the template file was opened, the second verifies COPY wrapping.
    # Using the 2-column version as the golden file for the "reads template" test.
    sql = generate_with_template(
        template_sql="""
        SELECT
            condition_occurrence_id AS observation_id,
            person_id AS person_id
        FROM read_parquet('@CONDITION_OCCURRENCE')
        """,
        schema={"observation": {"columns": {
            "observation_id": {"type": "BIGINT", "required": "True"},
            "person_id": {"type": "BIGINT", "required": "True"}
        }}},
        pk="observation_id",
        site="test_site",
        file_path="gs://bucket/2025-01-01/harmonized/",
        source_table="condition_occurrence",
        target_table="observation",
        etl_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
    )
    write_sql("transformer", "generate_omop_to_omop_sql_reads_template.sql", sql)

    # 2. test_generate_sql_wraps_in_copy_statement (single column)
    sql = generate_with_template(
        template_sql="""
        SELECT
            condition_occurrence_id AS observation_id
        FROM read_parquet('@CONDITION_OCCURRENCE')
        """,
        schema={"observation": {"columns": {
            "observation_id": {"type": "BIGINT", "required": "True"}
        }}},
        pk="observation_id",
        site="test_site",
        file_path="gs://bucket/2025-01-01/harmonized/",
        source_table="condition_occurrence",
        target_table="observation",
        etl_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
    )
    write_sql("transformer", "generate_omop_to_omop_sql_copy_statement.sql", sql)

    # 3. test_generate_sql_adds_cast_for_required_fields
    sql = generate_with_template(
        template_sql="""
        SELECT
            person_id AS person_id
        FROM read_parquet('@CONDITION_OCCURRENCE')
        """,
        schema={"observation": {"columns": {
            "person_id": {"type": "BIGINT", "required": "True"}
        }}},
        pk="observation_id",
        site="test_site",
        file_path="gs://bucket/2025-01-01/harmonized/",
        source_table="condition_occurrence",
        target_table="observation",
        etl_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
    )
    write_sql("transformer", "generate_omop_to_omop_sql_cast_required.sql", sql)

    # 4. test_generate_sql_adds_try_cast_for_optional_fields
    sql = generate_with_template(
        template_sql="""
        SELECT
            value_as_number AS value_as_number
        FROM read_parquet('@OBSERVATION')
        """,
        schema={"measurement": {"columns": {
            "value_as_number": {"type": "DOUBLE", "required": "False"}
        }}},
        pk="measurement_id",
        site="test_site",
        file_path="gs://bucket/2025-01-01/harmonized/",
        source_table="observation",
        target_table="measurement",
        etl_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
    )
    write_sql("transformer", "generate_omop_to_omop_sql_try_cast_optional.sql", sql)


if __name__ == "__main__":
    generate_file_processor_golden_files()
    generate_normalization_golden_files()
    generate_omop_client_golden_files()
    generate_vocab_manager_golden_files()
    generate_transformer_golden_files()
    print("\nDone! All golden SQL files generated.")
