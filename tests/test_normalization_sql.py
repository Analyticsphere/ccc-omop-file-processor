"""
Unit tests for normalization.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/normalization/
"""

from pathlib import Path

import pytest

from core.normalization import Normalizer

# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "normalization"


def normalize_sql(sql: str) -> str:
    """
    Normalize SQL for comparison by removing extra whitespace.
    Makes SQL comparison whitespace-insensitive.
    """
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    """Load reference SQL from file."""
    filepath = REFERENCE_DIR / filename
    with open(filepath, 'r') as f:
        return f.read()


class TestGenerateRowCountSql:
    """Tests for generate_row_count_sql()."""

    def test_standard_row_count(self):
        """Test SQL generation for counting rows in a parquet file."""
        parquet_file_path = "gs://synthea53/2025-01-01/artifacts/converted_files/person.parquet"

        result = Normalizer.generate_row_count_sql(parquet_file_path)

        expected = load_reference_sql("generate_row_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateNormalizationSql:
    """Tests for _generate_normalization_sql()."""

    def test_person_table_normalization(self):
        """
        Test SQL generation for person table.

        Person table has special handling:
        - birth_datetime calculation from year/month/day components
        - Natural primary key (person_id) - no surrogate key replacement
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("person", "5.4")
        actual_columns = list(schema["person"]["columns"].keys())

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/person.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_person.sql")
            assert normalize_sql(result) == normalize_sql(expected)

    def test_note_nlp_table_normalization(self):
        """
        Test SQL generation for note_nlp table.

        Note_nlp table has special handling:
        - Contains 'offset' column which is a reserved keyword in DuckDB
        - Surrogate key (note_nlp_id) - requires composite key generation
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("note_nlp", "5.4")
        actual_columns = list(schema["note_nlp"]["columns"].keys())

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/note_nlp.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_note_nlp.sql")
            assert normalize_sql(result) == normalize_sql(expected)

    def test_measurement_table_normalization(self):
        """
        Test SQL generation for measurement table.

        Measurement table has special handling:
        - Surrogate key (measurement_id) - requires composite key generation
        - Multiple date/datetime fields requiring format parsing
        - Large number of optional columns
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("measurement", "5.4")
        actual_columns = list(schema["measurement"]["columns"].keys())

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/measurement.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_measurement.sql")
            assert normalize_sql(result) == normalize_sql(expected)

    def test_person_with_connectid_and_personid(self):
        """
        Test SQL generation when file has both connect_id and person_id columns.

        When both columns exist:
        - connect_id value should be used for person_id
        - Both columns should appear in row hash calculation
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("person", "5.4")
        actual_columns = list(schema["person"]["columns"].keys())
        # Add connect_id to the list of actual columns
        actual_columns_with_connectid = ['person_id', 'connect_id'] + [col for col in actual_columns if col != 'person_id']

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns_with_connectid

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/person.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_person_connectid_with_personid.sql")
            assert normalize_sql(result) == normalize_sql(expected)

    def test_person_with_connectid_without_personid(self):
        """
        Test SQL generation when file has connect_id but no person_id column.

        When only connect_id exists:
        - connect_id value should be used for person_id
        - Only connect_id should appear in row hash calculation (not person_id)
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("person", "5.4")
        actual_columns = list(schema["person"]["columns"].keys())
        # Replace person_id with connect_id
        actual_columns_with_connectid = ['connect_id'] + [col for col in actual_columns if col != 'person_id']

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns_with_connectid

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/person.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_person_connectid_without_personid.sql")
            assert normalize_sql(result) == normalize_sql(expected)

    def test_condition_occurrence_with_connectid(self):
        """
        Test SQL generation for non-person table (condition_occurrence) with connect_id.

        Demonstrates that connect_id logic applies to ALL tables, not just person table.
        When connect_id exists in condition_occurrence:
        - connect_id value should be used for person_id
        - Surrogate key (condition_occurrence_id) should be generated from composite hash
        """
        from unittest.mock import patch

        import core.utils as utils

        # Get columns from actual OMOP CDM schema
        schema = utils.get_table_schema("condition_occurrence", "5.4")
        actual_columns = list(schema["condition_occurrence"]["columns"].keys())
        # Add connect_id to simulate a file that has both person_id and connect_id
        actual_columns_with_connectid = ['person_id', 'connect_id'] + [col for col in actual_columns if col != 'person_id']

        with patch('core.normalization.utils.get_columns_from_file') as mock_get_columns:
            mock_get_columns.return_value = actual_columns_with_connectid

            normalizer = Normalizer(
                file_path="test-bucket/2025-01-01/condition_occurrence.parquet",
                cdm_version="5.4",
                date_format="%Y-%m-%d",
                datetime_format="%Y-%m-%d %H:%M:%S"
            )

            result = normalizer._generate_normalization_sql()

            expected = load_reference_sql("generate_normalization_sql_condition_occurrence_connectid.sql")
            assert normalize_sql(result) == normalize_sql(expected)
