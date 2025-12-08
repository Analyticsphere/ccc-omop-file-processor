"""
Unit tests for file_processor.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/file_processor/
"""

import pytest
from pathlib import Path

from core.file_processor import (
    generate_process_incoming_parquet_sql,
    generate_csv_to_parquet_sql
)


# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "file_processor"


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


class TestGenerateProcessIncomingParquetSql:
    """Tests for generate_process_incoming_parquet_sql()."""

    def test_standard_columns(self):
        """Test SQL generation with standard columns."""
        result = generate_process_incoming_parquet_sql(
            file_path="synthea53/2025-01-01/person.parquet",
            parquet_columns=["person_id", "gender_concept_id", "year_of_birth", "birth_datetime"]
        )

        expected = load_reference_sql("generate_process_incoming_parquet_sql_standard_columns.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_offset_column_unquoted(self):
        """Test SQL generation with unquoted offset column (note_nlp table)."""
        result = generate_process_incoming_parquet_sql(
            file_path="synthea53/2025-01-01/note_nlp.parquet",
            parquet_columns=["note_nlp_id", "note_id", "offset", "lexical_variant"]
        )

        expected = load_reference_sql("generate_process_incoming_parquet_sql_offset_unquoted.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_offset_column_quoted(self):
        """Test SQL generation with quoted offset column."""
        result = generate_process_incoming_parquet_sql(
            file_path="synthea53/2025-01-01/note_nlp.parquet",
            parquet_columns=["note_nlp_id", "note_id", '"offset"', "lexical_variant"]
        )

        expected = load_reference_sql("generate_process_incoming_parquet_sql_offset_quoted.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCsvToParquetSql:
    """Tests for generate_csv_to_parquet_sql()."""

    def test_standard_no_options(self):
        """Test CSV to Parquet SQL generation without conversion options."""
        result = generate_csv_to_parquet_sql(
            file_path="synthea53/2025-01-01/person.csv",
            csv_column_names=["person_id", "gender_concept_id", "year_of_birth", "birth_datetime"],
            conversion_options=[]
        )

        expected = load_reference_sql("generate_csv_to_parquet_sql_standard_no_options.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_with_conversion_options(self):
        """Test CSV to Parquet SQL generation with error handling options."""
        result = generate_csv_to_parquet_sql(
            file_path="synthea53/2025-01-01/measurement.csv",
            csv_column_names=["measurement_id", "person_id", "measurement_concept_id", "measurement_date"],
            conversion_options=['store_rejects=True, ignore_errors=True, parallel=False']
        )

        expected = load_reference_sql("generate_csv_to_parquet_sql_with_options.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_with_offset_column(self):
        """Test CSV to Parquet SQL generation with offset reserved keyword."""
        result = generate_csv_to_parquet_sql(
            file_path="synthea53/2025-01-01/note_nlp.csv",
            csv_column_names=["note_nlp_id", "note_id", "offset", "lexical_variant"],
            conversion_options=[]
        )

        expected = load_reference_sql("generate_csv_to_parquet_sql_with_offset.sql")
        assert normalize_sql(result) == normalize_sql(expected)
