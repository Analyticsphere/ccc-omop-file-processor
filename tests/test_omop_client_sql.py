"""
Unit tests for omop_client.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/omop_client/
"""

import pytest
from pathlib import Path

from core.omop_client import (
    generate_upgrade_file_sql,
    generate_convert_vocab_sql
)


# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "omop_client"


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


class TestGenerateUpgradeFileSql:
    """Tests for generate_upgrade_file_sql()."""

    def test_standard_upgrade(self):
        """Test SQL generation for standard CDM upgrade."""
        upgrade_script = """
        SELECT
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            birth_datetime
    """
        result = generate_upgrade_file_sql(
            upgrade_script=upgrade_script,
            normalized_file_path="synthea53/2025-01-01/artifacts/converted_files/person.parquet"
        )

        expected = load_reference_sql("generate_upgrade_file_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_complex_upgrade(self):
        """Test SQL generation for complex CDM upgrade with DISTINCT, COALESCE, WHERE."""
        upgrade_script = """
        SELECT DISTINCT
            m.measurement_id,
            m.person_id,
            m.measurement_concept_id,
            COALESCE(m.value_as_number, 0) as value_as_number
        WHERE m.measurement_concept_id IS NOT NULL
    """
        result = generate_upgrade_file_sql(
            upgrade_script=upgrade_script,
            normalized_file_path="synthea53/2025-01-01/artifacts/converted_files/measurement.parquet"
        )

        expected = load_reference_sql("generate_upgrade_file_sql_complex.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateConvertVocabSql:
    """Tests for generate_convert_vocab_sql()."""

    def test_standard_vocabulary_columns(self):
        """Test SQL generation for standard vocabulary table without date columns."""
        result = generate_convert_vocab_sql(
            csv_file_path="vocab_root/v5.0_24-JAN-25/DOMAIN.csv",
            parquet_file_path="vocab_root/v5.0_24-JAN-25/optimized/domain.parquet",
            csv_columns=["domain_id", "domain_name", "domain_concept_id"]
        )

        expected = load_reference_sql("generate_convert_vocab_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_with_date_columns(self):
        """Test SQL generation for vocabulary table with date columns requiring special formatting."""
        result = generate_convert_vocab_sql(
            csv_file_path="vocab_root/v5.0_24-JAN-25/CONCEPT.csv",
            parquet_file_path="vocab_root/v5.0_24-JAN-25/optimized/concept.parquet",
            csv_columns=[
                "concept_id",
                "concept_name",
                "domain_id",
                "vocabulary_id",
                "valid_start_date",
                "valid_end_date",
                "invalid_reason"
            ]
        )

        expected = load_reference_sql("generate_convert_vocab_sql_with_dates.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_mixed_columns(self):
        """Test SQL generation for vocabulary table with mixed date and non-date columns."""
        result = generate_convert_vocab_sql(
            csv_file_path="vocab_root/v5.0_24-JAN-25/CONCEPT_RELATIONSHIP.csv",
            parquet_file_path="vocab_root/v5.0_24-JAN-25/optimized/concept_relationship.parquet",
            csv_columns=[
                "concept_id_1",
                "concept_id_2",
                "relationship_id",
                "valid_start_date",
                "valid_end_date"
            ]
        )

        expected = load_reference_sql("generate_convert_vocab_sql_mixed.sql")
        assert normalize_sql(result) == normalize_sql(expected)
