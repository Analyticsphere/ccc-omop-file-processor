"""
Unit tests for normalization.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/normalization/
"""

import pytest
from pathlib import Path

from core.normalization import generate_row_count_sql


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

        result = generate_row_count_sql(parquet_file_path)

        expected = load_reference_sql("generate_row_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)
