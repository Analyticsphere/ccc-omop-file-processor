"""
Unit tests for utils.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/utils/
"""

import pytest
from pathlib import Path

from core.utils import generate_report_consolidation_sql, generate_vocab_version_query_sql


# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "utils"


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


class TestGenerateReportConsolidationSql:
    """Tests for generate_report_consolidation_sql()."""

    def test_standard_report_consolidation(self):
        """Test SQL generation for consolidating multiple report parquet files into CSV."""
        select_statement = "SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file1.parquet') UNION ALL SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file2.parquet') UNION ALL SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file3.parquet')"
        output_path = "gs://bucket/2025-01-01/report/delivery_report_site1_2025-01-01.csv"

        result = generate_report_consolidation_sql(select_statement, output_path)

        expected = load_reference_sql("generate_report_consolidation_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateVocabVersionQuerySql:
    """Tests for generate_vocab_version_query_sql()."""

    def test_standard_vocab_version_query(self):
        """Test SQL generation for querying vocabulary version from vocabulary table."""
        vocabulary_file_path = "gs://vocab-bucket/synthea53/2025-01-01/artifacts/converted_files/vocabulary.parquet"

        result = generate_vocab_version_query_sql(vocabulary_file_path)

        expected = load_reference_sql("generate_vocab_version_query_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)
