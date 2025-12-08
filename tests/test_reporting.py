"""
Unit tests for reporting.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/reporting/
"""

from pathlib import Path

import pytest

from core.reporting import (generate_report_consolidation_sql,
                            get_report_tmp_artifacts_path)

# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "reporting"


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


@pytest.mark.parametrize(
    "bucket,delivery_date,expected_path",
    [
        ("synthea53", "2024-12-31", "synthea53/2024-12-31/artifacts/delivery_report/tmp/"),
        ("bucket", "folder", "bucket/folder/artifacts/delivery_report/tmp/"),
        ("test-bucket", "2025-10-04", "test-bucket/2025-10-04/artifacts/delivery_report/tmp/"),
    ]
)
def test_get_report_tmp_artifacts_path(bucket, delivery_date, expected_path):
    """Test path generation for temporary report artifacts directory."""
    assert get_report_tmp_artifacts_path(bucket, delivery_date) == expected_path
