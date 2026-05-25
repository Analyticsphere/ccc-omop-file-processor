"""
Unit tests for report_artifact.py ReportArtifact class.

Specifically guards against the precision-loss regression where
value_as_number was cast to 32-bit FLOAT in DuckDB SQL, silently
rounding large row counts (>~16.7M) to multiples of 8 and corrupting
the values reported in the delivery report CSV.
"""

from pathlib import Path
from unittest.mock import patch

from core.helpers.report_artifact import ReportArtifact

REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "report_artifact"


def normalize_sql(sql: str) -> str:
    """Normalize SQL for comparison by stripping whitespace and blank lines."""
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    """Load reference SQL from file."""
    with open(REFERENCE_DIR / filename, 'r') as f:
        return f.read()


class TestGenerateSaveArtifactSQL:
    """Tests for generate_save_artifact_sql static method.

    These guard the artifact-write SQL via golden files. Critically, the
    golden file pins value_as_number to TRY_CAST(... AS DOUBLE) — 32-bit
    FLOAT would silently round counts >~16.7M (the bug that corrupted
    the Sanford measurement count from 98,159,833 to 98,159,830).
    """

    def test_matches_golden_file_with_values(self):
        sql = ReportArtifact.generate_save_artifact_sql(
            file_path="gs://test-bucket/2025-01-15/tmp/delivery_report_part_test-uuid.parquet",
            metadata_id=123456789,
            concept_id=1147330,
            name="Final row count: measurement",
            value_as_string="measurement",
            value_as_concept_id=1147330,
            value_as_number=98159833.0,
            metadata_date="2025-01-15",
            metadata_datetime="2025-01-15 12:34:56",
        )

        expected = load_reference_sql("generate_save_artifact_sql_with_values.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_matches_golden_file_null_values(self):
        sql = ReportArtifact.generate_save_artifact_sql(
            file_path="gs://test-bucket/2025-01-15/tmp/delivery_report_part_test-uuid.parquet",
            metadata_id=987654321,
            concept_id=0,
            name="Invalid table name: foo",
            value_as_string=None,
            value_as_concept_id=0,
            value_as_number=None,
            metadata_date="2025-01-15",
            metadata_datetime="2025-01-15 12:34:56",
        )

        expected = load_reference_sql("generate_save_artifact_sql_null_values.sql")
        assert normalize_sql(sql) == normalize_sql(expected)


class TestSaveArtifactPrecision:
    """End-to-end: a large row count must round-trip through the artifact
    parquet and CSV consolidation exactly, with zero precision loss.

    The golden-file tests above pin the SQL text, but only an actual
    DuckDB round-trip proves the resulting numeric type is wide enough.
    """

    @patch('core.helpers.report_artifact.utils.get_report_tmp_artifacts_path',
           return_value="test-bucket/2025-01-15/tmp/")
    @patch('core.helpers.report_artifact.storage.get_uri')
    @patch('core.helpers.report_artifact.utils.execute_duckdb_sql')
    def test_large_count_roundtrips_exactly(
        self, mock_execute, mock_uri, _mock_tmp_path, tmp_path
    ):
        import duckdb

        parquet_path = tmp_path / "artifact.parquet"
        csv_path = tmp_path / "report.csv"
        mock_uri.return_value = str(parquet_path)
        mock_execute.side_effect = lambda sql, *_a, **_k: duckdb.sql(sql)

        true_count = 98_159_833  # the Sanford measurement count
        artifact = ReportArtifact(
            delivery_date="2025-01-15",
            artifact_bucket="test-bucket",
            concept_id=1147330,
            name="Final row count: measurement",
            value_as_string="measurement",
            value_as_concept_id=1147330,
            value_as_number=float(true_count),
        )
        artifact.save_artifact()

        # Mirror the consolidation step: artifact parquet -> CSV.
        duckdb.sql(
            f"COPY (SELECT * FROM read_parquet('{parquet_path}')) "
            f"TO '{csv_path}' (HEADER, DELIMITER ',')"
        )

        csv_value = duckdb.sql(
            f"SELECT value_as_number FROM read_csv('{csv_path}', header=true)"
        ).fetchone()[0]

        assert int(csv_value) == true_count, (
            f"Row count {true_count} corrupted to {int(csv_value)} "
            f"during artifact write + CSV serialization."
        )
