"""
Unit tests for participant_filter.py.

Tests table-level Connect participant exclusions and SQL generation.
"""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from core.participant_filter import ParticipantFilter

REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "participant_filter"


def normalize_sql(sql: str) -> str:
    """Normalize SQL for whitespace-insensitive comparison."""
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    """Load reference SQL from file."""
    filepath = REFERENCE_DIR / filename
    with open(filepath, 'r') as f:
        return f.read()


class TestParticipantFilterInit:
    """Tests for ParticipantFilter initialization."""

    def test_init_computes_derived_paths(self):
        """Test that initialization derives parquet and Connect artifact paths."""
        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet"
        )

        assert connect_filter.table_name == "condition_occurrence"
        assert connect_filter.bucket == "test-bucket"
        assert connect_filter.delivery_date == "2025-01-15"
        assert (
            connect_filter.parquet_file_path
            == "test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet"
        )
        assert (
            connect_filter.connect_data_path
            == "test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet"
        )


class TestParticipantFilterApplyExclusions:
    """Tests for applying Connect participant exclusions."""

    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_applies_exclusions_when_person_id_exists(
        self,
        mock_parquet_exists,
        mock_get_columns,
        mock_get_uri,
        mock_execute_sql
    ):
        """Test that parquet rewrite runs for tables with a person_id column."""
        mock_parquet_exists.side_effect = [True, True]
        mock_get_columns.return_value = ['person_id', 'condition_concept_id']
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet"
        )

        result = connect_filter.apply_exclusions()

        assert result is True
        mock_get_columns.assert_called_once_with(connect_filter.parquet_file_path)
        mock_execute_sql.assert_called_once()

        sql = mock_execute_sql.call_args[0][0]
        assert "FROM read_parquet('gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet')" in sql
        assert "INNER JOIN known_connect_ids known" in sql
        assert "LEFT JOIN excluded_connect_ids excluded" in sql
        assert "TRY_CAST(verified_status_concept_id AS BIGINT) != 197316935" in sql
        assert "consent_withdrawn_concept_id AS BIGINT) = 353358909" in sql

    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_skips_tables_without_person_id(
        self,
        mock_parquet_exists,
        mock_get_columns,
        mock_execute_sql
    ):
        """Test that tables without person_id are skipped."""
        mock_parquet_exists.side_effect = [True, True]
        mock_get_columns.return_value = ['visit_occurrence_id', 'visit_concept_id']

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/visit_occurrence.parquet"
        )

        result = connect_filter.apply_exclusions()

        assert result is False
        mock_execute_sql.assert_not_called()

    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_raises_when_connect_data_parquet_is_missing(self, mock_parquet_exists):
        """Test that missing Connect participant-status parquet raises an error."""
        mock_parquet_exists.side_effect = [True, False]

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/person.parquet"
        )

        with pytest.raises(Exception) as exc_info:
            connect_filter.apply_exclusions()

        assert "Connect data parquet not found" in str(exc_info.value)


class TestParticipantFilterGenerateFilterSQL:
    """Tests for SQL generation."""

    def test_generate_filter_sql_matches_golden_file(self):
        """Test that generated SQL matches the reference SQL file."""
        sql = ParticipantFilter.generate_filter_sql(
            table_uri="gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet",
            connect_data_uri="gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet"
        )

        expected = load_reference_sql("generate_filter_sql_standard.sql")
        assert normalize_sql(sql) == normalize_sql(expected)


class TestCreateConnectEligibilityReportArtifacts:
    """Tests for Connect eligibility report artifacts."""

    @patch('core.participant_filter.report_artifact.ReportArtifact')
    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.parquet_file_exists')
    @patch('core.participant_filter.utils.get_parquet_artifact_location')
    @patch('core.participant_filter.utils.get_bucket_and_delivery_date_from_path')
    def test_creates_status_and_reconciliation_artifacts(
        self,
        mock_get_bucket_and_date,
        mock_get_parquet_path,
        mock_parquet_exists,
        mock_get_uri,
        mock_execute_sql,
        mock_artifact
    ):
        """Test that current Connect eligibility artifacts are created."""
        mock_get_bucket_and_date.return_value = ("test-bucket", "2025-01-15")
        mock_get_parquet_path.return_value = "test-bucket/2025-01-15/artifacts/converted_files/person.parquet"
        mock_parquet_exists.return_value = True
        mock_get_uri.side_effect = lambda path: path if path.startswith("gs://") else f"gs://{path}"

        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        mock_execute_sql.side_effect = [
            [("Verified", 197316935, 2, "1001|1002"), ("Pending", 555555555, 1, "1003")],
            [("No", 104430631, 1, "1001"), ("Yes", 353358909, 1, "1002")],
            [("No", 104430631, 1, "1001"), ("Yes", 353358909, 2, "1002|1003")],
            [("No", 104430631, 2, "1001|1002"), ("Yes", 353358909, 1, "1003")],
            [(1, "9001")],
            [(2, "8001|8002")],
        ]

        ParticipantFilter.create_connect_eligibility_report_artifacts(
            connect_data_path="test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet",
            delivery_bucket="test-bucket/2025-01-15"
        )

        assert mock_execute_sql.call_count == 6
        executed_sql = [call_args.args[0] for call_args in mock_execute_sql.call_args_list]

        assert "FROM matched_patients" in executed_sql[0]
        assert "verified_status AS status_value" in executed_sql[0]
        assert "STRING_AGG(CAST(connect_id AS VARCHAR), '|'" in executed_sql[0]
        assert "hipaa_revoked AS status_value" in executed_sql[1]
        assert "consent_withdrawn AS status_value" in executed_sql[2]
        assert "data_destruction_requested AS status_value" in executed_sql[3]
        assert "unmatched_connect" in executed_sql[4]
        assert "unmatched_delivery" in executed_sql[5]

        expected_artifact_calls = [
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Study status",
                value_as_string="",
                value_as_concept_id=197316935,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Study status",
                value_as_string="1003",
                value_as_concept_id=555555555,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: HIPAA revoked status",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: HIPAA revoked status",
                value_as_string="1002",
                value_as_concept_id=353358909,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Consent withdrawn status",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Consent withdrawn status",
                value_as_string="1002|1003",
                value_as_concept_id=353358909,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Data destruction status",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Data destruction status",
                value_as_string="1003",
                value_as_concept_id=353358909,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Number of Connect patients not in delivery",
                value_as_string="9001",
                value_as_concept_id=None,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Number of delivery patients not in Connect data",
                value_as_string="8001|8002",
                value_as_concept_id=None,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Delivery patient IDs not in Connect data",
                value_as_string="8001|8002",
                value_as_concept_id=None,
                value_as_number=None
            ),
        ]

        assert mock_artifact.call_args_list == expected_artifact_calls
        assert mock_artifact_instance.save_artifact.call_count == 11
