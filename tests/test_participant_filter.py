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
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet",
            omop_version="5.4"
        )

        assert connect_filter.table_name == "condition_occurrence"
        assert connect_filter.bucket == "test-bucket"
        assert connect_filter.delivery_date == "2025-01-15"
        assert connect_filter.omop_version == "5.4"
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

    @patch('core.participant_filter.report_artifact.ReportArtifact')
    @patch('core.participant_filter.utils.get_cdm_schema')
    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_applies_exclusions_when_person_id_exists(
        self,
        mock_parquet_exists,
        mock_get_columns,
        mock_get_uri,
        mock_execute_sql,
        mock_get_cdm_schema,
        mock_artifact
    ):
        """Test that parquet rewrite runs for tables with a person_id column."""
        mock_parquet_exists.side_effect = [True, True]
        mock_get_columns.return_value = ['person_id', 'condition_concept_id']
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_get_cdm_schema.return_value = {
            'condition_occurrence': {'concept_id': 789012}
        }
        mock_execute_sql.side_effect = [[(2, 3, 4)], None]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet",
            omop_version="5.4"
        )

        result = connect_filter.apply_exclusions()

        assert result is True
        mock_get_columns.assert_called_once_with(connect_filter.parquet_file_path)
        assert mock_execute_sql.call_count == 2

        count_sql = mock_execute_sql.call_args_list[0][0][0]
        expected_count_sql = load_reference_sql("generate_row_removal_count_sql_condition_occurrence.sql")
        assert normalize_sql(count_sql) == normalize_sql(expected_count_sql)

        filter_sql = mock_execute_sql.call_args_list[1][0][0]
        expected_filter_sql = load_reference_sql("generate_filter_sql_condition_occurrence.sql")
        assert normalize_sql(filter_sql) == normalize_sql(expected_filter_sql)

        expected_artifact_calls = [
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to missing person_id values: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=2
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to identifier not in Connect database: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=3
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to Connect exclusion rules: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=4
            ),
        ]
        assert mock_artifact.call_args_list == expected_artifact_calls
        assert mock_artifact_instance.save_artifact.call_count == 3

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
            file_path="test-bucket/2025-01-15/visit_occurrence.parquet",
            omop_version="5.4"
        )

        result = connect_filter.apply_exclusions()

        assert result is False
        mock_execute_sql.assert_not_called()

    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_raises_when_connect_data_parquet_is_missing(self, mock_parquet_exists, mock_get_columns):
        """Test that missing Connect participant-status parquet raises an error."""
        mock_parquet_exists.side_effect = [True, False]
        mock_get_columns.return_value = ['person_id']

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/person.parquet",
            omop_version="5.4"
        )

        with pytest.raises(Exception) as exc_info:
            connect_filter.apply_exclusions()

        assert "Connect data parquet not found" in str(exc_info.value)

    @patch('core.participant_filter.report_artifact.ReportArtifact')
    @patch('core.participant_filter.utils.get_cdm_schema')
    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_creates_person_missing_person_id_artifact(
        self,
        mock_parquet_exists,
        mock_get_columns,
        mock_get_uri,
        mock_execute_sql,
        mock_get_cdm_schema,
        mock_artifact
    ):
        """Test that the person table keeps its existing missing-person_id artifact name."""
        mock_parquet_exists.side_effect = [True, True]
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_get_cdm_schema.return_value = {
            'person': {'concept_id': 123456}
        }
        mock_execute_sql.side_effect = [[(5, 1, 2)], None]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/person.parquet",
            omop_version="5.4"
        )

        result = connect_filter.apply_exclusions()

        assert result is True
        expected_artifact_calls = [
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=123456,
                name="Number of persons with missing person_id",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=5
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=123456,
                name="Number of rows removed due to missing person_id values: person",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=5
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=123456,
                name="Number of rows removed due to identifier not in Connect database: person",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=1
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=123456,
                name="Number of rows removed due to Connect exclusion rules: person",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=2
            ),
        ]
        assert mock_artifact.call_args_list == expected_artifact_calls
        assert mock_artifact_instance.save_artifact.call_count == 4

    @patch('core.participant_filter.report_artifact.ReportArtifact')
    @patch('core.participant_filter.utils.get_cdm_schema')
    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.parquet_file_exists')
    def test_creates_missing_person_id_artifact_even_when_count_is_zero(
        self,
        mock_parquet_exists,
        mock_get_columns,
        mock_get_uri,
        mock_execute_sql,
        mock_get_cdm_schema,
        mock_artifact
    ):
        """Test that the missing-person_id artifact is still created when no rows are removed."""
        mock_parquet_exists.side_effect = [True, True]
        mock_get_columns.return_value = ['person_id', 'condition_concept_id']
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_get_cdm_schema.return_value = {
            'condition_occurrence': {'concept_id': 789012}
        }
        mock_execute_sql.side_effect = [[(0, 0, 0)], None]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        connect_filter = ParticipantFilter(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet",
            omop_version="5.4"
        )

        result = connect_filter.apply_exclusions()

        assert result is True
        assert mock_execute_sql.call_count == 2
        expected_artifact_calls = [
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to missing person_id values: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to identifier not in Connect database: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=789012,
                name="Number of rows removed due to Connect exclusion rules: condition_occurrence",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=0
            ),
        ]
        assert mock_artifact.call_args_list == expected_artifact_calls
        assert mock_artifact_instance.save_artifact.call_count == 3


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

    def test_generate_row_removal_count_sql_matches_golden_file(self):
        """Test that row-removal count SQL matches the reference SQL file."""
        sql = ParticipantFilter.generate_row_removal_count_sql(
            table_uri="gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet",
            connect_data_uri="gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet"
        )

        expected = load_reference_sql("generate_row_removal_count_sql_standard.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generate_delivery_not_in_connect_sql_with_invalid_rows(self):
        """Test that delivery-not-in-Connect SQL includes non-numeric connect_ids from invalid rows."""
        sql = ParticipantFilter.generate_delivery_not_in_connect_sql(
            table_uri="gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet",
            connect_data_uri="gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet",
            invalid_rows_uri="gs://test-bucket/2025-01-15/artifacts/invalid_rows/person.parquet",
            has_connect_id_column=True
        )

        expected = load_reference_sql("generate_delivery_not_in_connect_sql_with_invalid_rows.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generate_delivery_not_in_connect_sql_without_invalid_rows(self):
        """Test that delivery-not-in-Connect SQL works without invalid rows."""
        sql = ParticipantFilter.generate_delivery_not_in_connect_sql(
            table_uri="gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet",
            connect_data_uri="gs://test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet"
        )

        expected = load_reference_sql("generate_delivery_not_in_connect_sql_without_invalid_rows.sql")
        assert normalize_sql(sql) == normalize_sql(expected)


class TestCreateConnectEligibilityReportArtifacts:
    """Tests for Connect eligibility report artifacts."""

    @patch('core.participant_filter.report_artifact.ReportArtifact')
    @patch('core.participant_filter.utils.execute_duckdb_sql')
    @patch('core.participant_filter.utils.get_columns_from_file')
    @patch('core.participant_filter.utils.get_invalid_rows_path_from_path')
    @patch('core.participant_filter.utils.list_files')
    @patch('core.participant_filter.storage.get_uri')
    @patch('core.participant_filter.utils.parquet_file_exists')
    @patch('core.participant_filter.utils.get_parquet_artifact_location')
    @patch('core.participant_filter.utils.get_bucket_and_delivery_date_from_path')
    def test_creates_status_and_per_table_reconciliation_artifacts(
        self,
        mock_get_bucket_and_date,
        mock_get_parquet_path,
        mock_parquet_exists,
        mock_get_uri,
        mock_list_files,
        mock_get_invalid_rows_path,
        mock_get_columns,
        mock_execute_sql,
        mock_artifact
    ):
        """Test that Connect eligibility artifacts include per-table delivery-not-in-Connect reports."""
        mock_get_bucket_and_date.return_value = ("test-bucket", "2025-01-15")
        mock_get_parquet_path.return_value = "test-bucket/2025-01-15/artifacts/converted_files/person.parquet"
        mock_get_uri.side_effect = lambda path: path if path.startswith("gs://") else f"gs://{path}"

        # list_files returns filenames only, not full paths
        mock_list_files.return_value = [
            "condition_occurrence.parquet",
            "person.parquet",
        ]

        # parquet_file_exists: person parquet (initial check), then invalid_rows for each table
        mock_parquet_exists.side_effect = [
            True,   # person parquet exists (initial check)
            False,  # condition_occurrence invalid_rows does not exist
            True,   # person invalid_rows exists
        ]

        # get_columns_from_file: for condition_occurrence converted, then person converted,
        # then person invalid_rows
        mock_get_columns.side_effect = [
            ['person_id', 'condition_concept_id'],  # condition_occurrence converted
            ['person_id', 'gender_concept_id'],      # person converted
            ['person_id', 'connect_id', 'gender_concept_id'],  # person invalid_rows
        ]

        mock_get_invalid_rows_path.side_effect = [
            "test-bucket/2025-01-15/artifacts/invalid_rows/condition_occurrence.parquet",
            "test-bucket/2025-01-15/artifacts/invalid_rows/person.parquet",
        ]

        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        mock_execute_sql.side_effect = [
            # 4 status breakdown queries
            [("Verified", 197316935, 2, "1001|1002"), ("Pending", 555555555, 1, "1003")],
            [("No", 104430631, 1, "1001"), ("Yes", 353358909, 1, "1002")],
            [("No", 104430631, 1, "1001"), ("Yes", 353358909, 2, "1002|1003")],
            [("No", 104430631, 2, "1001|1002"), ("Yes", 353358909, 1, "1003")],
            # Connect not in delivery
            [(1, "9001")],
            # Per-table delivery not in Connect: condition_occurrence (no invalid rows)
            [(1, "7001")],
            # Per-table delivery not in Connect: person (with invalid rows connect_id)
            [(3, "8001|8002|uuid-value")],
        ]

        ParticipantFilter.create_connect_eligibility_report_artifacts(
            connect_data_path="test-bucket/2025-01-15/artifacts/connect_data/participant_status.parquet",
            delivery_bucket="test-bucket/2025-01-15"
        )

        assert mock_execute_sql.call_count == 7
        executed_sql = [call_args.args[0] for call_args in mock_execute_sql.call_args_list]

        # Verify the first 5 SQL queries match existing golden files
        expected_sql_files = [
            "create_connect_eligibility_report_artifacts_study_status_standard.sql",
            "create_connect_eligibility_report_artifacts_hipaa_revoked_status_standard.sql",
            "create_connect_eligibility_report_artifacts_consent_withdrawn_status_standard.sql",
            "create_connect_eligibility_report_artifacts_data_destruction_status_standard.sql",
            "create_connect_eligibility_report_artifacts_connect_not_in_delivery_standard.sql",
        ]
        for executed, expected_file in zip(executed_sql[:5], expected_sql_files):
            expected_sql = load_reference_sql(expected_file)
            assert normalize_sql(executed) == normalize_sql(expected_sql)

        # Verify per-table SQL: condition_occurrence (no invalid rows)
        expected_condition_sql = load_reference_sql("generate_delivery_not_in_connect_sql_without_invalid_rows.sql")
        assert normalize_sql(executed_sql[5]) == normalize_sql(expected_condition_sql)

        # Verify per-table SQL: person (with invalid rows)
        expected_person_sql = load_reference_sql("generate_delivery_not_in_connect_sql_with_invalid_rows.sql")
        assert normalize_sql(executed_sql[6]) == normalize_sql(expected_person_sql)

        expected_artifact_calls = [
            # Status breakdown artifacts (unchanged)
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Study status (Verified)",
                value_as_string="",
                value_as_concept_id=197316935,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Study status (Pending)",
                value_as_string="1003",
                value_as_concept_id=555555555,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: HIPAA revoked status (No)",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: HIPAA revoked status (Yes)",
                value_as_string="1002",
                value_as_concept_id=353358909,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Consent withdrawn status (No)",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Consent withdrawn status (Yes)",
                value_as_string="1002|1003",
                value_as_concept_id=353358909,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Data destruction status (No)",
                value_as_string="",
                value_as_concept_id=104430631,
                value_as_number=2.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Connect participant breakdown: Data destruction status (Yes)",
                value_as_string="1003",
                value_as_concept_id=353358909,
                value_as_number=1.0
            ),
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Number of eligible Connect patients not in delivery",
                value_as_string="9001",
                value_as_concept_id=None,
                value_as_number=1.0
            ),
            # Per-table: condition_occurrence
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Delivered Connect ID values not found in Connect database: condition_occurrence",
                value_as_string="7001",
                value_as_concept_id=None,
                value_as_number=1.0
            ),
            # Per-table: person (with non-numeric connect_ids from invalid rows)
            call(
                delivery_date="2025-01-15",
                artifact_bucket="test-bucket",
                concept_id=0,
                name="Delivered Connect ID values not found in Connect database: person",
                value_as_string="8001|8002|uuid-value",
                value_as_concept_id=None,
                value_as_number=3.0
            ),
        ]

        assert mock_artifact.call_args_list == expected_artifact_calls
        assert mock_artifact_instance.save_artifact.call_count == 11
