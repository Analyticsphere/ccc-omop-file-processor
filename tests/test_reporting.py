"""
Unit tests for reporting.py ReportGenerator class.

Tests report generation including metadata artifacts creation
and consolidation of temporary report files.
"""

from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
import core.utils as utils
from core.reporting import ReportGenerator


class TestReportGeneratorInit:
    """Tests for ReportGenerator initialization."""

    def test_init_stores_all_parameters(self):
        """Test that initialization stores all report_data parameters."""
        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)

        assert generator.site == "test_site"
        assert generator.bucket == "test-bucket"
        assert generator.delivery_date == "2025-01-15"
        assert generator.site_display_name == "Test Site"
        assert generator.file_delivery_format == "parquet"
        assert generator.delivered_cdm_version == "5.3"
        assert generator.target_vocabulary_version == "v5.0 20-MAR-24"
        assert generator.target_cdm_version == "5.4"

    @patch('core.reporting.storage.get_uri')
    def test_init_computes_derived_attributes(self, mock_get_uri):
        """Test that initialization computes tmp_artifacts_path and output_path."""
        mock_get_uri.return_value = "s3://test-bucket/2025-01-15/artifacts/reports/delivery_report.csv"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)

        # Check tmp_artifacts_path (without scheme)
        expected_tmp_path = f"test-bucket/2025-01-15/{constants.ArtifactPaths.REPORT_TMP.value}"
        assert generator.tmp_artifacts_path == expected_tmp_path

        # Check output_path (with scheme via get_uri)
        assert generator.output_path == "s3://test-bucket/2025-01-15/artifacts/reports/delivery_report.csv"
        mock_get_uri.assert_called_once()


class TestReportGeneratorGenerate:
    """Tests for generate orchestration method."""

    @patch.object(ReportGenerator, '_consolidate_report_files')
    @patch.object(ReportGenerator, '_create_time_series_row_count_artifacts')
    @patch.object(ReportGenerator, '_create_final_row_count_artifacts')
    @patch.object(ReportGenerator, '_create_person_id_referential_integrity_artifacts')
    @patch.object(ReportGenerator, '_create_date_datetime_default_value_artifacts')
    @patch.object(ReportGenerator, '_create_vocabulary_breakdown_artifacts')
    @patch.object(ReportGenerator, '_create_type_concept_breakdown_artifacts')
    @patch.object(ReportGenerator, '_create_invalid_concept_id_artifacts')
    @patch.object(ReportGenerator, '_create_metadata_artifacts')
    def test_generate_calls_all_methods(self, mock_create_metadata, mock_create_invalid_concept_ids, mock_create_type_concept, mock_create_vocabulary, mock_create_date_defaults, mock_create_person_id_integrity, mock_create_final_row_count, mock_create_time_series, mock_consolidate):
        """Test that generate calls metadata, invalid concept_id, type concept, vocabulary, date/datetime defaults, person_id referential integrity, final row count, time series row count, and consolidation methods."""
        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator.generate()

        mock_create_metadata.assert_called_once()
        mock_create_invalid_concept_ids.assert_called_once()
        mock_create_type_concept.assert_called_once()
        mock_create_vocabulary.assert_called_once()
        mock_create_date_defaults.assert_called_once()
        mock_create_person_id_integrity.assert_called_once()
        mock_create_final_row_count.assert_called_once()
        mock_create_time_series.assert_called_once()
        mock_consolidate.assert_called_once()

    @patch.object(ReportGenerator, '_consolidate_report_files')
    @patch.object(ReportGenerator, '_create_time_series_row_count_artifacts')
    @patch.object(ReportGenerator, '_create_final_row_count_artifacts')
    @patch.object(ReportGenerator, '_create_person_id_referential_integrity_artifacts')
    @patch.object(ReportGenerator, '_create_date_datetime_default_value_artifacts')
    @patch.object(ReportGenerator, '_create_vocabulary_breakdown_artifacts')
    @patch.object(ReportGenerator, '_create_type_concept_breakdown_artifacts')
    @patch.object(ReportGenerator, '_create_invalid_concept_id_artifacts')
    @patch.object(ReportGenerator, '_create_metadata_artifacts')
    def test_generate_calls_in_correct_order(self, mock_create_metadata, mock_create_invalid_concept_ids, mock_create_type_concept, mock_create_vocabulary, mock_create_date_defaults, mock_create_person_id_integrity, mock_create_final_row_count, mock_create_time_series, mock_consolidate):
        """Test that methods are called in correct order: metadata, type concept, vocabulary, date/datetime defaults, invalid concept_id, person_id referential integrity, final row count, time series row count, consolidation."""
        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        call_order = []
        mock_create_metadata.side_effect = lambda: call_order.append('metadata')
        mock_create_invalid_concept_ids.side_effect = lambda: call_order.append('invalid_concept_ids')
        mock_create_type_concept.side_effect = lambda: call_order.append('type_concept')
        mock_create_vocabulary.side_effect = lambda: call_order.append('vocabulary')
        mock_create_date_defaults.side_effect = lambda: call_order.append('date_defaults')
        mock_create_person_id_integrity.side_effect = lambda: call_order.append('person_id_integrity')
        mock_create_final_row_count.side_effect = lambda: call_order.append('final_row_count')
        mock_create_time_series.side_effect = lambda: call_order.append('time_series')
        mock_consolidate.side_effect = lambda: call_order.append('consolidate')

        generator = ReportGenerator(report_data)
        generator.generate()

        assert call_order == ['metadata', 'type_concept', 'vocabulary', 'date_defaults', 'invalid_concept_ids', 'person_id_integrity', 'final_row_count', 'time_series', 'consolidate']


class TestReportGeneratorMetadataArtifacts:
    """Tests for _create_metadata_artifacts method."""

    @patch('core.reporting.utils.get_cdm_version_concept_id')
    @patch('core.reporting.utils.get_delivery_vocabulary_version')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.datetime')
    def test_creates_all_metadata_artifacts(self, mock_datetime, mock_artifact,
                                           mock_get_vocab_version, mock_get_cdm_concept):
        """Test that all 9 metadata artifacts are created."""
        # Setup mocks
        mock_datetime.today.return_value.strftime.return_value = "2025-01-20"
        mock_get_vocab_version.return_value = "v5.0 10-JAN-24"
        mock_get_cdm_concept.side_effect = [5300, 5400]  # For delivered and target CDM versions
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_metadata_artifacts()

        # Should create 9 artifacts
        assert mock_artifact.call_count == 9
        assert mock_artifact_instance.save_artifact.call_count == 9

    @patch('core.reporting.utils.get_cdm_version_concept_id')
    @patch('core.reporting.utils.get_delivery_vocabulary_version')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.datetime')
    @patch('core.reporting.os.getenv')
    def test_metadata_values_are_correct(self, mock_getenv, mock_datetime, mock_artifact,
                                        mock_get_vocab_version, mock_get_cdm_concept):
        """Test that metadata artifacts contain correct values."""
        # Setup mocks
        mock_getenv.return_value = "abc123def456"
        mock_datetime.today.return_value.strftime.return_value = "2025-01-20"
        mock_get_vocab_version.return_value = "v5.0 10-JAN-24"
        mock_get_cdm_concept.side_effect = [5300, 5400]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_metadata_artifacts()

        # Get all artifact creation calls
        artifact_calls = mock_artifact.call_args_list

        # Verify specific metadata items
        # Processed date
        assert artifact_calls[0].kwargs['value_as_string'] == "2025-01-20"
        assert artifact_calls[0].kwargs['name'] == constants.PROCESSED_DATE_REPORT_NAME

        # File processor version
        assert artifact_calls[1].kwargs['value_as_string'] == "abc123def456"
        assert artifact_calls[1].kwargs['name'] == constants.FILE_PROCESSOR_VERSION_REPORT_NAME

        # Delivery date
        assert artifact_calls[2].kwargs['value_as_string'] == "2025-01-15"
        assert artifact_calls[2].kwargs['name'] == constants.DELIVERY_DATE_REPORT_NAME

        # Site display name
        assert artifact_calls[3].kwargs['value_as_string'] == "Test Site"
        assert artifact_calls[3].kwargs['name'] == constants.SITE_DISPLAY_NAME_REPORT_NAME

        # CDM versions should have concept IDs
        delivered_cdm_call = [c for c in artifact_calls
                              if c.kwargs['name'] == constants.DELIVERED_CDM_VERSION_REPORT_NAME][0]
        assert delivered_cdm_call.kwargs['value_as_concept_id'] == 5300

        target_cdm_call = [c for c in artifact_calls
                           if c.kwargs['name'] == constants.TARGET_CDM_VERSION_REPORT_NAME][0]
        assert target_cdm_call.kwargs['value_as_concept_id'] == 5400

    @patch('core.reporting.utils.get_cdm_version_concept_id')
    @patch('core.reporting.utils.get_delivery_vocabulary_version')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.datetime')
    def test_all_artifacts_use_correct_bucket_and_date(self, mock_datetime, mock_artifact,
                                                       mock_get_vocab_version, mock_get_cdm_concept):
        """Test that all artifacts use the correct bucket and delivery date."""
        mock_datetime.today.return_value.strftime.return_value = "2025-01-20"
        mock_get_vocab_version.return_value = "v5.0 10-JAN-24"
        mock_get_cdm_concept.return_value = 5300

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_metadata_artifacts()

        # All artifacts should use same bucket and delivery_date
        for call_args in mock_artifact.call_args_list:
            assert call_args.kwargs['artifact_bucket'] == "test-bucket"
            assert call_args.kwargs['delivery_date'] == "2025-01-15"
            assert call_args.kwargs['concept_id'] == 0


class TestReportGeneratorConsolidateReportFiles:
    """Tests for _consolidate_report_files method."""

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.storage.get_uri')
    @patch('core.reporting.utils.list_files')
    def test_consolidates_multiple_files(self, mock_list_files, mock_get_uri, mock_execute_sql):
        """Test consolidation of multiple report files."""
        # Setup mocks
        mock_list_files.return_value = ['file1.parquet', 'file2.parquet', 'file3.parquet']
        mock_get_uri.side_effect = lambda path: f"s3://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._consolidate_report_files()

        # Verify list_files was called correctly
        expected_report_tmp_dir = f"2025-01-15/{constants.ArtifactPaths.REPORT_TMP.value}"
        mock_list_files.assert_called_once_with("test-bucket", expected_report_tmp_dir, constants.PARQUET)

        # Verify SQL was executed
        mock_execute_sql.assert_called_once()
        sql = mock_execute_sql.call_args[0][0]

        # SQL should contain UNION ALL with all three files
        assert "UNION ALL" in sql
        assert "SELECT * FROM read_parquet('s3://test-bucket/" in sql
        assert sql.count("SELECT * FROM read_parquet") == 3

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.storage.get_uri')
    @patch('core.reporting.utils.list_files')
    def test_no_files_returns_early(self, mock_list_files, mock_get_uri, mock_execute_sql):
        """Test that consolidation returns early when no tmp files exist."""
        # No files found
        mock_list_files.return_value = []
        mock_get_uri.return_value = "s3://test-bucket/output.csv"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        # get_uri called once during init, reset for test
        mock_get_uri.reset_mock()

        generator._consolidate_report_files()

        # Should not execute SQL when no files
        mock_execute_sql.assert_not_called()
        # get_uri should not be called again during consolidation when no files
        mock_get_uri.assert_not_called()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.storage.get_uri')
    @patch('core.reporting.utils.list_files')
    def test_single_file_consolidation(self, mock_list_files, mock_get_uri, mock_execute_sql):
        """Test consolidation with a single report file."""
        mock_list_files.return_value = ['single_file.parquet']
        mock_get_uri.side_effect = lambda path: f"s3://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._consolidate_report_files()

        # Should still execute SQL even with single file
        mock_execute_sql.assert_called_once()
        sql = mock_execute_sql.call_args[0][0]

        # Should not have UNION ALL with single file
        assert "UNION ALL" not in sql
        assert "SELECT * FROM read_parquet" in sql


class TestReportGeneratorConsolidationSQL:
    """Tests for generate_report_consolidation_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        select_statement = (
            "SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file1.parquet') UNION ALL "
            "SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file2.parquet') UNION ALL "
            "SELECT * FROM read_parquet('gs://bucket/2025-01-01/report_tmp/file3.parquet')"
        )
        output_path = "gs://bucket/2025-01-01/report/delivery_report_site1_2025-01-01.csv"

        sql = ReportGenerator.generate_report_consolidation_sql(select_statement, output_path)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_report_consolidation_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        # Normalize SQL for whitespace-insensitive comparison
        def normalize_sql(sql: str) -> str:
            lines = [line.strip() for line in sql.strip().split('\n')]
            lines = [line for line in lines if line]
            return '\n'.join(lines)

        assert normalize_sql(sql) == normalize_sql(expected_sql)

    def test_returns_string(self):
        """Test that the function returns a string."""
        select_statement = "SELECT * FROM read_parquet('file.parquet')"
        output_path = "gs://bucket/report.csv"

        sql = ReportGenerator.generate_report_consolidation_sql(select_statement, output_path)

        assert isinstance(sql, str)
        assert len(sql) > 0


class TestReportGeneratorDateDatetimeDefaultCountSQL:
    """Tests for generate_date_datetime_default_count_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        field_name = "visit_start_date"
        default_value = "'1970-01-01'"

        sql = ReportGenerator.generate_date_datetime_default_count_sql(table_uri, field_name, default_value)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_date_datetime_default_count_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        # Normalize SQL for whitespace-insensitive comparison
        def normalize_sql(sql: str) -> str:
            lines = [line.strip() for line in sql.strip().split('\n')]
            lines = [line for line in lines if line]
            return '\n'.join(lines)

        assert normalize_sql(sql) == normalize_sql(expected_sql)

    def test_matches_golden_file_timestamp(self):
        """Test that generated SQL for TIMESTAMP field matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        field_name = "visit_start_datetime"
        default_value = "'1901-01-01 00:00:00'"

        sql = ReportGenerator.generate_date_datetime_default_count_sql(table_uri, field_name, default_value)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_date_datetime_default_count_sql_timestamp.sql', 'r') as f:
            expected_sql = f.read()

        # Normalize SQL for whitespace-insensitive comparison
        def normalize_sql(sql: str) -> str:
            lines = [line.strip() for line in sql.strip().split('\n')]
            lines = [line for line in lines if line]
            return '\n'.join(lines)

        assert normalize_sql(sql) == normalize_sql(expected_sql)


class TestReportGeneratorHelpers:
    """Tests for helper methods."""

    @patch('core.reporting.storage.get_uri')
    def test_get_tmp_artifacts_path(self, mock_get_uri):
        """Test _get_tmp_artifacts_path returns correct path."""
        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        path = generator._get_tmp_artifacts_path()

        expected_path = f"test-bucket/2025-01-15/{constants.ArtifactPaths.REPORT_TMP.value}"
        assert path == expected_path

    @patch('core.reporting.storage.get_uri')
    def test_get_output_path(self, mock_get_uri):
        """Test _get_output_path generates correct output URI."""
        mock_get_uri.return_value = "s3://test-bucket/2025-01-15/artifacts/reports/delivery_report_test_site_2025-01-15.csv"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        path = generator._get_output_path()

        # Verify storage.get_uri was called with correct path
        expected_call = (
            f"test-bucket/2025-01-15/{constants.ArtifactPaths.REPORT.value}"
            f"delivery_report_test_site_2025-01-15{constants.CSV}"
        )
        mock_get_uri.assert_called_with(expected_call)
        assert path == "s3://test-bucket/2025-01-15/artifacts/reports/delivery_report_test_site_2025-01-15.csv"


class TestGetReportTmpArtifactsPath:
    """Tests for standalone utility function."""

    def test_returns_correct_path_structure(self):
        """Test that path structure is correct."""
        bucket = "test-bucket"
        delivery_date = "2025-01-15"

        path = utils.get_report_tmp_artifacts_path(bucket, delivery_date)

        expected_path = f"test-bucket/2025-01-15/{constants.ArtifactPaths.REPORT_TMP.value}"
        assert path == expected_path

    def test_does_not_include_scheme_prefix(self):
        """Test that returned path does not include storage scheme prefix."""
        bucket = "test-bucket"
        delivery_date = "2025-01-15"

        path = utils.get_report_tmp_artifacts_path(bucket, delivery_date)

        # Should not start with s3:// or gs:// or file://
        assert not path.startswith("s3://")
        assert not path.startswith("gs://")
        assert not path.startswith("file://")


class TestTypeConceptBreakdownSQL:
    """Tests for generate_type_concept_breakdown_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        concept_uri = "gs://vocab-bucket/v5.0/optimized/concept.parquet"
        type_field = "visit_type_concept_id"

        sql = ReportGenerator.generate_type_concept_breakdown_sql(table_uri, concept_uri, type_field)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_type_concept_breakdown_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        assert sql.strip() == expected_sql.strip()

    def test_returns_string(self):
        """Test that the function returns a string."""
        table_uri = "gs://test-bucket/table.parquet"
        concept_uri = "gs://vocab-bucket/concept.parquet"
        type_field = "visit_type_concept_id"

        sql = ReportGenerator.generate_type_concept_breakdown_sql(table_uri, concept_uri, type_field)

        assert isinstance(sql, str)
        assert len(sql) > 0


class TestGetTablePath:
    """Tests for _get_table_path helper method."""

    @patch('core.reporting.storage.get_uri')
    @patch('core.reporting.utils.get_omop_etl_table_path')
    def test_omop_etl_path_structure(self, mock_get_omop_etl_table_path, mock_get_uri):
        """Test that OMOP_ETL tables use subdirectory structure."""
        # Configure mocks to return expected paths with URI prefix
        mock_get_omop_etl_table_path.return_value = "gs://test-bucket/2025-01-15/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        path = generator._get_table_path("visit_occurrence", constants.ArtifactPaths.OMOP_ETL)

        # Now expects URI prefix since method returns full storage URI
        expected = "gs://test-bucket/2025-01-15/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        assert path == expected
        mock_get_omop_etl_table_path.assert_called_once_with("test-bucket", "2025-01-15", "visit_occurrence")

    @patch('core.reporting.storage.get_uri')
    def test_converted_files_path_structure(self, mock_get_uri):
        """Test that CONVERTED_FILES tables use direct structure."""
        # Configure mock to add URI prefix to input path
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        path = generator._get_table_path("death", constants.ArtifactPaths.CONVERTED_FILES)

        # Now expects URI prefix since method returns full storage URI
        expected = "gs://test-bucket/2025-01-15/artifacts/converted_files/death.parquet"
        assert path == expected

    @patch('core.reporting.storage.get_uri')
    def test_derived_files_path_structure(self, mock_get_uri):
        """Test that DERIVED_FILES tables use direct structure."""
        # Configure mock to add URI prefix to input path
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        path = generator._get_table_path("observation_period", constants.ArtifactPaths.DERIVED_FILES)

        # Now expects URI prefix since method returns full storage URI
        expected = "gs://test-bucket/2025-01-15/artifacts/derived_files/observation_period.parquet"
        assert path == expected

    @patch('core.reporting.utils.get_omop_etl_table_path')
    @patch('core.reporting.storage.get_uri')
    def test_all_type_concept_tables(self, mock_get_uri, mock_get_omop_etl_table_path):
        """Test that all tables in REPORTING_TABLE_CONFIG generate valid paths."""
        # Configure mocks to return paths with URI prefix
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_get_omop_etl_table_path.side_effect = lambda bucket, date, table: f"gs://{bucket}/{date}/artifacts/omop_etl/{table}/{table}.parquet"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0 20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)

        # Test each table in REPORTING_TABLE_CONFIG
        for table_name, config in constants.REPORTING_TABLE_CONFIG.items():
            location = config["location"]
            path = generator._get_table_path(table_name, location)

            # Path should contain the table name and end with .parquet
            assert table_name in path
            assert path.endswith(".parquet")
            assert "test-bucket" in path
            assert "2025-01-15" in path
            assert location.value in path


class TestCreateTypeConceptBreakdownArtifacts:
    """Tests for _create_type_concept_breakdown_artifacts method."""

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_creates_artifacts_for_existing_tables(self, mock_get_uri, mock_file_exists,
                                                    mock_artifact, mock_execute_sql):
        """Test that artifacts are created for tables that exist."""
        # Setup mocks
        mock_file_exists.return_value = True
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_execute_sql.return_value = [
            (44818518, 'Inpatient Visit', 100),
            (9202, 'Outpatient Visit', 50)
        ]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_type_concept_breakdown_artifacts()

        # Should have created artifacts for the query results
        # We have 14 tables and each returns 2 results, but we need to account for concept table check
        assert mock_artifact.call_count > 0
        assert mock_artifact_instance.save_artifact.call_count > 0

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_skips_missing_concept_table(self, mock_get_uri, mock_file_exists, mock_execute_sql):
        """Test that method returns early when concept table doesn't exist."""
        # Concept table doesn't exist
        mock_file_exists.return_value = False
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_type_concept_breakdown_artifacts()

        # Should not execute any SQL
        mock_execute_sql.assert_not_called()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_artifact_values_are_correct(self, mock_get_uri, mock_file_exists,
                                         mock_artifact, mock_execute_sql):
        """Test that artifact values are correctly populated."""
        # Setup mocks - concept table exists, but only one data table
        def file_exists_side_effect(path):
            if 'concept.parquet' in path:
                return True
            if 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_execute_sql.return_value = [
            (44818518, 'Inpatient Visit', 100),
            (0, 'No matching concept', 5)
        ]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_type_concept_breakdown_artifacts()

        # Get artifact creation calls
        artifact_calls = mock_artifact.call_args_list

        # First artifact should be for Inpatient Visit
        first_call = artifact_calls[0].kwargs
        assert first_call['delivery_date'] == "2025-01-15"
        assert first_call['artifact_bucket'] == "test-bucket"
        assert first_call['concept_id'] == 44818518
        assert first_call['name'] == "Type concept breakdown: visit_occurrence"
        assert first_call['value_as_string'] == 'Inpatient Visit'
        assert first_call['value_as_concept_id'] == 44818518
        assert first_call['value_as_number'] == 100.0

        # Second artifact should be for NULL/0 values
        second_call = artifact_calls[1].kwargs
        assert second_call['concept_id'] == 0
        assert second_call['value_as_string'] == 'No matching concept'
        assert second_call['value_as_number'] == 5.0


class TestInvalidConceptIdSQL:
    """Tests for generate_invalid_concept_id_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        concept_uri = "gs://vocab-bucket/v5.0/optimized/concept.parquet"
        concept_field = "visit_concept_id"

        sql = ReportGenerator.generate_invalid_concept_id_sql(table_uri, concept_uri, concept_field)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_invalid_concept_id_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        assert sql.strip() == expected_sql.strip()

    def test_returns_string(self):
        """Test that the function returns a string."""
        table_uri = "gs://test-bucket/table.parquet"
        concept_uri = "gs://vocab-bucket/concept.parquet"
        concept_field = "visit_concept_id"

        sql = ReportGenerator.generate_invalid_concept_id_sql(table_uri, concept_uri, concept_field)

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_sql_contains_required_components(self):
        """Test that generated SQL contains all required components."""
        table_uri = "gs://test-bucket/table.parquet"
        concept_uri = "gs://vocab-bucket/concept.parquet"
        concept_field = "measurement_concept_id"

        sql = ReportGenerator.generate_invalid_concept_id_sql(table_uri, concept_uri, concept_field)

        # Check for key SQL components
        assert "SELECT COUNT(*) as invalid_count" in sql
        assert f"FROM read_parquet('{table_uri}') t" in sql
        assert f"LEFT JOIN read_parquet('{concept_uri}') c" in sql
        assert f"ON t.{concept_field} = c.concept_id" in sql
        assert f"WHERE t.{concept_field} IS NOT NULL" in sql
        assert f"AND t.{concept_field} != 0" in sql
        assert "AND c.concept_id IS NULL" in sql


class TestCreateInvalidConceptIdArtifacts:
    """Tests for _create_invalid_concept_id_artifacts method."""

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_creates_artifacts_for_invalid_concept_ids(self, mock_get_uri, mock_file_exists,
                                                       mock_artifact, mock_execute_sql):
        """Test that artifacts are created for tables with invalid concept_ids."""
        # Setup mocks - concept table exists, and one data table exists
        def file_exists_side_effect(path):
            if 'concept.parquet' in path:
                return True
            if 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        # Return 15 invalid concept_ids found for visit_concept_id
        mock_execute_sql.return_value = [(15,)]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_invalid_concept_id_artifacts()

        # Should have executed SQL and created artifacts
        assert mock_execute_sql.call_count > 0
        assert mock_artifact.call_count > 0
        assert mock_artifact_instance.save_artifact.call_count > 0

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_skips_missing_concept_table(self, mock_get_uri, mock_file_exists, mock_execute_sql):
        """Test that method returns early when concept table doesn't exist."""
        # Concept table doesn't exist
        mock_file_exists.return_value = False
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_invalid_concept_id_artifacts()

        # Should not execute any SQL
        mock_execute_sql.assert_not_called()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_artifact_values_are_correct(self, mock_get_uri, mock_file_exists,
                                         mock_artifact, mock_execute_sql):
        """Test that artifact values are correctly populated."""
        # Setup mocks - concept table exists, but only one data table with one field
        def file_exists_side_effect(path):
            if 'concept.parquet' in path:
                return True
            if 'condition_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        # Return 42 invalid concept_ids found
        mock_execute_sql.return_value = [(42,)]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_invalid_concept_id_artifacts()

        # Get artifact creation calls
        artifact_calls = mock_artifact.call_args_list

        # Should have created at least one artifact
        assert len(artifact_calls) > 0

        # Check first artifact values
        first_call = artifact_calls[0].kwargs
        assert first_call['delivery_date'] == "2025-01-15"
        assert first_call['artifact_bucket'] == "test-bucket"
        assert first_call['concept_id'] == 0
        assert 'Invalid concept_id count' in first_call['name']
        assert first_call['value_as_number'] == 42.0

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_checks_all_concept_id_fields(self, mock_get_uri, mock_file_exists,
                                          mock_artifact, mock_execute_sql):
        """Test that all concept_id fields in a table are checked."""
        # Setup mocks - concept table exists, visit_occurrence has multiple concept_id fields
        def file_exists_side_effect(path):
            if 'concept.parquet' in path:
                return True
            if 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        # Return different counts for each field
        mock_execute_sql.return_value = [(10,)]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_invalid_concept_id_artifacts()

        # visit_occurrence has: visit_concept_id, visit_type_concept_id,
        # admitted_from_concept_id, discharged_to_concept_id
        # So should check at least 4 concept_id fields (vocabulary_fields + type_field)
        # Plus other tables that exist
        assert mock_execute_sql.call_count >= 4

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_skips_missing_data_tables(self, mock_get_uri, mock_file_exists, mock_execute_sql):
        """Test that method skips tables that don't exist."""
        # Only concept table exists, no data tables
        def file_exists_side_effect(path):
            if 'concept.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_invalid_concept_id_artifacts()

        # Should not execute any SQL when no data tables exist
        mock_execute_sql.assert_not_called()


class TestPersonIdReferentialIntegritySQL:
    """Tests for generate_person_id_referential_integrity_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        person_uri = "gs://test-bucket/2025-01-01/artifacts/converted_files/person.parquet"

        sql = ReportGenerator.generate_person_id_referential_integrity_sql(table_uri, person_uri)

        # Load golden file
        with open('tests/reference/sql/reporting/generate_person_id_referential_integrity_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        assert sql.strip() == expected_sql.strip()

    def test_returns_string(self):
        """Test that the function returns a string."""
        table_uri = "gs://test-bucket/table.parquet"
        person_uri = "gs://test-bucket/person.parquet"

        sql = ReportGenerator.generate_person_id_referential_integrity_sql(table_uri, person_uri)

        assert isinstance(sql, str)
        assert len(sql) > 0


class TestCreatePersonIdReferentialIntegrityArtifacts:
    """Tests for _create_person_id_referential_integrity_artifacts method."""

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_creates_artifacts_for_tables_with_violations(self, mock_get_uri, mock_get_schema,
                                                         mock_file_exists, mock_artifact,
                                                         mock_execute_sql):
        """Test that artifacts are created for tables with person_id violations."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        # Person table and visit_occurrence table exist
        def file_exists_side_effect(path):
            if 'person.parquet' in path or 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect

        # Mock schema with person_id fields
        mock_get_schema.return_value = {
            "person": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 0
            },
            "visit_occurrence": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 1234
            }
        }

        # First call returns row count (for visit_occurrence), second call returns violation count
        mock_execute_sql.side_effect = [
            [(100,)],  # Row count for visit_occurrence
            [(5,)]     # Violation count for visit_occurrence
        ]

        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_person_id_referential_integrity_artifacts()

        # Check that artifact was created
        mock_artifact.assert_called_once()
        artifact_call = mock_artifact.call_args.kwargs

        assert artifact_call['delivery_date'] == "2025-01-15"
        assert artifact_call['artifact_bucket'] == "test-bucket"
        assert artifact_call['concept_id'] == 1234
        assert artifact_call['name'] == "Person_id referential integrity violation count: visit_occurrence"
        assert artifact_call['value_as_string'] == "visit_occurrence"
        assert artifact_call['value_as_concept_id'] == 1234
        assert artifact_call['value_as_number'] == 5.0

        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_creates_artifacts_with_zero_violations(self, mock_get_uri, mock_get_schema,
                                                    mock_file_exists, mock_artifact,
                                                    mock_execute_sql):
        """Test that artifacts are created even when there are no violations."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        # Person table and visit_occurrence table exist
        def file_exists_side_effect(path):
            if 'person.parquet' in path or 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect

        # Mock schema with person_id fields
        mock_get_schema.return_value = {
            "person": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 0
            },
            "visit_occurrence": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 1234
            }
        }

        # First call returns row count, second call returns zero violations
        mock_execute_sql.side_effect = [
            [(100,)],  # Row count for visit_occurrence
            [(0,)]     # No violations
        ]

        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_person_id_referential_integrity_artifacts()

        # Check that artifact was created with 0 violations
        mock_artifact.assert_called_once()
        artifact_call = mock_artifact.call_args.kwargs
        assert artifact_call['value_as_number'] == 0.0

        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.storage.get_uri')
    def test_returns_early_when_person_table_missing(self, mock_get_uri, mock_file_exists):
        """Test that method returns early when person table doesn't exist."""
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = False  # Person table doesn't exist

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        # Should complete without error
        generator._create_person_id_referential_integrity_artifacts()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_skips_empty_tables(self, mock_get_uri, mock_get_schema,
                               mock_file_exists, mock_artifact, mock_execute_sql):
        """Test that method skips tables with zero rows."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        # Both tables exist
        def file_exists_side_effect(path):
            if 'person.parquet' in path or 'visit_occurrence.parquet' in path:
                return True
            return False

        mock_file_exists.side_effect = file_exists_side_effect

        # Mock schema
        mock_get_schema.return_value = {
            "person": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 0
            },
            "visit_occurrence": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 1234
            }
        }

        # Return 0 rows for visit_occurrence
        mock_execute_sql.return_value = [(0,)]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_person_id_referential_integrity_artifacts()

        # Should only call execute_duckdb_sql once for row count, not for violation check
        assert mock_execute_sql.call_count == 1
        # Should not create any artifacts
        mock_artifact.assert_not_called()

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_skips_person_table_itself(self, mock_get_uri, mock_get_schema,
                                      mock_file_exists, mock_execute_sql):
        """Test that method doesn't check person table against itself."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = True

        # Only person table in schema
        mock_get_schema.return_value = {
            "person": {
                "columns": {"person_id": {"type": "BIGINT"}},
                "concept_id": 0
            }
        }

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_person_id_referential_integrity_artifacts()

        # Should not execute any SQL (person table is skipped)
        mock_execute_sql.assert_not_called()


class TestTimeSeriesRowCountSQL:
    """Tests for generate_time_series_row_count_sql static method."""

    def test_matches_golden_file(self):
        """Test that generated SQL matches the golden file."""
        table_uri = "gs://test-bucket/2025-01-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet"
        date_field = "visit_start_date"
        start_date = "1970-01-01"
        end_date = "2025-01-01"

        sql = ReportGenerator.generate_time_series_row_count_sql(
            table_uri, date_field, start_date, end_date
        )

        # Load golden file
        with open('tests/reference/sql/reporting/generate_time_series_row_count_sql_standard.sql', 'r') as f:
            expected_sql = f.read()

        # Normalize SQL for whitespace-insensitive comparison
        def normalize_sql(sql: str) -> str:
            lines = [line.strip() for line in sql.strip().split('\n')]
            lines = [line for line in lines if line]
            return '\n'.join(lines)

        assert normalize_sql(sql) == normalize_sql(expected_sql)

    def test_uses_correct_date_field(self):
        """Test that SQL uses the specified date field."""
        table_uri = "gs://bucket/table.parquet"
        date_field = "measurement_date"
        start_date = "1970-01-01"
        end_date = "2025-01-01"

        sql = ReportGenerator.generate_time_series_row_count_sql(
            table_uri, date_field, start_date, end_date
        )

        # Verify the date field appears in the SQL
        assert "measurement_date" in sql
        assert "EXTRACT(YEAR FROM measurement_date)" in sql

    def test_includes_date_range_filter(self):
        """Test that SQL includes proper date range filtering."""
        table_uri = "gs://bucket/table.parquet"
        date_field = "observation_date"
        start_date = "1970-01-01"
        end_date = "2025-12-31"

        sql = ReportGenerator.generate_time_series_row_count_sql(
            table_uri, date_field, start_date, end_date
        )

        # Verify date range filters are present
        assert ">= '1970-01-01'" in sql
        assert "<= '2025-12-31'" in sql
        assert "IS NOT NULL" in sql


class TestCreateTimeSeriesRowCountArtifacts:
    """Tests for _create_time_series_row_count_artifacts method."""

    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_creates_artifacts_for_each_year(self, mock_get_uri, mock_get_schema,
                                            mock_file_exists, mock_execute_sql, mock_artifact):
        """Test that method creates one artifact per year with data."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = True

        # Mock schema
        mock_get_schema.return_value = {
            "visit_occurrence": {
                "columns": {"visit_occurrence_id": {"type": "BIGINT"}},
                "concept_id": 1147332
            }
        }

        # Mock SQL result: 3 years with data
        mock_execute_sql.return_value = [
            (2020, 100),
            (2021, 150),
            (2022, 200)
        ]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_time_series_row_count_artifacts()

        # Should create 3 artifacts (one per year) for each of the 10 tables
        # But since only visit_occurrence exists in the mock, we get 3 artifacts
        artifact_call_count = sum(1 for call in mock_artifact.call_args_list if call[1].get('name', '').startswith('Time series row count:'))
        assert artifact_call_count >= 3

    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_artifact_naming_format(self, mock_get_uri, mock_get_schema,
                                   mock_file_exists, mock_execute_sql, mock_artifact):
        """Test that artifacts use correct naming format."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = True

        mock_get_schema.return_value = {
            "measurement": {
                "columns": {"measurement_id": {"type": "BIGINT"}},
                "concept_id": 1147314
            }
        }

        # Mock SQL result
        mock_execute_sql.return_value = [(2023, 50)]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_time_series_row_count_artifacts()

        # Find the measurement artifact
        measurement_artifact_calls = [
            call for call in mock_artifact.call_args_list
            if 'measurement.2023' in str(call)
        ]
        assert len(measurement_artifact_calls) > 0

        # Verify the artifact was created with correct name format
        found = False
        for call in mock_artifact.call_args_list:
            if call[1].get('name') == 'Time series row count: measurement.2023':
                found = True
                assert call[1]['value_as_string'] == 'measurement.2023'
                assert call[1]['value_as_number'] == 50.0
                assert call[1]['concept_id'] == 1147314
        assert found

    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_skips_nonexistent_tables(self, mock_get_uri, mock_get_schema,
                                     mock_file_exists, mock_execute_sql):
        """Test that method skips tables that don't exist."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = False  # No tables exist

        mock_get_schema.return_value = {
            "visit_occurrence": {
                "columns": {"visit_occurrence_id": {"type": "BIGINT"}},
                "concept_id": 1147332
            }
        }

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_time_series_row_count_artifacts()

        # Should not execute any SQL if tables don't exist
        mock_execute_sql.assert_not_called()

    @patch('core.reporting.report_artifact.ReportArtifact')
    @patch('core.reporting.utils.execute_duckdb_sql')
    @patch('core.reporting.utils.parquet_file_exists')
    @patch('core.reporting.utils.get_cdm_schema')
    @patch('core.reporting.storage.get_uri')
    def test_uses_correct_date_fields_per_table(self, mock_get_uri, mock_get_schema,
                                               mock_file_exists, mock_execute_sql, mock_artifact):
        """Test that method uses the correct start date field for each table."""
        # Setup mocks
        mock_get_uri.side_effect = lambda path: f"gs://{path}"
        mock_file_exists.return_value = True

        # Define schema for multiple tables
        mock_get_schema.return_value = {
            "visit_occurrence": {
                "columns": {"visit_occurrence_id": {"type": "BIGINT"}},
                "concept_id": 1147332
            },
            "drug_exposure": {
                "columns": {"drug_exposure_id": {"type": "BIGINT"}},
                "concept_id": 1147330
            },
            "measurement": {
                "columns": {"measurement_id": {"type": "BIGINT"}},
                "concept_id": 1147314
            }
        }

        # Mock SQL result
        mock_execute_sql.return_value = [(2023, 10)]

        report_data = {
            "site": "test_site",
            "bucket": "test-bucket",
            "delivery_date": "2025-01-15",
            "site_display_name": "Test Site",
            "file_delivery_format": "parquet",
            "delivered_cdm_version": "5.3",
            "target_vocabulary_version": "v5.0_20-MAR-24",
            "target_cdm_version": "5.4"
        }

        generator = ReportGenerator(report_data)
        generator._create_time_series_row_count_artifacts()

        # Verify SQL was generated with correct date fields
        sql_calls = [str(call) for call in mock_execute_sql.call_args_list]

        # Check that appropriate date fields were used
        assert any('visit_start_date' in call for call in sql_calls)
        assert any('drug_exposure_start_date' in call for call in sql_calls)
        assert any('measurement_date' in call for call in sql_calls)
