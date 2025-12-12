"""
Unit tests for reporting.py ReportGenerator class.

Tests report generation including metadata artifacts creation
and consolidation of temporary report files.
"""

from datetime import datetime
from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
from core.reporting import ReportGenerator, get_report_tmp_artifacts_path


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
    @patch.object(ReportGenerator, '_create_metadata_artifacts')
    def test_generate_calls_both_methods(self, mock_create_metadata, mock_consolidate):
        """Test that generate calls both metadata and consolidation methods."""
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
        mock_consolidate.assert_called_once()

    @patch.object(ReportGenerator, '_consolidate_report_files')
    @patch.object(ReportGenerator, '_create_metadata_artifacts')
    def test_generate_calls_in_correct_order(self, mock_create_metadata, mock_consolidate):
        """Test that metadata is created before consolidation."""
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
        mock_consolidate.side_effect = lambda: call_order.append('consolidate')

        generator = ReportGenerator(report_data)
        generator.generate()

        assert call_order == ['metadata', 'consolidate']


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

    def test_generates_valid_sql_structure(self):
        """Test that SQL has correct structure."""
        select_statement = "SELECT * FROM read_parquet('file1.parquet')"
        output_path = "s3://bucket/report.csv"

        sql = ReportGenerator.generate_report_consolidation_sql(select_statement, output_path)

        # Check key SQL components
        assert "SET max_expression_depth TO 1000000" in sql
        assert "COPY (" in sql
        assert select_statement in sql
        assert f"TO '{output_path}'" in sql
        assert "(HEADER, DELIMITER ',')" in sql

    def test_preserves_union_all_statement(self):
        """Test that UNION ALL statements are preserved in generated SQL."""
        select_statement = (
            "SELECT * FROM read_parquet('file1.parquet') UNION ALL "
            "SELECT * FROM read_parquet('file2.parquet') UNION ALL "
            "SELECT * FROM read_parquet('file3.parquet')"
        )
        output_path = "s3://bucket/report.csv"

        sql = ReportGenerator.generate_report_consolidation_sql(select_statement, output_path)

        # Verify UNION ALL is preserved
        assert select_statement in sql
        assert sql.count("UNION ALL") == 2

    def test_handles_different_output_paths(self):
        """Test that different output path formats are handled correctly."""
        select_statement = "SELECT * FROM read_parquet('file.parquet')"

        # Test various path formats
        paths = [
            "s3://bucket/report.csv",
            "/local/path/report.csv",
            "gs://bucket/report.csv"
        ]

        for path in paths:
            sql = ReportGenerator.generate_report_consolidation_sql(select_statement, path)
            assert f"TO '{path}'" in sql


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

        path = get_report_tmp_artifacts_path(bucket, delivery_date)

        expected_path = f"test-bucket/2025-01-15/{constants.ArtifactPaths.REPORT_TMP.value}"
        assert path == expected_path

    def test_does_not_include_scheme_prefix(self):
        """Test that returned path does not include storage scheme prefix."""
        bucket = "test-bucket"
        delivery_date = "2025-01-15"

        path = get_report_tmp_artifacts_path(bucket, delivery_date)

        # Should not start with s3:// or gs:// or file://
        assert not path.startswith("s3://")
        assert not path.startswith("gs://")
        assert not path.startswith("file://")
