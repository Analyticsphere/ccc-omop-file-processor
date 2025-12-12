"""
Unit tests for omop_client.py OMOPClient class.

Tests OMOP CDM operations including file upgrades, BigQuery table creation,
cdm_source population, and derived data generation.
"""

from unittest.mock import MagicMock, call, mock_open, patch

import pytest

import core.constants as constants
from core.omop_client import OMOPClient


class TestOMOPClientUpgradeFile:
    """Tests for upgrade_file method."""

    @patch('core.omop_client.storage.delete_file')
    @patch('core.omop_client.utils.get_table_name_from_path')
    @patch('core.omop_client.utils.get_parquet_artifact_location')
    def test_upgrade_file_no_upgrade_needed(self, mock_get_location, mock_get_table_name, mock_delete):
        """Test that no upgrade happens when versions match."""
        mock_get_location.return_value = "bucket/2025-01-01/artifacts/converted_files/person.parquet"
        mock_get_table_name.return_value = "person"

        OMOPClient.upgrade_file(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            target_omop_version="5.4"
        )

        # Should not attempt any operations
        mock_delete.assert_not_called()

    @patch('core.omop_client.storage.delete_file')
    @patch('core.omop_client.utils.get_table_name_from_path')
    @patch('core.omop_client.utils.get_parquet_artifact_location')
    def test_upgrade_file_table_removed(self, mock_get_location, mock_get_table_name, mock_delete):
        """Test that table is deleted when marked as REMOVED in upgrade."""
        mock_get_location.return_value = "bucket/2025-01-01/artifacts/converted_files/attribute_definition.parquet"
        mock_get_table_name.return_value = "attribute_definition"

        OMOPClient.upgrade_file(
            file_path="bucket/2025-01-01/attribute_definition.parquet",
            cdm_version="5.3",
            target_omop_version="5.4"
        )

        # Should delete the file
        mock_delete.assert_called_once()

    @patch('core.omop_client.utils.execute_duckdb_sql')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM table")
    @patch('core.omop_client.utils.get_table_name_from_path')
    @patch('core.omop_client.utils.get_parquet_artifact_location')
    def test_upgrade_file_table_changed(self, mock_get_location, mock_get_table_name, mock_file, mock_execute):
        """Test that SQL upgrade script is applied when table is marked as CHANGED."""
        mock_get_location.return_value = "bucket/2025-01-01/artifacts/converted_files/measurement.parquet"
        mock_get_table_name.return_value = "measurement"

        OMOPClient.upgrade_file(
            file_path="bucket/2025-01-01/measurement.parquet",
            cdm_version="5.3",
            target_omop_version="5.4"
        )

        # Should read upgrade script and execute SQL
        mock_file.assert_called_once()
        mock_execute.assert_called_once()

    @patch('core.omop_client.utils.get_table_name_from_path')
    @patch('core.omop_client.utils.get_parquet_artifact_location')
    def test_upgrade_file_unsupported_version(self, mock_get_location, mock_get_table_name):
        """Test that exception is raised for unsupported CDM version."""
        mock_get_location.return_value = "bucket/2025-01-01/artifacts/converted_files/person.parquet"
        mock_get_table_name.return_value = "person"

        with pytest.raises(Exception) as exc_info:
            OMOPClient.upgrade_file(
                file_path="bucket/2025-01-01/person.parquet",
                cdm_version="5.2",
                target_omop_version="5.4"
            )

        assert "not supported" in str(exc_info.value)


class TestOMOPClientCreateMissingBQTables:
    """Tests for create_missing_bq_tables method."""

    @patch('core.omop_client.gcp_services.execute_bq_sql')
    def test_create_missing_bq_tables_success(self, mock_execute_bq):
        """Test successful BigQuery table creation."""
        ddl_content = "CREATE OR REPLACE TABLE @cdmDatabaseSchema.person (...)"

        with patch('builtins.open', mock_open(read_data=ddl_content)):
            OMOPClient.create_missing_bq_tables(
                project_id="my-project",
                dataset_id="my-dataset",
                omop_version="5.4"
            )

        # Should execute SQL in BigQuery
        mock_execute_bq.assert_called_once()

        # Verify placeholder was replaced
        call_args = mock_execute_bq.call_args[0][0]
        assert "my-project.my-dataset" in call_args
        assert "@cdmDatabaseSchema" not in call_args

    @patch('builtins.open', side_effect=FileNotFoundError("DDL file not found"))
    def test_create_missing_bq_tables_file_not_found(self, mock_file):
        """Test that exception is raised when DDL file not found."""
        with pytest.raises(Exception) as exc_info:
            OMOPClient.create_missing_bq_tables(
                project_id="my-project",
                dataset_id="my-dataset",
                omop_version="5.4"
            )

        assert "DDL file error" in str(exc_info.value)


class TestOMOPClientPopulateCdmSourceFile:
    """Tests for populate_cdm_source_file method."""

    @patch('core.omop_client.utils.execute_duckdb_sql')
    @patch('core.omop_client.utils.get_delivery_vocabulary_version')
    @patch('core.omop_client.utils.parquet_file_exists')
    def test_populate_cdm_source_file_does_not_exist(self, mock_file_exists, mock_get_vocab, mock_execute):
        """Test populating cdm_source when file doesn't exist."""
        mock_file_exists.return_value = False
        mock_get_vocab.return_value = "v5.0_24-JAN-25"

        cdm_source_data = {
            "gcs_bucket": "gs://test-bucket",
            "source_release_date": "2025-01-01",
            "cdm_source_name": "Test Site",
            "cdm_source_abbreviation": "test",
            "cdm_holder": "NIH/NCI",
            "source_description": "Test data",
            "cdm_release_date": "2025-01-15",
            "cdm_version": "5.4"
        }

        OMOPClient.populate_cdm_source_file(cdm_source_data)

        # Should execute SQL to populate file
        mock_execute.assert_called_once()

    @patch('core.omop_client.utils.execute_duckdb_sql')
    @patch('core.omop_client.utils.get_delivery_vocabulary_version')
    @patch('core.omop_client.utils.parquet_file_exists')
    def test_populate_cdm_source_file_exists_but_empty(self, mock_file_exists, mock_get_vocab, mock_execute):
        """Test populating cdm_source when file exists but is empty."""
        mock_file_exists.return_value = True
        mock_get_vocab.return_value = "v5.0_24-JAN-25"

        # First call for row count, second call for population
        mock_execute.side_effect = [
            [[0]],  # Row count is 0
            None    # Population succeeds
        ]

        cdm_source_data = {
            "gcs_bucket": "gs://test-bucket",
            "source_release_date": "2025-01-01",
            "cdm_source_name": "Test Site",
            "cdm_source_abbreviation": "test",
            "cdm_holder": "NIH/NCI",
            "source_description": "Test data",
            "cdm_release_date": "2025-01-15",
            "cdm_version": "5.4"
        }

        OMOPClient.populate_cdm_source_file(cdm_source_data)

        # Should call execute twice (row count + population)
        assert mock_execute.call_count == 2

    @patch('core.omop_client.utils.execute_duckdb_sql')
    @patch('core.omop_client.utils.parquet_file_exists')
    def test_populate_cdm_source_file_exists_with_data(self, mock_file_exists, mock_execute):
        """Test that cdm_source is not populated when file exists with data."""
        mock_file_exists.return_value = True
        mock_execute.return_value = [[5]]  # File has 5 rows

        cdm_source_data = {
            "gcs_bucket": "gs://test-bucket",
            "source_release_date": "2025-01-01",
            "cdm_source_name": "Test Site",
            "cdm_source_abbreviation": "test",
            "cdm_holder": "NIH/NCI",
            "source_description": "Test data",
            "cdm_release_date": "2025-01-15",
            "cdm_version": "5.4"
        }

        OMOPClient.populate_cdm_source_file(cdm_source_data)

        # Should only call execute once for row count, not for population
        assert mock_execute.call_count == 1


class TestOMOPClientGenerateDerivedDataFromHarmonized:
    """Tests for generate_derived_data_from_harmonized method."""

    def test_generate_derived_data_invalid_table_name(self):
        """Test that exception is raised for invalid derived data table name."""
        with pytest.raises(Exception) as exc_info:
            OMOPClient.generate_derived_data_from_harmonized(
                site="test_site",
                bucket="gs://test-bucket",
                delivery_date="2025-01-01",
                table_name="invalid_table",
                vocab_version="v5.0_24-JAN-25",
                vocab_path="gs://vocab-bucket/vocab"
            )

        assert "not a derived data table" in str(exc_info.value)

    @patch('core.omop_client.utils.parquet_file_exists')
    def test_generate_derived_data_missing_required_table(self, mock_file_exists):
        """Test that function returns early when required table is missing."""
        mock_file_exists.return_value = False

        OMOPClient.generate_derived_data_from_harmonized(
            site="test_site",
            bucket="gs://test-bucket",
            delivery_date="2025-01-01",
            table_name="observation_period",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="gs://vocab-bucket/vocab"
        )

        # Should return early without raising exception
        mock_file_exists.assert_called()

    @patch('core.omop_client.utils.execute_duckdb_sql')
    @patch('builtins.open', new_callable=mock_open, read_data="SELECT * FROM table")
    @patch('core.omop_client.utils.placeholder_to_harmonized_file_path')
    @patch('core.omop_client.utils.parquet_file_exists')
    def test_generate_derived_data_success(self, mock_file_exists, mock_placeholder, mock_file, mock_execute):
        """Test successful derived data generation."""
        mock_file_exists.return_value = True
        mock_placeholder.return_value = "SELECT * FROM table WITH PATHS"

        OMOPClient.generate_derived_data_from_harmonized(
            site="test_site",
            bucket="gs://test-bucket",
            delivery_date="2025-01-01",
            table_name="observation_period",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="gs://vocab-bucket/vocab"
        )

        # Should read SQL file and execute
        mock_file.assert_called()
        mock_execute.assert_called_once()


class TestOMOPClientStaticMethods:
    """Tests for static methods that generate SQL."""

    def test_generate_upgrade_file_sql(self):
        """Test SQL generation for file upgrade."""
        sql = OMOPClient.generate_upgrade_file_sql(
            upgrade_script="SELECT * FROM table",
            normalized_file_path="bucket/2025-01-01/artifacts/converted_files/person.parquet"
        )

        assert "COPY (" in sql
        assert "SELECT * FROM table" in sql
        assert "read_parquet" in sql

    def test_generate_populate_cdm_source_sql(self):
        """Test SQL generation for cdm_source population."""
        cdm_source_data = {
            "cdm_source_name": "Test Site",
            "cdm_source_abbreviation": "test",
            "cdm_holder": "NIH/NCI",
            "source_description": "Test data",
            "source_release_date": "2025-01-01",
            "cdm_release_date": "2025-01-15",
            "cdm_version": "5.4"
        }

        sql = OMOPClient.generate_populate_cdm_source_sql(
            cdm_source_data=cdm_source_data,
            vocab_version="v5.0_24-JAN-25",
            output_path="gs://test-bucket/cdm_source.parquet"
        )

        assert "COPY (" in sql
        assert "Test Site" in sql
        assert "v5.0_24-JAN-25" in sql
        assert "5.4" in sql
