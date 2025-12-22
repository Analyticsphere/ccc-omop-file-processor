"""
Unit tests for endpoints.py Flask application.

Tests all API endpoints for success scenarios, parameter validation,
and error handling.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

import core.constants as constants
from core.endpoints import app


@pytest.fixture
def client():
    """Create Flask test client."""
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client


class TestHeartbeatEndpoint:
    """Tests for /heartbeat endpoint."""

    def test_heartbeat_returns_healthy_status(self, client):
        """Test that heartbeat returns 200 with healthy status."""
        response = client.get('/heartbeat')
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['status'] == 'healthy'
        assert data['service'] == constants.SERVICE_NAME
        assert 'timestamp' in data


class TestCreateOptimizedVocabEndpoint:
    """Tests for /create_optimized_vocab endpoint."""

    @patch('core.endpoints.vocab_manager.VocabularyManager')
    def test_create_optimized_vocab_success(self, mock_manager, client):
        """Test successful vocabulary optimization."""
        mock_instance = MagicMock()
        mock_manager.return_value = mock_instance

        response = client.post('/create_optimized_vocab', json={
            'vocab_version': 'v5.0_24-JAN-25'
        })

        assert response.status_code == 200
        assert b"Created optimized vocabulary files" in response.data
        mock_instance.convert_to_parquet.assert_called_once()
        mock_instance.create_optimized_vocab_file.assert_called_once()

    def test_create_optimized_vocab_missing_parameter(self, client):
        """Test missing vocab_version parameter returns 400."""
        response = client.post('/create_optimized_vocab', json={})

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.vocab_manager.VocabularyManager')
    def test_create_optimized_vocab_exception(self, mock_manager, client):
        """Test exception handling returns 500."""
        mock_manager.side_effect = Exception("Vocab creation failed")

        response = client.post('/create_optimized_vocab', json={
            'vocab_version': 'v5.0_24-JAN-25'
        })

        assert response.status_code == 500
        assert b"Error creating optimized vocabulary" in response.data


class TestCreateArtifactDirectoriesEndpoint:
    """Tests for /create_artifact_directories endpoint."""

    @patch('core.endpoints.storage.create_directory')
    def test_create_artifact_directories_success(self, mock_create, client):
        """Test successful directory creation."""
        response = client.post('/create_artifact_directories', json={
            'delivery_bucket': 'test-bucket/2025-01-01'
        })

        assert response.status_code == 200
        assert b"Directories created successfully" in response.data
        # Verify create_directory was called for each artifact path
        assert mock_create.call_count == len(constants.ArtifactPaths)

    def test_create_artifact_directories_missing_parameter(self, client):
        """Test missing delivery_bucket returns 400."""
        response = client.post('/create_artifact_directories', json={})

        assert response.status_code == 400
        assert b"Missing required parameter" in response.data

    @patch('core.endpoints.storage.create_directory')
    def test_create_artifact_directories_exception(self, mock_create, client):
        """Test exception handling returns 500."""
        mock_create.side_effect = Exception("Directory creation failed")

        response = client.post('/create_artifact_directories', json={
            'delivery_bucket': 'test-bucket/2025-01-01'
        })

        assert response.status_code == 500
        assert b"Unable to create artifact directories" in response.data


class TestGetLogRowEndpoint:
    """Tests for /get_log_row endpoint."""

    @patch('core.endpoints.gcp_services.get_bq_log_row')
    def test_get_log_row_success(self, mock_get_row, client):
        """Test successful log row retrieval."""
        mock_get_row.return_value = ['site1', '2025-01-01', 'completed']

        response = client.get('/get_log_row?site=site1&delivery_date=2025-01-01')
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['status'] == 'healthy'
        assert data['log_row'] == ['site1', '2025-01-01', 'completed']

    def test_get_log_row_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.get('/get_log_row?site=site1')

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.gcp_services.get_bq_log_row')
    def test_get_log_row_exception(self, mock_get_row, client):
        """Test exception handling returns 500."""
        mock_get_row.side_effect = Exception("BigQuery error")

        response = client.get('/get_log_row?site=site1&delivery_date=2025-01-01')

        assert response.status_code == 500
        assert b"Unable to get get BigQuery log row" in response.data


class TestGetFileListEndpoint:
    """Tests for /get_file_list endpoint."""

    @patch('core.endpoints.utils.list_files')
    def test_get_file_list_success(self, mock_list, client):
        """Test successful file listing."""
        mock_list.return_value = ['person.csv', 'observation.csv']

        response = client.get('/get_file_list?bucket=test-bucket&folder=incoming&file_format=csv')
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['file_list'] == ['person.csv', 'observation.csv']

    def test_get_file_list_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.get('/get_file_list?bucket=test-bucket')

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.utils.list_files')
    def test_get_file_list_exception(self, mock_list, client):
        """Test exception handling returns 500."""
        mock_list.side_effect = Exception("Listing failed")

        response = client.get('/get_file_list?bucket=test-bucket&folder=incoming&file_format=csv')

        assert response.status_code == 500
        assert b"Unable to get list of files" in response.data


class TestProcessIncomingFileEndpoint:
    """Tests for /process_incoming_file endpoint."""

    @patch('core.endpoints.file_processor.FileProcessor')
    def test_process_file_success(self, mock_processor, client):
        """Test successful file processing."""
        mock_instance = MagicMock()
        mock_processor.return_value = mock_instance

        response = client.post('/process_incoming_file', json={
            'file_type': 'csv',
            'file_path': 'bucket/2025-01-01/person.csv'
        })

        assert response.status_code == 200
        assert b"Converted file to Parquet" in response.data
        mock_instance.process.assert_called_once()

    def test_process_file_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/process_incoming_file', json={
            'file_type': 'csv'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.file_processor.FileProcessor')
    def test_process_file_exception(self, mock_processor, client):
        """Test exception handling returns 500."""
        mock_processor.side_effect = Exception("Processing failed")

        response = client.post('/process_incoming_file', json={
            'file_type': 'csv',
            'file_path': 'bucket/2025-01-01/person.csv'
        })

        assert response.status_code == 500
        assert b"Unable to convert files to Parquet" in response.data


class TestValidateFileEndpoint:
    """Tests for /validate_file endpoint."""

    @patch('core.endpoints.file_validation.FileValidator')
    def test_validate_file_success(self, mock_validator, client):
        """Test successful file validation."""
        mock_instance = MagicMock()
        mock_validator.return_value = mock_instance

        response = client.post('/validate_file', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'omop_version': '5.4',
            'delivery_date': '2025-01-01',
            'storage_path': 'bucket/2025-01-01'
        })

        assert response.status_code == 200
        assert b"File successfully validated" in response.data
        mock_instance.validate.assert_called_once()

    def test_validate_file_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/validate_file', json={
            'file_path': 'bucket/2025-01-01/person.parquet'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.file_validation.FileValidator')
    def test_validate_file_exception(self, mock_validator, client):
        """Test exception handling returns 500."""
        mock_validator.side_effect = Exception("Validation failed")

        response = client.post('/validate_file', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'omop_version': '5.4',
            'delivery_date': '2025-01-01',
            'storage_path': 'bucket/2025-01-01'
        })

        assert response.status_code == 500
        assert b"Unable to run file validation" in response.data


class TestNormalizeParquetEndpoint:
    """Tests for /normalize_parquet endpoint."""

    @patch('core.endpoints.normalization.Normalizer')
    @patch('core.endpoints.utils.get_parquet_artifact_location')
    def test_normalize_parquet_success(self, mock_get_path, mock_normalizer, client):
        """Test successful parquet normalization."""
        mock_get_path.return_value = 'bucket/2025-01-01/parquet/person.parquet'
        mock_instance = MagicMock()
        mock_normalizer.return_value = mock_instance

        response = client.post('/normalize_parquet', json={
            'file_path': 'bucket/2025-01-01/person.csv',
            'omop_version': '5.4',
            'date_format': '%Y-%m-%d',
            'datetime_format': '%Y-%m-%d %H:%M:%S'
        })

        assert response.status_code == 200
        assert b"Normalized Parquet file" in response.data
        mock_instance.normalize.assert_called_once()

    def test_normalize_parquet_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/normalize_parquet', json={
            'file_path': 'bucket/2025-01-01/person.parquet'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.normalization.Normalizer')
    @patch('core.endpoints.utils.get_parquet_artifact_location')
    def test_normalize_parquet_exception(self, mock_get_path, mock_normalizer, client):
        """Test exception handling returns 500."""
        mock_get_path.return_value = 'bucket/2025-01-01/parquet/person.parquet'
        mock_normalizer.side_effect = Exception("Normalization failed")

        response = client.post('/normalize_parquet', json={
            'file_path': 'bucket/2025-01-01/person.csv',
            'omop_version': '5.4',
            'date_format': '%Y-%m-%d',
            'datetime_format': '%Y-%m-%d %H:%M:%S'
        })

        assert response.status_code == 500
        assert b"Unable to normalize Parquet file" in response.data


class TestUpgradeCdmEndpoint:
    """Tests for /upgrade_cdm endpoint."""

    @patch('core.endpoints.omop_client.OMOPClient.upgrade_file')
    def test_upgrade_cdm_success(self, mock_upgrade, client):
        """Test successful CDM upgrade."""
        response = client.post('/upgrade_cdm', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'omop_version': '5.3',
            'target_omop_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Upgraded file" in response.data
        mock_upgrade.assert_called_once_with('bucket/2025-01-01/person.parquet', '5.3', '5.4')

    def test_upgrade_cdm_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/upgrade_cdm', json={
            'file_path': 'bucket/2025-01-01/person.parquet'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.omop_client.OMOPClient.upgrade_file')
    def test_upgrade_cdm_exception(self, mock_upgrade, client):
        """Test exception handling returns 500."""
        mock_upgrade.side_effect = Exception("Upgrade failed")

        response = client.post('/upgrade_cdm', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'omop_version': '5.3',
            'target_omop_version': '5.4'
        })

        assert response.status_code == 500
        assert b"Unable to upgrade file" in response.data


class TestClearBqDatasetEndpoint:
    """Tests for /clear_bq_dataset endpoint."""

    @patch('core.endpoints.gcp_services.remove_all_tables')
    def test_clear_bq_dataset_success(self, mock_remove, client):
        """Test successful BigQuery dataset clearing."""
        response = client.post('/clear_bq_dataset', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 200
        assert b"Removed all tables" in response.data
        mock_remove.assert_called_once_with('test-project', 'test_dataset')

    def test_clear_bq_dataset_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/clear_bq_dataset', json={
            'project_id': 'test-project'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.gcp_services.remove_all_tables')
    def test_clear_bq_dataset_exception(self, mock_remove, client):
        """Test exception handling returns 500."""
        mock_remove.side_effect = Exception("Removal failed")

        response = client.post('/clear_bq_dataset', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 500
        assert b"Unable to delete tables within dataset" in response.data


class TestHarmonizeVocabEndpoint:
    """Tests for /harmonize_vocab endpoint."""

    @patch('core.endpoints.vocab_harmonization.VocabHarmonizer')
    def test_harmonize_vocab_standard_step(self, mock_harmonizer, client):
        """Test vocabulary harmonization with standard step."""
        mock_instance = MagicMock()
        mock_instance.perform_harmonization.return_value = None
        mock_harmonizer.return_value = mock_instance

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/observation.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'omop_version': '5.4',
            'site': 'test_site',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'step': constants.SOURCE_TARGET
        })

        data = json.loads(response.data)
        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['step'] == constants.SOURCE_TARGET

    @patch('core.endpoints.vocab_harmonization.VocabHarmonizer')
    def test_harmonize_vocab_discover_step(self, mock_harmonizer, client):
        """Test vocabulary harmonization with discovery step returns table configs."""
        mock_instance = MagicMock()
        mock_instance.perform_harmonization.return_value = [
            {'table': 'observation', 'config': 'test'}
        ]
        mock_harmonizer.return_value = mock_instance

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/observation.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'omop_version': '5.4',
            'site': 'test_site',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'step': constants.DISCOVER_TABLES_FOR_DEDUP
        })

        data = json.loads(response.data)
        assert response.status_code == 200
        assert data['status'] == 'success'
        assert 'table_configs' in data

    def test_harmonize_vocab_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/observation.parquet'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.vocab_harmonization.VocabHarmonizer')
    def test_harmonize_vocab_exception(self, mock_harmonizer, client):
        """Test exception handling returns 500."""
        mock_harmonizer.side_effect = Exception("Harmonization failed")

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/observation.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'omop_version': '5.4',
            'site': 'test_site',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'step': constants.SOURCE_TARGET
        })

        assert response.status_code == 500
        assert b"Unable to harmonize vocabulary" in response.data


class TestGenerateDerivedTablesEndpoint:
    """Tests for /generate_derived_tables_from_harmonized endpoint."""

    @patch('core.endpoints.omop_client.OMOPClient.generate_derived_data_from_harmonized')
    def test_generate_derived_tables_success(self, mock_generate, client):
        """Test successful derived table generation."""
        response = client.post('/generate_derived_tables_from_harmonized', json={
            'site': 'test_site',
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'table_name': 'drug_era',
            'vocab_version': 'v5.0_24-JAN-25'
        })

        assert response.status_code == 200
        assert b"Created derived table from harmonized data" in response.data

    def test_generate_derived_tables_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/generate_derived_tables_from_harmonized', json={
            'site': 'test_site'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.omop_client.OMOPClient.generate_derived_data_from_harmonized')
    def test_generate_derived_tables_exception(self, mock_generate, client):
        """Test exception handling returns 500."""
        mock_generate.side_effect = Exception("Generation failed")

        response = client.post('/generate_derived_tables_from_harmonized', json={
            'site': 'test_site',
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'table_name': 'drug_era',
            'vocab_version': 'v5.0_24-JAN-25'
        })

        assert response.status_code == 500
        assert b"Unable to create derived table" in response.data


class TestLoadTargetVocabEndpoint:
    """Tests for /load_target_vocab endpoint."""

    @patch('core.endpoints.vocab_manager.VocabularyManager')
    def test_load_target_vocab_success(self, mock_manager, client):
        """Test successful vocabulary loading to BigQuery."""
        mock_instance = MagicMock()
        mock_manager.return_value = mock_instance

        response = client.post('/load_target_vocab', json={
            'table_file_name': 'CONCEPT.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 200
        assert b"Successfully loaded vocabulary" in response.data

    def test_load_target_vocab_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/load_target_vocab', json={
            'vocab_version': 'v5.0_24-JAN-25'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.vocab_manager.VocabularyManager')
    def test_load_target_vocab_exception(self, mock_manager, client):
        """Test exception handling returns 500."""
        mock_manager.side_effect = Exception("Loading failed")

        response = client.post('/load_target_vocab', json={
            'table_file_name': 'CONCEPT.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 500
        assert b"Unable to load vocabulary" in response.data


class TestParquetToBqEndpoint:
    """Tests for /parquet_to_bq endpoint."""

    @patch('core.endpoints.gcp_services.load_parquet_to_bigquery')
    def test_parquet_to_bq_success(self, mock_load, client):
        """Test successful Parquet loading to BigQuery."""
        response = client.post('/parquet_to_bq', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'table_name': 'person',
            'write_type': 'specific_file'
        })

        assert response.status_code == 200
        assert b"Loaded Parquet file to BigQuery" in response.data

    def test_parquet_to_bq_invalid_write_type(self, client):
        """Test invalid write_type returns 400."""
        response = client.post('/parquet_to_bq', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'table_name': 'person',
            'write_type': 'INVALID_TYPE'
        })

        assert response.status_code == 400
        assert b"Invalid write_disposition" in response.data

    def test_parquet_to_bq_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/parquet_to_bq', json={
            'file_path': 'bucket/2025-01-01/person.parquet'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.gcp_services.load_parquet_to_bigquery')
    def test_parquet_to_bq_exception(self, mock_load, client):
        """Test exception handling returns 500."""
        mock_load.side_effect = Exception("Loading failed")

        response = client.post('/parquet_to_bq', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'table_name': 'person',
            'write_type': 'specific_file'
        })

        assert response.status_code == 500
        assert b"Unable to load Parquet file" in response.data


class TestGenerateDeliveryReportCsvEndpoint:
    """Tests for /generate_delivery_report_csv endpoint."""

    @patch('core.endpoints.reporting.ReportGenerator')
    def test_generate_delivery_report_csv_success(self, mock_generator, client):
        """Test successful delivery report CSV generation."""
        mock_instance = MagicMock()
        mock_generator.return_value = mock_instance

        response = client.post('/generate_delivery_report_csv', json={
            'delivery_date': '2025-01-01',
            'site': 'test_site',
            'additional_field': 'test'
        })

        assert response.status_code == 200
        assert b"Generated delivery report CSV file" in response.data

    def test_generate_delivery_report_csv_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/generate_delivery_report_csv', json={
            'site': 'test_site'
        })

        assert response.status_code == 400
        assert b"Missing required parameters" in response.data

    @patch('core.endpoints.reporting.ReportGenerator')
    def test_generate_delivery_report_csv_exception(self, mock_generator, client):
        """Test exception handling returns 500."""
        mock_generator.side_effect = Exception("Report generation failed")

        response = client.post('/generate_delivery_report_csv', json={
            'delivery_date': '2025-01-01',
            'site': 'test_site'
        })

        assert response.status_code == 500
        assert b"Unable to generate delivery report CSV" in response.data


class TestCreateMissingTablesEndpoint:
    """Tests for /create_missing_tables endpoint."""

    @patch('core.endpoints.omop_client.OMOPClient.create_missing_bq_tables')
    def test_create_missing_tables_success(self, mock_create, client):
        """Test successful missing table creation."""
        response = client.post('/create_missing_tables', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'omop_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Created missing tables" in response.data

    def test_create_missing_tables_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/create_missing_tables', json={
            'project_id': 'test-project'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.omop_client.OMOPClient.create_missing_bq_tables')
    def test_create_missing_tables_exception(self, mock_create, client):
        """Test exception handling returns 500."""
        mock_create.side_effect = Exception("Table creation failed")

        response = client.post('/create_missing_tables', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'omop_version': '5.4'
        })

        assert response.status_code == 500
        assert b"Unable to create missing tables" in response.data


class TestPopulateCdmSourceFileEndpoint:
    """Tests for /populate_cdm_source_file endpoint."""

    @patch('core.endpoints.omop_client.OMOPClient.populate_cdm_source_file')
    def test_populate_cdm_source_file_success(self, mock_populate, client):
        """Test successful cdm_source file population."""
        response = client.post('/populate_cdm_source_file', json={
            'source_release_date': '2025-01-01',
            'cdm_source_abbreviation': 'TEST_SITE',
            'additional_field': 'test'
        })

        assert response.status_code == 200
        assert b"cdm_source file populated if needed" in response.data

    def test_populate_cdm_source_file_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/populate_cdm_source_file', json={
            'source_release_date': '2025-01-01'
        })

        assert response.status_code == 400
        assert b"Missing required parameters" in response.data

    @patch('core.endpoints.omop_client.OMOPClient.populate_cdm_source_file')
    def test_populate_cdm_source_file_exception(self, mock_populate, client):
        """Test exception handling returns 500."""
        mock_populate.side_effect = Exception("Population failed")

        response = client.post('/populate_cdm_source_file', json={
            'source_release_date': '2025-01-01',
            'cdm_source_abbreviation': 'TEST_SITE'
        })

        assert response.status_code == 500
        assert b"Unable to populate cdm_source file" in response.data


class TestHarmonizedParquetsToBqEndpoint:
    """Tests for /harmonized_parquets_to_bq endpoint."""

    @patch('core.endpoints.gcp_services.load_harmonized_parquets_to_bq')
    def test_harmonized_parquets_to_bq_success(self, mock_load, client):
        """Test successful harmonized Parquets loading."""
        mock_load.return_value = {
            'loaded': ['observation', 'measurement']
        }

        response = client.post('/harmonized_parquets_to_bq', json={
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 200
        assert b"Successfully loaded 2 table(s)" in response.data

    def test_harmonized_parquets_to_bq_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/harmonized_parquets_to_bq', json={
            'bucket': 'test-bucket'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.gcp_services.load_harmonized_parquets_to_bq')
    def test_harmonized_parquets_to_bq_exception(self, mock_load, client):
        """Test exception handling returns 500."""
        mock_load.side_effect = Exception("Loading failed")

        response = client.post('/harmonized_parquets_to_bq', json={
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 500
        assert b"Error loading harmonized parquets" in response.data


class TestLoadDerivedTablesToBqEndpoint:
    """Tests for /load_derived_tables_to_bq endpoint."""

    @patch('core.endpoints.gcp_services.load_derived_tables_to_bq')
    def test_load_derived_tables_to_bq_success(self, mock_load, client):
        """Test successful derived tables loading."""
        mock_load.return_value = {
            'loaded': ['drug_era', 'condition_era'],
            'skipped': []
        }

        response = client.post('/load_derived_tables_to_bq', json={
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 200
        assert b"Successfully loaded 2 derived table(s)" in response.data

    @patch('core.endpoints.gcp_services.load_derived_tables_to_bq')
    def test_load_derived_tables_to_bq_none_found(self, mock_load, client):
        """Test when no derived tables are found."""
        mock_load.return_value = {
            'loaded': [],
            'skipped': []
        }

        response = client.post('/load_derived_tables_to_bq', json={
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 200
        assert b"No derived tables found" in response.data

    def test_load_derived_tables_to_bq_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/load_derived_tables_to_bq', json={
            'bucket': 'test-bucket'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.gcp_services.load_derived_tables_to_bq')
    def test_load_derived_tables_to_bq_exception(self, mock_load, client):
        """Test exception handling returns 500."""
        mock_load.side_effect = Exception("Loading failed")

        response = client.post('/load_derived_tables_to_bq', json={
            'bucket': 'test-bucket',
            'delivery_date': '2025-01-01',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 500
        assert b"Error loading derived tables" in response.data


class TestPipelineLogEndpoint:
    """Tests for /pipeline_log endpoint."""

    @patch('core.endpoints.pipeline_log.PipelineLog')
    def test_pipeline_log_success(self, mock_log, client):
        """Test successful pipeline logging."""
        mock_instance = MagicMock()
        mock_log.return_value = mock_instance

        response = client.post('/pipeline_log', json={
            'site_name': 'test_site',
            'delivery_date': '2025-01-01',
            'status': 'completed',
            'run_id': 'run-123',
            'message': 'Test message',
            'file_type': 'csv',
            'omop_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Successfully logged to BigQuery" in response.data

    def test_pipeline_log_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/pipeline_log', json={
            'site_name': 'test_site',
            'delivery_date': '2025-01-01'
        })

        assert response.status_code == 400
        assert b"Missing a required parameter" in response.data

    @patch('core.endpoints.pipeline_log.PipelineLog')
    def test_pipeline_log_exception(self, mock_log, client):
        """Test exception handling returns 500."""
        mock_log.side_effect = Exception("Logging failed")

        response = client.post('/pipeline_log', json={
            'site_name': 'test_site',
            'delivery_date': '2025-01-01',
            'status': 'completed',
            'run_id': 'run-123'
        })

        assert response.status_code == 500
        assert b"Unable to save logging information" in response.data
