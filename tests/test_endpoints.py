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


def assert_missing_fields(response, *fields):
    """Assert that a 400 response lists the expected missing request fields."""
    assert response.status_code == 400
    assert b"Missing required parameters" in response.data
    for field in fields:
        assert field.encode() in response.data


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

        assert_missing_fields(response, 'vocab_version')

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

        assert_missing_fields(response, 'delivery_bucket')

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

        assert_missing_fields(response, 'delivery_date')

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

        assert_missing_fields(response, 'folder', 'file_format')

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

        assert_missing_fields(response, 'file_path')

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
            'cdm_version': '5.4',
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

        assert_missing_fields(response, 'cdm_version', 'delivery_date', 'storage_path')

    @patch('core.endpoints.file_validation.FileValidator')
    def test_validate_file_exception(self, mock_validator, client):
        """Test exception handling returns 500."""
        mock_validator.side_effect = Exception("Validation failed")

        response = client.post('/validate_file', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'cdm_version': '5.4',
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
            'cdm_version': '5.4',
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

        assert_missing_fields(response, 'cdm_version', 'date_format', 'datetime_format')

    @patch('core.endpoints.normalization.Normalizer')
    @patch('core.endpoints.utils.get_parquet_artifact_location')
    def test_normalize_parquet_exception(self, mock_get_path, mock_normalizer, client):
        """Test exception handling returns 500."""
        mock_get_path.return_value = 'bucket/2025-01-01/parquet/person.parquet'
        mock_normalizer.side_effect = Exception("Normalization failed")

        response = client.post('/normalize_parquet', json={
            'file_path': 'bucket/2025-01-01/person.csv',
            'cdm_version': '5.4',
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
            'cdm_version': '5.3',
            'target_cdm_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Upgraded file" in response.data
        mock_upgrade.assert_called_once_with('bucket/2025-01-01/person.parquet', '5.3', '5.4')

    def test_upgrade_cdm_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/upgrade_cdm', json={
            'file_path': 'bucket/2025-01-01/person.parquet'
        })

        assert_missing_fields(response, 'cdm_version', 'target_cdm_version')

    @patch('core.endpoints.omop_client.OMOPClient.upgrade_file')
    def test_upgrade_cdm_exception(self, mock_upgrade, client):
        """Test exception handling returns 500."""
        mock_upgrade.side_effect = Exception("Upgrade failed")

        response = client.post('/upgrade_cdm', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'cdm_version': '5.3',
            'target_cdm_version': '5.4'
        })

        assert response.status_code == 500
        assert b"Unable to upgrade file" in response.data


class TestGetConnectDataEndpoint:
    """Tests for /get_connect_data endpoint."""

    @patch('core.endpoints.gcp_services.export_connect_data_to_parquet')
    def test_get_connect_data_success_with_delivery_bucket(self, mock_export, client):
        """Test successful retrieval with delivery_bucket."""
        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'delivery_bucket': 'test-bucket/2025-01-01',
            'site_connect_id': '123456'
        })

        assert response.status_code == 200
        assert b"Retrieved Connect study data" in response.data
        mock_export.assert_called_once_with(
            'test-project', 'test_dataset', 'test-bucket/2025-01-01', None, '123456'
        )

    @patch('core.endpoints.gcp_services.export_connect_data_to_parquet')
    def test_get_connect_data_success_with_parquet_destination(self, mock_export, client):
        """Test successful retrieval with parquet_destination instead of delivery_bucket."""
        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'parquet_destination': 'gs://output-bucket/connect/status.parquet',
            'site_connect_id': '123456'
        })

        assert response.status_code == 200
        assert b"Retrieved Connect study data" in response.data
        mock_export.assert_called_once_with(
            'test-project', 'test_dataset', None, 'gs://output-bucket/connect/status.parquet', '123456'
        )

    @patch('core.endpoints.gcp_services.export_connect_data_to_parquet')
    def test_get_connect_data_success_without_site_connect_id(self, mock_export, client):
        """Test successful retrieval without site_connect_id returns all sites."""
        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'delivery_bucket': 'test-bucket/2025-01-01'
        })

        assert response.status_code == 200
        mock_export.assert_called_once_with(
            'test-project', 'test_dataset', 'test-bucket/2025-01-01', None, None
        )

    def test_get_connect_data_missing_project_and_dataset(self, client):
        """Test missing project_id and dataset_id returns 400."""
        response = client.post('/get_connect_data', json={
            'delivery_bucket': 'test-bucket/2025-01-01'
        })

        assert_missing_fields(response, 'project_id', 'dataset_id')

    def test_get_connect_data_invalid_parquet_destination(self, client):
        """Test parquet_destination not ending in .parquet returns 400."""
        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'parquet_destination': 'some-bucket/no_extension'
        })

        assert response.status_code == 400
        assert b"parquet_destination must end with '.parquet'" in response.data

    def test_get_connect_data_missing_bucket_and_destination(self, client):
        """Test missing both delivery_bucket and parquet_destination returns 400."""
        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset'
        })

        assert response.status_code == 400
        assert b"delivery_bucket or parquet_destination" in response.data

    @patch('core.endpoints.gcp_services.export_connect_data_to_parquet')
    def test_get_connect_data_exception(self, mock_export, client):
        """Test exception handling returns 500."""
        mock_export.side_effect = Exception("BigQuery error")

        response = client.post('/get_connect_data', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'delivery_bucket': 'test-bucket/2025-01-01',
            'site_connect_id': '123456'
        })

        assert response.status_code == 500
        assert b"Unable to retrieve Connect study data" in response.data


class TestFilterConnectParticipantsEndpoint:
    """Tests for /filter_connect_participants endpoint."""

    @patch('core.endpoints.participant_filter.ParticipantFilter')
    def test_filter_connect_participants_success(self, mock_filter, client):
        """Test successful Connect participant filtering."""
        mock_instance = MagicMock()
        mock_instance.apply_exclusions.return_value = True
        mock_filter.return_value = mock_instance

        response = client.post('/filter_connect_participants', json={
            'file_path': 'bucket/2025-01-01/condition_occurrence.parquet',
            'cdm_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Applied Connect participant filtering" in response.data
        mock_filter.assert_called_once_with(
            file_path='bucket/2025-01-01/condition_occurrence.parquet',
            cdm_version='5.4'
        )
        mock_instance.apply_exclusions.assert_called_once()

    @patch('core.endpoints.participant_filter.ParticipantFilter')
    def test_filter_connect_participants_skips_tables_without_person_id(self, mock_filter, client):
        """Test skip response for tables without person_id."""
        mock_instance = MagicMock()
        mock_instance.apply_exclusions.return_value = False
        mock_filter.return_value = mock_instance

        response = client.post('/filter_connect_participants', json={
            'file_path': 'bucket/2025-01-01/vocabulary.parquet',
            'cdm_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Skipped Connect participant filtering for table without person_id" in response.data
        mock_filter.assert_called_once_with(
            file_path='bucket/2025-01-01/vocabulary.parquet',
            cdm_version='5.4'
        )

    def test_filter_connect_participants_missing_parameters(self, client):
        """Test missing file_path and cdm_version return 400."""
        response = client.post('/filter_connect_participants', json={})

        assert_missing_fields(response, 'file_path', 'cdm_version')

    @patch('core.endpoints.participant_filter.ParticipantFilter')
    def test_filter_connect_participants_exception(self, mock_filter, client):
        """Test exception handling returns 500."""
        mock_filter.side_effect = Exception("Connect filtering failed")

        response = client.post('/filter_connect_participants', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'cdm_version': '5.4'
        })

        assert response.status_code == 500
        assert b"Unable to apply Connect participant filtering" in response.data


class TestUniqueNaturalKeysEndpoint:
    """Tests for /unique_natural_keys endpoint."""

    @patch('core.endpoints.natural_keys.NaturalKeyProcessor')
    def test_unique_natural_keys_success(self, mock_processor, client):
        """Test successful natural-key rewrite."""
        mock_instance = MagicMock()
        mock_instance.apply.return_value = True
        mock_processor.return_value = mock_instance

        response = client.post('/unique_natural_keys', json={
            'file_path': 'bucket/2025-01-01/visit_occurrence.parquet',
            'cdm_version': '5.4',
            'site': 'site_alpha'
        })

        assert response.status_code == 200
        assert b"Applied natural-key rewrite" in response.data
        mock_processor.assert_called_once_with(
            file_path='bucket/2025-01-01/visit_occurrence.parquet',
            cdm_version='5.4',
            site='site_alpha'
        )
        mock_instance.apply.assert_called_once()

    @patch('core.endpoints.natural_keys.NaturalKeyProcessor')
    def test_unique_natural_keys_skips_excluded_tables(self, mock_processor, client):
        """Test skip response for excluded tables (vocab tables, person)."""
        mock_instance = MagicMock()
        mock_instance.apply.return_value = False
        mock_processor.return_value = mock_instance

        response = client.post('/unique_natural_keys', json={
            'file_path': 'bucket/2025-01-01/person.parquet',
            'cdm_version': '5.4',
            'site': 'site_alpha'
        })

        assert response.status_code == 200
        assert b"Skipped natural-key rewrite for table not in scope" in response.data
        mock_processor.assert_called_once_with(
            file_path='bucket/2025-01-01/person.parquet',
            cdm_version='5.4',
            site='site_alpha'
        )

    def test_unique_natural_keys_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/unique_natural_keys', json={})

        assert_missing_fields(response, 'file_path', 'cdm_version', 'site')

    def test_unique_natural_keys_missing_site_only(self, client):
        """Test missing site parameter returns 400."""
        response = client.post('/unique_natural_keys', json={
            'file_path': 'bucket/2025-01-01/visit_occurrence.parquet',
            'cdm_version': '5.4'
        })

        assert_missing_fields(response, 'site')

    @patch('core.endpoints.natural_keys.NaturalKeyProcessor')
    def test_unique_natural_keys_exception(self, mock_processor, client):
        """Test exception handling returns 500."""
        mock_processor.side_effect = Exception("Rewrite failed")

        response = client.post('/unique_natural_keys', json={
            'file_path': 'bucket/2025-01-01/visit_occurrence.parquet',
            'cdm_version': '5.4',
            'site': 'site_alpha'
        })

        assert response.status_code == 500
        assert b"Unable to apply natural-key rewrite" in response.data


class TestPostProcessingEndpoint:
    """Tests for /post_processing endpoint."""

    _required_body = {
        'site': 'site_alpha',
        'bucket': 'test-bucket',
        'delivery_date': '2025-01-15',
        'cdm_version': '5.4',
        'vocab_version': 'v5.0_24-JAN-25',
        'task_name': 'example_task',
    }

    @patch('core.endpoints.post_processing.PostProcessor')
    def test_post_processing_success(self, mock_class, client):
        """Test successful post-processing run with changes."""
        mock_instance = MagicMock()
        mock_instance.apply.return_value = {
            'condition_occurrence': {'added': 0, 'removed': 47},
            'person': {'added': 0, 'removed': 3},
        }
        mock_class.return_value = mock_instance

        response = client.post('/post_processing', json=self._required_body)

        assert response.status_code == 200
        assert b"2 table(s) affected" in response.data
        assert b"condition_occurrence: +0/-47" in response.data
        assert b"person: +0/-3" in response.data
        mock_instance.apply.assert_called_once()

    @patch('core.endpoints.post_processing.PostProcessor')
    def test_post_processing_no_changes(self, mock_class, client):
        """Test no-op task still returns 200."""
        mock_instance = MagicMock()
        mock_instance.apply.return_value = {}
        mock_class.return_value = mock_instance

        response = client.post('/post_processing', json=self._required_body)

        assert response.status_code == 200
        assert b"0 table(s) affected" in response.data
        assert b"no changes" in response.data

    def test_post_processing_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/post_processing', json={})

        assert_missing_fields(
            response,
            'site', 'bucket', 'delivery_date', 'cdm_version', 'vocab_version', 'task_name'
        )

    def test_post_processing_missing_task_name_only(self, client):
        """Test missing task_name returns 400."""
        body = {k: v for k, v in self._required_body.items() if k != 'task_name'}
        response = client.post('/post_processing', json=body)

        assert_missing_fields(response, 'task_name')

    @patch('core.endpoints.post_processing.PostProcessor')
    def test_post_processing_unknown_task_returns_400(self, mock_class, client):
        """Test missing task SQL script returns 400."""
        mock_instance = MagicMock()
        mock_instance.apply.side_effect = FileNotFoundError(
            "Post-processing task SQL script not found at reference/sql/post_processing/nope.sql"
        )
        mock_class.return_value = mock_instance

        response = client.post('/post_processing', json=self._required_body)

        assert response.status_code == 400
        assert b"Post-processing task script missing" in response.data

    @patch('core.endpoints.post_processing.PostProcessor')
    def test_post_processing_exception_returns_500(self, mock_class, client):
        """Test unhandled exception returns 500."""
        mock_class.side_effect = Exception("boom")

        response = client.post('/post_processing', json=self._required_body)

        assert response.status_code == 500
        assert b"Unable to apply post-processing task" in response.data

    @patch('core.endpoints.post_processing.PostProcessor')
    def test_post_processing_vocab_write_returns_400(self, mock_class, client):
        """Test that a task attempting to write to a vocabulary file returns 400."""
        mock_instance = MagicMock()
        mock_instance.apply.side_effect = ValueError(
            "Post-processing task 'evil_task' attempts to write to vocabulary file "
            "'concept.parquet'. Vocabulary files must never be modified by "
            "post-processing tasks. Refusing to run."
        )
        mock_class.return_value = mock_instance

        response = client.post('/post_processing', json=self._required_body)

        assert response.status_code == 400
        assert b"Post-processing task rejected" in response.data
        assert b"vocabulary file" in response.data


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

        assert_missing_fields(response, 'dataset_id')

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
            'cdm_version': '5.4',
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
    def test_harmonize_vocab_source_concept_backfill_step(self, mock_harmonizer, client):
        """Test vocabulary harmonization with source_concept_backfill step."""
        mock_instance = MagicMock()
        mock_instance.perform_harmonization.return_value = None
        mock_harmonizer.return_value = mock_instance

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/condition_occurrence.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'cdm_version': '5.4',
            'site': 'test_site',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'step': constants.SOURCE_CONCEPT_BACKFILL
        })

        data = json.loads(response.data)
        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['step'] == constants.SOURCE_CONCEPT_BACKFILL
        mock_instance.perform_harmonization.assert_called_once_with(constants.SOURCE_CONCEPT_BACKFILL)

    @patch('core.endpoints.vocab_harmonization.VocabHarmonizer')
    def test_harmonize_vocab_secondary_concept_backfill_step(self, mock_harmonizer, client):
        """Test vocabulary harmonization with secondary_concept_backfill step."""
        mock_instance = MagicMock()
        mock_instance.perform_harmonization.return_value = None
        mock_harmonizer.return_value = mock_instance

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/measurement.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'cdm_version': '5.4',
            'site': 'test_site',
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'step': constants.SECONDARY_CONCEPT_BACKFILL
        })

        data = json.loads(response.data)
        assert response.status_code == 200
        assert data['status'] == 'success'
        assert data['step'] == constants.SECONDARY_CONCEPT_BACKFILL
        mock_instance.perform_harmonization.assert_called_once_with(constants.SECONDARY_CONCEPT_BACKFILL)

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
            'cdm_version': '5.4',
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

        assert_missing_fields(response, 'vocab_version', 'cdm_version', 'site', 'project_id', 'dataset_id', 'step')

    @patch('core.endpoints.vocab_harmonization.VocabHarmonizer')
    def test_harmonize_vocab_exception(self, mock_harmonizer, client):
        """Test exception handling returns 500."""
        mock_harmonizer.side_effect = Exception("Harmonization failed")

        response = client.post('/harmonize_vocab', json={
            'file_path': 'bucket/2025-01-01/observation.parquet',
            'vocab_version': 'v5.0_24-JAN-25',
            'cdm_version': '5.4',
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

        assert_missing_fields(response, 'bucket', 'delivery_date', 'table_name', 'vocab_version')

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

        assert_missing_fields(response, 'table_file_name', 'project_id', 'dataset_id')

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

        assert_missing_fields(response, 'project_id', 'dataset_id', 'table_name', 'write_type')

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
            'cdm_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Created missing tables" in response.data

    def test_create_missing_tables_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/create_missing_tables', json={
            'project_id': 'test-project'
        })

        assert_missing_fields(response, 'dataset_id', 'cdm_version')

    @patch('core.endpoints.omop_client.OMOPClient.create_missing_bq_tables')
    def test_create_missing_tables_exception(self, mock_create, client):
        """Test exception handling returns 500."""
        mock_create.side_effect = Exception("Table creation failed")

        response = client.post('/create_missing_tables', json={
            'project_id': 'test-project',
            'dataset_id': 'test_dataset',
            'cdm_version': '5.4'
        })

        assert response.status_code == 500
        assert b"Unable to create missing tables" in response.data


class TestPopulateCdmSourceFileEndpoint:
    """Tests for /populate_cdm_source_file endpoint."""

    VALID_PAYLOAD = {
        'bucket': 'test-bucket',
        'delivery_date': '2025-01-15',
        'source_release_date': '2024-12-31',
        'cdm_source_name': 'Test Source',
        'cdm_source_abbreviation': 'TEST_SITE',
        'cdm_holder': 'Test Holder',
        'source_description': 'Test description',
        'target_cdm_version': '5.4',
        'target_vocab_version': 'v5.0_24-JAN-25',
        'cdm_release_date': '2024-12-15',
        'date_format': '%Y-%m-%d',
    }

    @patch('core.endpoints.omop_client.OMOPClient.populate_cdm_source_file')
    def test_populate_cdm_source_file_success(self, mock_populate, client):
        """Test successful cdm_source file population."""
        response = client.post('/populate_cdm_source_file', json=self.VALID_PAYLOAD)

        assert response.status_code == 200
        assert b"cdm_source file populated if needed" in response.data

    def test_populate_cdm_source_file_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/populate_cdm_source_file', json={
            'source_release_date': '2025-01-01'
        })

        assert_missing_fields(
            response,
            'bucket',
            'delivery_date',
            'cdm_source_name',
            'cdm_source_abbreviation',
            'cdm_holder',
            'source_description',
            'target_cdm_version',
            'target_vocab_version',
            'cdm_release_date',
            'date_format'
        )

    @patch('core.endpoints.omop_client.OMOPClient.populate_cdm_source_file')
    def test_populate_cdm_source_file_exception(self, mock_populate, client):
        """Test exception handling returns 500."""
        mock_populate.side_effect = Exception("Population failed")

        response = client.post('/populate_cdm_source_file', json=self.VALID_PAYLOAD)

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

        assert_missing_fields(response, 'delivery_date', 'project_id', 'dataset_id')

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

        assert_missing_fields(response, 'delivery_date', 'project_id', 'dataset_id')

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
            'cdm_version': '5.4'
        })

        assert response.status_code == 200
        assert b"Successfully logged to BigQuery" in response.data

    def test_pipeline_log_missing_parameters(self, client):
        """Test missing parameters return 400."""
        response = client.post('/pipeline_log', json={
            'site_name': 'test_site',
            'delivery_date': '2025-01-01'
        })

        assert_missing_fields(response, 'status', 'run_id')

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


class TestGetLatestCompletedDeliveryEndpoint:
    """Tests for /get_latest_completed_delivery endpoint."""

    @patch('core.endpoints.pipeline_log.get_latest_completed_delivery')
    def test_success(self, mock_get, client):
        mock_get.return_value = '2025-03-01'

        response = client.post('/get_latest_completed_delivery', json={'site': 'siteA'})
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['delivery_date'] == '2025-03-01'
        mock_get.assert_called_once_with('siteA')

    @patch('core.endpoints.pipeline_log.get_latest_completed_delivery')
    def test_no_completed_delivery_returns_null(self, mock_get, client):
        mock_get.return_value = None

        response = client.post('/get_latest_completed_delivery', json={'site': 'siteA'})
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['delivery_date'] is None

    def test_missing_parameter(self, client):
        response = client.post('/get_latest_completed_delivery', json={})
        assert_missing_fields(response, 'site')

    @patch('core.endpoints.pipeline_log.get_latest_completed_delivery')
    def test_exception(self, mock_get, client):
        mock_get.side_effect = Exception("BigQuery error")

        response = client.post('/get_latest_completed_delivery', json={'site': 'siteA'})

        assert response.status_code == 500
        assert b"Unable to get latest completed delivery" in response.data


class TestGetDeliveryCdmVersionEndpoint:
    """Tests for /get_delivery_cdm_version endpoint."""

    @patch('core.endpoints.omop_client.OMOPClient.get_delivery_cdm_version')
    def test_success(self, mock_get, client):
        mock_get.return_value = {'cdm_version': '5.4', 'vocabulary_version': 'v5.0 27-AUG-25'}

        response = client.post('/get_delivery_cdm_version', json={
            'bucket': 'siteA',
            'delivery_date': '2025-01-01'
        })
        data = json.loads(response.data)

        assert response.status_code == 200
        assert data['cdm_version'] == '5.4'
        assert data['vocabulary_version'] == 'v5.0 27-AUG-25'
        mock_get.assert_called_once_with('siteA', '2025-01-01')

    def test_missing_parameters(self, client):
        response = client.post('/get_delivery_cdm_version', json={'bucket': 'siteA'})
        assert_missing_fields(response, 'delivery_date')

    @patch('core.endpoints.omop_client.OMOPClient.get_delivery_cdm_version')
    def test_exception(self, mock_get, client):
        mock_get.side_effect = Exception("read failed")

        response = client.post('/get_delivery_cdm_version', json={
            'bucket': 'siteA',
            'delivery_date': '2025-01-01'
        })

        assert response.status_code == 500
        assert b"Unable to read cdm_version" in response.data


class TestExtractParticipantChunkEndpoint:
    """Tests for /extract_participant_chunk endpoint."""

    @patch('core.endpoints.merge.MergeProcessor.extract_chunk')
    def test_success_all_scope(self, mock_extract, client):
        response = client.post('/extract_participant_chunk', json={
            'source_uri': 'siteA/2025-01-01/artifacts/converted_files/measurement.parquet',
            'chunk_uri': 'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet',
            'participant_scope': 'ALL'
        })

        assert response.status_code == 200
        assert b"Extracted participant chunk" in response.data
        # Optional person_id_column defaults to the constant; site_display_name is None (no stamp).
        mock_extract.assert_called_once_with(
            'siteA/2025-01-01/artifacts/converted_files/measurement.parquet',
            'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet',
            'ALL',
            constants.DEFAULT_PERSON_ID_COLUMN,
            None
        )

    @patch('core.endpoints.merge.MergeProcessor.extract_chunk')
    def test_success_with_site_display_name(self, mock_extract, client):
        response = client.post('/extract_participant_chunk', json={
            'source_uri': 'siteA/2025-01-01/artifacts/converted_files/person.parquet',
            'chunk_uri': 'ehr_merged/2026-06-24/artifacts/merge_chunks/person/chunk.parquet',
            'participant_scope': 'ALL',
            'site_display_name': 'Site A',
        })

        assert response.status_code == 200
        # site_display_name is forwarded so person's care_site_id gets stamped.
        mock_extract.assert_called_once_with(
            'siteA/2025-01-01/artifacts/converted_files/person.parquet',
            'ehr_merged/2026-06-24/artifacts/merge_chunks/person/chunk.parquet',
            'ALL',
            constants.DEFAULT_PERSON_ID_COLUMN,
            'Site A'
        )

    def test_missing_parameters(self, client):
        response = client.post('/extract_participant_chunk', json={
            'source_uri': 'siteA/2025-01-01/artifacts/converted_files/measurement.parquet'
        })
        assert_missing_fields(response, 'chunk_uri', 'participant_scope')

    @patch('core.endpoints.merge.MergeProcessor.extract_chunk')
    def test_exception(self, mock_extract, client):
        mock_extract.side_effect = Exception("extract failed")

        response = client.post('/extract_participant_chunk', json={
            'source_uri': 'siteA/2025-01-01/artifacts/converted_files/measurement.parquet',
            'chunk_uri': 'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet',
            'participant_scope': 'ALL'
        })

        assert response.status_code == 500
        assert b"Unable to extract participant chunk" in response.data


class TestReconcileChunksEndpoint:
    """Tests for /reconcile_chunks endpoint."""

    @patch('core.endpoints.merge.MergeProcessor.reconcile_chunks')
    def test_success(self, mock_reconcile, client):
        response = client.post('/reconcile_chunks', json={
            'chunk_glob': 'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/*.parquet',
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet'
        })

        assert response.status_code == 200
        assert b"Reconciled merge chunks" in response.data
        mock_reconcile.assert_called_once_with(
            'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/*.parquet',
            'ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet'
        )

    def test_missing_parameters(self, client):
        response = client.post('/reconcile_chunks', json={})
        assert_missing_fields(response, 'chunk_glob', 'output_uri')

    @patch('core.endpoints.merge.MergeProcessor.reconcile_chunks')
    def test_exception(self, mock_reconcile, client):
        mock_reconcile.side_effect = Exception("no chunks matched glob")

        response = client.post('/reconcile_chunks', json={
            'chunk_glob': 'ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/*.parquet',
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet'
        })

        assert response.status_code == 500
        assert b"Unable to reconcile merge chunks" in response.data


class TestBuildCareSiteEndpoint:
    """Tests for /build_care_site endpoint."""

    @patch('core.endpoints.merge.MergeProcessor.build_care_site')
    def test_success(self, mock_build, client):
        response = client.post('/build_care_site', json={
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet',
            'site_display_names': ['Site A', 'Site B'],
            'cdm_version': '5.4',
        })

        assert response.status_code == 200
        assert b"Built care_site table" in response.data
        mock_build.assert_called_once_with(
            'ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet',
            ['Site A', 'Site B'],
            '5.4'
        )

    def test_missing_parameters(self, client):
        response = client.post('/build_care_site', json={
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet'
        })
        assert_missing_fields(response, 'site_display_names', 'cdm_version')

    @patch('core.endpoints.merge.MergeProcessor.build_care_site')
    def test_exception(self, mock_build, client):
        mock_build.side_effect = Exception("build failed")

        response = client.post('/build_care_site', json={
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet',
            'site_display_names': ['Site A'],
            'cdm_version': '5.4',
        })

        assert response.status_code == 500
        assert b"Unable to build care_site table" in response.data


class TestBuildMergeCdmSourceEndpoint:
    """Tests for /build_merge_cdm_source endpoint."""

    PAYLOAD = {
        'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/cdm_source.parquet',
        'source_cdm_source_uris': [
            'siteA/2025-01-01/artifacts/converted_files/cdm_source.parquet',
            'siteB/2025-02-01/artifacts/converted_files/cdm_source.parquet',
        ],
        'site_count': 2,
        'cdm_version': '5.4',
        'vocabulary_version': 'v5.0 27-AUG-25',
        'cdm_release_date': '2026-06-24',
    }

    @patch('core.endpoints.merge.MergeProcessor.build_cdm_source')
    def test_success(self, mock_build, client):
        response = client.post('/build_merge_cdm_source', json=self.PAYLOAD)

        assert response.status_code == 200
        assert b"Built cdm_source" in response.data
        mock_build.assert_called_once_with(
            self.PAYLOAD['output_uri'],
            self.PAYLOAD['source_cdm_source_uris'],
            2,
            '5.4',
            'v5.0 27-AUG-25',
            '2026-06-24',
        )

    def test_missing_parameters(self, client):
        response = client.post('/build_merge_cdm_source', json={
            'output_uri': 'ehr_merged/2026-06-24/artifacts/converted_files/cdm_source.parquet'
        })
        assert_missing_fields(
            response, 'source_cdm_source_uris', 'site_count', 'cdm_version', 'vocabulary_version', 'cdm_release_date'
        )

    @patch('core.endpoints.merge.MergeProcessor.build_cdm_source')
    def test_exception(self, mock_build, client):
        mock_build.side_effect = Exception("build failed")
        response = client.post('/build_merge_cdm_source', json=self.PAYLOAD)

        assert response.status_code == 500
        assert b"Unable to build cdm_source" in response.data


class TestGenerateMergeReportEndpoint:
    """Tests for /generate_merge_report endpoint."""

    @patch('core.endpoints.merge_reporting.MergeReporter.generate_merge_report')
    def test_success(self, mock_report, client):
        deliveries = [
            {'site': 'siteA', 'delivery_date': '2025-01-01'},
            {'site': 'siteB', 'delivery_date': '2025-02-01'},
        ]
        response = client.post('/generate_merge_report', json={
            'merge_bucket': 'ehr_merged',
            'run_date': '2026-06-24',
            'site': 'merged_ehr',
            'deliveries': deliveries,
        })

        assert response.status_code == 200
        assert b"Generated merge report" in response.data
        mock_report.assert_called_once_with('ehr_merged', '2026-06-24', 'merged_ehr', deliveries)

    def test_missing_parameters(self, client):
        response = client.post('/generate_merge_report', json={
            'merge_bucket': 'ehr_merged',
            'run_date': '2026-06-24',
        })
        assert_missing_fields(response, 'site', 'deliveries')

    @patch('core.endpoints.merge_reporting.MergeReporter.generate_merge_report')
    def test_exception(self, mock_report, client):
        mock_report.side_effect = Exception("count failed")

        response = client.post('/generate_merge_report', json={
            'merge_bucket': 'ehr_merged',
            'run_date': '2026-06-24',
            'site': 'merged_ehr',
            'deliveries': [{'site': 'siteA', 'delivery_date': '2025-01-01'}],
        })

        assert response.status_code == 500
        assert b"Unable to generate merge report" in response.data
