"""
Unit tests for storage_backend.py StorageBackend class.

Tests storage abstraction layer for both GCS and local filesystem backends.
"""

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from core.storage_backend import StorageBackend


class TestStorageBackendInit:
    """Tests for StorageBackend initialization."""

    def test_init_gcs_backend(self):
        """Test initialization with GCS backend."""
        backend = StorageBackend(backend='gcs')

        assert backend.backend == 'gcs'
        assert backend.scheme == 'gs://'

    def test_init_local_backend(self):
        """Test initialization with local backend."""
        backend = StorageBackend(backend='local')

        assert backend.backend == 'local'
        assert backend.scheme == 'file://'

    def test_init_invalid_backend_defaults_to_gcs(self):
        """Test that invalid backend defaults to GCS."""
        backend = StorageBackend(backend='invalid')

        assert backend.backend == 'gcs'
        assert backend.scheme == 'gs://'

    def test_init_no_backend_defaults_to_gcs(self):
        """Test that no backend parameter defaults to GCS."""
        backend = StorageBackend()

        assert backend.backend == 'gcs'
        assert backend.scheme == 'gs://'


class TestStorageBackendGetUri:
    """Tests for get_uri method."""

    def test_get_uri_gcs_adds_scheme(self):
        """Test that get_uri adds gs:// scheme for GCS backend."""
        backend = StorageBackend(backend='gcs')

        result = backend.get_uri('bucket/path/file.parquet')

        assert result == 'gs://bucket/path/file.parquet'

    def test_get_uri_local_adds_scheme_and_data_root(self):
        """Test that get_uri adds file:// and DATA_ROOT for local backend."""
        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            backend = StorageBackend(backend='local')

            result = backend.get_uri('synthea53/2025-01-01/person.parquet')

            assert result == 'file:///data/synthea53/2025-01-01/person.parquet'

    def test_get_uri_local_absolute_path(self):
        """Test that get_uri handles absolute paths for local backend."""
        backend = StorageBackend(backend='local')

        result = backend.get_uri('/data/synthea53/2025-01-01/person.parquet')

        assert result == 'file:///data/synthea53/2025-01-01/person.parquet'

    def test_get_uri_strips_existing_scheme(self):
        """Test that get_uri strips existing scheme before adding new one."""
        backend = StorageBackend(backend='gcs')

        result = backend.get_uri('file:///data/bucket/file.parquet')

        # After stripping file://, we get /data/bucket/file.parquet (absolute path)
        assert result == 'gs:///data/bucket/file.parquet'

    def test_get_uri_local_uses_custom_data_root(self):
        """Test that get_uri uses custom DATA_ROOT environment variable."""
        with patch.dict(os.environ, {'DATA_ROOT': '/custom/path'}):
            backend = StorageBackend(backend='local')

            result = backend.get_uri('synthea53/file.parquet')

            assert result == 'file:///custom/path/synthea53/file.parquet'


class TestStorageBackendStripScheme:
    """Tests for strip_scheme method."""

    def test_strip_scheme_removes_gcs_scheme(self):
        """Test that strip_scheme removes gs:// prefix."""
        backend = StorageBackend()

        result = backend.strip_scheme('gs://bucket/path/file.parquet')

        assert result == 'bucket/path/file.parquet'

    def test_strip_scheme_removes_file_scheme(self):
        """Test that strip_scheme removes file:// prefix."""
        backend = StorageBackend()

        result = backend.strip_scheme('file:///data/path/file.parquet')

        assert result == '/data/path/file.parquet'

    def test_strip_scheme_no_scheme(self):
        """Test that strip_scheme returns path unchanged if no scheme."""
        backend = StorageBackend()

        result = backend.strip_scheme('bucket/path/file.parquet')

        assert result == 'bucket/path/file.parquet'


class TestStorageBackendCreateDirectoryLocal:
    """Tests for create_directory method with local backend."""

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.iterdir')
    @patch('pathlib.Path.exists')
    def test_create_local_directory_new(self, mock_exists, mock_iterdir, mock_mkdir):
        """Test creating new local directory."""
        mock_exists.return_value = False
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            backend.create_directory('test-bucket/artifacts')

        mock_mkdir.assert_called_once()

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.iterdir')
    @patch('os.path.exists')
    def test_create_local_directory_delete_existing(self, mock_exists, mock_iterdir, mock_mkdir):
        """Test creating local directory with delete_existing_files=True."""
        mock_exists.return_value = True
        mock_file = MagicMock()
        mock_file.is_file.return_value = True
        mock_iterdir.return_value = [mock_file]

        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            backend.create_directory('test-bucket/artifacts', delete_existing_files=True)

        mock_file.unlink.assert_called_once()

    @patch('pathlib.Path.mkdir')
    @patch('pathlib.Path.iterdir')
    @patch('os.path.exists')
    def test_create_local_directory_delete_subdirectory(self, mock_exists, mock_iterdir, mock_mkdir):
        """Test creating local directory deletes subdirectories when requested."""
        mock_exists.return_value = True
        mock_dir = MagicMock()
        mock_dir.is_file.return_value = False
        mock_dir.is_dir.return_value = True
        mock_iterdir.return_value = [mock_dir]

        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            with patch('shutil.rmtree') as mock_rmtree:
                backend.create_directory('test-bucket/artifacts', delete_existing_files=True)

        mock_rmtree.assert_called_once_with(mock_dir)


class TestStorageBackendCreateDirectoryGcs:
    """Tests for create_directory method with GCS backend."""

    @patch('core.utils.get_bucket_and_delivery_date_from_path')
    @patch('core.storage_backend.gcs_storage.Client')
    def test_create_gcs_directory_new(self, mock_client, mock_get_bucket):
        """Test creating new GCS directory."""
        mock_get_bucket.return_value = ('test-bucket', '2025-01-01')
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_bucket.list_blobs.return_value = []
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        backend.create_directory('test-bucket/2025-01-01/artifacts')

        mock_blob.upload_from_string.assert_called_once_with('')

    @patch('core.utils.get_bucket_and_delivery_date_from_path')
    @patch('core.storage_backend.gcs_storage.Client')
    def test_create_gcs_directory_delete_existing(self, mock_client, mock_get_bucket):
        """Test creating GCS directory with delete_existing_files=True."""
        mock_get_bucket.return_value = ('test-bucket', '2025-01-01')
        mock_bucket = MagicMock()
        mock_existing_blob = MagicMock()
        mock_existing_blob.name = 'artifacts/file.parquet'
        mock_bucket.list_blobs.return_value = [mock_existing_blob]
        mock_delete_blob = MagicMock()
        mock_bucket.blob.side_effect = [mock_delete_blob, MagicMock()]
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        backend.create_directory('test-bucket/2025-01-01/artifacts', delete_existing_files=True)

        mock_delete_blob.delete.assert_called_once()

    @patch('core.utils.get_bucket_and_delivery_date_from_path')
    @patch('core.storage_backend.gcs_storage.Client')
    def test_create_gcs_directory_exception(self, mock_client, mock_get_bucket):
        """Test GCS directory creation exception handling."""
        mock_get_bucket.return_value = ('test-bucket', '2025-01-01')
        mock_bucket = MagicMock()
        mock_bucket.list_blobs.side_effect = Exception("GCS error")
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')

        with pytest.raises(Exception) as exc_info:
            backend.create_directory('test-bucket/2025-01-01/artifacts')

        assert "Unable to create artifact directories" in str(exc_info.value)


class TestStorageBackendFileExists:
    """Tests for file_exists method."""

    @patch('os.path.exists')
    def test_file_exists_local_true(self, mock_exists):
        """Test file_exists returns True for existing local file."""
        mock_exists.return_value = True
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.file_exists('synthea53/2025-01-01/person.parquet')

        assert result is True

    @patch('os.path.exists')
    def test_file_exists_local_false(self, mock_exists):
        """Test file_exists returns False for non-existent local file."""
        mock_exists.return_value = False
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.file_exists('synthea53/2025-01-01/person.parquet')

        assert result is False

    @patch('core.utils.get_bucket_and_delivery_date_from_path')
    @patch('core.storage_backend.gcs_storage.Client')
    def test_file_exists_gcs_true(self, mock_client, mock_get_bucket):
        """Test file_exists returns True for existing GCS file."""
        mock_get_bucket.return_value = ('test-bucket', '2025-01-01')
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.file_exists('test-bucket/2025-01-01/person.parquet')

        assert result is True

    @patch('core.utils.get_bucket_and_delivery_date_from_path')
    @patch('core.storage_backend.gcs_storage.Client')
    def test_file_exists_gcs_false(self, mock_client, mock_get_bucket):
        """Test file_exists returns False for non-existent GCS file."""
        mock_get_bucket.return_value = ('test-bucket', '2025-01-01')
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.file_exists('test-bucket/2025-01-01/person.parquet')

        assert result is False


class TestStorageBackendListFiles:
    """Tests for list_files method."""

    @patch('os.listdir')
    @patch('os.path.exists')
    @patch('os.path.isfile')
    def test_list_files_local_no_pattern(self, mock_isfile, mock_exists, mock_listdir):
        """Test listing local files without pattern."""
        mock_exists.return_value = True
        mock_listdir.return_value = ['person.parquet', 'observation.parquet']
        mock_isfile.return_value = True

        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.list_files('synthea53/2025-01-01')

        assert result == ['person.parquet', 'observation.parquet']

    @patch('glob.glob')
    @patch('os.path.exists')
    @patch('os.path.isfile')
    def test_list_files_local_with_pattern(self, mock_isfile, mock_exists, mock_glob):
        """Test listing local files with pattern."""
        mock_exists.return_value = True
        mock_glob.return_value = ['/data/synthea53/2025-01-01/person.csv', '/data/synthea53/2025-01-01/observation.csv']
        mock_isfile.return_value = True

        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.list_files('synthea53/2025-01-01', pattern='*.csv')

        assert result == ['person.csv', 'observation.csv']

    @patch('os.path.exists')
    def test_list_files_local_directory_not_found(self, mock_exists):
        """Test listing files in non-existent local directory returns empty list."""
        mock_exists.return_value = False
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.list_files('nonexistent/path')

        assert result == []

    @patch('core.storage_backend.gcs_storage.Client')
    def test_list_files_gcs_no_pattern(self, mock_client):
        """Test listing GCS files without pattern."""
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = 'incoming/person.parquet'
        mock_blob2 = MagicMock()
        mock_blob2.name = 'incoming/observation.parquet'
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.list_files('test-bucket/incoming')

        assert 'person.parquet' in result
        assert 'observation.parquet' in result

    @patch('core.storage_backend.gcs_storage.Client')
    def test_list_files_gcs_with_pattern(self, mock_client):
        """Test listing GCS files with pattern."""
        mock_bucket = MagicMock()
        mock_blob1 = MagicMock()
        mock_blob1.name = 'incoming/person.csv'
        mock_blob2 = MagicMock()
        mock_blob2.name = 'incoming/observation.parquet'
        mock_bucket.list_blobs.return_value = [mock_blob1, mock_blob2]
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.list_files('test-bucket/incoming', pattern='*.csv')

        assert 'person.csv' in result
        assert 'observation.parquet' not in result


class TestStorageBackendDeleteFile:
    """Tests for delete_file method."""

    @patch('os.remove')
    @patch('os.path.exists')
    def test_delete_file_local_exists(self, mock_exists, mock_remove):
        """Test deleting existing local file."""
        mock_exists.return_value = True
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            backend.delete_file('synthea53/2025-01-01/person.parquet')

        mock_remove.assert_called_once()

    @patch('os.remove')
    @patch('os.path.exists')
    def test_delete_file_local_not_exists(self, mock_exists, mock_remove):
        """Test deleting non-existent local file does nothing."""
        mock_exists.return_value = False
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            backend.delete_file('synthea53/2025-01-01/person.parquet')

        mock_remove.assert_not_called()

    @patch('core.storage_backend.gcs_storage.Client')
    def test_delete_file_gcs_exists(self, mock_client):
        """Test deleting existing GCS file."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = True
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        backend.delete_file('test-bucket/2025-01-01/person.parquet')

        mock_blob.delete.assert_called_once()

    @patch('core.storage_backend.gcs_storage.Client')
    def test_delete_file_gcs_not_exists(self, mock_client):
        """Test deleting non-existent GCS file does nothing."""
        mock_bucket = MagicMock()
        mock_blob = MagicMock()
        mock_blob.exists.return_value = False
        mock_bucket.blob.return_value = mock_blob
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        backend.delete_file('test-bucket/2025-01-01/person.parquet')

        mock_blob.delete.assert_not_called()


class TestStorageBackendListSubdirectories:
    """Tests for list_subdirectories method."""

    @patch('os.listdir')
    @patch('os.path.exists')
    @patch('os.path.isdir')
    def test_list_subdirectories_local(self, mock_isdir, mock_exists, mock_listdir):
        """Test listing local subdirectories."""
        mock_exists.return_value = True
        mock_listdir.return_value = ['dir1', 'dir2', 'file.txt']
        mock_isdir.side_effect = [True, True, False]

        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.list_subdirectories('synthea53/2025-01-01')

        assert result == ['dir1/', 'dir2/']

    @patch('os.path.exists')
    def test_list_subdirectories_local_not_found(self, mock_exists):
        """Test listing subdirectories in non-existent local directory."""
        mock_exists.return_value = False
        backend = StorageBackend(backend='local')

        with patch.dict(os.environ, {'DATA_ROOT': '/data'}):
            result = backend.list_subdirectories('nonexistent/path')

        assert result == []

    @patch('core.storage_backend.gcs_storage.Client')
    def test_list_subdirectories_gcs(self, mock_client):
        """Test listing GCS subdirectories."""
        mock_bucket = MagicMock()
        mock_page = MagicMock()
        mock_page.prefixes = ['2025-01-01/', '2025-01-02/']
        mock_blobs = MagicMock()
        mock_blobs.pages = [mock_page]
        mock_bucket.list_blobs.return_value = mock_blobs
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.list_subdirectories('test-bucket')

        assert '2025-01-01/' in result
        assert '2025-01-02/' in result

    @patch('core.storage_backend.gcs_storage.Client')
    def test_list_subdirectories_gcs_with_prefix(self, mock_client):
        """Test listing GCS subdirectories with prefix."""
        mock_bucket = MagicMock()
        mock_page = MagicMock()
        mock_page.prefixes = ['synthea53/2025-01-01/artifacts/', 'synthea53/2025-01-01/incoming/']
        mock_blobs = MagicMock()
        mock_blobs.pages = [mock_page]
        mock_bucket.list_blobs.return_value = mock_blobs
        mock_client.return_value.bucket.return_value = mock_bucket

        backend = StorageBackend(backend='gcs')
        result = backend.list_subdirectories('test-bucket/synthea53/2025-01-01')

        assert 'artifacts/' in result
        assert 'incoming/' in result


class TestStorageBackendInvalidBackend:
    """Tests for unsupported backend methods."""

    def test_create_directory_invalid_backend(self):
        """Test that invalid backend raises ValueError."""
        backend = StorageBackend()
        backend.backend = 'invalid'

        with pytest.raises(ValueError) as exc_info:
            backend.create_directory('test-bucket/path')

        assert "Unsupported storage backend" in str(exc_info.value)

    def test_file_exists_invalid_backend(self):
        """Test that invalid backend raises ValueError."""
        backend = StorageBackend()
        backend.backend = 'invalid'

        with pytest.raises(ValueError) as exc_info:
            backend.file_exists('test-bucket/file.parquet')

        assert "Unsupported storage backend" in str(exc_info.value)

    def test_list_files_invalid_backend(self):
        """Test that invalid backend raises ValueError."""
        backend = StorageBackend()
        backend.backend = 'invalid'

        with pytest.raises(ValueError) as exc_info:
            backend.list_files('test-bucket/path')

        assert "Unsupported storage backend" in str(exc_info.value)

    def test_delete_file_invalid_backend(self):
        """Test that invalid backend raises ValueError."""
        backend = StorageBackend()
        backend.backend = 'invalid'

        with pytest.raises(ValueError) as exc_info:
            backend.delete_file('test-bucket/file.parquet')

        assert "Unsupported storage backend" in str(exc_info.value)

    def test_list_subdirectories_invalid_backend(self):
        """Test that invalid backend raises ValueError."""
        backend = StorageBackend()
        backend.backend = 'invalid'

        with pytest.raises(ValueError) as exc_info:
            backend.list_subdirectories('test-bucket/path')

        assert "Unsupported storage backend" in str(exc_info.value)
