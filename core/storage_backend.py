import os
import pathlib
from typing import List

from google.cloud import storage as gcs_storage


class StorageBackend:
    """
    Storage backend abstraction for handling different storage systems (cloud or local).

    This module provides a simple abstraction layer that allows the codebase to work with
    different storage backends by configuring environment variables.

    Configuration:
        Environment variables:
        - STORAGE_BACKEND: Backend type ('gcs' or 'local', default: 'gcs')
        - DATA_ROOT: Root directory for local storage (default: '/data')

        For local backend, relative paths are resolved to DATA_ROOT:
        - Input: "synthea_53/file.csv"
        - Output: "file:///data/synthea_53/file.csv"
    """

    # Supported storage backends and their URI schemes
    BACKENDS = {
        'gcs': 'gs://',
        'local': 'file://',
    }

    def __init__(self, backend: str = 'gcs'):
        """
        Initialize the storage backend.

        Args:
            backend: The storage backend to use. One of: 'gcs', 'local'
                    Defaults to 'gcs' if not specified or if an invalid value is provided.
        """
        self.backend = backend if backend in self.BACKENDS else 'gcs'
        self.scheme = self.BACKENDS[self.backend]

    def get_uri(self, path: str) -> str:
        """
        Add storage scheme to path if not already present.

        Args:
            path: File path without storage scheme (e.g., "bucket/path/file.parquet")
                  or with any storage scheme (will be normalized)

        Returns:
            Complete URI with the configured storage scheme
        """
        # Strip any existing scheme first to normalize
        path = self.strip_scheme(path)

        # For local backend, convert relative paths to absolute paths using DATA_ROOT
        if self.backend == 'local' and not path.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path = f"{data_root}/{path}"

        return f"{self.scheme}{path}"

    def strip_scheme(self, path: str) -> str:
        """
        Remove any storage scheme from path.

        Args:
            path: File path with or without storage scheme

        Returns:
            Path without any storage scheme prefix
        """
        for scheme in self.BACKENDS.values():
            if path.startswith(scheme):
                return path[len(scheme):]
        return path

    def create_directory(self, directory_path: str, delete_existing_files: bool = True) -> None:
        """
        Create a directory in the configured storage backend.

        For local filesystem: Creates directory using os.makedirs
        For GCS: Creates directory by uploading empty blob marker

        Args:
            directory_path: Path to directory (without storage scheme)
            delete_existing_files: If True, delete existing files in directory first
        """
        if self.backend == 'local':
            self._create_local_directory(directory_path, delete_existing_files)
        elif self.backend == 'gcs':
            self._create_gcs_directory(directory_path, delete_existing_files)
        else:
            raise ValueError(f"Unsupported storage backend: {self.backend}")

    def _create_local_directory(self, directory_path: str, delete_existing_files: bool) -> None:
        """Create directory on local filesystem."""
        # Strip any scheme prefix
        path = self.strip_scheme(directory_path)

        # Convert to absolute path if relative
        if not path.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path = f"{data_root}/{path}"

        # Create directory
        pathlib.Path(path).mkdir(parents=True, exist_ok=True)

        # Delete existing files if requested
        if delete_existing_files and os.path.exists(path):
            for item in pathlib.Path(path).iterdir():
                if item.is_file():
                    item.unlink()
                elif item.is_dir():
                    import shutil
                    shutil.rmtree(item)

    def _create_gcs_directory(self, directory_path: str, delete_existing_files: bool) -> None:
        """Create directory in GCS."""
        from core import utils

        # Parse bucket and path
        bucket_name, _ = utils.get_bucket_and_delivery_date_from_path(directory_path)
        blob_name = '/'.join(directory_path.split('/')[1:])

        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)

        try:
            # Check if directory exists and has files
            blobs = bucket.list_blobs(prefix=blob_name)

            if delete_existing_files:
                # Delete any existing files in the directory
                for blob in blobs:
                    try:
                        bucket.blob(blob.name).delete()
                    except Exception as e:
                        import logging
                        logging.error(f"Failed to delete file {blob.name}: {e}")

            # Create the directory marker
            blob = bucket.blob(blob_name)
            if not blob.exists():
                blob.upload_from_string('')

        except Exception as e:
            raise Exception(f"Unable to create artifact directories in storage path {directory_path}: {e}") from e

    def file_exists(self, file_path: str) -> bool:
        """
        Check if a file exists in the configured storage backend.

        Args:
            file_path: Path to file (without storage scheme)

        Returns:
            True if file exists, False otherwise
        """
        if self.backend == 'local':
            return self._file_exists_local(file_path)
        elif self.backend == 'gcs':
            return self._file_exists_gcs(file_path)
        else:
            raise ValueError(f"Unsupported storage backend: {self.backend}")

    def _file_exists_local(self, file_path: str) -> bool:
        """Check if file exists on local filesystem."""
        path = self.strip_scheme(file_path)
        if not path.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path = f"{data_root}/{path}"
        return os.path.exists(path)

    def _file_exists_gcs(self, file_path: str) -> bool:
        """Check if file exists in GCS."""
        from core import utils

        path_without_prefix = self.strip_scheme(file_path)
        bucket_name, _ = utils.get_bucket_and_delivery_date_from_path(path_without_prefix)
        blob_path = '/'.join(path_without_prefix.split('/')[1:])

        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        return blob.exists()

    def list_files(self, directory_path: str, pattern: str = None) -> List[str]:
        """
        List files in a directory.

        Args:
            directory_path: Path to directory (without storage scheme)
            pattern: Optional glob pattern to filter files

        Returns:
            List of file names (not full paths)
        """
        if self.backend == 'local':
            return self._list_files_local(directory_path, pattern)
        elif self.backend == 'gcs':
            return self._list_files_gcs(directory_path, pattern)
        else:
            raise ValueError(f"Unsupported storage backend: {self.backend}")

    def _list_files_local(self, directory_path: str, pattern: str = None) -> List[str]:
        """List files on local filesystem."""
        import glob

        path = self.strip_scheme(directory_path)
        if not path.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path = f"{data_root}/{path}"

        if not os.path.exists(path):
            return []

        if pattern:
            search_path = os.path.join(path, pattern)
            files = [os.path.basename(f) for f in glob.glob(search_path) if os.path.isfile(f)]
        else:
            files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]

        return files

    def _list_files_gcs(self, directory_path: str, pattern: str = None) -> List[str]:
        """List files in GCS."""
        from core import utils

        path_without_prefix = self.strip_scheme(directory_path)
        bucket_name = path_without_prefix.split('/')[0]
        folder_path = '/'.join(path_without_prefix.split('/')[1:])

        if not folder_path.endswith('/'):
            folder_path += '/'

        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blobs = bucket.list_blobs(prefix=folder_path, delimiter='/')

        files = []
        for blob in blobs:
            if blob.name != folder_path:  # Skip directory marker
                file_name = blob.name.replace(folder_path, '')
                if '/' not in file_name:  # Only files, not subdirectories
                    if pattern is None or file_name.endswith(pattern.replace('*', '')):
                        files.append(file_name)

        return files

    def delete_file(self, file_path: str) -> None:
        """
        Delete a file from the configured storage backend.

        Args:
            file_path: Path to file (without storage scheme)
        """
        if self.backend == 'local':
            self._delete_file_local(file_path)
        elif self.backend == 'gcs':
            self._delete_file_gcs(file_path)
        else:
            raise ValueError(f"Unsupported storage backend: {self.backend}")

    def _delete_file_local(self, file_path: str) -> None:
        """Delete file from local filesystem."""
        path = self.strip_scheme(file_path)
        if not path.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path = f"{data_root}/{path}"

        if os.path.exists(path):
            os.remove(path)

    def _delete_file_gcs(self, file_path: str) -> None:
        """Delete file from GCS."""
        from core import utils

        path_without_prefix = self.strip_scheme(file_path)
        bucket_name = path_without_prefix.split('/')[0]
        blob_path = '/'.join(path_without_prefix.split('/')[1:])

        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        if blob.exists():
            blob.delete()

    def list_subdirectories(self, directory_path: str) -> List[str]:
        """
        List all subdirectories within a given directory path.

        Returns a list of subdirectory paths (with trailing slashes).
        """
        if self.backend == 'local':
            return self._list_subdirectories_local(directory_path)
        elif self.backend == 'gcs':
            return self._list_subdirectories_gcs(directory_path)
        else:
            raise ValueError(f"Unsupported storage backend: {self.backend}")

    def _list_subdirectories_local(self, directory_path: str) -> List[str]:
        """List subdirectories in local filesystem."""
        import os

        from core import utils

        # Remove scheme and resolve to absolute path
        path_without_prefix = self.strip_scheme(directory_path)
        if not path_without_prefix.startswith('/'):
            data_root = os.getenv('DATA_ROOT', '/data')
            path_without_prefix = f"{data_root}/{path_without_prefix}"

        utils.logger.info(f"Listing subdirectories in local path: {path_without_prefix}")

        if not os.path.exists(path_without_prefix):
            utils.logger.warning(f"Directory does not exist: {path_without_prefix}")
            return []

        subdirectories = []
        for item in os.listdir(path_without_prefix):
            item_path = os.path.join(path_without_prefix, item)
            if os.path.isdir(item_path):
                # Return relative paths with trailing slashes to match GCS behavior
                subdirectories.append(f"{item}/")

        utils.logger.info(f"Found {len(subdirectories)} subdirectories")
        return subdirectories

    def _list_subdirectories_gcs(self, directory_path: str) -> List[str]:
        """List subdirectories in GCS."""
        from core import utils

        # Split the path into bucket name and prefix
        path_without_prefix = self.strip_scheme(directory_path)
        parts = path_without_prefix.split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        utils.logger.info(f"Listing subdirectories in bucket {bucket_name} with prefix: {prefix}")

        # Initialize the client
        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)

        # List blobs with the specified prefix and delimiter
        blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

        # Extract subdirectory names
        subdirectories = set()
        for page in blobs.pages:
            subdirectories.update(page.prefixes)

        # Convert to list and extract just the subdirectory name (not full path)
        result = []
        for subdir in sorted(subdirectories):
            # Extract just the last part of the path after the prefix
            if prefix:
                relative_path = subdir.replace(prefix, '', 1).lstrip('/')
            else:
                relative_path = subdir
            result.append(relative_path)

        utils.logger.info(f"Found {len(result)} subdirectories")
        return result


# Global storage backend instance
# Configured via STORAGE_BACKEND environment variable (defaults to 'gcs')
storage = StorageBackend(backend=os.getenv('STORAGE_BACKEND', 'gcs'))
