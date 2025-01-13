from google.cloud import storage
import logging
import sys
import uuid
import duckdb
from fsspec import filesystem
import core.constants as constants
from typing import Optional


"""
Set up a logging instance that will write to stdout (and therefor show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def list_gcs_files(bucket_name: str, folder_prefix: str) -> list[str]:
    """
    Lists files within a specific folder in a GCS bucket (non-recursively).
    """
    try:
        # Initialize the GCS client
        storage_client = storage.Client()
        
        # Get the bucket
        logger.info(f"Attempting to access bucket to list files: {bucket_name}")
        bucket = storage_client.bucket(bucket_name)
        
        # Verify bucket exists
        if not bucket.exists():
            raise Exception(f"Bucket {bucket_name} does not exist")
        
        # Ensure folder_prefix ends with '/' for consistent path handling
        if folder_prefix and not folder_prefix.endswith('/'):
            folder_prefix += '/'
        
        # List all blobs with the prefix
        blobs = bucket.list_blobs(prefix=folder_prefix, delimiter='/')
        
        # Get only the files in this directory level (not in subdirectories)
        files = [blob.name for blob in blobs 
                if blob.name != folder_prefix]
        
        return files
    
    except Exception as e:
        raise Exception(f"Error listing files in GCS: {str(e)}")

def gcs_path_exists(path: str) -> bool:
    """
    Check if a GCS path exists.
    """
    try:
        # Split bucket and prefix
        bucket_name, prefix = path.split('/', 1)
        
        # Initialize client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        
        # List objects with the prefix and max_results=1 to check existence
        blobs = list(bucket.list_blobs(prefix=prefix, max_results=1))
        
        # If we got any results or the prefix itself exists, return True
        return len(blobs) > 0 or bucket.blob(prefix).exists()
    except Exception as e:
        logger.error(f"Error checking path existence for {path}: {str(e)}")
        return False

def delete_gcs_file(file_path: str) -> None:
    """
    Delete a single file from GCS.
    """
    try:
        # Remove 'gs://' prefix if present
        if file_path.startswith('gs://'):
            file_path = file_path[5:]
        
        # Split into bucket and blob path
        bucket_name, blob_path = file_path.split('/', 1)
        
        # Initialize client
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(blob_path)
        
        # Delete the blob
        blob.delete()

    except Exception as e:
        raise Exception(f"Error deleting file {file_path}: {str(e)}")

def create_gcs_directory(directory_path: str) -> None:
    """Creates a directory in GCS by creating an empty blob.
    
    Args: directory_path: Full path including bucket name (e.g. 'bucket/path/to/dir/')
    """
    bucket_name = directory_path.split('/')[0]
    blob_name = '/'.join(directory_path.split('/')[1:])
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    
    try:
        if not blob.exists():
            blob.upload_from_string('')
    except Exception as e:
        logger.error(f"Unable to create GCS bucket: {e}")
        sys.exit(1)

def create_duckdb_connection() -> tuple[duckdb.DuckDBPyConnection, str, str]:
    # Creates a DuckDB instance with a local database
    # Returns tuple of DuckDB object, name of db file, and path to db file
    try:
        random_string = str(uuid.uuid4())
        local_db_file = f"/tmp/{random_string}.db"
        tmp_dir = f"/tmp/"

        conn = duckdb.connect(local_db_file)
        conn.execute(f"SET temp_directory='{tmp_dir}'")
        # Set memory limit based on host machine hardware
        # Should be 2-3GB under the maximum alloted to Docker
        conn.execute(f"SET memory_limit = '{constants.DUCKDB_MEMORY_LIMIT}'")
        # Set max size to allow on disk
        conn.execute(f"SET max_temp_directory_size='{constants.DUCKDB_MAX_SIZE}'")

        # Register GCS filesystem to read/write to GCS buckets
        conn.register_filesystem(filesystem('gcs'))

        return conn, local_db_file, tmp_dir
    except Exception as e:
        logger.error(f"Unable to create DuckDB instance: {e}")
        sys.exit(1)

def close_duckdb_connection(conn: duckdb.DuckDBPyConnection, local_db_file: str, tmp_dir: str) -> None:
    # Destory DuckDB object to free memory, and remove temporary files
    try:
        conn.close()


        #os.remove(local_db_file)
        #shutil.rmtree(tmp_dir)
    except Exception as e:
        logger.error(f"Unable to close DuckDB connection: {e}")


def parse_duckdb_csv_error(error: duckdb.InvalidInputException) -> Optional[str]:
    """
    Parse DuckDB CSV error messages to identify specific error types.
    Returns error type as string or None if unrecognized.
    DuckDB doesn't have very specific exception types; this function allows us to catch and handle specific errors
    """
    error_msg = str(error).lower()
    
    if "invalid unicode" in error_msg or "byte sequence mismatch" in error_msg:
        return "INVALID_UNICODE"
    elif "unterminated quote" in error_msg:
        return "UNTERMINATED_QUOTE"
    elif "csv error on line" in error_msg:  # Generic CSV error fallback
        return "CSV_FORMAT_ERROR"
    return None
