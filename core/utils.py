from google.cloud import storage # type: ignore
import logging
import sys
import uuid
import duckdb # type: ignore
from fsspec import filesystem # type: ignore
import core.constants as constants
from typing import Optional, Tuple
import json

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

def create_gcs_directory(directory_path: str) -> None:
    """Creates a directory in GCS by creating an empty blob.
    If directory exists, deletes any existing files first.
    """
    bucket_name, _ = get_bucket_and_delivery_date_from_gcs_path(directory_path) #directory_path.split('/')[0]
    blob_name = '/'.join(directory_path.split('/')[1:])
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    try:
        # First check if directory exists and has files
        blobs = bucket.list_blobs(prefix=blob_name)
        
        # Delete any existing files in the directory
        for blob in blobs:
            try:
                bucket.blob(blob.name).delete()
                logger.info(f"Deleted existing file: {blob.name}")
            except Exception as e:
                logger.warning(f"Failed to delete file {blob.name}: {e}")
        
        # Create the directory marker
        blob = bucket.blob(blob_name)
        if not blob.exists():
            blob.upload_from_string('')
            logger.info(f"Created directory: {directory_path}")
            
    except Exception as e:
        logger.error(f"Unable to process GCS directory {directory_path}: {e}")
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

def get_table_name_from_gcs_path(gcs_file_path: str) -> str:
    # Extract file name from a GCS path and removes extension
    # e.g. synthea53/2024-12-31/care_site.parquet -> care_site
    return (
        gcs_file_path.split('/')[-1]
        .replace(constants.PARQUET, '')
        .replace(constants.CSV, '')
        .replace(constants.FIXED_FILE_TAG_STRING, '')
        .lower()
    )

def get_table_schema(table_name: str, cdm_version: str) -> dict:
    # Returns schema for specified OMOP table, if table exists in CDM
    # Returns empty dictionary if table is not in OMOP
    table_name = table_name.lower()
    schema_file = f"{constants.CDM_SCHEMA_PATH}{cdm_version}/{constants.CDM_SCHEMA_FILE_NAME}"

    try:
        with open(schema_file, 'r') as f:
            schema_json = f.read()
            schema = json.loads(schema_json)

        # Check if table exists in schema
        if table_name in schema:
            return {table_name: schema[table_name]}
        else:
            return {}
            
    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error getting table schema: {str(e)}")
    
def get_bucket_and_delivery_date_from_gcs_path(gcs_file_path: str) -> Tuple[str, str]:
    # Returns a tuple of the bucket_name and delivery date for a given file in GCS
    # e.g. synthea53/2024-12-31/care_site.parquet -> synthea53, 2024-12-31
    bucket_name, delivery_date = gcs_file_path.split('/')[:2]
    return bucket_name, delivery_date

def get_columns_from_parquet(gcs_file_path: str) -> list:
    """
    Reads Parquet file schema from the specified 'gs://{gcs_file_path}' 
    using DuckDB to introspect its columns. Returns a list of columns found 
    in the Parquet file.

    This function:
        1. Creates a temporary DuckDB table from the Parquet file, limited to 0 rows.
        2. Uses PRAGMA table_info(...) to retrieve column metadata.
        3. Drops the temporary table.
        4. Returns a list of the actual column names present in the file.
    """

    # Create a unique or table-specific name for introspection
    table_name_for_introspection = "temp_introspect_table"

    
    conn, local_db_file, tmp_dir = create_duckdb_connection()
    try:
        with conn:
            
            # Drop any existing temp table with the same name, just to be safe
            conn.execute(f"DROP TABLE IF EXISTS {table_name_for_introspection}")

            # Create a temp table from the Parquet file with zero rows
            conn.execute(f"""
                CREATE TEMP TABLE {table_name_for_introspection} AS
                SELECT * FROM 'gs://{gcs_file_path}' LIMIT 0
            """)

            # Retrieve column metadata from DuckDB
            pragma_info = conn.execute(
                f"PRAGMA table_info({table_name_for_introspection})"
            ).fetchall() # Okay to use fetchall() because we are certain list will fit in memory

            # The second element of each row in PRAGMA table_info is the column name
            actual_columns = [row[1] for row in pragma_info]

            # Drop the temp table
            conn.execute(f"DROP TABLE IF EXISTS {table_name_for_introspection}")
    except Exception as e:
        logger.error(f"Unable to get Parquet column list: {e}")
        sys.exit(0)
    finally:
        close_duckdb_connection(conn, local_db_file, tmp_dir)
        
    return actual_columns