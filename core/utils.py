import gc
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime
from typing import Optional, Tuple

import duckdb  # type: ignore
from fsspec import filesystem  # type: ignore

import core.constants as constants
import core.omop_client as omop_client
import core.vocab_manager as vocab_manager
from core.storage_backend import storage

"""
Set up a logging instance that will write to stdout (and therefor show up in Google Cloud logs)
"""
logging.basicConfig(
    level=logging.INFO,
    format='omop-fp %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
# Create the logger at module level so its settings are applied throughout code base
logger = logging.getLogger(__name__)

def create_duckdb_connection() -> tuple[duckdb.DuckDBPyConnection, str]:
    """
    Create DuckDB connection with optimized settings for processing OMOP files.
    Configures memory limits, threading, and filesystem support.
    """
    try:
        random_string = str(uuid.uuid4())

        # Use configurable temp directory from environment variable
        tmp_dir = os.getenv('DUCKDB_TEMP_DIR', '/mnt/data/')
        local_db_file = f"{tmp_dir}{random_string}.db"

        conn = duckdb.connect(local_db_file)
        conn.execute(f"SET temp_directory='{tmp_dir}'")
        conn.execute(f"SET memory_limit='{constants.DUCKDB_MEMORY_LIMIT}'")
        conn.execute(f"SET max_memory='{constants.DUCKDB_MEMORY_LIMIT}'")

        # Improves performance for large queries
        conn.execute("SET preserve_insertion_order = false")

        # Set to <= number of CPU cores
        # https://duckdb.org/docs/configuration/overview.html#global-configuration-options
        conn.execute(f"SET threads={constants.DUCKDB_THREADS}")

        # Set max size to allow on disk
        conn.execute(f"SET max_temp_directory_size='{constants.DUCKDB_MAX_SIZE}'")

        # Register filesystem for cloud storage if using GCS backend
        if constants.STORAGE_BACKEND == constants.GCS_BACKEND:
            conn.register_filesystem(filesystem('gcs'))

        return conn, local_db_file
    except Exception as e:
        raise Exception(f"Unable to create DuckDB instance: {e}") from e

def close_duckdb_connection(conn: duckdb.DuckDBPyConnection, local_db_file: Optional[str]) -> None:
    """Close DuckDB connection, remove temporary files, and free memory."""
    try:
        # Close the DuckDB connection
        conn.close()

        # Remove the local database file if it exists
        if local_db_file and os.path.exists(local_db_file):
            os.remove(local_db_file)

    except Exception as e:
        logger.error(f"Unable to close DuckDB connection: {e}")
    finally:
        # Manually run garabage collection here to reclaim memory
        gc.collect()

def execute_duckdb_sql(sql: str, error_msg: str, return_results: bool = False):
    """
    Execute SQL statement using DuckDB with automatic connection management.

    Args:
        sql: SQL statement to execute
        error_msg: Error message to display if execution fails
        return_results: If True, returns all query results as a list. If False, returns None. Defaults to False.

    Returns:
        If return_results=True: List of result rows from the query
        If return_results=False: None
    """
    conn = None
    local_db_file = None
    try:
        conn, local_db_file = create_duckdb_connection()

        with conn:
            result = conn.execute(sql)
            if return_results:
                # Fetch all results before closing connection
                return result.fetchall()
            return None
    except Exception as e:
        raise Exception(f"{error_msg}: {str(e)}") from e
    finally:
        if conn is not None:
            close_duckdb_connection(conn, local_db_file)

def get_table_name_from_path(file_path: str) -> str:
    """
    Extract table name from file path by removing directory and extension.
    Example: synthea53/2024-12-31/care_site.parquet -> care_site
    """
    file_name = file_path.split('/')[-1].lower()

    for ext in constants.FILE_EXTENSIONS:
        file_name = file_name.replace(ext, '')

    # If using a 'fixed' file, remove the appened string indicating it's a fixed file
    file_name = file_name.replace(constants.FIXED_FILE_TAG_STRING, '')

    return file_name

def get_cdm_schema(cdm_version: str) -> dict:
    """Load OMOP CDM schema JSON for specified version."""
    schema_file = f"{constants.CDM_SCHEMA_PATH}{cdm_version}/{constants.CDM_SCHEMA_FILE_NAME}"
    try:
        with open(schema_file, 'r') as f:
            schema_json = f.read()
        schema = json.loads(schema_json)
        return schema
    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")

def get_table_schema(table_name: str, cdm_version: str) -> dict:
    """
    Get schema for specified OMOP table from CDM schema.
    Returns empty dict if table doesn't exist in OMOP CDM.
    """
    table_name = table_name.lower()

    try:
        schema = get_cdm_schema(cdm_version=cdm_version)

        # Check if table exists in schema
        if table_name in schema:
            return {table_name: schema[table_name]}
        else:
            return {} 
    except Exception as e:
        raise Exception(f"Unexpected error getting table {table_name} schema: {str(e)}")
    
def get_bucket_and_delivery_date_from_path(file_path: str) -> Tuple[str, str]:
    """
    Extract bucket name and delivery date from file path.
    Example: synthea53/2024-12-31/care_site.parquet -> (synthea53, 2024-12-31)
    """
    file_path = storage.strip_scheme(file_path)
    bucket_name, delivery_date = file_path.split('/')[:2]
    return bucket_name, delivery_date

def get_columns_from_file(file_path: str) -> list:
    """
    Reads file schema from the specified file path using DuckDB
    to introspect its columns. Supports both Parquet and CSV files.
    Returns a list of columns found in the file.

    This function:
        1. Determines file type based on extension (.parquet or .csv)
        2. Creates a temporary DuckDB table from the file, limited to 0 rows.
        3. Uses PRAGMA table_info(...) to retrieve column metadata.
        4. Drops the temporary table.
        5. Returns a list of the actual column names present in the file.
    """

    file_path = storage.strip_scheme(file_path)
    
    # Determine file type by extension
    is_csv = file_path.lower().endswith(constants.CSV) | file_path.lower().endswith(constants.CSV_GZ)
    
    # Create a unique table name for introspection
    table_name_for_introspection = "temp_introspect_table"

    conn = None
    local_db_file = None
    try:
        conn, local_db_file = create_duckdb_connection()
        with conn:
            # Drop any existing temp table with the same name
            conn.execute(f"DROP TABLE IF EXISTS {table_name_for_introspection}")

            # Create a temp table based on file type with zero rows
            if is_csv:
                tmp_tbl_sql = f"""
                    CREATE TEMP TABLE {table_name_for_introspection} AS
                    SELECT * FROM read_csv('{storage.get_uri(file_path)}',
                                          null_padding=True,
                                          ALL_VARCHAR=True,
                                          strict_mode=False,
                                          ignore_errors=True)
                    LIMIT 0
                """

            else:  # Parquet file
                tmp_tbl_sql = f"""
                    CREATE TEMP TABLE {table_name_for_introspection} AS
                    SELECT * FROM '{storage.get_uri(file_path)}'
                    LIMIT 0
                """

            conn.execute(tmp_tbl_sql)

            # Retrieve column metadata from DuckDB
            pragma_info = conn.execute(
                f"PRAGMA table_info({table_name_for_introspection})"
            ).fetchall()

            # The second element of each row in PRAGMA table_info is the column name
            actual_columns = [row[1] for row in pragma_info]

            # Drop the temp table
            conn.execute(f"DROP TABLE IF EXISTS {table_name_for_introspection}")
    except Exception as e:
        raise Exception(f"Unable to get column list from {'CSV' if is_csv else 'Parquet'} file: {e}") from e
    finally:
        if conn is not None:
            close_duckdb_connection(conn, local_db_file)
        
    return actual_columns

def valid_parquet_file(file_path: str) -> bool:
    """Check if Parquet file exists and can be read by DuckDB."""
    if not parquet_file_exists(file_path):
        return False

    conn = None
    local_db_file = None
    try:
        conn, local_db_file = create_duckdb_connection()
        with conn:
            # If the file is not a valid Parquet file, this will throw an exception
            conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{storage.get_uri(file_path)}')")

            # If we get to this point, we were able to describe the Parquet file and will assume it's valid
            return True
    except Exception as e:
        logger.error(f"Unable to validate Parquet file: {e}")
        return False
    finally:
        if conn is not None:
            close_duckdb_connection(conn, local_db_file)

def get_parquet_artifact_location(file_path: str) -> str:
    """Get path to processed Parquet artifact in converted_files directory."""
    file_name = get_table_name_from_path(file_path)
    base_directory, delivery_date = get_bucket_and_delivery_date_from_path(file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
    
    # Create the parquet file name
    parquet_file_name = f"{file_name.lower()}{constants.PARQUET}"
    
    # Construct the final parquet path
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{parquet_file_name}"

    return parquet_path

def get_parquet_harmonized_path(file_path: str) -> str:
    """Get path to directory for vocabulary-harmonized Parquet artifacts."""
    file_name = get_table_name_from_path(file_path)
    base_directory, delivery_date = get_bucket_and_delivery_date_from_path(file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
        
    # Construct the final parquet path
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}{file_name}/"

    return parquet_path

def get_omop_etl_destination_path(file_path: str) -> str:
    """Get path to OMOP ETL artifacts directory for transformed tables."""
    base_directory, delivery_date = get_bucket_and_delivery_date_from_path(file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
        
    # Construct the path to the OMOP ETL directory to store ETL'ed files
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}"

    return parquet_path

def get_invalid_rows_path_from_path(file_path: str) -> str:
    """Get path to invalid rows Parquet file for tables that failed normalization."""
    table_name = get_table_name_from_path(file_path).lower()
    bucket, subfolder = get_bucket_and_delivery_date_from_path(file_path)
    invalid_rows_path = f"{bucket}/{subfolder}/{constants.ArtifactPaths.INVALID_ROWS.value}{table_name}{constants.PARQUET}"

    return invalid_rows_path

def parquet_file_exists(file_path: str) -> bool:
    """
    Check if a Parquet file exists in the configured storage backend.
    """
    try:
        return storage.file_exists(file_path)
    except Exception as e:
        logger.error(f"Error checking Parquet file existence: {e}")
        return False

def get_optimized_vocab_file_path(vocab_version: str, vocab_path: str) -> str:
    """Get path to optimized vocabulary Parquet file."""
    optimized_vocab_path = f"{vocab_path}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{constants.OPTIMIZED_VOCAB_FILE_NAME}"
    return optimized_vocab_path

def get_delivery_vocabulary_version(bucket: str, delivery_date: str) -> str:
    """Extract vocabulary version from vocabulary table in a site's delivery."""

    vocabulary_parquet_file = f"{bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}vocabulary{constants.PARQUET}"

    if parquet_file_exists(vocabulary_parquet_file):
        try:
            # Generate SQL to query vocabulary version
            vocab_version_query = vocab_manager.VocabularyManager.generate_vocab_version_query_sql(storage.get_uri(vocabulary_parquet_file))
            result = execute_duckdb_sql(vocab_version_query, "Unable to query vocabulary version", return_results=True)
            if result and len(result) > 0:
                return result[0][0]
            return "Unknown vocabulary version"
        except Exception as e:
            logger.error(f"Unable to query vocabulary version: {e}")
            return "Unknown vocabulary version"
    else:
        return "No vocabulary file provided"

def get_cdm_version_concept_id(cdm_version: str) -> int:
    """Get OMOP concept_id for CDM version."""
    if cdm_version == constants.CDM_v53:
        return constants.CDM_v53_CONCEPT_ID
    elif cdm_version == constants.CDM_v54:
        return constants.CDM_v54_CONCEPT_ID
    else:
        return 0

def list_files(bucket_name: str, folder_prefix: str, file_format: str) -> list[str]:
    """
    Lists files within a specific folder (non-recursively).
    """
    try:
        # Construct the directory path
        directory_path = f"{bucket_name}/{folder_prefix}" if folder_prefix else bucket_name

        # Use storage backend to list files with pattern
        files = storage.list_files(directory_path, pattern=f"*{file_format}")

        return files

    except Exception as e:
        raise Exception(f"Error listing files: {str(e)}")

def get_primary_key_column(table_name: str, cdm_version: str) -> str:
    """Get primary key column name for OMOP table, or empty string if no primary key exists."""
    schema = get_table_schema(table_name, cdm_version)
    columns = schema[table_name]["columns"]

    # Iterate over each column name and its properties
    for column_name, column_properties in columns.items():
        if "primary_key" in column_properties and column_properties["primary_key"] == "true":
            return column_name
    
    # For tables with no primary key, return ""
    return ""

def placeholder_to_file_path(site: str, site_bucket: str, delivery_date: str, sql_script: str, vocab_version: str, vocab_path: str) -> str:
    """
    Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
    """
    replacement_result = sql_script

    for placeholder, replacement in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
        clinical_data_table_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{replacement}{constants.PARQUET}")
        replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

    # Replaces vocab table place holder strings in SQL scripts with paths to target vocabulary version
    for placeholder, replacement in constants.VOCAB_PATH_PLACEHOLDERS.items():
        vocab_table_path = storage.get_uri(f"{vocab_path}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{replacement}{constants.PARQUET}")
        replacement_result = replacement_result.replace(placeholder, vocab_table_path)

    # Add site name
    replacement_result = replacement_result.replace(constants.SITE_PLACEHOLDER_STRING, site)

    # Add current date
    replacement_result = replacement_result.replace(constants.CURRENT_DATE_PLACEHOLDER_STRING, datetime.now().strftime('%Y-%m-%d'))

    return replacement_result

def placeholder_to_harmonized_file_path(site: str, site_bucket: str, delivery_date: str, sql_script: str, vocab_version: str, vocab_path: str) -> str:
    """
    Replaces clinical data table placeholder strings in SQL scripts with paths to the appropriate parquet files.

    This intelligently determines the correct path based on whether the table underwent vocabulary harmonization:
    - Harmonized tables (in VOCAB_HARMONIZED_TABLES): {scheme}://{bucket}/{date}/artifacts/omop_etl/{table}/{table}.parquet
    - Non-harmonized tables (not in list): {scheme}://{bucket}/{date}/artifacts/converted_files/{table}.parquet

    This is used for derived table generation after vocabulary harmonization is complete.
    """
    replacement_result = sql_script

    # Replace clinical data placeholders with the appropriate file paths
    for placeholder, table_name in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
        # Check if this table underwent vocabulary harmonization
        if table_name in constants.VOCAB_HARMONIZED_TABLES:
            # Harmonized tables are in: omop_etl/{table_name}/{table_name}.parquet
            table_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}{table_name}/{table_name}{constants.PARQUET}")
        else:
            # Non-harmonized tables are in: converted_files/{table_name}.parquet
            table_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{table_name}{constants.PARQUET}")

        replacement_result = replacement_result.replace(placeholder, table_path)

    # Replaces vocab table place holder strings in SQL scripts with paths to target vocabulary version
    for placeholder, replacement in constants.VOCAB_PATH_PLACEHOLDERS.items():
        vocab_table_path = storage.get_uri(f"{vocab_path}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{replacement}{constants.PARQUET}")
        replacement_result = replacement_result.replace(placeholder, vocab_table_path)

    # Add site name
    replacement_result = replacement_result.replace(constants.SITE_PLACEHOLDER_STRING, site)

    # Add current date
    replacement_result = replacement_result.replace(constants.CURRENT_DATE_PLACEHOLDER_STRING, datetime.now().strftime('%Y-%m-%d'))

    return replacement_result

def clean_column_name_for_sql(name: str) -> str:
    """
    Remove any character that is not a Unicode word character (letter, digit, underscore).
    Also strips leading/trailing whitespace and lowercases the name.
    """
    cleaned = re.sub(r'[^\w]', '', name, flags=re.UNICODE)
    cleaned = cleaned.strip()
    cleaned = cleaned.replace('"', '')
    cleaned = cleaned.lower()
    cleaned = cleaned.replace(' ', '_')

    return cleaned

def get_placeholder_value(column_name: str, column_type: str) -> str:
    """
    Get default value for column based on type.
    """
    
    # Concept ID columns default to 0 per OHDSI convention for unknown concepts.
    if column_name.endswith("_concept_id"):
        return "'0'"

    default_value = constants.DEFAULT_COLUMN_VALUES[column_type]
    
    return default_value