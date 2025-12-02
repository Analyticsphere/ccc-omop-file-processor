import gc
import json
import logging
import os
import re
import sys
import uuid
from datetime import datetime
from typing import Tuple

import duckdb  # type: ignore
from fsspec import filesystem  # type: ignore
from google.cloud import storage as gcs_storage  # type: ignore

import core.constants as constants
import core.helpers.report_artifact as report_artifact
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
    Configures memory limits, threading, and GCS filesystem support.
    """
    try:
        random_string = str(uuid.uuid4())
        
        # GCS bucket mounted to /mnt/data/ in clouldbuild.yml
        tmp_dir = f"/mnt/data/"
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
        # Unneeded when writing to GCS
        conn.execute(f"SET max_temp_directory_size='{constants.DUCKDB_MAX_SIZE}'")

        # Register GCS filesystem to read/write to GCS buckets
        conn.register_filesystem(filesystem('gcs'))

        return conn, local_db_file
    except Exception as e:
        raise Exception(f"Unable to create DuckDB instance: {e}") from e

def close_duckdb_connection(conn: duckdb.DuckDBPyConnection, local_db_file: str) -> None:
    """Close DuckDB connection, remove temporary files, and free memory."""
    try:
        # Close the DuckDB connection
        conn.close()

        # Remove the local database file if it exists
        if os.path.exists(local_db_file):
            os.remove(local_db_file)

    except Exception as e:
        logger.error(f"Unable to close DuckDB connection: {e}")
    finally:
        # Manually run garabage collection here to reclaim memory
        gc.collect()

def execute_duckdb_sql(sql: str, error_msg: str) -> None:
    """Execute SQL statement using DuckDB with automatic connection management."""
    try:
        conn, local_db_file = create_duckdb_connection()

        with conn:
            conn.execute(sql)
    except Exception as e:
        raise Exception(f"{error_msg}: {str(e)}") from e
    finally:
        close_duckdb_connection(conn, local_db_file)

def get_table_name_from_gcs_path(gcs_file_path: str) -> str:
    """
    Extract table name from file path by removing directory and extension.
    Example: synthea53/2024-12-31/care_site.parquet -> care_site
    """
    file_name = gcs_file_path.split('/')[-1].lower()

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
    
def get_bucket_and_delivery_date_from_gcs_path(gcs_file_path: str) -> Tuple[str, str]:
    """
    Extract bucket name and delivery date from file path.
    Example: synthea53/2024-12-31/care_site.parquet -> (synthea53, 2024-12-31)
    """
    gcs_file_path = storage.strip_scheme(gcs_file_path)
    bucket_name, delivery_date = gcs_file_path.split('/')[:2]
    return bucket_name, delivery_date

def get_columns_from_file(gcs_file_path: str) -> list:
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

    gcs_file_path = storage.strip_scheme(gcs_file_path)
    
    # Determine file type by extension
    is_csv = gcs_file_path.lower().endswith(constants.CSV) | gcs_file_path.lower().endswith(constants.CSV_GZ)
    
    # Create a unique table name for introspection
    table_name_for_introspection = "temp_introspect_table"

    conn, local_db_file = create_duckdb_connection()
    try:
        with conn:
            # Drop any existing temp table with the same name
            conn.execute(f"DROP TABLE IF EXISTS {table_name_for_introspection}")

            # Create a temp table based on file type with zero rows
            if is_csv:
                tmp_tbl_sql = f"""
                    CREATE TEMP TABLE {table_name_for_introspection} AS
                    SELECT * FROM read_csv('{storage.get_uri(gcs_file_path)}',
                                          null_padding=True,
                                          ALL_VARCHAR=True,
                                          strict_mode=False,
                                          ignore_errors=True)
                    LIMIT 0
                """

            else:  # Parquet file
                tmp_tbl_sql = f"""
                    CREATE TEMP TABLE {table_name_for_introspection} AS
                    SELECT * FROM '{storage.get_uri(gcs_file_path)}'
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
        close_duckdb_connection(conn, local_db_file)
        
    return actual_columns

def valid_parquet_file(gcs_file_path: str) -> bool:
    """Check if Parquet file exists and can be read by DuckDB."""
    conn, local_db_file = create_duckdb_connection()

    if not parquet_file_exists(gcs_file_path):
        return False

    try:
        with conn:
            # If the file is not a valid Parquet file, this will throw an exception
            conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{storage.get_uri(gcs_file_path)}')")

            # If we get to this point, we were able to describe the Parquet file and will assume it's valid
            return True
    except Exception as e:
        logger.error(f"Unable to validate Parquet file: {e}")
        return False
    finally:
        close_duckdb_connection(conn, local_db_file)

def get_parquet_artifact_location(gcs_file_path: str) -> str:
    """Get path to processed Parquet artifact in converted_files directory."""
    file_name = get_table_name_from_gcs_path(gcs_file_path)
    base_directory, delivery_date = get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
    
    # Create the parquet file name
    parquet_file_name = f"{file_name.lower()}{constants.PARQUET}"
    
    # Construct the final parquet path
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{parquet_file_name}"

    return parquet_path

def get_parquet_harmonized_path(gcs_file_path: str) -> str:
    """Get path to directory for vocabulary-harmonized Parquet artifacts."""
    file_name = get_table_name_from_gcs_path(gcs_file_path)
    base_directory, delivery_date = get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
        
    # Construct the final parquet path
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}{file_name}/"

    return parquet_path

def get_omop_etl_destination_path(gcs_file_path: str) -> str:
    """Get path to OMOP ETL artifacts directory for transformed tables."""
    base_directory, delivery_date = get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
        
    # Construct the path to the OMOP ETL directory to store ETL'ed files
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}"

    return parquet_path

def get_invalid_rows_path_from_gcs_path(gcs_file_path: str) -> str:
    """Get path to invalid rows Parquet file for tables that failed normalization."""
    table_name = get_table_name_from_gcs_path(gcs_file_path).lower()
    bucket, subfolder = get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
    invalid_rows_path = f"{bucket}/{subfolder}/{constants.ArtifactPaths.INVALID_ROWS.value}{table_name}{constants.PARQUET}"

    return invalid_rows_path

def parquet_file_exists(file_path: str) -> bool:
    """
    Check if a Parquet file exists in Google Cloud Storage.
    """
    # Strip storage scheme prefix if it exists
    gcs_path = storage.strip_scheme(file_path)
    
    # Parse bucket and blob name
    path_parts = gcs_path.split('/')
    bucket_name = path_parts[0]
    blob_name = '/'.join(path_parts[1:])
    
    try:
        # Initialize storage client with default credentials
        storage_client = gcs_storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        
        return blob.exists()
    except Exception as e:
        logger.error(f"Error checking Parquet file existence: {e}")
        return False

def get_optimized_vocab_file_path(vocab_version: str, vocab_gcs_bucket: str) -> str:
    """Get path to optimized vocabulary Parquet file."""
    optimized_vocab_path = f"{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{constants.OPTIMIZED_VOCAB_FILE_NAME}"
    return optimized_vocab_path

def get_delivery_vocabulary_version(gcs_bucket: str, delivery_date: str) -> str:
    """Extract vocabulary version from vocabulary table in a site's delivery."""
    vocabulary_parquet_file = f"{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}vocabulary{constants.PARQUET}"

    if parquet_file_exists(vocabulary_parquet_file):
        conn, local_db_file = create_duckdb_connection()
        try:
            with conn:
                vocab_version_query = f"""
                    SELECT vocabulary_version
                    FROM read_parquet('{storage.get_uri(vocabulary_parquet_file)}')
                    WHERE vocabulary_id = 'None'
                """
                result = conn.execute(vocab_version_query).fetchone()
                if result:
                    return result[0]
                return "Unknown vocabulary version"
        except Exception as e:
            logger.error(f"Unable to upgrade file: {e}")
            return "Unknown vocabulary version"
        finally:
            close_duckdb_connection(conn, local_db_file)
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

def create_final_report_artifacts(report_data: dict) -> None:
    """Create report artifacts with delivery and processing metadata."""
    gcs_bucket = report_data["gcs_bucket"]
    delivery_date = report_data["delivery_date"]

    # Create tuples to represent a value to add to delivery report, and what that value describes
    delivery_date_value = (delivery_date, constants.DELIVERY_DATE_REPORT_NAME)
    site_display_name = (report_data["site_display_name"], constants.SITE_DISPLAY_NAME_REPORT_NAME)
    file_delivery_format = (report_data["file_delivery_format"], constants.FILE_DELIVERY_FORMAT_REPORT_NAME)
    delivered_cdm_version = (report_data["delivered_cdm_version"], constants.DELIVERED_CDM_VERSION_REPORT_NAME)
    delivered_vocab_version = (get_delivery_vocabulary_version(gcs_bucket, delivery_date), constants.DELIVERED_VOCABULARY_VERSION_REPORT_NAME)
    target_vocabulary_version = (report_data["target_vocabulary_version"], constants.TARGET_VOCABULARY_VERSION_REPORT_NAME)
    target_cdm_version = (report_data["target_cdm_version"], constants.TARGET_CDM_VERSION_REPORT_NAME)
    target_cdm_version = (report_data["target_cdm_version"], constants.TARGET_CDM_VERSION_REPORT_NAME)
    file_processor_version = (os.getenv('COMMIT_SHA'), constants.FILE_PROCESSOR_VERSION_REPORT_NAME)
    processed_date = (datetime.today().strftime('%Y-%m-%d'), constants.PROCESSED_DATE_REPORT_NAME)

    # Create a list of the tuples
    report_data_points = [processed_date, file_processor_version, delivery_date_value, site_display_name, file_delivery_format, delivered_cdm_version, delivered_vocab_version, target_vocabulary_version, target_cdm_version]

    # Iterate over each tuple
    for report_data_point in report_data_points:
        # Seperate value and the thing it describes from tuple
        value, reporting_item = report_data_point
        value_as_concept_id: int = 0

        if reporting_item in [constants.DELIVERED_CDM_VERSION_REPORT_NAME, constants.TARGET_CDM_VERSION_REPORT_NAME]:
            value_as_concept_id = get_cdm_version_concept_id(value)

        ra = report_artifact.ReportArtifact(
            delivery_date=delivery_date,
            artifact_bucket=gcs_bucket,
            concept_id=0,
            name=f"{reporting_item}",
            value_as_string=value,
            value_as_concept_id=value_as_concept_id,
            value_as_number=None
        )
        ra.save_artifact()

def list_gcs_files(bucket_name: str, folder_prefix: str, file_format: str) -> list[str]:
    """
    Lists files within a specific folder in a GCS bucket (non-recursively).
    """
    try:
        # Initialize the GCS client
        storage_client = gcs_storage.Client()

        # Get the bucket
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
        # Files must be of specific type
        files = [
            os.path.basename(blob.name)
            for blob in blobs
            if blob.name != folder_prefix and blob.name.lower().endswith(file_format)
        ]

        return files

    except Exception as e:
        raise Exception(f"Error listing files in GCS: {str(e)}")

def generate_report(report_data: dict) -> None:
    """
    Generate final delivery report by consolidating all report artifacts into CSV.
    Combines temporary report files and creates metadata entries.
    """
    create_final_report_artifacts(report_data)

    site = report_data["site"]
    gcs_bucket = report_data["gcs_bucket"]
    delivery_date = report_data["delivery_date"]

    report_tmp_dir = f"{delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
    tmp_files = list_gcs_files(gcs_bucket, report_tmp_dir, constants.PARQUET)

    if len(tmp_files) > 0:
        conn, local_db_file = create_duckdb_connection()
        # Increase max_expression_depth in case there are many report artifacts
        conn.execute("SET max_expression_depth TO 1000000")

        # Build UNION ALL SELECT statement to join together files
        file_paths = [storage.get_uri(f"{gcs_bucket}/{report_tmp_dir}{file}") for file in tmp_files]
        select_statement = " UNION ALL ".join([f"SELECT * FROM read_parquet('{path}')" for path in file_paths])

        try:
            with conn:
                output_path = storage.get_uri(f"{gcs_bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT.value}delivery_report_{site}_{delivery_date}{constants.CSV}")
                join_files_query = f"""
                    COPY (
                        {select_statement}
                    ) TO
                        '{output_path}'
                        (HEADER, DELIMITER ',')
                """ 
                conn.execute(join_files_query)
        except Exception as e:
            logger.error(f"Unable to merge reporting artifacts: {e}")
            raise Exception(f"Unable to merge reporting artifacts: {e}") from e
        finally:
            close_duckdb_connection(conn, local_db_file)

def get_report_tmp_artifacts_gcs_path(bucket: str, delivery_date: str) -> str:
    """
    Returns the path to the temporary report artifacts directory.

    NOTE: This function now returns the path WITHOUT storage scheme prefix (e.g., gs://),
    consistent with all other path utility functions. Callers should use storage.get_uri()
    if they need the full URI.
    """
    report_tmp_dir = f"{bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
    return report_tmp_dir

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

def placeholder_to_file_path(site: str, site_bucket: str, delivery_date: str, sql_script: str, vocab_version: str, vocab_gcs_bucket: str) -> str:
    """
    Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
    """
    replacement_result = sql_script

    for placeholder, replacement in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
        clinical_data_table_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{replacement}{constants.PARQUET}")
        replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

    # Replaces vocab table place holder strings in SQL scripts with paths to target vocabulary version
    for placeholder, replacement in constants.VOCAB_PATH_PLACEHOLDERS.items():
        vocab_table_path = storage.get_uri(f"{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{replacement}{constants.PARQUET}")
        replacement_result = replacement_result.replace(placeholder, vocab_table_path)

    # Add site name 
    replacement_result = replacement_result.replace(constants.SITE_PLACEHOLDER_STRING, site)

    # Add current date
    replacement_result = replacement_result.replace(constants.CURRENT_DATE_PLACEHOLDER_STRING, datetime.now().strftime('%Y-%m-%d'))

    return replacement_result

def placeholder_to_harmonized_file_path(site: str, site_bucket: str, delivery_date: str, sql_script: str, vocab_version: str, vocab_gcs_bucket: str) -> str:
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
        vocab_table_path = storage.get_uri(f"{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{replacement}{constants.PARQUET}")
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