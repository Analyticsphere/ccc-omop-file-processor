import os
from typing import Optional

from google.cloud import bigquery  # type: ignore
from google.cloud import storage  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore

import core.constants as constants
import core.utils as utils


def remove_all_tables(project_id: str, dataset_id: str) -> None:
    """
    Deletes all tables within a given BigQuery dataset
    """
    try:
        client = bigquery.Client()
        qualified_dataset_id = f"{project_id}.{dataset_id}"

        # List all tables in the dataset
        tables = client.list_tables(qualified_dataset_id)

        # Delete each table
        for table in tables:
            table_id_full = f"{project_id}.{dataset_id}.{table.table_id}"
            client.delete_table(table_id_full)
            utils.logger.info(f"Deleted table {table_id_full}")
    except Exception as e:
        raise Exception(f"Unable to delete BigQuery table {table_id_full}: {e}") from e

def load_parquet_to_bigquery(file_path: str, project_id: str, dataset_id: str, table_name: str, write_type: constants.BQWriteTypes) -> None:
    """
    Load Parquet artifact file from GCS directly into BigQuery.
    """
    # SPECIFIC_FILE -> overwrite table with the exact Parquet file in file_path
    if write_type == constants.BQWriteTypes.SPECIFIC_FILE:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        parquet_path = file_path
    # PROCESSED_FILE -> overwrite table with the pipeline-processed version of the file in file_path
    elif write_type == constants.BQWriteTypes.PROCESSED_FILE:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        parquet_path = f"gs://{utils.get_parquet_artifact_location(file_path)}"
        
    # When upgrading to 5.4, some Parquet files may get deleted
    # First confirm that Parquet file does exist before trying to load to BQ
    if not utils.parquet_file_exists(parquet_path):
        utils.logger.warning(f"Parquet file {parquet_path} does not exist, skipping")
        return

    try:
        client = bigquery.Client(project=project_id)
        table_id_full = f"{project_id}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=write_disposition,
            # autodetect=True  # Schema is explicity defined in Parquet file
        )
        
        # Start the load job
        load_job = client.load_table_from_uri(
            parquet_path,
            table_id_full,
            job_config=job_config
        )
        
        # Execute result() so we wait for load to complete
        load_job.result()
        
        utils.logger.info(f"Loaded data to BigQuery table {table_id_full}")
    except Exception as e:
        raise Exception(f"Error loading Parquet file {parquet_path} to BigQuery: {e}") from e

def get_bq_log_row(site: str, date_to_check: str) -> list:
    client = bigquery.Client()

    # Check if the table exists. If it doesn't, return an empty list.
    try:
        client.get_table(constants.BQ_LOGGING_TABLE)
    except NotFound:
        # Table does not exist, so return an empty list without error. This will occur on first run.
        return []

    # Construct the query to retrieve logs for the given site and delivery date.
    query = f"""
        SELECT *
        FROM `{constants.BQ_LOGGING_TABLE}`
        WHERE site_name = @site
          AND delivery_date = @delivery_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("site", "STRING", site),
            bigquery.ScalarQueryParameter("delivery_date", "DATE", date_to_check),
        ]
    )

    try:
        query_job = client.query(query, job_config=job_config)
        
        # Convert Row objects to dictionaries
        results = [dict(row) for row in query_job.result()]
        
        return results
    except Exception as e:
        raise Exception(f"Failed to retrieve BigQuery pipeline logs for site '{site}' and date '{date_to_check}': {e}") from e

def create_gcs_directory(directory_path: str, delete_exisiting_files: bool = True) -> None:
    """Creates a directory in GCS by creating an empty blob.
    If directory exists and delete_exisiting_files is True, deletes any existing files first.
    """
    bucket_name, _ = utils.get_bucket_and_delivery_date_from_gcs_path(directory_path) #directory_path.split('/')[0]
    blob_name = '/'.join(directory_path.split('/')[1:])

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    try:
        # First check if directory exists and has files
        blobs = bucket.list_blobs(prefix=blob_name)

        if delete_exisiting_files:
            # Delete any existing files in the directory
            for blob in blobs:
                try:
                    bucket.blob(blob.name).delete()
                except Exception as e:
                    utils.logger.error(f"Failed to delete file {blob.name}: {e}")

        # Create the directory marker
        blob = bucket.blob(blob_name)
        if not blob.exists():
            blob.upload_from_string('')

    except Exception as e:
        raise Exception(f"Unable to create artifact directories in GCS path {directory_path}: {e}") from e

def delete_gcs_file(gcs_path: str) -> None:
    """
    Deletes a file from Google Cloud Storage.
    """
    try:
        # Initialize GCS client
        storage_client = storage.Client()

        # Extract bucket name and blob path
        # Remove 'gs://' prefix and split into bucket and path
        path_without_prefix = gcs_path.replace('gs://', '')
        bucket_name = path_without_prefix.split('/')[0]
        blob_path = '/'.join(path_without_prefix.split('/')[1:])

        # Get bucket and blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_path)

        # Delete the file
        blob.delete()
    except Exception as e:
        raise Exception(f"Error deleting file {gcs_path}: {e}") from e

def vocab_gcs_path_exists(gcs_path: str) -> bool:
    """
    Check if a specific GCS path exists.
    For directory paths (ending with /), checks if any files exist with that prefix.
    For file paths, checks if the exact blob exists.
    """
    try:
        # Split the path into bucket name and blob path
        parts = gcs_path.split('/', 1)
        bucket_name = parts[0]
        blob_path = parts[1] if len(parts) > 1 else None

        # Initialize the client
        client = storage.Client()

        # Check if bucket exists
        try:
            bucket = client.get_bucket(bucket_name)
        except Exception:
            return False

        # If no blob path, we're just checking bucket existence
        if not blob_path:
            return True

        # If path ends with /, treat it as a directory prefix
        # Check if any blobs exist with this prefix
        if blob_path.endswith('/'):
            blobs = bucket.list_blobs(prefix=blob_path, max_results=1)
            # Check if at least one blob exists with this prefix
            return any(True for _ in blobs)
        else:
            # For file paths, check exact blob existence
            blob = bucket.blob(blob_path)
            return blob.exists()

    except Exception as e:
        # Handle any other unexpected errors
        utils.logger.error(f"Error checking GCS path: {e}")
        return False

def execute_bq_sql(sql_script: str, job_config: Optional[bigquery.QueryJobConfig]) -> bigquery.table.RowIterator:
    # Initialize the BigQuery client
    client = bigquery.Client()

    try:
        # Run the query
        if job_config:
            query_job = client.query(sql_script, job_config=job_config)
        else:
            query_job = client.query(sql_script)

        # Wait for the job to complete
        result = query_job.result()
        return result

    except Exception as e:
        raise Exception(f"Error executing query: {e}")

def download_from_gcs(gcs_file_path: str) -> str:
    """
    Downloads a CSV file from Google Cloud Storage to a local directory.
    """
    try:
        # Define paths
        bucket, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
        filename = f"{utils.get_table_name_from_gcs_path(gcs_file_path)}"
        source_blob = f"{delivery_date}/{filename}{constants.CSV}"
        destination_file_path = f"/tmp/{filename}{constants.CSV}"

        # Create directory to store file
        os.makedirs('/tmp', exist_ok=True)

        # Initialize a storage client
        storage_client = storage.Client()

        # Get the bucket
        bucket = storage_client.bucket(bucket)

        # Get the blob (file)
        blob = bucket.blob(source_blob)

        # Get the remote file size before downloading
        blob.reload()  # Ensure we have the latest metadata
        remote_size_bytes = blob.size
        remote_size_gb = float(float(remote_size_bytes) / float((1024 * 1024 * 1024)))

        # If the size of the file is greater than half of what is allocated to DuckDB, return exception
        if remote_size_gb > float(constants.DUCKDB_MEMORY_LIMIT.replace('GB', '')) * 0.5:
            raise Exception(f"CSV file {gcs_file_path} has invalid quoting, but cannot be fixed due to size constraints. Allocate at least {remote_size_gb * 2}GB of memory to the Cloud Run function")

        # Download the file
        blob.download_to_filename(destination_file_path)

        return destination_file_path

    except Exception as e:
        raise Exception(f"Error downloading file {gcs_file_path}: {e}")

def upload_to_gcs(local_file_path: str, bucket_name: str, destination_blob_name: str) -> None:
    """
    Uploads a file to the specified GCS bucket.
    """
    # Initialize the GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)

    # Create a blob object and upload the file
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_file_path)

def list_gcs_subdirectories(gcs_path: str) -> list:
    """
    Lists all subdirectories within a given GCS path.
    """
    try:
        # Split the path into bucket name and prefix
        parts = gcs_path.replace('gs://', '').split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        # Initialize the client
        client = storage.Client()
        bucket = client.bucket(bucket_name)

        # List blobs with the specified prefix
        blobs = bucket.list_blobs(prefix=prefix, delimiter='/')

        # Extract subdirectory names
        subdirectories = set()
        for page in blobs.pages:
            subdirectories.update(page.prefixes)

        return list(subdirectories)

    except Exception as e:
        raise Exception(f"Error listing subdirectories in GCS path {gcs_path}: {e}") from e 

def load_harmonized_parquets_to_bq(gcs_bucket: str, delivery_date: str, project_id: str, dataset_id: str) -> dict[str, list[str]]:
    """
    Load consolidated OMOP ETL parquet files from GCS to BigQuery.
    
    Discovers all consolidated parquet files in the OMOP_ETL artifacts directory
    and loads each one to its corresponding BigQuery table.
    
    Args:
        gcs_bucket: GCS bucket containing the ETL files
        delivery_date: Delivery date for path construction
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID
        
    Returns:
        Dictionary with 'loaded' and 'skipped' keys, each containing a list of table names
        
    Raises:
        Exception: If no table directories are found or if there's an error during processing
    """
    # Construct the OMOP_ETL directory path
    etl_folder = f"{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}"
    gcs_path = f"gs://{gcs_bucket}/{etl_folder}"
    
    utils.logger.info(f"Looking for consolidated parquet files in {gcs_path}")
    
    # Get list of table subdirectories
    subdirectories = list_gcs_subdirectories(gcs_path)
    
    # Extract just the table names from the full paths
    table_names = [subdir.rstrip('/').split('/')[-1] for subdir in subdirectories]
    
    if not table_names:
        utils.logger.warning(f"No table directories found in {gcs_path}")
        raise Exception(f"No table directories found in {gcs_path}")
    
    utils.logger.info(f"Found {len(table_names)} table(s) to load: {sorted(table_names)}")
    
    # Load each consolidated parquet file to BigQuery
    loaded_tables = []
    skipped_tables = []
    
    for table_name in sorted(table_names):
        # Construct path to consolidated parquet file
        consolidated_file_path = f"gs://{gcs_bucket}/{etl_folder}{table_name}/{table_name}{constants.PARQUET}"
        
        # Check if the consolidated file exists
        if not utils.parquet_file_exists(consolidated_file_path):
            utils.logger.warning(f"Consolidated parquet file not found: {consolidated_file_path}")
            skipped_tables.append(table_name)
            continue
        
        try:
            utils.logger.info(f"Loading {table_name} from {consolidated_file_path} to BigQuery")
            load_parquet_to_bigquery(
                consolidated_file_path,
                project_id,
                dataset_id,
                table_name,
                constants.BQWriteTypes.SPECIFIC_FILE
            )
            loaded_tables.append(table_name)
            utils.logger.info(f"Successfully loaded {table_name} to BigQuery")
        except Exception as e:
            utils.logger.error(f"Failed to load {table_name}: {str(e)}")
            skipped_tables.append(table_name)
    
    return {
        'loaded': loaded_tables,
        'skipped': skipped_tables
    }

def load_derived_tables_to_bq(gcs_bucket: str, delivery_date: str, project_id: str, dataset_id: str) -> dict[str, list[str]]:
    """
    Load derived table parquet files from GCS to BigQuery.

    Discovers all derived table parquet files in the DERIVED_FILES artifacts directory
    and loads each one to its corresponding BigQuery table.

    Args:
        gcs_bucket: GCS bucket containing the derived files
        delivery_date: Delivery date for path construction
        project_id: BigQuery project ID
        dataset_id: BigQuery dataset ID

    Returns:
        Dictionary with 'loaded' and 'skipped' keys, each containing a list of table names

    Raises:
        Exception: If there's an error during processing
    """
    # Construct the DERIVED_FILES directory path
    derived_folder = f"{delivery_date}/{constants.ArtifactPaths.DERIVED_FILES.value}"
    gcs_path = f"gs://{gcs_bucket}/{derived_folder}"

    utils.logger.info(f"Looking for derived table parquet files in {gcs_path}")

    # List all parquet files in the derived_files directory
    client = storage.Client(project=project_id)
    bucket = client.bucket(gcs_bucket)
    prefix = derived_folder

    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_files = [blob.name for blob in blobs if blob.name.endswith(constants.PARQUET)]

    if not parquet_files:
        utils.logger.warning(f"No derived table parquet files found in {gcs_path}")
        return {
            'loaded': [],
            'skipped': []
        }

    # Extract table names from file paths
    # File format: {delivery_date}/artifacts/derived_files/{table_name}.parquet
    table_names = [file_path.split('/')[-1].replace(constants.PARQUET, '') for file_path in parquet_files]

    utils.logger.info(f"Found {len(table_names)} derived table(s) to load: {sorted(table_names)}")

    # Load each derived table parquet file to BigQuery
    loaded_tables = []
    skipped_tables = []

    for table_name in sorted(table_names):
        # Construct path to derived table parquet file
        derived_file_path = f"gs://{gcs_bucket}/{derived_folder}{table_name}{constants.PARQUET}"

        # Check if the file exists
        if not utils.parquet_file_exists(derived_file_path):
            utils.logger.warning(f"Derived table parquet file not found: {derived_file_path}")
            skipped_tables.append(table_name)
            continue

        try:
            utils.logger.info(f"Loading derived table {table_name} from {derived_file_path} to BigQuery")
            load_parquet_to_bigquery(
                derived_file_path,
                project_id,
                dataset_id,
                table_name,
                constants.BQWriteTypes.SPECIFIC_FILE
            )
            loaded_tables.append(table_name)
            utils.logger.info(f"Successfully loaded derived table {table_name} to BigQuery")
        except Exception as e:
            utils.logger.error(f"Failed to load derived table {table_name}: {str(e)}")
            skipped_tables.append(table_name)

    return {
        'loaded': loaded_tables,
        'skipped': skipped_tables
    }