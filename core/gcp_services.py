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
    if write_type == constants.BQWriteTypes.SPECIFIC_FILE:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        parquet_path = file_path
    elif write_type == constants.BQWriteTypes.PROCESSED_FILE:
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        parquet_path = f"gs://{utils.get_parquet_artifact_location(file_path)}"
        
    elif write_type == constants.BQWriteTypes.ETLed_FILE:
        write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        parquet_path = file_path

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

        utils.logger.warning(f"bucket_name is {bucket_name}")
        utils.logger.warning(f"blob path is {blob_path}")

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
            print(f"GCP_SERVICES Checking if bucket {bucket_name} exists")
            bucket = client.get_bucket(bucket_name)
        except Exception:
            print(f"GCP_SERVICES Returning false because bucket {bucket_name} does not exist")
            return False
        print(f"GCP_SERVICES Bucket {bucket_name} does exist")

        # If no blob path, we're just checking bucket existence
        if not blob_path:
            print(f"GCP_SERVICES returning true because bucket {bucket_name} exists and no blob path specified")
            return True

        # Check if blob exists
        blob = bucket.blob(blob_path)
        print(f"GCP_SERVICES Checking if blob {blob_path} exists in bucket {bucket_name}")
        print(f"GCP_SERVICES Blob exists is: {blob.exists()}")


        # If blob_path ends with a slash, remove the trailing slash for modified_blob_path
        if blob_path.endswith('/'):
            modified_blob_path = blob_path[:-1]
        else:
            modified_blob_path = blob_path
        print(f"GCP_SERVICES Checking if blob {modified_blob_path} exists in bucket {bucket_name}")
        print(f"GCP_SERVICES Modified Blob exists is: {bucket.blob(modified_blob_path).exists()}")
        
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

