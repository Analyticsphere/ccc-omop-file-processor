from google.cloud import bigquery  # type: ignore

import core.constants as constants
import core.utils as utils
from google.cloud.exceptions import NotFound  # type: ignore


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
        utils.logger.error(f"Unable to delete BigQuery table: {e}")
        raise Exception(f"Unable to delete BigQuery table: {e}") from e

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
        utils.logger.error(f"Error loading Parquet file to BigQuery: {e}")
        raise Exception(f"Error loading Parquet file to BigQuery: {e}") from e

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
        results = list(query_job.result())
        return results
    except Exception as e:
        raise Exception(f"Failed to retrieve BigQuery pipeline logs for site '{site}' and date '{date_to_check}': {e}") from e

