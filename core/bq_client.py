from google.cloud import bigquery  # type: ignore

import core.utils as utils
import core.constants as constants


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

#def load_parquet_to_bigquery(gcs_path: str, project_id: str, dataset_id: str, write_disposition: str = bigquery.WriteDisposition.WRITE_TRUNCATE, derive_path: bool = True) -> None:
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
        #parquet_path = f"gs://{file_path}transformed/{table_name}{constants.PARQUET}"
        parquet_path = file_path
        utils.logger.warning(f"looking for ETLed parquet in {parquet_path} and will load to {table_name}")

        
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
