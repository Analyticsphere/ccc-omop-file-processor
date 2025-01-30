from google.cloud import bigquery
import core.utils as utils
import sys


def load_parquet_to_bigquery(gcs_path: str, project_id: str, dataset_id: str) -> None:
    """
    Load Parquet artifact file from GCS directly into BigQuery.
    """
    table_name = utils.get_table_name_from_gcs_path(gcs_path)
    parquet_path = utils.get_parquet_artifact_location(gcs_path)

    try:
        client = bigquery.Client(project=project_id)
        table_id_full = f"{project_id}.{dataset_id}.{table_name}"
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
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
        
        # Get information about the loaded table
        table = client.get_table(table_id_full)
        utils.logger.info(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id_full}")
    except Exception as e:
        utils.logger.error(f"Error loading Parquet file to BigQuery: {e}")
        sys.exit(1)
