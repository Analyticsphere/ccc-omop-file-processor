import os
from typing import Optional

import fsspec  # type: ignore
from google.cloud import bigquery  # type: ignore
from google.cloud import storage as gcs_storage  # type: ignore
from google.cloud.exceptions import NotFound  # type: ignore
import pyarrow.parquet as pq  # type: ignore

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


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
        parquet_path = storage.get_uri(utils.get_converted_parquet_artifact_location(file_path))
        
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
    """Retrieve pipeline log entries from BigQuery for specified site and delivery date."""
    client = bigquery.Client()

    # Check if the logging table exists. If it doesn't, return an empty list.
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

def execute_bq_sql(sql_script: str, job_config: Optional[bigquery.QueryJobConfig]) -> bigquery.table.RowIterator:
    """Execute BigQuery SQL query and return results."""
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
        raise Exception(f"Error executing BigQuery SQL: {e}")

def write_query_results_to_parquet(
    sql_script: str,
    output_path: str,
    project_id: Optional[str] = None,
    job_config: Optional[bigquery.QueryJobConfig] = None
) -> str:
    """Execute a BigQuery SQL query and write the result set to a Parquet file."""
    client = bigquery.Client(project=project_id)
    query_job = client.query(sql_script, job_config=job_config)
    results = query_job.result()
    result_table = results.to_arrow(create_bqstorage_client=False)

    output_uri = storage.get_uri(output_path)

    if constants.STORAGE_BACKEND == constants.LOCAL_BACKEND:
        os.makedirs(os.path.dirname(storage.strip_scheme(output_uri)), exist_ok=True)

    with fsspec.open(output_uri, 'wb') as parquet_file:
        pq.write_table(
            result_table,
            parquet_file,
            compression='zstd',
            compression_level=1
        )

    utils.logger.info(f"Saved query results to {output_uri}")
    return output_uri

def export_connect_data_to_parquet(project_id: str, dataset_id: str, delivery_bucket: str, site_connect_id: str) -> str:
    """Build the Connect participant-status query and export the results to Parquet."""
    sql_path = os.path.join(constants.SQL_PATH, "connect_data", "participant_status.sql")
    with open(sql_path, 'r') as sql_file:
        sql_script = sql_file.read()

    sql_script = sql_script.replace("@PROJECT_ID", project_id)
    sql_script = sql_script.replace("@DATASET_ID", dataset_id)
    # connect_id's are integers but are stored as strings in table
    sql_script = sql_script.replace("@SITE_CONNECT_ID", str(site_connect_id)) 

    output_path = f"{constants.ArtifactPaths.CONNECT_DATA.value}participant_status{constants.PARQUET}"
    output_path = f"{delivery_bucket}/{output_path}"

    output_uri = write_query_results_to_parquet(
        sql_script=sql_script,
        output_path=output_path,
        project_id=project_id
    )

    _create_connect_data_report_artifacts(output_uri, delivery_bucket)
    return output_uri

def _create_connect_data_report_artifacts(connect_data_path: str, delivery_bucket: str) -> None:
    """Create delivery report artifacts from Connect data joined to the normalized person parquet."""
    bucket, delivery_date = utils.get_bucket_and_delivery_date_from_path(delivery_bucket)
    person_parquet_path = utils.get_normalized_parquet_artifact_location(f"{delivery_bucket}/person.parquet")

    if not utils.parquet_file_exists(person_parquet_path):
        raise Exception(f"Normalized person parquet not found at {person_parquet_path}")

    person_uri = storage.get_uri(person_parquet_path)
    connect_data_uri = storage.get_uri(connect_data_path)

    base_cte = f"""
    WITH person_delivery AS (
        SELECT DISTINCT
            TRY_CAST(person_id AS BIGINT) AS person_id
        FROM read_parquet('{person_uri}')
        WHERE TRY_CAST(person_id AS BIGINT) IS NOT NULL
    ),
    connect_data AS (
        SELECT DISTINCT
            TRY_CAST(Connect_ID AS BIGINT) AS connect_id,
            COALESCE(NULLIF(TRIM(CAST(verified_status AS VARCHAR)), ''), 'UNKNOWN') AS verified_status,
            TRY_CAST(verified_status_concept_id AS BIGINT) AS verified_status_concept_id,
            COALESCE(NULLIF(TRIM(CAST(consent_withdrawn AS VARCHAR)), ''), 'UNKNOWN') AS consent_withdrawn,
            TRY_CAST(consent_withdrawn_concept_id AS BIGINT) AS consent_withdrawn_concept_id,
            COALESCE(NULLIF(TRIM(CAST(hipaa_revoked AS VARCHAR)), ''), 'UNKNOWN') AS hipaa_revoked,
            TRY_CAST(hipaa_revoked_concept_id AS BIGINT) AS hipaa_revoked_concept_id,
            COALESCE(NULLIF(TRIM(CAST(data_destruction_requested AS VARCHAR)), ''), 'UNKNOWN') AS data_destruction_requested,
            TRY_CAST(data_destruction_requested_concept_id AS BIGINT) AS data_destruction_requested_concept_id
        FROM read_parquet('{connect_data_uri}')
        WHERE TRY_CAST(Connect_ID AS BIGINT) IS NOT NULL
    ),
    matched_patients AS (
        SELECT DISTINCT
            p.person_id,
            cd.connect_id,
            cd.verified_status,
            cd.verified_status_concept_id,
            cd.consent_withdrawn,
            cd.consent_withdrawn_concept_id,
            cd.hipaa_revoked,
            cd.hipaa_revoked_concept_id,
            cd.data_destruction_requested,
            cd.data_destruction_requested_concept_id
        FROM person_delivery p
        INNER JOIN connect_data cd
            ON p.person_id = cd.connect_id
    )
    """

    def save_artifact(
        name: str,
        value_as_number: Optional[float] = None,
        value_as_string: Optional[str] = None,
        value_as_concept_id: Optional[int] = None
    ) -> None:
        artifact = report_artifact.ReportArtifact(
            delivery_date=delivery_date,
            artifact_bucket=bucket,
            concept_id=0,
            name=name,
            value_as_string=value_as_string,
            value_as_concept_id=value_as_concept_id,
            value_as_number=value_as_number
        )
        artifact.save_artifact()

    report_configs = [
        (
            "Connect participant breakdown: Study status",
            "verified_status",
            "verified_status_concept_id"
        ),
        (
            "Connect participant breakdown: HIPAA revoked status",
            "hipaa_revoked",
            "hipaa_revoked_concept_id"
        ),
        (
            "Connect participant breakdown: Consent withdrawn status",
            "consent_withdrawn",
            "consent_withdrawn_concept_id"
        ),
        (
            "Connect participant breakdown: Data destruction status",
            "data_destruction_requested",
            "data_destruction_requested_concept_id"
        )
    ]

    for artifact_name, status_column, concept_id_column in report_configs:
        status_count_sql = f"""
        {base_cte}
        SELECT
            {status_column} AS status_value,
            {concept_id_column} AS status_concept_id,
            COUNT(DISTINCT person_id) AS patient_count
        FROM matched_patients
        GROUP BY 1, 2
        ORDER BY 1, 2
        """

        status_counts = utils.execute_duckdb_sql(
            status_count_sql,
            f"Unable to create Connect data report artifacts for {status_column}",
            return_results=True
        )

        for status_value, status_concept_id, patient_count in status_counts:
            save_artifact(
                name=artifact_name,
                value_as_string=status_value,
                value_as_concept_id=status_concept_id,
                value_as_number=float(patient_count)
            )

    connect_not_in_delivery_sql = f"""
    {base_cte}
    SELECT
        COUNT(*) AS patient_count,
        COALESCE(STRING_AGG(CAST(connect_id AS VARCHAR), '|' ORDER BY connect_id), '') AS patient_ids
    FROM (
        SELECT DISTINCT cd.connect_id
        FROM connect_data cd
        LEFT JOIN person_delivery p
            ON p.person_id = cd.connect_id
        WHERE p.person_id IS NULL
    ) unmatched_connect
    """
    connect_not_in_delivery = utils.execute_duckdb_sql(
        connect_not_in_delivery_sql,
        "Unable to create Connect-vs-delivery reconciliation artifacts for Connect-only patients",
        return_results=True
    )

    connect_only_count, connect_only_ids = connect_not_in_delivery[0] if connect_not_in_delivery else (0, "")
    save_artifact(
        name="Number of Connect patients not in delivery",
        value_as_number=float(connect_only_count)
    )
    save_artifact(
        name="Connect patient IDs not in delivery",
        value_as_string=connect_only_ids or ""
    )

    delivery_not_in_connect_sql = f"""
    {base_cte}
    SELECT
        COUNT(*) AS patient_count,
        COALESCE(STRING_AGG(CAST(person_id AS VARCHAR), '|' ORDER BY person_id), '') AS patient_ids
    FROM (
        SELECT DISTINCT p.person_id
        FROM person_delivery p
        LEFT JOIN connect_data cd
            ON p.person_id = cd.connect_id
        WHERE cd.connect_id IS NULL
    ) unmatched_delivery
    """
    delivery_not_in_connect = utils.execute_duckdb_sql(
        delivery_not_in_connect_sql,
        "Unable to create Connect-vs-delivery reconciliation artifacts for delivery-only patients",
        return_results=True
    )

    delivery_only_count, delivery_only_ids = delivery_not_in_connect[0] if delivery_not_in_connect else (0, "")
    save_artifact(
        name="Number of delivery patients not in Connect data",
        value_as_number=float(delivery_only_count)
    )
    save_artifact(
        name="Delivery patient IDs not in Connect data",
        value_as_string=delivery_only_ids or ""
    )

def list_gcs_subdirectories(gcs_path: str) -> list:
    """
    Lists all subdirectories within a given GCS path.
    """
    try:
        # Split the path into bucket name and prefix
        parts = storage.strip_scheme(gcs_path).split('/', 1)
        bucket_name = parts[0]
        prefix = parts[1] if len(parts) > 1 else ''

        # Initialize the client
        client = gcs_storage.Client()
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
        Dictionary with 'loaded' keys a list of table names
    """
    # Construct the OMOP_ETL directory path
    etl_folder = f"{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}"
    gcs_path = storage.get_uri(f"{gcs_bucket}/{etl_folder}")

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
    
    for table_name in sorted(table_names):
        # Construct path to consolidated parquet file
        consolidated_file_path = storage.get_uri(f"{gcs_bucket}/{etl_folder}{table_name}/{table_name}{constants.PARQUET}")
        
        # Check if the consolidated file exists
        if not utils.parquet_file_exists(consolidated_file_path):
            raise Exception(f"Consolidated parquet file not found: {consolidated_file_path}")
        
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
    
    return {
        'loaded': loaded_tables
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
        Dictionary with 'loaded' keys containing a list of table names
    """
    # Construct the DERIVED_FILES directory path
    derived_folder = f"{delivery_date}/{constants.ArtifactPaths.DERIVED_FILES.value}"
    gcs_path = storage.get_uri(f"{gcs_bucket}/{derived_folder}")

    utils.logger.info(f"Looking for derived table parquet files in {gcs_path}")

    # List all parquet files in the derived_files directory
    client = gcs_storage.Client(project=project_id)
    bucket = client.bucket(gcs_bucket)
    prefix = derived_folder

    blobs = list(bucket.list_blobs(prefix=prefix))
    parquet_files = [blob.name for blob in blobs if blob.name.endswith(constants.PARQUET)]

    if not parquet_files:
        utils.logger.warning(f"No derived table parquet files found in {gcs_path}")
        return {
            'loaded': [],
        }

    # Extract table names from file paths
    # File format: {delivery_date}/artifacts/derived_files/{table_name}.parquet
    table_names = [file_path.split('/')[-1].replace(constants.PARQUET, '') for file_path in parquet_files]

    utils.logger.info(f"Found {len(table_names)} derived table(s) to load: {sorted(table_names)}")

    # Load each derived table parquet file to BigQuery
    loaded_tables = []

    for table_name in sorted(table_names):
        # Construct path to derived table parquet file
        derived_file_path = storage.get_uri(f"{gcs_bucket}/{derived_folder}{table_name}{constants.PARQUET}")

        # Check if the file exists
        if not utils.parquet_file_exists(derived_file_path):
            raise Exception(f"Derived table parquet file not found: {derived_file_path}")

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

    return {
        'loaded': loaded_tables,
    }
