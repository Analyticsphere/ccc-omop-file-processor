from google.cloud import bigquery  # type: ignore

import core.constants as constants
import core.utils as utils


def create_optimized_vocab_file(vocab_version: str, vocab_gcs_bucket: str) -> None:
    vocab_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    optimized_vocab_path = utils.get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket)

    # Create the optimized vocabulary file if it doesn't exist
    if not utils.parquet_file_exists(optimized_vocab_path):
        # Ensure exisiting vocab file can be read
        if not utils.valid_parquet_file(optimized_vocab_path):
            # Ensure vocabulary version actually exists
            if utils.vocab_gcs_path_exists(vocab_path):
                conn, local_db_file = utils.create_duckdb_connection()

                try:
                    with conn:
                        transform_query = f"""
                        COPY (
                            SELECT DISTINCT
                                c1.concept_id AS concept_id, -- Every concept_id from concept table
                                c1.standard_concept AS concept_id_standard, 
                                c1.domain_id AS concept_id_domain,
                                cr.relationship_id, 
                                cr.concept_id_2 AS target_concept_id, -- targets to concept_id's
                                c2.standard_concept AS target_concept_id_standard, 
                                c2.domain_id AS target_concept_id_domain
                            FROM read_csv('gs://{vocab_path}CONCEPT.csv', delim='\t',strict_mode=False) c1
                            LEFT JOIN read_csv('gs://{vocab_path}CONCEPT_RELATIONSHIP.csv', delim='\t',strict_mode=False) cr on c1.concept_id = cr.concept_id_1
                            LEFT JOIN read_csv('gs://{vocab_path}CONCEPT.csv', delim='\t',strict_mode=False) c2 on cr.concept_id_2 = c2.concept_id
                            WHERE IFNULL(cr.relationship_id, '') 
                                IN ('', {constants.MAPPING_RELATIONSHIPS},{constants.REPLACEMENT_RELATIONSHIPS})
                        ) TO 'gs://{optimized_vocab_path}' {constants.DUCKDB_FORMAT_STRING}
                        """
                        conn.execute(transform_query)
                except Exception as e:
                    utils.logger.error(f"Unable to create optimized vocab file: {e}")
                    raise Exception(f"Unable to create optimized vocab file: {e}") from e
                finally:
                    utils.close_duckdb_connection(conn, local_db_file)
            else:
                utils.logger.error(f"Vocabulary GCS bucket {vocab_path} not found")
                raise Exception(f"Vocabulary GCS bucket {vocab_path} not found")
    else:
        utils.logger.info(f"Optimized vocabulary already exists")

def create_missing_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    ddl_file = f"{constants.DDL_SQL_PATH}{omop_version}/{constants.DDL_FILE_NAME}"

    # Get DDL with CREATE OR REPLACE TABLE statements
    try:
        with open(ddl_file, 'r') as f:
            ddl_sql = f.read()
        
        # Add project_id and data_set to SQL statement
        create_sql = ddl_sql.replace(constants.DDL_PLACEHOLDER_STRING, f"{project_id}.{dataset_id}")
    except Exception as e:
        raise Exception(f"DDL file error: {e}")
    
    # Execute the CREATE OR REPLACE TABLE statements in BigQuery
    # Initialize the BigQuery client
    client = bigquery.Client()

    # Run the query
    query_job = client.query(create_sql)

    # Wait for the job to complete
    _ = query_job.result()

def populate_cdm_source(cdm_source_data: dict) -> None:
    # Add a record to the cdm_source table, if it doesn't have any rows
    project_id = cdm_source_data["project_id"]
    dataset_id = cdm_source_data["dataset_id"]
    gcs_bucket = cdm_source_data["gcs_bucket"]
    delivery_date = cdm_source_data["source_release_date"]
    cdm_version = cdm_source_data["cdm_version"]

    vocab_version = utils.get_delivery_vocabulary_version(gcs_bucket, delivery_date)
    cdm_version_concept_id = utils.get_cdm_version_concept_id(cdm_version)

    try:
        # Construct a BigQuery client object
        client = bigquery.Client()

        # Build the insert statement
        query = f"""
            INSERT INTO {project_id}.{dataset_id}.cdm_source (
                cdm_source_name,
                cdm_source_abbreviation,
                cdm_holder,
                source_description,
                source_documentation_reference,
                cdm_etl_reference,
                source_release_date,
                cdm_release_date,
                cdm_version,
                cdm_version_concept_id,
                vocabulary_version
            )
            SELECT
                @cdm_source_name,
                @cdm_source_abbreviation,
                @cdm_holder,
                @source_description,
                @source_documentation_reference,
                @cdm_etl_reference,
                @source_release_date,
                @cdm_release_date,
                @cdm_version,
                @cdm_version_concept_id,
                @vocabulary_version
            FROM (SELECT 1) dummy_table
            WHERE NOT EXISTS (
                SELECT 1
                FROM {project_id}.{dataset_id}.cdm_source
                LIMIT 1
            );
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("cdm_source_name", "STRING", cdm_source_data["cdm_source_name"]),
                bigquery.ScalarQueryParameter("cdm_source_abbreviation", "STRING", cdm_source_data["cdm_source_abbreviation"]),
                bigquery.ScalarQueryParameter("cdm_holder", "STRING", cdm_source_data["cdm_holder"]),
                bigquery.ScalarQueryParameter("source_description", "STRING", cdm_source_data["source_description"]),
                bigquery.ScalarQueryParameter("source_documentation_reference", "STRING", cdm_source_data["source_documentation_reference"]),
                bigquery.ScalarQueryParameter("cdm_etl_reference", "STRING", cdm_source_data["cdm_etl_reference"]),
                bigquery.ScalarQueryParameter("source_release_date", "DATE", delivery_date),
                bigquery.ScalarQueryParameter("cdm_release_date", "DATE", cdm_source_data["cdm_release_date"]),
                bigquery.ScalarQueryParameter("cdm_version", "STRING", cdm_version),
                bigquery.ScalarQueryParameter("cdm_version_concept_id", "INT64", cdm_version_concept_id),
                bigquery.ScalarQueryParameter("vocabulary_version", "STRING", vocab_version),
            ]
        )

        # Run the query as a job and wait for it to complete.
        query_job = client.query(query, job_config=job_config)
        query_job.result()  # Wait for the job to complete.
    except Exception as e:
        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
        }
        utils.logger.error(f"Unable to add pipeline log record: {error_details}")
        raise Exception(f"Unable to add pipeline log record: {error_details}") from e

