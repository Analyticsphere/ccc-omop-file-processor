from google.cloud import bigquery  # type: ignore

import core.constants as constants
import core.utils as utils
import core.bq_client as bq_client

def upgrade_file(gcs_file_path: str, cdm_version: str, target_omop_version: str) -> None:
    """
    Upgrades an OMOP CDM table file from one version to another by applying version-specific transformations.
    Currently supports upgrading from CDM v5.3 to v5.4.

    The function handles three cases for table upgrades:
    1. No changes needed (table remains the same in new version)
    2. Table removed (file is deleted in new version)
    3. Table changed and overwritten (SQL upgrade script is applied to transform the data)
    """

    normalized_file_path = utils.get_parquet_artifact_location(gcs_file_path)
    table_name = utils.get_table_name_from_gcs_path(gcs_file_path)

    if cdm_version == target_omop_version:
        utils.logger.info(f"CDM upgrade not needed")
        pass
    elif cdm_version == constants.CDM_v53 and target_omop_version == constants.CDM_v54:
        if table_name in constants.CDM_53_TO_54:
            if constants.CDM_53_TO_54[table_name] == constants.REMOVED:
                utils.delete_gcs_file(normalized_file_path)
            elif constants.CDM_53_TO_54[table_name] == constants.CHANGED:
                try:
                    upgrade_file_path = f"{constants.CDM_UPGRADE_SCRIPT_PATH}{cdm_version}_to_{target_omop_version}/{table_name}.sql"
                    with open(upgrade_file_path, 'r') as f:
                        upgrade_script = f.read()
                
                    conn, local_db_file = utils.create_duckdb_connection()
                    try:
                        with conn:
                            select_statement = f"""
                                COPY (
                                    {upgrade_script}
                                    FROM read_parquet('gs://{normalized_file_path}')
                                ) TO 'gs://{normalized_file_path}' {constants.DUCKDB_FORMAT_STRING}
                            """
                            conn.execute(select_statement)
                    except Exception as e:
                        utils.logger.error(f"Unable to upgrade file: {e}")
                        raise Exception(f"Unable to upgrade file: {e}") from e
                    finally:
                        utils.close_duckdb_connection(conn, local_db_file)

                except Exception as e:
                    utils.logger.error(f"Unable to open SQL upgrade file: {e}")
                    raise Exception(f"Unable to open SQL upgrade file: {e}") from e
        else:
            utils.logger.info(f"No changes in {table_name} when upgrading from 5.3 to 5.4")
    else:
        utils.logger.error(f"OMOP CDM version {cdm_version} not supported")
        raise Exception(f"OMOP CDM version {cdm_version} not supported")

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
    utils.execute_bq_sql(create_sql, None)
    # # Initialize the BigQuery client
    # client = bigquery.Client()

    # # Run the query
    # query_job = client.query(create_sql)

    # # Wait for the job to complete
    # query_job.result()

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
        # query_job = client.query(query, job_config=job_config)
        # query_job.result()  # Wait for the job to complete.
        utils.execute_bq_sql(query, job_config)
    except Exception as e:
        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
        }
        utils.logger.error(f"Unable to add pipeline log record: {error_details}")
        raise Exception(f"Unable to add pipeline log record: {error_details}") from e

def generate_derived_data(site: str, delivery_date: str, table_name: str, project_id: str, dataset_id: str) -> None:
    utils.logger.warning(f"IN generate_derived_data and site is {site} and delivery_date is {delivery_date} and table_name is {table_name}")

    # Execute SQL scripts to generate derived data table Parquet files
    if table_name not in constants.DERIVED_DATA_TABLES_REQUIREMENTS.keys():
        raise Exception(f"{table_name} is not a derived data table")

    # Check if tables necessary to generate dervied data exist in delivery
    for required_table in constants.DERIVED_DATA_TABLES_REQUIREMENTS[table_name]:
        parquet_path = f"{site}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{required_table}{constants.PARQUET}"
        utils.logger.warning(f"checking for required table parquet in {parquet_path}")
        if not utils.parquet_file_exists(parquet_path):
            # Don't raise execption if required table doesn't exist, just log error
            utils.logger.error(f"Required table {required_table} not in data delivery, cannot generate derived data table {table_name}")
            return
    
    # Get SQL script with place holder values for table locations
    try:
        sql_path = f"{constants.SQL_PATH}{table_name}.sql"
        utils.logger.warning(f"looking for derived table sql in {sql_path}")
        with open(sql_path, 'r') as f:
            select_statement = f.read()

        # Add table locations
        select_statement = placeholder_to_table_path(site, delivery_date, select_statement)
        printselect = select_statement.replace('\n', ' ')
        utils.logger.warning(f"script with replacements is {printselect}")

        try:
            conn, local_db_file = utils.create_duckdb_connection()

            with conn:
                # Generate the derived table parquet file
                parquet_gcs_path = f"gs://{site}/{delivery_date}/{constants.ArtifactPaths.CREATED_FILES.value}{table_name}{constants.PARQUET}"
                sql_statement = f"""
                    COPY (
                        {select_statement}
                    ) TO '{parquet_gcs_path}' {constants.DUCKDB_FORMAT_STRING}
                """
                sqlstatementprint = sql_statement.replace('\n',' ')
                utils.logger.warning(f"duckdb table sql is {sqlstatementprint}")
                conn.execute(sql_statement)

                # Load the Parquet to BigQuery
                # Because the task that executes this function occurs after load_bq(), 
                #   this will overwrite the derived data delievered by the site
                bq_client.load_parquet_to_bigquery(parquet_gcs_path, project_id, dataset_id, False)
        except Exception as e:
            raise Exception(f"Unable to execute SQl to generate {table_name}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)


    except Exception as e:
        raise Exception(f"Unable to generate {table_name} derived data: {str(e)}") from e

def placeholder_to_table_path(site: str, delivery_date: str, sql_script: str) -> str:
    # Replaces table place holder strings in SQL scripts with paths to table parquet files
    for placeholder, replacement in constants.PATH_PLACEHOLDERS.items():
        table_path = f"gs://{site}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{replacement}{constants.PARQUET}"
        result = sql_script.replace(placeholder, table_path)

    return result
