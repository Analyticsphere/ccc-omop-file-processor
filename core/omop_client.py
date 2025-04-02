from datetime import datetime

from google.cloud import bigquery  # type: ignore

import core.bq_client as bq_client
import core.constants as constants
import core.utils as utils


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
        return
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

def convert_vocab_to_parquet(vocab_version: str, vocab_gcs_bucket: str) -> None:
    """
    Convert CSV vocabulary files from Athena to Parquet format
    """
    vocab_root_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    
    # Confirm desired vocabulary version exists in GCS
    if utils.vocab_gcs_path_exists(vocab_root_path):
        vocab_files = utils.list_gcs_files(vocab_gcs_bucket, vocab_version, constants.CSV)
        for vocab_file in vocab_files:
            vocab_file_name = vocab_file.replace(constants.CSV, '').lower()
            parquet_file_path = f"{vocab_root_path}{constants.OPTIMIZED_VOCAB_FOLDER}/{vocab_file_name}{constants.PARQUET}"
            csv_file_path = f"{vocab_root_path}{vocab_file}"
            table_name = utils.get_table_name_from_gcs_path(csv_file_path)

            # Continue only if the vocabulary file has not been created or is not valid
            if not utils.parquet_file_exists(parquet_file_path) or not utils.valid_parquet_file(parquet_file_path):
                
                conn, local_db_file = utils.create_duckdb_connection()

                # Get expected columns from schema in correct order
                schema = utils.get_cdm_schema(vocab_version)
                predefined_columns = list(schema[table_name]['fields'].keys())

                # Get column names
                csv_columns = utils.get_columns_from_file(csv_file_path)

                if not predefined_columns:
                    # Proceed with CSV columns as they are
                    select_statement = ', '.join([f'"{col}"' for col in csv_columns])
                else:
                    # Build the SELECT statement with columns in the predefined order
                    select_columns = []
                    for col in predefined_columns:
                        if col in csv_columns:
                            if col in ('valid_start_date', 'valid_end_date'):
                                # Handle date fields; need special handling or they're interpred as numeric values
                                select_columns.append(
                                    f'CAST(STRPTIME(CAST("{col}" AS VARCHAR), \'%Y%m%d\') AS DATE) AS "{col}"'
                                )
                            else:
                                select_columns.append(f'"{col}"')
                        else:
                            # Column not in CSV, select NULL as that column
                            select_columns.append(f'NULL AS "{col}"')
                    select_statement = ', '.join(select_columns)

                # Execute the COPY command to convert CSV to Parquet with columns in the correct order
                try:
                    with conn:
                        convert_query = f"""
                            COPY (
                                SELECT {select_statement}
                                FROM read_csv('gs://{csv_file_path}', delim='\t',strict_mode=False)
                            ) TO 'gs://{parquet_file_path}' {constants.DUCKDB_FORMAT_STRING};
                        """
                        conn.execute(convert_query)
                except Exception as e:
                    utils.logger.error(f"convert_query failed to execute: \n{convert_query}")
                    raise Exception(f"Unable to convert vocabulary CSV to Parquet: {e}") from e
                finally:
                    utils.close_duckdb_connection(conn, local_db_file)

                # Hard code exceptions to treet with custom sql
                # something like: f'CAST(STRPTIME(CAST("{col}" AS VARCHAR), \'%Y%m%d\') AS DATE) AS "{col}"'
                # May need temporary view, but try without and read directly from the CSV if possible
                # try:
                #     with conn:
                #         #TODO Here where we select *, don't select *, select predefined list of columns (to ensure order) but 
                #         convert_query = f"""
                #             COPY (
                #                 SELECT * FROM read_csv('gs://{csv_file_path}', delim='\t',strict_mode=False)
                #             ) TO 'gs://{parquet_file_path}' {constants.DUCKDB_FORMAT_STRING}
                #         """
                #         conn.execute(convert_query)
                # except Exception as e:
                #     raise Exception(f"Unable to convert vocabulary CSV to Parquet: {e}") from e
                # finally:
                #     utils.close_duckdb_connection(conn, local_db_file)
                
def create_optimized_vocab_file(vocab_version: str, vocab_gcs_bucket: str) -> None:
    vocab_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    optimized_file_path = utils.get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket)

    # Create the optimized vocabulary file if it doesn't exist
    if not utils.parquet_file_exists(optimized_file_path):
        # Ensure exisiting vocab file can be read
        if not utils.valid_parquet_file(optimized_file_path):
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
                            FROM read_parquet('gs://{vocab_path}{constants.OPTIMIZED_VOCAB_FOLDER}/concept{constants.PARQUET}') c1
                            LEFT JOIN read_parquet('gs://{vocab_path}{constants.OPTIMIZED_VOCAB_FOLDER}/concept_relationship{constants.PARQUET}') cr on c1.concept_id = cr.concept_id_1
                            LEFT JOIN read_parquet('gs://{vocab_path}{constants.OPTIMIZED_VOCAB_FOLDER}/concept{constants.PARQUET}') c2 on cr.concept_id_2 = c2.concept_id
                            WHERE IFNULL(cr.relationship_id, '') 
                                IN ('', {constants.MAPPING_RELATIONSHIPS},{constants.REPLACEMENT_RELATIONSHIPS})
                        ) TO 'gs://{optimized_file_path}' {constants.DUCKDB_FORMAT_STRING}
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

        # Execute the CREATE OR REPLACE TABLE statements in BigQuery
        utils.execute_bq_sql(create_sql, None)

    except Exception as e:
        utils.logger.error("This DDL failed: \n{create_sql}")
        raise Exception(f"DDL file error: {e}")

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
        # Build the insert statement
        query = f"""
            INSERT INTO `{project_id}.{dataset_id}.cdm_source` (
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
                FROM `{project_id}.{dataset_id}.cdm_source`
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
        utils.execute_bq_sql(query, job_config)
    except Exception as e:
        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
        }
        utils.logger.error(f"Unable to add pipeline log record: {error_details}")
        raise Exception(f"Unable to add pipeline log record: {error_details}") from e

def generate_derived_data(site: str, site_bucket: str, delivery_date: str, table_name: str, project_id: str, dataset_id: str, vocab_version: str, vocab_gcs_bucket: str) -> None:
    """
    Execute SQL scripts to generate derived data table Parquet files
    """
    sql_script_name = table_name

    if table_name not in constants.DERIVED_DATA_TABLES_REQUIREMENTS.keys():
        raise Exception(f"{table_name} is not a derived data table")

    # Check if tables necessary to generate dervied data exist in delivery
    for required_table in constants.DERIVED_DATA_TABLES_REQUIREMENTS[table_name]:
        parquet_path = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{required_table}{constants.PARQUET}"
        if not utils.parquet_file_exists(parquet_path):
            # Don't raise execption if required table doesn't exist, just log warning
            utils.logger.warning(f"Required table {required_table} not in {site}'s {delivery_date} data delivery, cannot generate derived data table {table_name}")
            return
    
    # observation_period table requires special logic
    # observation_period records are necessary when using OHDSI analytic tools
    # Create observation_period records using standard logic for all sites
        # https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html
    if table_name == constants.OBSERVATION_PERIOD:
        visit_occurrence_table = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}visit_occurrence{constants.PARQUET}"
        death_table = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}death{constants.PARQUET}"

        # Need seperate SQL scripts for different file delivery scenarios 
        # DuckDB doesn't support branch logic based on table/file availablity so choosing SQL script via Python
        if utils.parquet_file_exists(visit_occurrence_table) and utils.parquet_file_exists(death_table):
            sql_script_name = "observation_period_vod"
        elif utils.parquet_file_exists(visit_occurrence_table):
            sql_script_name = "observation_period_vo"
        else:
            sql_script_name = table_name

    # Get SQL script with place holder values for table locations
    try:
        create_statement = ""
        # The drug_era script provided by OHDSI is resource intenstive
        # The script is split into two parts: 
        #   1) SQL statements that iteratively creates tables to derive drug_era records. Creating tables offloads data from memory to disk.
        #   2) Performs a final select statement against "last" temp table
        if table_name == constants.DRUG_ERA:
            create_statement_path = f"{constants.DERIVED_TABLE_PATH}{sql_script_name}_create.sql"
            with open(create_statement_path, 'r') as f:
                create_statement_raw = f.read()
            create_statement = placeholder_to_table_path(site, site_bucket, delivery_date, create_statement_raw, vocab_version, vocab_gcs_bucket)

        sql_path = f"{constants.DERIVED_TABLE_PATH}{sql_script_name}.sql"
        with open(sql_path, 'r') as f:
            select_statement_raw = f.read()

        # Add table locations
        select_statement = placeholder_to_table_path(site, site_bucket, delivery_date, select_statement_raw, vocab_version, vocab_gcs_bucket)

        try:
            conn, local_db_file = utils.create_duckdb_connection()

            with conn:
                # Generate the derived table parquet file
                parquet_gcs_path = f"gs://{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CREATED_FILES.value}{table_name}{constants.PARQUET}"
                sql_statement = f"""
                    {create_statement}

                    COPY (
                        {select_statement}
                    ) TO '{parquet_gcs_path}' {constants.DUCKDB_FORMAT_STRING}
                """
                conn.execute(sql_statement)

                # Load the Parquet to BigQuery
                # Because the task that executes this function occurs after load_bq(), 
                #   this will overwrite the derived data delievered by the site
                bq_client.load_parquet_to_bigquery(parquet_gcs_path, project_id, dataset_id, False)
        except Exception as e:
            raise Exception(f"Unable to execute SQL to generate {table_name}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

    except Exception as e:
        raise Exception(f"Unable to generate {table_name} derived data: {str(e)}") from e

def placeholder_to_table_path(site: str, site_bucket: str, delivery_date: str, sql_script: str, vocab_version: str, vocab_gcs_bucket: str) -> str:
    """
    Replaces clinical data table place holder strings in SQL scripts with paths to table parquet files
    """
    replacement_result = sql_script

    for placeholder, replacement in constants.CLINICAL_DATA_PATH_PLACEHOLDERS.items():
        clinical_data_table_path = f"gs://{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{replacement}{constants.PARQUET}"
        replacement_result = replacement_result.replace(placeholder, clinical_data_table_path)

    # Replaces vocab table place holder strings in SQL scripts with paths to target vocabulary version
    for placeholder, replacement in constants.VOCAB_PATH_PLACEHOLDERS.items():
        vocab_table_path = f"gs://{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{replacement}{constants.PARQUET}"
        replacement_result = replacement_result.replace(placeholder, vocab_table_path)
    
    # Add site name 
    replacement_result = replacement_result.replace(constants.SITE_PLACEHOLDER_STRING, site)

    # Add current date
    replacement_result = replacement_result.replace(constants.CURRENT_DATE_PLACEHOLDER_STRING, datetime.now().strftime('%Y-%m-%d'))

    return replacement_result

def load_vocabulary_table(vocab_version: str, vocab_gcs_bucket: str, table_file_name: str, project_id: str, dataset_id: str) -> None:
    # Loads an optimized vocabulary file to BigQuery as a table
    
    vocab_parquet_path = f"gs://{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{table_file_name}{constants.PARQUET}"

    if utils.parquet_file_exists(vocab_parquet_path) and utils.valid_parquet_file(vocab_parquet_path):
        bq_client.load_parquet_to_bigquery(vocab_parquet_path, project_id, dataset_id, False)
    else:
        raise Exception(f"Vocabulary table {table_file_name} not found at {vocab_parquet_path}")