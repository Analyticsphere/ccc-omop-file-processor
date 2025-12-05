
from google.cloud import bigquery  # type: ignore

import core.constants as constants
import core.gcp_services as gcp_services
import core.utils as utils
from core.storage_backend import storage


def upgrade_file(file_path: str, cdm_version: str, target_omop_version: str) -> None:
    """
    Upgrades an OMOP CDM table file from one version to another by applying version-specific transformations.
    Currently supports upgrading from CDM v5.3 to v5.4.

    The function handles three cases for table upgrades:
    1. No changes needed (table remains the same in new version)
    2. Table removed (file is deleted in new version)
    3. Table changed and overwritten (SQL upgrade script is applied to transform the data)
    """

    normalized_file_path = utils.get_parquet_artifact_location(file_path)
    table_name = utils.get_table_name_from_path(file_path)

    if cdm_version == target_omop_version:
        utils.logger.info(f"CDM upgrade not needed for file {file_path}")
        return
    elif cdm_version == constants.CDM_v53 and target_omop_version == constants.CDM_v54:
        if table_name in constants.CDM_53_TO_54:
            if constants.CDM_53_TO_54[table_name] == constants.REMOVED:
                storage.delete_file(normalized_file_path)
            elif constants.CDM_53_TO_54[table_name] == constants.CHANGED:
                try:
                    upgrade_file_path = f"{constants.CDM_UPGRADE_SCRIPT_PATH}{cdm_version}_to_{target_omop_version}/{table_name}.sql"
                    with open(upgrade_file_path, 'r') as f:
                        upgrade_script = f.read()

                    select_statement = f"""
                        COPY (
                            {upgrade_script}
                            FROM read_parquet('{storage.get_uri(normalized_file_path)}')
                        ) TO '{storage.get_uri(normalized_file_path)}' {constants.DUCKDB_FORMAT_STRING}
                    """
                    utils.execute_duckdb_sql(select_statement, f"Unable to upgrade file {file_path}:")

                except Exception as e:
                    raise Exception(f"Unable to open SQL upgrade file {upgrade_file_path}: {e}") from e
        else:
            utils.logger.info(f"No changes in {file_path} when upgrading from 5.3 to 5.4")
    else:
        raise Exception(f"OMOP CDM version {cdm_version} not supported")

def convert_vocab_to_parquet(vocab_version: str, vocab_gcs_bucket: str) -> None:
    """
    Convert CSV vocabulary files from Athena to Parquet format
    """
    vocab_root_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    # Confirm desired vocabulary version exists in GCS
    if gcp_services.vocab_gcs_path_exists(vocab_root_path):
        vocab_files = utils.list_gcs_files(vocab_gcs_bucket, vocab_version, constants.CSV)
        for vocab_file in vocab_files:
            vocab_file_name = vocab_file.replace(constants.CSV, '').lower()
            parquet_file_path = f"{vocab_root_path}{constants.OPTIMIZED_VOCAB_FOLDER}/{vocab_file_name}{constants.PARQUET}"
            csv_file_path = f"{vocab_root_path}{vocab_file}"

            # Continue only if the vocabulary file has not been created or is not valid
            if not utils.parquet_file_exists(parquet_file_path) or not utils.valid_parquet_file(parquet_file_path):
                conn, local_db_file = utils.create_duckdb_connection()
                
                # Get column names
                csv_columns = utils.get_columns_from_file(csv_file_path)

                # Build the SELECT statement with columns in the predefined order
                select_columns = []
                for col in csv_columns:
                    if col in ('valid_start_date', 'valid_end_date'):
                        # Handle date fields; need special handling or they're interpreted as numeric values
                        select_columns.append(
                            f'CAST(STRPTIME(CAST("{col}" AS VARCHAR), \'%Y%m%d\') AS DATE) AS "{col}"'
                        )
                    else:
                        select_columns.append(f'"{col}"')

                select_statement = ', '.join(select_columns)
                
                
                # Execute the COPY command to convert CSV to Parquet with columns in the correct order
                try:
                    convert_query = f"""
                    COPY (
                        SELECT {select_statement}
                        FROM read_csv('{storage.get_uri(csv_file_path)}', delim='\t',strict_mode=False)
                    ) TO '{storage.get_uri(parquet_file_path)}' {constants.DUCKDB_FORMAT_STRING};
                    """
                    with conn:
                        conn.execute(convert_query)
                except Exception as e:
                    raise Exception(f"Unable to convert vocabulary CSV to Parquet: {e}") from e
                finally:
                    utils.close_duckdb_connection(conn, local_db_file)
    else:
        raise Exception(f"Vocabulary GCS path {vocab_root_path} not found")

def create_optimized_vocab_file(vocab_version: str, vocab_gcs_bucket: str) -> None:
    """
    Create optimized vocabulary file by denormalizing concept and concept_relationship tables.
    Combines concept IDs with their mapping/replacement relationships into a single table/file for efficient lookups.
    """
    vocab_path = f"{vocab_gcs_bucket}/{vocab_version}/"
    optimized_file_path = utils.get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket)

    # Create the optimized vocabulary file if it doesn't exist
    if not utils.parquet_file_exists(optimized_file_path):
        # Ensure exisiting vocab file can be read
        if not utils.valid_parquet_file(optimized_file_path):
            # Ensure vocabulary version actually exists
            if gcp_services.vocab_gcs_path_exists(vocab_path):
                # Build paths for read_parquet statements
                concept_path = storage.get_uri(f"{vocab_path}{constants.OPTIMIZED_VOCAB_FOLDER}/concept{constants.PARQUET}")
                concept_relationship_path = storage.get_uri(f"{vocab_path}{constants.OPTIMIZED_VOCAB_FOLDER}/concept_relationship{constants.PARQUET}")
                output_path = storage.get_uri(optimized_file_path)

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
                    FROM read_parquet('{concept_path}') c1
                    LEFT JOIN read_parquet('{concept_relationship_path}') cr on c1.concept_id = cr.concept_id_1
                    LEFT JOIN read_parquet('{concept_path}') c2 on cr.concept_id_2 = c2.concept_id
                    WHERE IFNULL(cr.relationship_id, '')
                        IN ('', {constants.MAPPING_RELATIONSHIPS},{constants.REPLACEMENT_RELATIONSHIPS})
                ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
                """
                utils.execute_duckdb_sql(transform_query, "Unable to create optimized vocab file")

            else:
                raise Exception(f"Vocabulary GCS bucket {vocab_path} not found")

def create_missing_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
    """Create OMOP CDM tables in BigQuery dataset using DDL scripts for specified CDM version."""
    ddl_file = f"{constants.DDL_SQL_PATH}{omop_version}/{constants.DDL_FILE_NAME}"

    # Get DDL with CREATE OR REPLACE TABLE statements
    try:
        with open(ddl_file, 'r') as f:
            ddl_sql = f.read()

        # Add project_id and data_set to SQL statement
        create_sql = ddl_sql.replace(constants.DDL_PLACEHOLDER_STRING, f"{project_id}.{dataset_id}")

        # Execute the CREATE OR REPLACE TABLE statements in BigQuery
        gcp_services.execute_bq_sql(create_sql, None)

    except Exception as e:
        raise Exception(f"DDL file error: {e}")

def populate_cdm_source(cdm_source_data: dict) -> None:
    """Populate cdm_source table with metadata about the CDM instance."""
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
        gcp_services.execute_bq_sql(query, job_config)
    except Exception as e:
        error_details = {
            'error_type': type(e).__name__,
            'error_message': str(e),
        }
        raise Exception(f"Unable to add pipeline log record: {error_details}") from e

def generate_derived_data_from_harmonized(site: str, site_bucket: str, delivery_date: str, table_name: str, vocab_version: str, vocab_gcs_bucket: str) -> None:
    """
    Execute SQL scripts to generate derived data table Parquet files from HARMONIZED data.

    This function is called AFTER vocabulary harmonization is complete and reads from the
    harmonized Parquet files in the omop_etl directory. The output is written to the
    derived_files directory and will be loaded to BigQuery in a separate step.
    """
    sql_script_name = table_name

    if table_name not in constants.DERIVED_DATA_TABLES_REQUIREMENTS.keys():
        raise Exception(f"{table_name} is not a derived data table")

    # Check if tables necessary to generate derived data exist in harmonized delivery
    # For vocab-harmonized tables, check in omop_etl/
    # For non-harmonized tables (like person), check in converted_files/
    for required_table in constants.DERIVED_DATA_TABLES_REQUIREMENTS[table_name]:
        # Check if this table goes through vocabulary harmonization
        if required_table in constants.VOCAB_HARMONIZED_TABLES:
            # Look for harmonized version
            parquet_path = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}{required_table}/{required_table}{constants.PARQUET}"
        else:
            # Look for converted version (e.g., person, death)
            parquet_path = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{required_table}{constants.PARQUET}"

        if not utils.parquet_file_exists(parquet_path):
            # Don't raise exception if required table doesn't exist, just log warning
            utils.logger.warning(f"Required table {required_table} not in {site}'s {delivery_date} data delivery, cannot generate derived data table {table_name}")
            return

    # observation_period table requires special logic
    # observation_period records are necessary when using OHDSI analytic tools
    # Create observation_period records using standard logic for all sites
    # https://ohdsi.github.io/CommonDataModel/ehrObsPeriods.html
    if table_name == constants.OBSERVATION_PERIOD:
        # Check for harmonized visit_occurrence
        visit_occurrence_table = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.OMOP_ETL.value}visit_occurrence/visit_occurrence{constants.PARQUET}"
        # Death table doesn't go through harmonization
        death_table = f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}death{constants.PARQUET}"

        # Need separate SQL scripts for different file delivery scenarios
        # DuckDB doesn't support branch logic based on table/file availability so choosing SQL script via Python
        if utils.parquet_file_exists(visit_occurrence_table) and utils.parquet_file_exists(death_table):
            sql_script_name = "observation_period_vod"
        elif utils.parquet_file_exists(visit_occurrence_table):
            sql_script_name = "observation_period_vo"
        else:
            sql_script_name = table_name

    # Get SQL script with placeholder values for table locations
    try:
        create_statement = ""
        # The drug_era script provided by OHDSI is resource intensive
        # The script is split into two parts:
        #   1) SQL statements that iteratively creates tables to derive drug_era records. Creating tables offloads data from memory to disk.
        #   2) Performs a final select statement against "last" temp table
        if table_name == constants.DRUG_ERA:
            create_statement_path = f"{constants.DERIVED_TABLE_SCRIPT_PATH}{sql_script_name}_create.sql"
            with open(create_statement_path, 'r') as f:
                create_statement_raw = f.read()
            create_statement = utils.placeholder_to_harmonized_file_path(site, site_bucket, delivery_date, create_statement_raw, vocab_version, vocab_gcs_bucket)

        sql_path = f"{constants.DERIVED_TABLE_SCRIPT_PATH}{sql_script_name}.sql"
        with open(sql_path, 'r') as f:
            select_statement_raw = f.read()

        # Add table locations using harmonized file paths
        select_statement = utils.placeholder_to_harmonized_file_path(site, site_bucket, delivery_date, select_statement_raw, vocab_version, vocab_gcs_bucket)

        # Output to derived_files directory
        parquet_gcs_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.DERIVED_FILES.value}{table_name}{constants.PARQUET}")

        # Generate and execute final SQL
        sql_statement = f"""
            {create_statement}

            COPY (
                {select_statement}
            ) TO '{parquet_gcs_path}' {constants.DUCKDB_FORMAT_STRING}
        """
        utils.execute_duckdb_sql(sql_statement, f"Unable to execute SQL to generate {table_name}")

        utils.logger.info(f"Successfully generated derived table {table_name} from harmonized data to {parquet_gcs_path}")

    except Exception as e:
        raise Exception(f"Unable to generate {table_name} derived data from harmonized files: {str(e)}") from e

def load_vocabulary_table(vocab_version: str, vocab_gcs_bucket: str, table_file_name: str, project_id: str, dataset_id: str) -> None:
    """Load vocabulary Parquet file from GCS to BigQuery table."""
    vocab_parquet_path = storage.get_uri(f"{vocab_gcs_bucket}/{vocab_version}/{constants.OPTIMIZED_VOCAB_FOLDER}/{table_file_name}{constants.PARQUET}")

    if utils.parquet_file_exists(vocab_parquet_path) and utils.valid_parquet_file(vocab_parquet_path):
        gcp_services.load_parquet_to_bigquery(vocab_parquet_path, project_id, dataset_id, table_file_name, constants.BQWriteTypes.SPECIFIC_FILE)
    else:
        raise Exception(f"Vocabulary table {table_file_name} not found at {vocab_parquet_path}")