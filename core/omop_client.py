import core.constants as constants
import core.gcp_services as gcp_services
import core.normalization as normalization
import core.utils as utils
from core.storage_backend import storage


class OMOPClient:
    """
    OMOP CDM client operations including upgrades, derived data generation, and BigQuery management.

    Handles:
    - CDM version upgrades (e.g., 5.3 to 5.4)
    - cdm_source file population
    - Derived data table generation from harmonized data
    - BigQuery table creation
    """

    @staticmethod
    def upgrade_file(file_path: str, cdm_version: str, target_omop_version: str) -> None:
        """
        Upgrade an OMOP CDM table file from one version to another.

        Applies version-specific transformations to upgrade table schema and data.
        Currently supports upgrading from CDM v5.3 to v5.4.

        Handles three cases for table upgrades:
        1. No changes needed (table remains the same in new version)
        2. Table removed (file is deleted in new version)
        3. Table changed and overwritten (SQL upgrade script is applied to transform the data)

        Args:
            file_path: Path to the file to upgrade
            cdm_version: Current CDM version of the file
            target_omop_version: Target CDM version to upgrade to

        Raises:
            Exception: If CDM version not supported or upgrade fails
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

                        # Generate SQL
                        select_statement = OMOPClient.generate_upgrade_file_sql(upgrade_script, normalized_file_path)

                        # Execute SQL
                        utils.execute_duckdb_sql(select_statement, f"Unable to upgrade file {file_path}:")

                    except Exception as e:
                        raise Exception(f"Unable to open SQL upgrade file {upgrade_file_path}: {e}") from e
            else:
                utils.logger.info(f"No changes in {file_path} when upgrading from 5.3 to 5.4")
        else:
            raise Exception(f"OMOP CDM version {cdm_version} not supported")

    @staticmethod
    def create_missing_bq_tables(project_id: str, dataset_id: str, omop_version: str) -> None:
        """
        Create OMOP CDM tables in BigQuery dataset using DDL scripts.

        Executes CREATE OR REPLACE TABLE statements for all OMOP CDM tables
        defined in the DDL script for the specified CDM version.

        Args:
            project_id: GCP project ID for BigQuery
            dataset_id: BigQuery dataset ID
            omop_version: OMOP CDM version (e.g., '5.4')

        Raises:
            Exception: If DDL file not found or table creation fails
        """
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

    @staticmethod
    def populate_cdm_source_file(cdm_source_data: dict) -> None:
        """
        Populate cdm_source Parquet file with metadata if empty or non-existent.

        Checks if site delivered cdm_source file and populates it with metadata if needed:
        - If file exists and has rows: do nothing (site provided data)
        - If file exists but is empty: populate with metadata
        - If file doesn't exist: create and populate with metadata

        Args:
            cdm_source_data: Dictionary containing CDM source metadata fields:
                - gcs_bucket: GCS bucket path
                - source_release_date: Release date of source data
                - cdm_source_name: Name of the CDM source
                - cdm_source_abbreviation: Abbreviation
                - cdm_holder: Organization holding the data
                - source_description: Description of data source
                - cdm_version: OMOP CDM version
                - (optional) source_documentation_reference: Documentation URL
                - (optional) cdm_etl_reference: ETL documentation URL
                - cdm_release_date: Release date of CDM
        """
        bucket = cdm_source_data["gcs_bucket"]
        delivery_date = cdm_source_data["source_release_date"]

        cdm_source_path = f"{bucket}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}cdm_source{constants.PARQUET}"

        file_exists = utils.parquet_file_exists(cdm_source_path)

        if file_exists:
            # Check if file has rows
            row_count_sql = normalization.Normalizer.generate_row_count_sql(storage.get_uri(cdm_source_path))
            result = utils.execute_duckdb_sql(row_count_sql, "Unable to count rows in cdm_source", return_results=True)
            row_count = result[0][0] if result else 0

            if row_count > 0:
                utils.logger.info(f"cdm_source file has {row_count} rows, skipping population")
                return

        utils.logger.info(f"Populating cdm_source Parquet file for {delivery_date} delivery")

        vocab_version = utils.get_delivery_vocabulary_version(bucket, delivery_date)
        populate_sql = OMOPClient.generate_populate_cdm_source_sql(cdm_source_data, vocab_version, storage.get_uri(cdm_source_path))
        utils.execute_duckdb_sql(populate_sql, "Unable to populate cdm_source file")

    @staticmethod
    def generate_derived_data_from_harmonized(site: str, site_bucket: str, delivery_date: str, table_name: str, vocab_version: str, vocab_path: str) -> None:
        """
        Execute SQL scripts to generate derived data table Parquet files from HARMONIZED data.

        This function is called AFTER vocabulary harmonization is complete and reads from the
        harmonized Parquet files in the omop_etl directory. The output is written to the
        derived_files directory and will be loaded to BigQuery in a separate step.

        Args:
            site: Site identifier
            site_bucket: GCS bucket for the site
            delivery_date: Date of data delivery
            table_name: Name of derived table to generate (observation_period, drug_era, etc.)
            vocab_version: Vocabulary version to use for harmonization lookups
            vocab_path: Path to vocabulary files

        Raises:
            Exception: If table is not a derived data table or generation fails
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
                create_statement = utils.placeholder_to_harmonized_file_path(site, site_bucket, delivery_date, create_statement_raw, vocab_version, vocab_path)

            sql_path = f"{constants.DERIVED_TABLE_SCRIPT_PATH}{sql_script_name}.sql"
            with open(sql_path, 'r') as f:
                select_statement_raw = f.read()

            # Add table locations using harmonized file paths
            select_statement = utils.placeholder_to_harmonized_file_path(site, site_bucket, delivery_date, select_statement_raw, vocab_version, vocab_path)

            # Output to derived_files directory
            parquet_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.DERIVED_FILES.value}{table_name}{constants.PARQUET}")

            # Generate and execute final SQL
            sql_statement = f"""
            {create_statement}

            COPY (
                {select_statement}
            ) TO '{parquet_path}' {constants.DUCKDB_FORMAT_STRING}
        """
            utils.execute_duckdb_sql(sql_statement, f"Unable to execute SQL to generate {table_name}")

            utils.logger.info(f"Successfully generated derived table {table_name} from harmonized data to {parquet_path}")

        except Exception as e:
            raise Exception(f"Unable to generate {table_name} derived data from harmonized files: {str(e)}") from e

    @staticmethod
    def generate_upgrade_file_sql(upgrade_script: str, normalized_file_path: str) -> str:
        """
        Generate SQL to upgrade an OMOP CDM table file.

        Creates SQL that:
        - Applies version-specific upgrade transformation script
        - Reads from normalized Parquet file
        - Overwrites the same file with upgraded data

        Args:
            upgrade_script: SQL transformation script for the table upgrade
            normalized_file_path: Path to the normalized Parquet file

        Returns:
            SQL string for upgrading the file
        """
        return f"""
        COPY (
            {upgrade_script}
            FROM read_parquet('{storage.get_uri(normalized_file_path)}')
        ) TO '{storage.get_uri(normalized_file_path)}' {constants.DUCKDB_FORMAT_STRING}
    """

    @staticmethod
    def generate_populate_cdm_source_sql(cdm_source_data: dict, vocab_version: str, output_path: str) -> str:
        """
        Generate SQL to create cdm_source Parquet file with metadata.

        Creates a single-row Parquet file containing CDM source metadata including
        version information, release dates, and vocabulary version.

        Args:
            cdm_source_data: Dictionary containing CDM source metadata fields
            vocab_version: Vocabulary version string
            output_path: Path where cdm_source Parquet file should be written

        Returns:
            SQL statement that creates single-row cdm_source Parquet file
        """
        cdm_version_concept_id = utils.get_cdm_version_concept_id(cdm_source_data["cdm_version"])

        return f"""
        COPY (
            SELECT
                '{cdm_source_data["cdm_source_name"]}' AS cdm_source_name,
                '{cdm_source_data["cdm_source_abbreviation"]}' AS cdm_source_abbreviation,
                '{cdm_source_data["cdm_holder"]}' AS cdm_holder,
                '{cdm_source_data["source_description"]}' AS source_description,
                '{cdm_source_data.get("source_documentation_reference", "")}' AS source_documentation_reference,
                '{cdm_source_data.get("cdm_etl_reference", "")}' AS cdm_etl_reference,
                CAST('{cdm_source_data["source_release_date"]}' AS DATE) AS source_release_date,
                CAST('{cdm_source_data["cdm_release_date"]}' AS DATE) AS cdm_release_date,
                '{cdm_source_data["cdm_version"]}' AS cdm_version,
                {cdm_version_concept_id} AS cdm_version_concept_id,
                '{vocab_version}' AS vocabulary_version
        ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
    """
