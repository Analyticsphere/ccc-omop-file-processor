import uuid
from typing import Optional

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.transformer as transformer
import core.utils as utils
from core.storage_backend import storage


class VocabHarmonizer:
    """
    A class for harmonizing OMOP parquet files according to specified vocabulary version.
    """

    def __init__(self, file_path: str, cdm_version: str, site: str, vocab_version: str, vocab_path: str, project_id: str, dataset_id: str):
        """
        Initialize a VocabHarmonizer with common parameters needed across all operations.
        """
        self.file_path = file_path
        self.cdm_version = cdm_version
        self.site = site
        self.vocab_version = vocab_version
        self.vocab_path = vocab_path
        self.source_table_name = utils.get_table_name_from_path(file_path)
        self.bucket = utils.get_bucket_and_delivery_date_from_path(file_path)[0]
        self.delivery_date = utils.get_bucket_and_delivery_date_from_path(file_path)[1]
        self.source_parquet_path = utils.get_parquet_artifact_location(file_path)
        self.harmonized_parquet_path = utils.get_parquet_harmonized_path(file_path)
        self.harmonized_parquet_file = storage.get_uri(f"{self.harmonized_parquet_path}*{constants.PARQUET}")
        self.project_id = project_id
        self.dataset_id = dataset_id

    def perform_harmonization(self, step: str) -> Optional[list[dict]]:
        """
        Perform a specific harmonization step.

        Returns:
            For DISCOVER_TABLES_FOR_DEDUP step, returns a list of table configs.
            For all other steps, returns None.
        """
        utils.logger.info(f"Performing vocabulary harmonization against {self.file_path}: {step}")

        if step == constants.SOURCE_TARGET:
            self.source_target_remapping()
        elif step == constants.DOMAIN_CHECK:
            self.domain_table_check()
        elif step == constants.TARGET_REMAP:
            self.check_new_targets(constants.TARGET_REMAP)
        elif step == constants.TARGET_REPLACEMENT:
            self.check_new_targets(constants.TARGET_REPLACEMENT)
        elif step == constants.OMOP_ETL:
            self.omop_etl()
        elif step == constants.CONSOLIDATE_ETL:
            self.consolidate_etl_tables()
        elif step == constants.DEDUPLICATE_PRIMARY_KEYS:
            self.deduplicate_primary_keys_all_tables()
        elif step == constants.DISCOVER_TABLES_FOR_DEDUP:
            return self.discover_tables_for_deduplication()
        elif step == constants.DEDUPLICATE_SINGLE_TABLE:
            self.deduplicate_single_table()
        else:
            raise Exception(f"Unknown harmonization step {step}")

        return None

    def source_target_remapping(self) -> None:
        """
        Generate and execute SQL to check for and update non-standard source-to-target mappings to standard
        """
        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order

        # Get _concept_id and _source_concept_id columns for table
        target_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']
        source_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']
        primary_key = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

        # specimen and note tables don't have _source_concept_id columns so can't be evaluated with this method
        if not source_concept_id_column or source_concept_id_column == "":
            return

        # Generate complete SQL
        output_path = storage.get_uri(f"{self.harmonized_parquet_path}{self.source_table_name}_source_target_remap{constants.PARQUET}")
        final_sql = VocabHarmonizer.generate_source_target_remapping_sql(
            source_table_name=self.source_table_name,
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column=target_concept_id_column,
            source_concept_id_column=source_concept_id_column,
            primary_key=primary_key,
            site=self.site,
            bucket=self.bucket,
            delivery_date=self.delivery_date,
            vocab_version=self.vocab_version,
            vocab_path=self.vocab_path,
            output_path=output_path
        )

        # Execute SQL
        utils.execute_duckdb_sql(final_sql, f"Unable to execute SQL to harominze vocabulary in table {self.source_table_name}")

    def check_new_targets(self, mapping_type: str) -> None:
        """
        Generate and execute SQL to check for cases in which a non-standard target_concept_id
        has a mapping or replacement to a standard concept_id
        """
        if mapping_type == constants.TARGET_REMAP:
            vocab_status_string = "existing non-standard target remapped to standard code"
            mapping_relationships = "'Maps to', 'Maps to value'"
            table_name = "target_remap"
        elif mapping_type == constants.TARGET_REPLACEMENT:
            vocab_status_string = "existing non-standard target replaced with standard code"
            mapping_relationships = "'Concept replaced by'"
            table_name = "target_replacement"

        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order

        # Get _concept_id and _source_concept_id columns for table
        target_concept_id_column = constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']
        source_concept_id_column = '0' if constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id'] == "" \
            else f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']}"
        primary_key_column = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

        # Don't perform target remapping on rows which have already been harmonized
        # primary_key_column values were made unique per row values in normalization step,
        #   so they can be used for identification here
        existing_files_where_clause = ""
        if utils.valid_parquet_file(self.harmonized_parquet_file):
            existing_files_where_clause = f"""
                AND tbl.{primary_key_column} NOT IN (
                    SELECT {primary_key_column} FROM read_parquet('{self.harmonized_parquet_file}')
                )
            """

        # Generate complete SQL
        output_path = storage.get_uri(f"{self.harmonized_parquet_path}{self.source_table_name}_{table_name}{constants.PARQUET}")
        final_sql = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name=self.source_table_name,
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column=target_concept_id_column,
            source_concept_id_column=source_concept_id_column,
            primary_key_column=primary_key_column,
            vocab_status_string=vocab_status_string,
            mapping_relationships=mapping_relationships,
            existing_files_where_clause=existing_files_where_clause,
            site=self.site,
            bucket=self.bucket,
            delivery_date=self.delivery_date,
            vocab_version=self.vocab_version,
            vocab_path=self.vocab_path,
            output_path=output_path
        )

        # Execute SQL
        utils.execute_duckdb_sql(final_sql, f"Unable to execute SQL to check for new targets ({mapping_type}) {self.source_table_name}")

    def domain_table_check(self) -> None:
        """
        Assign current domain and target table to concepts not remapped in previous steps.
        Handles domain changes between vocabulary versions and site ETL misalignments.
        """
        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order
        target_concept_id_column = f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']}"
        source_concept_id_column = '0' if constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id'] == "" \
            else f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']}"
        primary_key_column = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

        # Don't perform domain check on rows which have already been harmonized
        # primary_key_column values were made unique per row values in normalization step,
        #   so they can be used for identification here
        existing_files_where_clause = ""
        if utils.valid_parquet_file(self.harmonized_parquet_file):
            existing_files_where_clause = f"""
                WHERE tbl.{primary_key_column} NOT IN (
                    SELECT {primary_key_column} FROM read_parquet('{self.harmonized_parquet_file}')
                )
            """

        # Generate complete SQL
        output_path = storage.get_uri(f"{self.harmonized_parquet_path}{self.source_table_name}_domain_check{constants.PARQUET}")
        final_sql = VocabHarmonizer.generate_domain_table_check_sql(
            source_table_name=self.source_table_name,
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column=target_concept_id_column,
            source_concept_id_column=source_concept_id_column,
            existing_files_where_clause=existing_files_where_clause,
            site=self.site,
            bucket=self.bucket,
            delivery_date=self.delivery_date,
            vocab_version=self.vocab_version,
            vocab_path=self.vocab_path,
            output_path=output_path
        )

        # Execute SQL
        utils.execute_duckdb_sql(final_sql, f"Unable to perform domain check against {self.source_table_name}")

    def generate_table_transition_artifacts(self) -> None:
        """
        Generate report artifacts showing how many rows transitioned from the source table
        to each target table during vocabulary harmonization.

        Args:
            transition_counts: List of tuples containing (target_table, row_count)
        """
        try:
            utils.logger.info(f"Generating table transition report for {self.source_table_name}")

            transition_count_sql = VocabHarmonizer.generate_table_transition_count_sql(self.harmonized_parquet_file)
            transition_counts = utils.execute_duckdb_sql(
                transition_count_sql,
                f"Unable to get table transition counts from Parquet file {self.file_path}",
                return_results=True
            )
            
            # Get the source table concept_id from the schema
            schema = utils.get_cdm_schema(cdm_version=self.cdm_version)
            source_table_concept_id = schema.get(self.source_table_name, {}).get('concept_id')

            # Create a report artifact for each target table
            for target_table, row_count in transition_counts:
                ra = report_artifact.ReportArtifact(
                    delivery_date=self.delivery_date,
                    artifact_bucket=self.bucket,
                    concept_id=source_table_concept_id,
                    name=f"Vocab harmonization table transition: {self.source_table_name} to {target_table}",
                    value_as_string=None,
                    value_as_concept_id=None,
                    value_as_number=row_count
                )
                ra.save_artifact()
                utils.logger.info(f"Table transition: {self.source_table_name} → {target_table}: {row_count} rows")

        except Exception as e:
            # Log the error but don't fail the entire process
            utils.logger.error(f"Error generating table transition report: {str(e)}")

    def generate_vocab_status_artifacts(self) -> None:
        """
        Generate report artifacts showing counts of each vocab harmonization status type.

        This tracks what kinds of harmonization changes occurred (e.g., "source_concept_id mapped to new target",
        "existing non-standard target remapped to standard code", etc.) at the table level.

        Args:
            status_counts: List of tuples containing (vocab_harmonization_status, row_count)
        """
        try:
            utils.logger.info(f"Generating vocab harmonization status report for {self.source_table_name}")

            vocab_status_count_sql = VocabHarmonizer.generate_vocab_status_count_sql(self.harmonized_parquet_file)
            status_counts = utils.execute_duckdb_sql(
                vocab_status_count_sql,
                f"Unable to get vocab status counts from Parquet file {self.file_path}",
                return_results=True
            )

            # Get the source table concept_id from the schema
            schema = utils.get_cdm_schema(cdm_version=self.cdm_version)
            source_table_concept_id = schema.get(self.source_table_name, {}).get('concept_id')

            # Create a report artifact for each status type
            for status, row_count in status_counts:
                ra = report_artifact.ReportArtifact(
                    delivery_date=self.delivery_date,
                    artifact_bucket=self.bucket,
                    concept_id=source_table_concept_id,
                    name=f"Vocab harmonization status: {self.source_table_name} - {status}",
                    value_as_string=None,
                    value_as_concept_id=None,
                    value_as_number=row_count
                )
                ra.save_artifact()
                utils.logger.info(f"Vocab status: {self.source_table_name} - {status}: {row_count} rows")

        except Exception as e:
            # Log the error but don't fail the entire process
            utils.logger.error(f"Error generating vocab status report: {str(e)}")

    def omop_etl(self) -> None:
        """
        Partition harmonized data and transform to target OMOP tables.
        Discovers all target tables and transforms each using OMOP-to-OMOP ETL.
        """
        utils.logger.info(f"Partitioning and ETLing source file {self.file_path} to appropriate target table(s)")

        # Generate reports before transformation
        # Must be done before OMOP ETL step modifies the data
        self.generate_table_transition_artifacts()
        self.generate_vocab_status_artifacts()

        # Generate and execute SQL to get target tables
        target_tables_sql = VocabHarmonizer.generate_get_target_tables_sql(self.harmonized_parquet_file)
        target_tables_result = utils.execute_duckdb_sql(
            target_tables_sql,
            f"Unable to get target tables from Parquet file {self.file_path}",
            return_results=True
        )
        target_tables_list = [row[0] for row in target_tables_result]

        # Create a new Parquet file for each target table with the appropriate structure
        for target_table in target_tables_list:
            omop_transformer = transformer.Transformer(
                self.site, self.harmonized_parquet_path, self.cdm_version, self.source_table_name, target_table, utils.get_omop_etl_destination_path(self.file_path)
            )

            # Perform the OMOP-to-OMOP ETL for this target table
            omop_transformer.omop_to_omop_etl()

    def consolidate_etl_tables(self) -> None:
        """
        Orchestrates consolidation across all ETL'd tables.
        
        Discovers all table subdirectories in the OMOP_ETL artifacts directory
        and consolidates each one by delegating to the single-table processing method.
        """
        utils.logger.info(f"Consolidating ETL files for {self.file_path}")
        
        # Get the OMOP ETL directory path
        etl_base_path = utils.get_omop_etl_destination_path(self.file_path)
        bucket_name, directory_path = utils.get_bucket_and_delivery_date_from_path(etl_base_path)
        etl_folder = f"{directory_path}/{constants.ArtifactPaths.OMOP_ETL.value}"
        storage_path = storage.get_uri(f"{bucket_name}/{etl_folder}")

        utils.logger.info(f"Looking for table directories in {storage_path}")

        # Get list of table subdirectories
        subdirectories = storage.list_subdirectories(storage_path)

        # Extract just the table names from the full paths
        table_names = [subdir.rstrip('/').split('/')[-1] for subdir in subdirectories]

        if not table_names:
            utils.logger.warning(f"No table directories found in {storage_path}")
            return

        utils.logger.info(f"Found {len(table_names)} table(s) to consolidate: {sorted(table_names)}")
        
        # Process each table directory
        utils.logger.info("Starting consolidation for each table...")
        for table_name in sorted(table_names):
            self._process_single_table(bucket_name, etl_folder, table_name)
        utils.logger.info("Completed consolidation for all tables.")

    def deduplicate_primary_keys_all_tables(self) -> None:
        """
        Orchestrates deduplication of primary keys across all consolidated ETL tables.
        
        Discovers all table subdirectories in the OMOP_ETL artifacts directory
        and deduplicates primary keys for each table that has surrogate keys.
        """
        utils.logger.info(f"Deduplicating primary keys for ETL files for {self.file_path}")
        
        # Get the OMOP ETL directory path
        etl_base_path = utils.get_omop_etl_destination_path(self.file_path)
        bucket_name, directory_path = utils.get_bucket_and_delivery_date_from_path(etl_base_path)
        etl_folder = f"{directory_path}/{constants.ArtifactPaths.OMOP_ETL.value}"
        storage_path = storage.get_uri(f"{bucket_name}/{etl_folder}")

        utils.logger.info(f"Looking for table directories in {storage_path}")

        # Get list of table subdirectories
        subdirectories = storage.list_subdirectories(storage_path)

        # Extract just the table names from the full paths
        table_names = [subdir.rstrip('/').split('/')[-1] for subdir in subdirectories]

        if not table_names:
            utils.logger.warning(f"No table directories found in {storage_path}")
            return

        utils.logger.info(f"Found {len(table_names)} table(s) to check for deduplication: {sorted(table_names)}")
        
        # Process each table directory
        utils.logger.info("Starting deduplication for each table...")
        for table_name in sorted(table_names):
            # Construct the consolidated file path
            table_dir = f"{etl_folder}{table_name}/"
            consolidated_file_path = storage.get_uri(f"{bucket_name}/{table_dir}{table_name}{constants.PARQUET}")
            
            # Deduplicate primary keys for this table
            self._deduplicate_primary_keys(consolidated_file_path, table_name)
        utils.logger.info("Completed deduplication for all tables.")

    def _process_single_table(self, bucket_name: str, etl_folder: str, table_name: str) -> None:
        """
        Combine all parquet files for a single table into one consolidated file.

        Args:
            bucket_name: Storage bucket/directory name
            etl_folder: Path to the ETL folder within the storage location
            table_name: Name of the OMOP table to consolidate
        """
        utils.logger.info(f"Consolidating files for table: {table_name}")
        
        # Construct paths
        table_dir = f"{etl_folder}{table_name}/"
        source_parquet_pattern = storage.get_uri(f"{bucket_name}/{table_dir}parts/*.parquet")
        consolidated_file_path = storage.get_uri(f"{bucket_name}/{table_dir}{table_name}{constants.PARQUET}")

        # Generate and execute SQL to combine all parquet files into one
        combine_sql = VocabHarmonizer.generate_consolidate_single_table_sql(
            source_parquet_pattern=source_parquet_pattern,
            output_path=consolidated_file_path
        )
        utils.execute_duckdb_sql(combine_sql, f"Unable to consolidate files for table {table_name}")
        utils.logger.info(f"Successfully consolidated {table_name}")

    def _deduplicate_primary_keys(self, file_path: str, table_name: str) -> None:
        """
        Check for and fix duplicate primary keys in a consolidated parquet file.
        Only applies to tables with surrogate keys.

        Note: This method manually manages its DuckDB connection (unlike other methods
        that use execute_duckdb_sql) because it needs to maintain a temp table
        ('duplicate_keys') across multiple SQL statements within the same session.
        Temp tables only exist within a single connection session.

        Args:
            file_path: Path to the consolidated parquet file
            table_name: Name of the OMOP table
        """
        # Only process tables with surrogate keys
        if table_name not in constants.SURROGATE_KEY_TABLES:
            utils.logger.info(f"Table {table_name} does not use surrogate keys. Skipping deduplication.")
            return
        
        # Get primary key column
        primary_key_column = utils.get_primary_key_column(table_name, self.cdm_version)
        if not primary_key_column:
            utils.logger.info(f"No primary key column defined for table {table_name}. Skipping deduplication.")
            return
        
        # Get primary key data type
        schema = utils.get_table_schema(table_name, self.cdm_version)
        if table_name not in schema:
            utils.logger.warning(f"Schema not found for table {table_name}. Skipping deduplication.")
            return
        
        target_columns_dict = schema[table_name]["columns"]
        if primary_key_column not in target_columns_dict:
            utils.logger.warning(f"Primary key column {primary_key_column} not found in schema. Skipping deduplication.")
            return
        
        primary_key_type = target_columns_dict[primary_key_column]['type']

        conn = None
        local_db_file = None
        try:
            conn, local_db_file = utils.create_duckdb_connection()
            with conn:
                # Check if duplicates exist
                utils.logger.info(f"Checking for duplicate primary keys in {table_name}...")
                check_sql = VocabHarmonizer.generate_check_duplicates_sql(file_path, primary_key_column)
                duplicates = conn.execute(check_sql).fetchall()

                if not duplicates:
                    utils.logger.info(f"No duplicate primary keys found in {table_name}")
                    return

                utils.logger.info(f"Found duplicate primary keys in {table_name}. Fixing...")

                # Create temp table with duplicate keys only
                create_temp_table_sql = VocabHarmonizer.generate_create_duplicate_keys_table_sql(file_path, primary_key_column)
                conn.execute(create_temp_table_sql)

                count_sql = VocabHarmonizer.generate_count_duplicates_sql()
                dup_count_result = conn.execute(count_sql).fetchone()
                dup_count = dup_count_result[0] if dup_count_result else 0
                utils.logger.info(f"Found {dup_count} unique keys in {table_name} with duplicates")
                
                # Generate temp file paths
                tmp_id = uuid.uuid4()
                bucket_path = file_path.rsplit('/', 1)[0]
                tmp_non_dup = f"{bucket_path}/tmp/tmp_non_dup_{tmp_id}.parquet"
                tmp_dup_fixed = f"{bucket_path}/tmp/tmp_dup_fixed_{tmp_id}.parquet"
                
                # Pass 1: Write non-duplicate rows
                utils.logger.info(f"Processing non-duplicate rows in in {table_name}...")
                write_non_dup_sql = VocabHarmonizer.generate_write_non_duplicates_sql(
                    file_path, primary_key_column, tmp_non_dup
                )
                conn.execute(write_non_dup_sql)

                # Pass 2: Fix duplicate rows
                utils.logger.info(f"Processing duplicate rows with fixes in {table_name}...")
                fix_dup_sql = VocabHarmonizer.generate_fix_duplicates_sql(
                    file_path, primary_key_column, primary_key_type, tmp_dup_fixed
                )
                conn.execute(fix_dup_sql)

                # Combine both temp files and overwrite original
                utils.logger.info(f"Merging non-duplicate and fixed duplicate rows in {table_name}...")
                merge_sql = VocabHarmonizer.generate_merge_deduplicated_sql(
                    tmp_non_dup, tmp_dup_fixed, file_path
                )
                conn.execute(merge_sql)
                
                # Cleanup temporary files
                utils.logger.info(f"Cleaning up temporary files for {table_name}...")
                try:
                    storage.delete_file(tmp_non_dup)
                    storage.delete_file(tmp_dup_fixed)
                    utils.logger.info(f"Successfully cleaned up temporary files for {table_name}")
                except Exception as cleanup_error:
                    utils.logger.warning(f"Failed to clean up temporary files for {table_name}: {str(cleanup_error)}")
                
                utils.logger.info(f"Successfully deduplicated primary keys in {table_name}")

        except Exception as e:
            raise Exception(f"Unable to deduplicate primary keys for table {table_name}: {str(e)}") from e
        finally:
            if conn is not None:
                utils.close_duckdb_connection(conn, local_db_file)

    def discover_tables_for_deduplication(self) -> list[dict]:
        """
        Discover all tables in the OMOP_ETL artifacts directory that need deduplication.
        Returns a list of table configuration dictionaries for parallel processing.

        Returns:
            List of dicts, each containing:
                - site: Site identifier
                - delivery_date: Delivery date
                - table_name: Name of the OMOP table
                - bucket_name: Storage bucket/directory name
                - etl_folder: Path to ETL folder
                - file_path: Full path to the consolidated parquet file
        """
        utils.logger.info(f"Discovering tables for deduplication for {self.file_path}")

        # Get the OMOP ETL directory path
        etl_base_path = utils.get_omop_etl_destination_path(self.file_path)
        bucket_name, directory_path = utils.get_bucket_and_delivery_date_from_path(etl_base_path)
        etl_folder = f"{directory_path}/{constants.ArtifactPaths.OMOP_ETL.value}"
        storage_path = storage.get_uri(f"{bucket_name}/{etl_folder}")

        utils.logger.info(f"Looking for table directories in {storage_path}")

        # Get list of table subdirectories
        subdirectories = storage.list_subdirectories(storage_path)

        # Extract table names from the full paths
        table_names = [subdir.rstrip('/').split('/')[-1] for subdir in subdirectories]

        if not table_names:
            utils.logger.warning(f"No table directories found in {storage_path}")
            return []

        utils.logger.info(f"Found {len(table_names)} table(s) for potential deduplication: {sorted(table_names)}")

        # Build configuration for each table
        table_configs = []
        for table_name in sorted(table_names):
            table_dir = f"{etl_folder}{table_name}/"
            consolidated_file_path = storage.get_uri(f"{bucket_name}/{table_dir}{table_name}{constants.PARQUET}")

            table_config = {
                "site": self.site,
                "delivery_date": self.delivery_date,
                "table_name": table_name,
                "bucket_name": bucket_name,
                "etl_folder": etl_folder,
                "file_path": consolidated_file_path,
                "cdm_version": self.cdm_version,
                "project_id": self.project_id,
                "dataset_id": self.dataset_id
            }
            table_configs.append(table_config)

        utils.logger.info(f"Prepared {len(table_configs)} table configuration(s) for deduplication")
        return table_configs

    def deduplicate_single_table(self) -> None:
        """
        Deduplicate primary keys for a single table based on the configuration in file_path.

        This method expects self.file_path to be a JSON string containing the table configuration
        with keys: table_name, file_path (path to consolidated parquet), cdm_version

        This is designed to be called per-table in parallel by the Airflow DAG.
        """
        import json

        # The file_path parameter will contain JSON configuration for the table
        try:
            # Parse the table configuration from file_path
            table_config = json.loads(self.file_path)
            table_name = table_config['table_name']
            consolidated_file_path = table_config['file_path']

            utils.logger.info(f"Deduplicating primary keys for table: {table_name}")
            utils.logger.info(f"Consolidated file path: {consolidated_file_path}")

            # Call the existing _deduplicate_primary_keys method with the table-specific info
            self._deduplicate_primary_keys(consolidated_file_path, table_name)

            utils.logger.info(f"Successfully completed deduplication for table: {table_name}")

        except json.JSONDecodeError as e:
            raise Exception(f"Invalid table configuration JSON: {str(e)}") from e
        except KeyError as e:
            raise Exception(f"Missing required key in table configuration: {str(e)}") from e
        except Exception as e:
            raise Exception(f"Unable to deduplicate table: {str(e)}") from e

    @staticmethod
    def generate_source_target_remapping_sql(
        source_table_name: str,
        ordered_omop_columns: list[str],
        target_concept_id_column: str,
        source_concept_id_column: str,
        primary_key: str,
        site: str,
        bucket: str,
        delivery_date: str,
        vocab_version: str,
        vocab_path: str,
        output_path: str
    ) -> str:
        """
        Generate complete executable SQL for source-to-target remapping including COPY statement.

        This method generates the full SQL statement ready for execution, including:
        - CTE with placeholder replacement for file paths
        - COPY statement wrapping the CTE
        - Output path specification

        Args:
            source_table_name: Name of the source OMOP table
            ordered_omop_columns: List of column names in schema order
            target_concept_id_column: Name of the target concept_id column
            source_concept_id_column: Name of the source concept_id column
            primary_key: Name of the primary key column
            site: Site identifier for path resolution
            bucket: Storage bucket name
            delivery_date: Delivery date string
            vocab_version: Vocabulary version for path resolution
            vocab_path: Path to vocabulary files
            output_path: Full output path with storage scheme (e.g., 'gs://bucket/path/file.parquet')

        Returns:
            Complete executable SQL statement with COPY wrapped CTE
        """
        # Generate base SQL with placeholders
        initial_select_exprs: list = []
        final_select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            final_select_exprs.append(column_name)

            # Replace new target concept_id in target_concept_id_column
            if column_name == f"tbl.{target_concept_id_column}":
                column_name = f"vocab.target_concept_id AS {target_concept_id_column}"

            initial_select_exprs.append(column_name)

        # Add columns to store metadata related to vocab harmonization for later reporting
        metadata_columns = [
            "vocab.target_concept_id_domain AS target_domain",
            "'source_concept_id mapped to new target' AS vocab_harmonization_status",
            f"tbl.{source_concept_id_column} AS source_concept_id",
            f"tbl.{target_concept_id_column} AS previous_target_concept_id",
            "vocab.target_concept_id AS target_concept_id"
        ]
        for metadata_column in metadata_columns:
            initial_select_exprs.append(metadata_column)

            # Only include the alias in the second select statement
            alias = metadata_column.split(" AS ")[1]
            final_select_exprs.append(alias)

        initial_select_sql = ",\n                ".join(initial_select_exprs)

        initial_from_sql = f"""
                FROM read_parquet('@{source_table_name.upper()}') AS tbl
                INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                    ON tbl.{source_concept_id_column} = vocab.concept_id
                WHERE tbl.{source_concept_id_column} != 0
                AND tbl.{target_concept_id_column} != vocab.target_concept_id
                AND vocab.relationship_id IN ('Maps to', 'Maps to value')
                AND vocab.target_concept_id_standard = 'S'
            """

        pivot_cte = f"""
                -- Pivot so that Meas Value mappings get associated with target_concept_id_column
                SELECT
                    tbl.{primary_key},
                    MAX(vocab.target_concept_id) AS vh_value_as_concept_id
                FROM read_parquet('@{source_table_name.upper()}') AS tbl
                INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                    ON tbl.{source_concept_id_column} = vocab.concept_id
                WHERE vocab.target_concept_id_domain = 'Meas Value'
                GROUP BY tbl.{primary_key}
            """

        # Add column to final select that store Meas Value mapping
        final_select_exprs.append("mv_cte.vh_value_as_concept_id")

        # Add target table to final output
        case_when_target_table = f"""
                CASE
                    WHEN tbl.target_domain = 'Visit' THEN 'visit_occurrence'
                    WHEN tbl.target_domain = 'Condition' THEN 'condition_occurrence'
                    WHEN tbl.target_domain = 'Drug' THEN 'drug_exposure'
                    WHEN tbl.target_domain = 'Procedure' THEN 'procedure_occurrence'
                    WHEN tbl.target_domain = 'Device' THEN 'device_exposure'
                    WHEN tbl.target_domain = 'Measurement' THEN 'measurement'
                    WHEN tbl.target_domain = 'Observation' THEN 'observation'
                    WHEN tbl.target_domain = 'Note' THEN 'note'
                    WHEN tbl.target_domain = 'Specimen' THEN 'specimen'
                ELSE '{source_table_name}' END AS target_table
            """
        final_select_exprs.append(case_when_target_table)
        final_select_sql = ",\n                ".join(final_select_exprs)

        final_from_sql = f"""
                FROM base AS tbl
                LEFT JOIN meas_value AS mv_cte
                    ON tbl.{primary_key} = mv_cte.{primary_key}
                WHERE tbl.target_domain != 'Meas Value'
            """

        cte_with_placeholders = f"""
                WITH base AS (
                    SELECT
                        {initial_select_sql}
                    {initial_from_sql}
                ), meas_value AS (
                    {pivot_cte}
                )
                SELECT
                    {final_select_sql}
                {final_from_sql}
            """

        # Replace placeholders with actual file paths
        final_cte = utils.placeholder_to_file_path(
            site,
            bucket,
            delivery_date,
            cte_with_placeholders,
            vocab_version,
            vocab_path
        )

        # Wrap in COPY statement
        final_sql = f"""
            COPY (
                {final_cte}
            ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

        return final_sql

    @staticmethod
    def generate_check_new_targets_sql(
        source_table_name: str,
        ordered_omop_columns: list[str],
        target_concept_id_column: str,
        source_concept_id_column: str,
        primary_key_column: str,
        vocab_status_string: str,
        mapping_relationships: str,
        existing_files_where_clause: str,
        site: str,
        bucket: str,
        delivery_date: str,
        vocab_version: str,
        vocab_path: str,
        output_path: str
    ) -> str:
        """
        Generate complete executable SQL for checking new target mappings including COPY statement.

        This method generates the full SQL statement ready for execution, including:
        - CTE with placeholder replacement for file paths
        - COPY statement wrapping the CTE
        - Output path specification

        Args:
            source_table_name: Name of the source OMOP table
            ordered_omop_columns: List of column names in schema order
            target_concept_id_column: Name of the target concept_id column
            source_concept_id_column: Name/value of source concept_id column
            primary_key_column: Name of the primary key column
            vocab_status_string: Status message for harmonization
            mapping_relationships: SQL string of relationship types to check
            existing_files_where_clause: Optional WHERE clause to exclude already-harmonized rows
            site: Site identifier for path resolution
            bucket: Storage bucket name
            delivery_date: Delivery date string
            vocab_version: Vocabulary version for path resolution
            vocab_path: Path to vocabulary files
            output_path: Full output path with storage scheme (e.g., 'gs://bucket/path/file.parquet')

        Returns:
            Complete executable SQL statement with COPY wrapped CTE
        """
        # Generate base SQL with placeholders
        initial_select_exprs: list = []
        final_select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            final_select_exprs.append(column_name)

            # Replace new target concept_id in target_concept_id_column
            if column_name == f"tbl.{target_concept_id_column}":
                column_name = f"vocab.target_concept_id AS {target_concept_id_column}"

            # Set _source_concept_id value to previous target
            if column_name == f"{source_concept_id_column}":
                column_name = f"tbl.{target_concept_id_column} AS {source_concept_id_column.replace('tbl.','')}"

            initial_select_exprs.append(column_name)

        # Add columns to store metadata related to vocab harmonization for later reporting
        metadata_columns = [
            "vocab.target_concept_id_domain AS target_domain",
            f"'{vocab_status_string}' AS vocab_harmonization_status",
            f"{source_concept_id_column} AS source_concept_id",
            f"tbl.{target_concept_id_column} AS previous_target_concept_id",
            "vocab.target_concept_id AS target_concept_id"
        ]
        for metadata_column in metadata_columns:
            initial_select_exprs.append(metadata_column)

            # Only include the alias in the second select statement
            alias = metadata_column.split(" AS ")[1]
            final_select_exprs.append(alias)

        initial_select_sql = ",\n                ".join(initial_select_exprs)

        initial_from_sql = f"""
                FROM read_parquet('@{source_table_name.upper()}') AS tbl
                INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                    ON tbl.{target_concept_id_column} = vocab.concept_id
                WHERE tbl.{target_concept_id_column} != vocab.target_concept_id
                AND vocab.relationship_id IN ({mapping_relationships})
                AND vocab.target_concept_id_standard = 'S'
            """

        # Add existing files exclusion if provided
        if existing_files_where_clause:
            initial_from_sql = initial_from_sql + existing_files_where_clause

        pivot_cte = f"""
                -- Pivot so that Meas Value mappings get associated with target_concept_id_column
                SELECT
                    tbl.{primary_key_column},
                    MAX(vocab.target_concept_id) AS vh_value_as_concept_id
                FROM read_parquet('@{source_table_name.upper()}') AS tbl
                INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                    ON tbl.{target_concept_id_column} = vocab.concept_id
                WHERE vocab.target_concept_id_domain = 'Meas Value'
                GROUP BY tbl.{primary_key_column}
            """

        # Add column to final select that store Meas Value mapping
        final_select_exprs.append("mv_cte.vh_value_as_concept_id")

        # Add target table to final output
        case_when_target_table = f"""
                CASE
                    WHEN tbl.target_domain = 'Visit' THEN 'visit_occurrence'
                    WHEN tbl.target_domain = 'Condition' THEN 'condition_occurrence'
                    WHEN tbl.target_domain = 'Drug' THEN 'drug_exposure'
                    WHEN tbl.target_domain = 'Procedure' THEN 'procedure_occurrence'
                    WHEN tbl.target_domain = 'Device' THEN 'device_exposure'
                    WHEN tbl.target_domain = 'Measurement' THEN 'measurement'
                    WHEN tbl.target_domain = 'Observation' THEN 'observation'
                    WHEN tbl.target_domain = 'Note' THEN 'note'
                    WHEN tbl.target_domain = 'Specimen' THEN 'specimen'
                ELSE '{source_table_name}' END AS target_table
            """
        final_select_exprs.append(case_when_target_table)
        final_select_sql = ",\n                ".join(final_select_exprs)

        final_from_sql = f"""
                FROM base AS tbl
                LEFT JOIN meas_value AS mv_cte
                    ON tbl.{primary_key_column} = mv_cte.{primary_key_column}
                WHERE tbl.target_domain != 'Meas Value'
            """

        cte_with_placeholders = f"""
                WITH base AS (
                    SELECT
                        {initial_select_sql}
                    {initial_from_sql}
                ), meas_value AS (
                    {pivot_cte}
                )
                SELECT
                    {final_select_sql}
                {final_from_sql}
            """

        # Replace placeholders with actual file paths
        final_cte = utils.placeholder_to_file_path(
            site,
            bucket,
            delivery_date,
            cte_with_placeholders,
            vocab_version,
            vocab_path
        )

        # Wrap in COPY statement
        final_sql = f"""
            COPY (
                {final_cte}
            ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

        return final_sql

    @staticmethod
    def generate_domain_table_check_sql(
        source_table_name: str,
        ordered_omop_columns: list[str],
        target_concept_id_column: str,
        source_concept_id_column: str,
        existing_files_where_clause: str,
        site: str,
        bucket: str,
        delivery_date: str,
        vocab_version: str,
        vocab_path: str,
        output_path: str
    ) -> str:
        """
        Generate complete executable SQL for domain table check including COPY statement.

        This method generates the full SQL statement ready for execution, including:
        - CTE with placeholder replacement for file paths
        - COPY statement wrapping the CTE
        - Output path specification

        Args:
            source_table_name: Name of the source OMOP table
            ordered_omop_columns: List of column names in schema order
            target_concept_id_column: Full column reference (e.g., 'tbl.condition_concept_id')
            source_concept_id_column: Full column reference or value
            existing_files_where_clause: Optional WHERE clause to exclude already-harmonized rows
            site: Site identifier for path resolution
            bucket: Storage bucket name
            delivery_date: Delivery date string
            vocab_version: Vocabulary version for path resolution
            vocab_path: Path to vocabulary files
            output_path: Full output path with storage scheme (e.g., 'gs://bucket/path/file.parquet')

        Returns:
            Complete executable SQL statement with COPY wrapped CTE
        """
        # Generate base SQL with placeholders
        select_exprs: list = []

        for column_name in ordered_omop_columns:
            column_name = f"tbl.{column_name}"
            select_exprs.append(column_name)

        # Add columns to store metadata related to vocab harmonization for later reporting
        metadata_columns = [
            "vocab.concept_id_domain AS target_domain",
            "'domain check' AS vocab_harmonization_status",
            f"{source_concept_id_column} AS source_concept_id",
            f"{target_concept_id_column} AS previous_target_concept_id",
            f"{target_concept_id_column} AS target_concept_id"
        ]
        for metadata_column in metadata_columns:
            select_exprs.append(metadata_column)

        # Add vh_value_as_concept_id field to keep structure consistent with remapped tables
        select_exprs.append("CAST(NULL AS BIGINT) AS vh_value_as_concept_id")

        # Add target table statement
        case_when_target_table = f"""
                CASE
                    WHEN vocab.concept_id_domain = 'Visit' THEN 'visit_occurrence'
                    WHEN vocab.concept_id_domain = 'Condition' THEN 'condition_occurrence'
                    WHEN vocab.concept_id_domain = 'Drug' THEN 'drug_exposure'
                    WHEN vocab.concept_id_domain = 'Procedure' THEN 'procedure_occurrence'
                    WHEN vocab.concept_id_domain = 'Device' THEN 'device_exposure'
                    WHEN vocab.concept_id_domain = 'Measurement' THEN 'measurement'
                    WHEN vocab.concept_id_domain = 'Observation' THEN 'observation'
                    WHEN vocab.concept_id_domain = 'Note' THEN 'note'
                    WHEN vocab.concept_id_domain = 'Specimen' THEN 'specimen'
                ELSE '{source_table_name}' END AS target_table
        """
        select_exprs.append(case_when_target_table)

        select_sql = ",\n                ".join(select_exprs)

        from_sql = f"""
                FROM read_parquet('@{source_table_name.upper()}') AS tbl
                INNER JOIN vocab
                    ON {target_concept_id_column} = vocab.concept_id
                """

        inner_sql = f"""
                    WITH vocab AS (
                        SELECT DISTINCT
                            concept_id,
                            concept_id_domain
                        FROM read_parquet('@OPTIMIZED_VOCABULARY')
                    )
                    SELECT {select_sql}
                    {from_sql}
                    {existing_files_where_clause}
                """

        # Wrap in COPY statement with output path
        sql_statement = f"""
            COPY (
                {inner_sql}
            ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

        # Replace placeholders with actual file paths
        final_sql_statement = utils.placeholder_to_file_path(
            site,
            bucket,
            delivery_date,
            sql_statement,
            vocab_version,
            vocab_path
        )

        return final_sql_statement

    @staticmethod
    def generate_consolidate_single_table_sql(
        source_parquet_pattern: str,
        output_path: str
    ) -> str:
        """
        Generate SQL to consolidate multiple parquet files into a single file.

        This method generates a simple COPY statement that reads from multiple
        parquet files matching a pattern and writes them to a single output file.

        Args:
            source_parquet_pattern: Glob pattern for source parquet files
                                   (e.g., 'gs://bucket/path/parts/*.parquet')
            output_path: Full output path with storage scheme
                        (e.g., 'gs://bucket/path/table.parquet')

        Returns:
            Complete executable SQL COPY statement
        """
        return f"""
            COPY (
                SELECT * FROM read_parquet('{source_parquet_pattern}')
            ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def generate_get_target_tables_sql(parquet_path: str) -> str:
        """
        Generate SQL to get distinct target tables from harmonized parquet files.

        This method generates a query that reads harmonized parquet files and
        returns a list of distinct target tables that data should be ETL'd into.

        Args:
            parquet_path: Path or glob pattern to harmonized parquet files
                         (e.g., 'gs://bucket/path/harmonized/*.parquet')

        Returns:
            SQL SELECT statement that returns distinct target_table values
        """
        return f"""
            SELECT DISTINCT target_table FROM read_parquet('{parquet_path}')
        """

    @staticmethod
    def generate_table_transition_count_sql(parquet_path: str) -> str:
        """
        Generate SQL to count rows by target table from harmonized parquet files.

        This method generates a query that reads harmonized parquet files and
        counts how many rows transition to each target table, used for reporting.

        Args:
            parquet_path: Path or glob pattern to harmonized parquet files
                         (e.g., 'gs://bucket/path/harmonized/*.parquet')

        Returns:
            SQL SELECT statement that returns target_table and row_count
        """
        return f"""
            SELECT
                target_table,
                COUNT(*) as row_count
            FROM read_parquet('{parquet_path}')
            GROUP BY target_table
            ORDER BY target_table
        """

    @staticmethod
    def generate_vocab_status_count_sql(parquet_path: str) -> str:
        """
        Generate SQL to count rows by vocab harmonization status from harmonized parquet files.

        This method generates a query that reads harmonized parquet files and
        counts how many rows have each type of vocab harmonization status, used for reporting.

        Args:
            parquet_path: Path or glob pattern to harmonized parquet files
                         (e.g., 'gs://bucket/path/harmonized/*.parquet')

        Returns:
            SQL SELECT statement that returns vocab_harmonization_status and row_count
        """
        return f"""
            SELECT
                vocab_harmonization_status,
                COUNT(*) as row_count
            FROM read_parquet('{parquet_path}')
            GROUP BY vocab_harmonization_status
            ORDER BY vocab_harmonization_status
        """

    @staticmethod
    def generate_check_duplicates_sql(file_path: str, primary_key_column: str) -> str:
        """
        Generate SQL to check for duplicate primary keys in a parquet file.

        Args:
            file_path: Full path to the parquet file
            primary_key_column: Name of the primary key column

        Returns:
            SQL that groups by primary key and returns any keys with count > 1
        """
        return f"""
            SELECT
                {primary_key_column},
                COUNT(*) as dup_count
            FROM read_parquet('{file_path}')
            GROUP BY {primary_key_column}
            HAVING COUNT(*) > 1
            LIMIT 1
        """

    @staticmethod
    def generate_create_duplicate_keys_table_sql(file_path: str, primary_key_column: str) -> str:
        """
        Generate SQL to create a temp table containing all duplicate primary keys.

        Args:
            file_path: Full path to the parquet file
            primary_key_column: Name of the primary key column

        Returns:
            SQL that creates a temp table named 'duplicate_keys' with all duplicate keys
        """
        return f"""
            CREATE TEMP TABLE duplicate_keys AS
            SELECT {primary_key_column}
            FROM read_parquet('{file_path}')
            GROUP BY {primary_key_column}
            HAVING COUNT(*) > 1
        """

    @staticmethod
    def generate_count_duplicates_sql() -> str:
        """
        Generate SQL to count the number of duplicate keys in the temp table.

        Returns:
            SQL that counts rows in the duplicate_keys temp table
        """
        return "SELECT COUNT(*) FROM duplicate_keys"

    @staticmethod
    def generate_write_non_duplicates_sql(
        file_path: str,
        primary_key_column: str,
        tmp_output_path: str
    ) -> str:
        """
        Generate SQL to write all non-duplicate rows to a temporary file.

        Args:
            file_path: Full path to the source parquet file
            primary_key_column: Name of the primary key column
            tmp_output_path: Path where non-duplicate rows should be written

        Returns:
            SQL COPY statement that writes non-duplicate rows to temp file
        """
        return f"""
            COPY (
                SELECT *
                FROM read_parquet('{file_path}')
                WHERE {primary_key_column} NOT IN (SELECT {primary_key_column} FROM duplicate_keys)
            ) TO '{tmp_output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def generate_fix_duplicates_sql(
        file_path: str,
        primary_key_column: str,
        primary_key_type: str,
        tmp_output_path: str
    ) -> str:
        """
        Generate SQL to fix duplicate primary keys using hash-based key generation.

        For rows with duplicate keys:
        - First occurrence keeps original key
        - Subsequent occurrences get new hash-based keys

        Args:
            file_path: Full path to the source parquet file
            primary_key_column: Name of the primary key column
            primary_key_type: Data type of the primary key (e.g., 'BIGINT', 'INTEGER')
            tmp_output_path: Path where fixed duplicate rows should be written

        Returns:
            SQL COPY statement that writes duplicate rows with regenerated keys
        """
        return f"""
            COPY (
                SELECT
                    CASE
                        WHEN row_num = 1 THEN {primary_key_column}
                        ELSE CAST(hash(CONCAT(CAST({primary_key_column} AS VARCHAR), CAST(row_num AS VARCHAR))) % 9223372036854775807 AS {primary_key_type})
                    END AS {primary_key_column},
                    * EXCLUDE ({primary_key_column}, row_num)
                FROM (
                    SELECT *,
                        ROW_NUMBER() OVER (PARTITION BY {primary_key_column} ORDER BY (SELECT 1)) as row_num
                    FROM read_parquet('{file_path}')
                    WHERE {primary_key_column} IN (SELECT {primary_key_column} FROM duplicate_keys)
                )
            ) TO '{tmp_output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def generate_merge_deduplicated_sql(
        tmp_non_dup_path: str,
        tmp_dup_fixed_path: str,
        output_path: str
    ) -> str:
        """
        Generate SQL to merge non-duplicate and fixed duplicate rows into final output.

        Args:
            tmp_non_dup_path: Path to temp file with non-duplicate rows
            tmp_dup_fixed_path: Path to temp file with fixed duplicate rows
            output_path: Final output path (overwrites original file)

        Returns:
            SQL COPY statement that combines both temp files with UNION ALL
        """
        return f"""
            COPY (
                SELECT * FROM read_parquet('{tmp_non_dup_path}')
                UNION ALL
                SELECT * FROM read_parquet('{tmp_dup_fixed_path}')
            ) TO '{output_path}' {constants.DUCKDB_FORMAT_STRING}
        """

