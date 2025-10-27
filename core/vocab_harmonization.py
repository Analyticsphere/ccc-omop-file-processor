import logging
import sys

import core.constants as constants
import core.gcp_services as gcp_services
import core.helpers.report_artifact as report_artifact
import core.transformer as transformer
import core.utils as utils


class VocabHarmonizer:
    """
    A class for harmonizing OMOP parquet files according to specified vocabulary version.
    Handles the entire process from reading parquet files to generating SQL and saving the harmonized output to BQ.
    """
    
    def __init__(self, file_path: str, cdm_version: str, site: str, vocab_version: str, vocab_gcs_bucket: str, project_id: str, dataset_id):
        """
        Initialize a VocabHarmonizer with common parameters needed across all operations.
        """
        self.file_path = file_path
        self.cdm_version = cdm_version
        self.site = site
        self.vocab_version = vocab_version
        self.vocab_gcs_bucket = vocab_gcs_bucket
        self.source_table_name = utils.get_table_name_from_gcs_path(file_path)
        self.bucket = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)[0]
        self.delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)[1]
        self.source_parquet_path = utils.get_parquet_artifact_location(file_path)
        self.target_parquet_path = utils.get_parquet_harmonized_path(file_path)
        self.project_id = project_id
        self.dataset_id = dataset_id

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[logging.StreamHandler(sys.stdout)]
        )
        # Create the logger at module level so its settings are applied throughout class
        self.logger = logging.getLogger(__name__)


    def perform_harmonization(self, step: str) -> None:
        """
        Perform a specific harmonization step.
        """
        self.logger.info(f"Performing vocabulary harmonization against {self.file_path}: {step}")

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
            self.consolidate_and_deduplicate_etl_tables()
        else:
            raise Exception(f"Unknown harmonization step {step}")

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
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
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
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
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
            ELSE '{self.source_table_name}' END AS target_table
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

        final_cte = utils.placeholder_to_file_path(
            self.site, 
            self.bucket, 
            self.delivery_date, 
            cte_with_placeholders, 
            self.vocab_version, 
            self.vocab_gcs_bucket
        )

        final_sql = f"""
            COPY (
                {final_cte}
            ) TO 'gs://{self.target_parquet_path}{self.source_table_name}_source_target_remap{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        """

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
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
            INNER JOIN read_parquet('@OPTIMIZED_VOCABULARY') AS vocab
                ON tbl.{target_concept_id_column} = vocab.concept_id
            WHERE tbl.{target_concept_id_column} != vocab.target_concept_id
            AND vocab.relationship_id IN ({mapping_relationships})
            AND vocab.target_concept_id_standard = 'S' 
            --AND tbl.{source_concept_id_column} = 0
        """

        # Don't perform target remapping on rows which have already been harominzed
        # primary_key_column values were made unique per row values in normalization step, 
        #   so they can be used for identification here
        exisiting_files = utils.valid_parquet_file(f'{self.target_parquet_path}*{constants.PARQUET}')
        if exisiting_files:
            where_sql = f"""
                AND tbl.{primary_key_column} NOT IN (
                    SELECT {primary_key_column} FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                )
            """
            initial_from_sql = initial_from_sql + where_sql

        pivot_cte = f"""
            -- Pivot so that Meas Value mappings get associated with target_concept_id_column
            SELECT 
                tbl.{primary_key_column},
                MAX(vocab.target_concept_id) AS vh_value_as_concept_id
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
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
            ELSE '{self.source_table_name}' END AS target_table
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

        final_cte = utils.placeholder_to_file_path(
            self.site, 
            self.bucket, 
            self.delivery_date, 
            cte_with_placeholders, 
            self.vocab_version, 
            self.vocab_gcs_bucket
        )

        final_sql = f"""
            COPY (
                {final_cte}
            ) TO 'gs://{self.target_parquet_path}{self.source_table_name}_{table_name}{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        """

        utils.execute_duckdb_sql(final_sql, f"Unable to execute SQL to check for new targets ({mapping_type}) {self.source_table_name}")

    def domain_table_check(self) -> None:
        # The domain of a concept_id may change between different vocabulary versions
        # Sites may also ETL data into an OMOP table that doesn't align with its domain
        # Add current domain_id and appropriate target table for all concepts which weren't remapped 
        schema = utils.get_table_schema(self.source_table_name, self.cdm_version)

        columns = schema[self.source_table_name]["columns"]
        ordered_omop_columns = list(columns.keys())  # preserve column order
        target_concept_id_column = f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['target_concept_id']}"
        source_concept_id_column = '0' if constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id'] == "" \
            else f"tbl.{constants.SOURCE_TARGET_COLUMNS[self.source_table_name]['source_concept_id']}"
        primary_key_column = utils.get_primary_key_column(self.source_table_name, self.cdm_version)

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
            ELSE '{self.source_table_name}' END AS target_table
        """
        select_exprs.append(case_when_target_table)

        select_sql = ",\n                ".join(select_exprs)

        from_sql = f"""
            FROM read_parquet('@{self.source_table_name.upper()}') AS tbl
            INNER JOIN vocab
                ON {target_concept_id_column} = vocab.concept_id
        """

        # Don't perform domain check on rows which have already been harominzed
        # primary_key_column values were made unique per row values in normalization step, 
        #   so they can be used for identification here
        exisiting_files = utils.valid_parquet_file(f'{self.target_parquet_path}*{constants.PARQUET}')
        where_sql = ""
        if exisiting_files:
            where_sql = f"""
                WHERE tbl.{primary_key_column} NOT IN (
                    SELECT {primary_key_column} FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                )
            """

        # Create vocab CTE with distinct concept_id and domain_id values
        # Without the CTE, duplicates will occur when 1 concept_id is mapped to more than 1 target
        sql_statement = f"""
            COPY (
                WITH vocab AS (
                    SELECT DISTINCT
                        concept_id,
                        concept_id_domain
                    FROM read_parquet('@OPTIMIZED_VOCABULARY')
                )
                SELECT {select_sql}
                {from_sql}
                {where_sql}
            ) TO 'gs://{self.target_parquet_path}{self.source_table_name}_domain_check{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        """

        final_sql_statement = utils.placeholder_to_file_path(
            self.site,
            self.bucket,
            self.delivery_date,
            sql_statement,
            self.vocab_version,
            self.vocab_gcs_bucket
        )

        utils.execute_duckdb_sql(final_sql_statement, f"Unable to perform domain check against {self.source_table_name}")

    def generate_table_transition_report(self, conn) -> None:
        """
        Generate report artifacts showing how many rows transitioned from the source table 
        to each target table during vocabulary harmonization.
        
        Args:
            conn: Active DuckDB connection to use for queries
        """
        try:
            self.logger.info(f"Generating table transition report for {self.source_table_name}")
            
            # Get the source table concept_id from the schema
            schema = utils.get_cdm_schema(cdm_version=self.cdm_version)
            source_table_concept_id = schema.get(self.source_table_name, {}).get('concept_id')
            
            # Query to count rows by target table
            count_query = f"""
                SELECT 
                    target_table,
                    COUNT(*) as row_count
                FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                GROUP BY target_table
                ORDER BY target_table
            """
            
            results = conn.execute(count_query).fetchall()
            
            # Create a report artifact for each target table
            for target_table, row_count in results:
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
                self.logger.info(f"Table transition: {self.source_table_name} → {target_table}: {row_count} rows")
                
        except Exception as e:
            # Log the error but don't fail the entire process
            self.logger.error(f"Error generating table transition report: {str(e)}")

    def omop_etl(self) -> None:
        self.logger.info(f"Partitioning and ETLing source file {self.file_path} to appropriate target table(s)")

        # Find all target tables in the source file
        # Each of the target tables will be transformed to its own Parquet file with the appropriate structure
        # That Parquet file will then be loaded to BQ
        conn, local_db_file = utils.create_duckdb_connection()
        try:
            with conn:
                target_tables = f"""
                    SELECT DISTINCT target_table FROM read_parquet('gs://{self.target_parquet_path}*{constants.PARQUET}')
                """
                        
                target_tables_list = conn.execute(target_tables).fetch_df()['target_table'].tolist()
                
                # Generate table transition report before transformation
                self.generate_table_transition_report(conn)
        except Exception as e:
            raise Exception(f"Unable to get target tables from Parquet file {self.file_path}: {e}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

        # Create a new Parquet file for each target table with the appropriate structure
        for target_table in target_tables_list:
            omop_transformer = transformer.Transformer(
                self.site, self.target_parquet_path, self.cdm_version, self.source_table_name, target_table, utils.get_omop_etl_destination_path(self.file_path)
            )

            # Generate the transformed file
            omop_transformer.omop_to_omop_etl()

            self.logger.info(f"Loading harmonized data from {self.file_path} to {target_table}...")
            #Load the file to BQ; ETLed_FILE write type ensures append only
            gcp_services.load_parquet_to_bigquery(
                file_path=f"gs://{omop_transformer.get_transformed_path()}",
                project_id=self.project_id,
                dataset_id=self.dataset_id,
                table_name=target_table,
                write_type=constants.BQWriteTypes.ETLed_FILE
            )

    def consolidate_and_deduplicate_etl_tables(self) -> None:
        """
        Orchestrates consolidation and deduplication across all ETL'd tables.
        
        Discovers all table subdirectories in the OMOP_ETL artifacts directory
        and consolidates each one by delegating to the single-table processing method.
        """
        self.logger.info(f"Consolidating ETL files for {self.file_path}")
        
        # Get the OMOP ETL directory path
        etl_base_path = utils.get_omop_etl_destination_path(self.file_path)
        bucket_name, directory_path = utils.get_bucket_and_delivery_date_from_gcs_path(etl_base_path)
        etl_folder = f"{directory_path}/{constants.ArtifactPaths.OMOP_ETL.value}"
        gcs_path = f"gs://{bucket_name}/{etl_folder}"
        
        self.logger.info(f"Looking for table directories in {gcs_path}")
        
        # Get list of table subdirectories using existing utility
        subdirectories = gcp_services.list_gcs_subdirectories(gcs_path)
        
        # Extract just the table names from the full paths
        table_names = [subdir.rstrip('/').split('/')[-1] for subdir in subdirectories]
        
        if not table_names:
            self.logger.warning(f"No table directories found in {gcs_path}")
            return
        
        self.logger.info(f"Found {len(table_names)} table(s) to consolidate: {sorted(table_names)}")
        
        # Process each table directory
        for table_name in sorted(table_names):
            self._process_single_table(bucket_name, etl_folder, table_name)

    def _process_single_table(self, bucket_name: str, etl_folder: str, table_name: str) -> None:
        """
        Combine all parquet files for a single table into one consolidated file,
        then deduplicate primary keys.
        
        Args:
            bucket_name: GCS bucket name
            etl_folder: Path to the ETL folder within the bucket
            table_name: Name of the OMOP table to consolidate
        """
        self.logger.info(f"Consolidating files for table: {table_name}")
        
        # Construct paths
        table_dir = f"{etl_folder}{table_name}/"
        parquet_pattern = f"gs://{bucket_name}/{table_dir}*.parquet"
        consolidated_file_path = f"gs://{bucket_name}/{table_dir}{table_name}{constants.PARQUET}"
        
        # Check if there are any files to consolidate
        if not utils.valid_parquet_file(parquet_pattern):
            self.logger.warning(f"No valid parquet files found for {table_name} at {parquet_pattern}")
            return
        
        # Combine all parquet files into one
        conn, local_db_file = utils.create_duckdb_connection()
        
        try:
            with conn:
                combine_sql = f"""
                    COPY (
                        SELECT * FROM read_parquet('{parquet_pattern}')
                    ) TO '{consolidated_file_path}' {constants.DUCKDB_FORMAT_STRING}
                """
                conn.execute(combine_sql)
                self.logger.info(f"Successfully consolidated {table_name}")
                
        except Exception as e:
            raise Exception(f"Unable to consolidate files for table {table_name}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)
        
        # Now deduplicate primary keys in the consolidated file
        self._deduplicate_primary_keys(consolidated_file_path, table_name)

    def _deduplicate_primary_keys(self, file_path: str, table_name: str) -> None:
        """
        Check for and fix duplicate primary keys in a consolidated parquet file.
        Only applies to tables with surrogate keys.
        
        Args:
            file_path: Path to the consolidated parquet file
            table_name: Name of the OMOP table
        """
        # Only process tables with surrogate keys
        if table_name not in constants.SURROGATE_KEY_TABLES:
            self.logger.info(f"Table {table_name} does not use surrogate keys. Skipping deduplication.")
            return
        
        # Get primary key column
        primary_key_column = utils.get_primary_key_column(table_name, self.cdm_version)
        if not primary_key_column:
            self.logger.info(f"No primary key column defined for table {table_name}. Skipping deduplication.")
            return
        
        # Get primary key data type
        schema = utils.get_table_schema(table_name, self.cdm_version)
        if table_name not in schema:
            self.logger.warning(f"Schema not found for table {table_name}. Skipping deduplication.")
            return
        
        target_columns_dict = schema[table_name]["columns"]
        if primary_key_column not in target_columns_dict:
            self.logger.warning(f"Primary key column {primary_key_column} not found in schema. Skipping deduplication.")
            return
        
        primary_key_type = target_columns_dict[primary_key_column]['type']
        
        conn, local_db_file = utils.create_duckdb_connection()
        
        try:
            with conn:
                # Check if duplicates exist
                self.logger.info(f"Checking for duplicate primary keys in {table_name}...")
                check_sql = f"""
                    SELECT 
                        {primary_key_column}, 
                        COUNT(*) as dup_count
                    FROM read_parquet('{file_path}')
                    GROUP BY {primary_key_column}
                    HAVING COUNT(*) > 1
                    LIMIT 1
                """
                duplicates = conn.execute(check_sql).fetchall()
                
                if not duplicates:
                    self.logger.info(f"No duplicate primary keys found in {table_name}")
                    return
                
                self.logger.info(f"Found duplicate primary keys in {table_name}. Fixing...")
                
                # Create temp table with duplicate keys only
                conn.execute(f"""
                    CREATE TEMP TABLE duplicate_keys AS
                    SELECT {primary_key_column}
                    FROM read_parquet('{file_path}')
                    GROUP BY {primary_key_column}
                    HAVING COUNT(*) > 1
                """)
                
                dup_count = conn.execute("SELECT COUNT(*) FROM duplicate_keys").fetchone()[0]
                self.logger.info(f"Found {dup_count} unique keys with duplicates")
                
                # Generate temp file paths
                import uuid
                temp_id = uuid.uuid4()
                bucket_path = file_path.rsplit('/', 1)[0]
                temp_non_dup = f"{bucket_path}/temp_non_dup_{temp_id}.parquet"
                temp_dup_fixed = f"{bucket_path}/temp_dup_fixed_{temp_id}.parquet"
                
                # Pass 1: Write non-duplicate rows
                self.logger.info(f"Processing non-duplicate rows...")
                conn.execute(f"""
                    COPY (
                        SELECT *
                        FROM read_parquet('{file_path}')
                        WHERE {primary_key_column} NOT IN (SELECT {primary_key_column} FROM duplicate_keys)
                    ) TO '{temp_non_dup}' {constants.DUCKDB_FORMAT_STRING}
                """)
                
                # Pass 2: Fix duplicate rows
                self.logger.info(f"Processing duplicate rows with fixes...")
                conn.execute(f"""
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
                    ) TO '{temp_dup_fixed}' {constants.DUCKDB_FORMAT_STRING}
                """)
                
                # Combine both temp files and overwrite original
                self.logger.info(f"Merging non-duplicate and fixed duplicate rows...")
                conn.execute(f"""
                    COPY (
                        SELECT * FROM read_parquet('{temp_non_dup}')
                        UNION ALL
                        SELECT * FROM read_parquet('{temp_dup_fixed}')
                    ) TO '{file_path}' {constants.DUCKDB_FORMAT_STRING}
                """)
                
                self.logger.info(f"Successfully deduplicated primary keys in {table_name}")
                
        except Exception as e:
            raise Exception(f"Unable to deduplicate primary keys for table {table_name}: {str(e)}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)
