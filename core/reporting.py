import os
from datetime import datetime
from typing import Any

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


class ReportGenerator:
    """
    Generates delivery report CSV files for OMOP data processing.

    Consolidates temporary report artifacts into a final CSV file that serves
    as a data source for visual reporting and dashboards.
    """

    def __init__(self, report_data: dict):
        """
        Initialize report generator with delivery metadata.

        Args:
            report_data: Dictionary containing delivery metadata:
            site: Site identifier
            bucket: Storage bucket for site data
            delivery_date: Delivery date string (YYYY-MM-DD)
            site_display_name: Human-readable site name
            file_delivery_format: Format of delivered files (e.g., .csv, .parquet)
            delivered_cdm_version: OMOP CDM version delivered by site
            target_vocabulary_version: Target vocabulary version
            target_cdm_version: Target OMOP CDM version
        """
        self.site = report_data["site"]
        self.bucket = report_data["bucket"]
        self.delivery_date = report_data["delivery_date"]
        self.site_display_name = report_data["site_display_name"]
        self.file_delivery_format = report_data["file_delivery_format"]
        self.delivered_cdm_version = report_data["delivered_cdm_version"]
        self.target_vocabulary_version = report_data["target_vocabulary_version"]
        self.target_cdm_version = report_data["target_cdm_version"]
        self.tmp_artifacts_path = self._get_tmp_artifacts_path()
        self.output_path = self._get_output_path()

    def generate(self) -> None:
        """
        Generate complete delivery report CSV file.

        Creates report artifacts with metadata and consolidates
        temporary report files into final CSV for downstream reporting.
        """
        # Generate additional reporting artifacts
        self._create_metadata_artifacts()
        self._create_type_concept_breakdown_artifacts()
        self._create_vocabulary_breakdown_artifacts()
        self._create_date_datetime_default_value_artifacts()
        self._create_invalid_concept_id_artifacts()
        self._create_person_id_referential_integrity_artifacts()
        self._create_final_row_count_artifacts()
        self._create_time_series_row_count_artifacts()

        # Generate the final, single report CSV file
        self._consolidate_report_files()

    def _create_metadata_artifacts(self) -> None:
        """Create report artifacts documenting delivery and processing metadata."""

        utils.logger.info("Creating metadata report artifacts")
        # Get delivered vocabulary version
        delivered_vocab_version = utils.get_delivery_vocabulary_version(
            self.bucket,
            self.delivery_date
        )

        # Define metadata to include in report
        metadata_items = [
            (datetime.today().strftime('%Y-%m-%d'), constants.PROCESSED_DATE_REPORT_NAME),
            (os.getenv('COMMIT_SHA'), constants.FILE_PROCESSOR_VERSION_REPORT_NAME),
            (self.delivery_date, constants.DELIVERY_DATE_REPORT_NAME),
            (self.site_display_name, constants.SITE_DISPLAY_NAME_REPORT_NAME),
            (self.file_delivery_format, constants.FILE_DELIVERY_FORMAT_REPORT_NAME),
            (self.delivered_cdm_version, constants.DELIVERED_CDM_VERSION_REPORT_NAME),
            (delivered_vocab_version, constants.DELIVERED_VOCABULARY_VERSION_REPORT_NAME),
            (self.target_vocabulary_version, constants.TARGET_VOCABULARY_VERSION_REPORT_NAME),
            (self.target_cdm_version, constants.TARGET_CDM_VERSION_REPORT_NAME)
        ]

        # Create report artifact for each metadata item
        for value, item_name in metadata_items:
            value_as_concept_id = 0

            # Convert CDM versions to concept IDs
            if item_name in [constants.DELIVERED_CDM_VERSION_REPORT_NAME,
                            constants.TARGET_CDM_VERSION_REPORT_NAME]:
                value_as_concept_id = utils.get_cdm_version_concept_id(value)

            artifact = report_artifact.ReportArtifact(
                delivery_date=self.delivery_date,
                artifact_bucket=self.bucket,
                concept_id=0,
                name=item_name,
                value_as_string=value,
                value_as_concept_id=value_as_concept_id,
                value_as_number=None
            )
            artifact.save_artifact()

            utils.logger.info("Created metadata artifacts")

    def _consolidate_report_files(self) -> None:
        """
        Consolidate temporary report files into final CSV file.

        Discovers all temporary report parquet files and combines them into a single
        CSV file that serves as a data source for visual reporting and dashboards.
        """
        # Find all temporary report files
        report_tmp_dir = f"{self.delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
        tmp_files = utils.list_files(self.bucket, report_tmp_dir, constants.PARQUET)

        if len(tmp_files) == 0:
            return

        # Build UNION ALL query to combine all report files
        file_paths = [
            storage.get_uri(f"{self.bucket}/{report_tmp_dir}{file}")
            for file in tmp_files
        ]
        select_statement = " UNION ALL ".join([
            f"SELECT * FROM read_parquet('{path}')"
            for path in file_paths
        ])

        # Generate and execute consolidation SQL
        sql = self.generate_report_consolidation_sql(select_statement, self.output_path)
        utils.execute_duckdb_sql(sql, "Unable to merge reporting artifacts")

    def _get_tmp_artifacts_path(self) -> str:
        """Get path to temporary report artifacts directory."""
        return utils.get_report_tmp_artifacts_path(self.bucket, self.delivery_date)

    def _get_output_path(self) -> str:
        """Get full URI path for final report CSV output."""
        return storage.get_uri(
            f"{self.bucket}/{self.delivery_date}/{constants.ArtifactPaths.REPORT.value}"
            f"delivery_report_{self.site}_{self.delivery_date}{constants.CSV}"
        )

    def _get_table_path(self, table_name: str, location: constants.ArtifactPaths) -> str:
        """
        Construct full storage URI path to a table's Parquet file based on its location.

        Args:
            table_name: Name of the OMOP table
            location: ArtifactPaths enum value indicating where the file is stored

        Returns:
            Full storage URI to the table's Parquet file
        """
        if location == constants.ArtifactPaths.OMOP_ETL:
            # OMOP ETL tables are stored in subdirectories
            return utils.get_omop_etl_table_path(self.bucket, self.delivery_date, table_name)
        else:
            # Converted and derived files are stored directly in their directories
            path = f"{self.bucket}/{self.delivery_date}/{location.value}{table_name}{constants.PARQUET}"
            return storage.get_uri(path)

    def _create_type_concept_breakdown_artifacts(self) -> None:
        """
        Create report artifacts for type_concept_id breakdowns across all OMOP tables.

        For each table with a type_concept_id field, queries the breakdown of type concepts
        and their counts, joining with the concept vocabulary table to get concept names.
        Creates one report artifact per table-type_concept pair.
        """
        utils.logger.info("Creating type concept breakdown report artifacts")

        # Get target vocabulary version for this delivery
        target_vocab_version = self.target_vocabulary_version

        # Construct path to concept table in vocabulary
        concept_path = storage.get_uri(f"{constants.VOCAB_PATH}/{target_vocab_version}/optimized/concept{constants.PARQUET}")

        # Check if concept table exists
        if not utils.parquet_file_exists(concept_path):
            utils.logger.warning(f"Concept table not found at {concept_path}, skipping type concept breakdown")
            return

        # Process each table with type_concept_id field
        for table_name, table_config_obj in constants.REPORTING_TABLE_CONFIG.items():
            table_config: Any = table_config_obj
            type_field = table_config["type_field"]

            # Skip tables without type_concept_id field
            if type_field is None:
                continue

            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping type concept breakdown")
                continue

            # Generate and execute SQL query for type_concept_id breakdown
            sql = self.generate_type_concept_breakdown_sql(table_path, concept_path, type_field)

            try:
                result = utils.execute_duckdb_sql(sql, f"Unable to query type concept breakdown for {table_name}", return_results=True)

                # Create report artifact for each type_concept_id
                for row in result:
                    type_concept_id, concept_name, record_count = row

                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=type_concept_id,
                        name=f"Type concept breakdown: {table_name}",
                        value_as_string=concept_name,
                        value_as_concept_id=type_concept_id,
                        value_as_number=float(record_count)
                    )
                    artifact.save_artifact()

            except Exception as e:
                utils.logger.error(f"Error processing type concept breakdown for {table_name}: {e}")
                continue
        
        utils.logger.info("Created type concept breakdown report artifacts")

    def _create_vocabulary_breakdown_artifacts(self) -> None:
        """
        Create report artifacts for vocabulary breakdowns across concept fields.

        For each table with concept_id fields, queries the breakdown of vocabularies
        for both source and target concepts, joining with the concept vocabulary table
        to get vocabulary names. Creates report artifacts for each table-field-vocabulary combination.
        """
        utils.logger.info("Creating vocabulary breakdown report artifacts")

        # Get target vocabulary version for this delivery
        target_vocab_version = self.target_vocabulary_version

        # Construct path to concept table in vocabulary
        concept_path = storage.get_uri(f"{constants.VOCAB_PATH}/{target_vocab_version}/optimized/concept{constants.PARQUET}")

        # Check if concept table exists
        if not utils.parquet_file_exists(concept_path):
            utils.logger.warning(f"Concept table not found at {concept_path}, skipping vocabulary breakdown")
            return

        # Process each table with vocabulary concept fields
        for table_name, table_config_obj in constants.REPORTING_TABLE_CONFIG.items():
            table_config: Any = table_config_obj
            vocabulary_fields = table_config["vocabulary_fields"]

            # Skip tables without vocabulary fields
            if not vocabulary_fields:
                continue

            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping vocabulary breakdown")
                continue

            # Process each concept field in the table
            for field_config in vocabulary_fields:
                concept_id_field = field_config["concept_id"]
                source_concept_id_field = field_config["source_concept_id"]

                # Process target vocabulary (from concept_id field)
                try:
                    sql = self.generate_vocabulary_breakdown_sql(
                        table_path,
                        concept_path,
                        concept_id_field,
                        is_source=False
                    )
                    result = utils.execute_duckdb_sql(
                        sql,
                        f"Unable to query target vocabulary breakdown for {table_name}.{concept_id_field}",
                        return_results=True
                    )

                    # Create report artifact for each vocabulary
                    for row in result:
                        vocabulary_id, record_count = row

                        artifact = report_artifact.ReportArtifact(
                            delivery_date=self.delivery_date,
                            artifact_bucket=self.bucket,
                            concept_id=0,
                            name=f"Target vocabulary breakdown: {table_name}.{concept_id_field}",
                            value_as_string=vocabulary_id,
                            value_as_concept_id=0,
                            value_as_number=float(record_count)
                        )
                        artifact.save_artifact()

                except Exception as e:
                    utils.logger.error(f"Error processing target vocabulary breakdown for {table_name}.{concept_id_field}: {e}")

                # Process source vocabulary (from source_concept_id field)
                if source_concept_id_field is None:
                    # Create a single artifact indicating source not captured
                    try:
                        artifact = report_artifact.ReportArtifact(
                            delivery_date=self.delivery_date,
                            artifact_bucket=self.bucket,
                            concept_id=0,
                            name=f"Source vocabulary breakdown: {table_name}.{concept_id_field}",
                            value_as_string="Source vocabulary not captured in OMOP",
                            value_as_concept_id=0,
                            value_as_number=None
                        )
                        artifact.save_artifact()
                    except Exception as e:
                        utils.logger.error(f"Error creating source not captured artifact for {table_name}.{concept_id_field}: {e}")
                else:
                    # Query and create artifacts for source vocabularies
                    try:
                        sql = self.generate_vocabulary_breakdown_sql(
                            table_path,
                            concept_path,
                            source_concept_id_field,
                            is_source=True
                        )
                        result = utils.execute_duckdb_sql(
                            sql,
                            f"Unable to query source vocabulary breakdown for {table_name}.{source_concept_id_field}",
                            return_results=True
                        )

                        # Create report artifact for each vocabulary
                        for row in result:
                            vocabulary_id, record_count = row

                            artifact = report_artifact.ReportArtifact(
                                delivery_date=self.delivery_date,
                                artifact_bucket=self.bucket,
                                concept_id=0,
                                name=f"Source vocabulary breakdown: {table_name}.{concept_id_field}",
                                value_as_string=vocabulary_id,
                                value_as_concept_id=0,
                                value_as_number=float(record_count)
                            )
                            artifact.save_artifact()

                    except Exception as e:
                        utils.logger.error(f"Error processing source vocabulary breakdown for {table_name}.{source_concept_id_field}: {e}")

        utils.logger.info("Created vocabulary breakdown report artifacts")

    def _create_date_datetime_default_value_artifacts(self) -> None:
        """
        Create report artifacts for date/datetime fields with default values.

        For each table, identifies date and datetime fields from the OMOP schema.
        For each date/datetime field, counts how many rows have the default placeholder value
        (e.g., '1970-01-01' for DATE, '1970-01-01 00:00:00' for TIMESTAMP/DATETIME).
        Creates one report artifact per table-field combination.
        """
        utils.logger.info("Creating date/datetime default value report artifacts")

        # Process each table in reporting config
        for table_name, table_config_obj in constants.REPORTING_TABLE_CONFIG.items():
            table_config: Any = table_config_obj
            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping date/datetime default value check")
                continue

            # Get OMOP schema for this table
            schema = utils.get_cdm_schema(cdm_version=self.target_cdm_version)
            if table_name not in schema:
                utils.logger.warning(f"Table {table_name} not found in CDM schema, skipping")
                continue

            table_schema = schema[table_name]
            columns = table_schema.get("columns", {})

            # Find date/datetime fields
            date_datetime_fields = []
            for column_name, column_props in columns.items():
                column_type = column_props.get("type", "")
                if column_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                    date_datetime_fields.append((column_name, column_type))

            # Skip if no date/datetime fields
            if not date_datetime_fields:
                utils.logger.debug(f"Table {table_name} has no date/datetime fields, skipping")
                continue

            # Get table concept_id for the artifact
            table_concept_id = table_schema.get("concept_id", 0)

            # Process each date/datetime field
            for field_name, field_type in date_datetime_fields:
                try:
                    # Get the default value for this field
                    default_value = utils.get_placeholder_value(field_name, field_type)

                    # Generate and execute SQL to count rows with default value
                    sql = self.generate_date_datetime_default_count_sql(
                        table_path,
                        field_name,
                        default_value
                    )

                    result = utils.execute_duckdb_sql(
                        sql,
                        f"Unable to query default value count for {table_name}.{field_name}",
                        return_results=True
                    )

                    # Extract count from result
                    default_count = result[0][0] if result else 0

                    # Create report artifact
                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=table_concept_id,
                        name=f"Date/datetime default value count: {table_name}.{field_name}",
                        value_as_string=None,
                        value_as_concept_id=None,
                        value_as_number=float(default_count)
                    )
                    artifact.save_artifact()

                    utils.logger.info(f"Created default value artifact for {table_name}.{field_name}: {default_count} rows")

                except Exception as e:
                    utils.logger.error(f"Error processing default value count for {table_name}.{field_name}: {e}")
                    continue

        utils.logger.info("Created date/datetime default value report artifacts")

    def _create_invalid_concept_id_artifacts(self) -> None:
        """
        Create report artifacts for concept_id values that don't exist in the OMOP vocabulary.

        For each table with concept_id fields, identifies concept_id values that are:
        - Not NULL
        - Not 0 (which represents "No matching concept")
        - Don't exist in the vocabulary

        Note: Only checks standard concept_id fields and type_concept_id fields.
        Source concept_id fields (e.g., condition_source_concept_id) are NOT checked.

        Creates one report artifact per table-field combination showing the count of rows
        with invalid concept_ids. This helps identify data quality issues where concept_ids
        reference non-existent vocabulary entries.
        """
        utils.logger.info("Creating invalid concept_id report artifacts")

        # Get target vocabulary version for this delivery
        target_vocab_version = self.target_vocabulary_version

        # Construct path to concept table in vocabulary
        concept_path = storage.get_uri(f"{constants.VOCAB_PATH}/{target_vocab_version}/optimized/concept{constants.PARQUET}")

        # Check if concept table exists
        if not utils.parquet_file_exists(concept_path):
            utils.logger.warning(f"Concept table not found at {concept_path}, skipping invalid concept_id check")
            return

        # Process each table with vocabulary concept fields
        for table_name, table_config_obj in constants.REPORTING_TABLE_CONFIG.items():
            table_config: Any = table_config_obj
            vocabulary_fields = table_config["vocabulary_fields"]
            type_field = table_config["type_field"]

            # Skip tables without any concept fields
            if not vocabulary_fields and not type_field:
                continue

            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping invalid concept_id check")
                continue

            # Collect all concept_id fields to check (including type_field)
            concept_fields_to_check = []

            # Add vocabulary fields (excluding source_concept_id fields)
            if vocabulary_fields:
                for field_config in vocabulary_fields:
                    concept_id_field = field_config["concept_id"]
                    # Only check the standard concept_id field, not source_concept_id
                    concept_fields_to_check.append(concept_id_field)

            # Add type field if present
            if type_field:
                concept_fields_to_check.append(type_field)

            # Check each concept_id field for invalid values
            for concept_field in concept_fields_to_check:
                try:
                    # Generate and execute SQL to find invalid concept_ids
                    sql = self.generate_invalid_concept_id_sql(
                        table_path,
                        concept_path,
                        concept_field
                    )

                    result = utils.execute_duckdb_sql(
                        sql,
                        f"Unable to query invalid concept_ids for {table_name}.{concept_field}",
                        return_results=True
                    )

                    # Get total count of invalid concept_ids
                    invalid_count = result[0][0] if result else 0

                    # Create report artifact
                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=0,
                        name=f"Invalid concept_id count: {table_name}.{concept_field}",
                        value_as_string=f"{table_name}.{concept_field}",
                        value_as_concept_id=0,
                        value_as_number=float(invalid_count)
                    )
                    artifact.save_artifact()

                    if invalid_count > 0:
                        utils.logger.warning(f"Found {invalid_count} invalid concept_ids in {table_name}.{concept_field}")

                except Exception as e:
                    utils.logger.error(f"Error checking invalid concept_ids for {table_name}.{concept_field}: {e}")
                    continue

        utils.logger.info("Created invalid concept_id report artifacts")

    def _create_person_id_referential_integrity_artifacts(self) -> None:
        """
        Create report artifacts for person_id referential integrity violations.

        For each table with a person_id field, identifies rows where:
        - person_id IS NOT NULL
        - person_id does not exist in the person table

        This helps identify data quality issues where records reference non-existent persons.
        Even if no violations are found (count = 0), a report artifact is created.
        Only processes tables that exist and have at least one row.
        """
        utils.logger.info("Creating person_id referential integrity report artifacts")

        # Get path to person table
        person_table_config: Any = constants.REPORTING_TABLE_CONFIG.get("person")
        if person_table_config is None:
            utils.logger.error("Person table not found in REPORTING_TABLE_CONFIG, skipping referential integrity check")
            return

        person_location = person_table_config["location"]
        person_path = self._get_table_path("person", person_location)

        # Check if person table exists
        if not utils.parquet_file_exists(person_path):
            utils.logger.warning(f"Person table not found at {person_path}, skipping person_id referential integrity check")
            return

        # Get CDM schema to find tables with person_id
        try:
            schema = utils.get_cdm_schema(self.target_cdm_version)
        except Exception as e:
            utils.logger.error(f"Unable to load CDM schema version {self.target_cdm_version}: {e}")
            return

        # Find all tables with person_id column
        tables_with_person_id = [
            table_name for table_name, table_schema in schema.items()
            if 'person_id' in table_schema.get('columns', {})
        ]

        # Process each table with person_id (except person itself)
        for table_name in tables_with_person_id:
            # Skip the person table itself
            if table_name == "person":
                continue

            table_config: Any = constants.REPORTING_TABLE_CONFIG.get(table_name)

            # Skip if table not in config
            if table_config is None:
                utils.logger.warning(f"Table {table_name} not in REPORTING_TABLE_CONFIG, skipping referential integrity check")
                continue

            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping person_id referential integrity check")
                continue

            # Check if table has at least one row
            try:
                row_count_sql = self.generate_row_count_sql(table_path)
                row_count_result = utils.execute_duckdb_sql(
                    row_count_sql,
                    f"Unable to count rows for {table_name}",
                    return_results=True
                )
                row_count = row_count_result[0][0] if row_count_result else 0

                # Skip tables with no rows
                if row_count == 0:
                    utils.logger.info(f"Table {table_name} has no rows, skipping person_id referential integrity check")
                    continue

            except Exception as e:
                utils.logger.error(f"Error checking row count for {table_name}: {e}")
                continue

            # Generate and execute SQL to find person_id referential integrity violations
            try:
                sql = self.generate_person_id_referential_integrity_sql(
                    table_path,
                    person_path
                )

                result = utils.execute_duckdb_sql(
                    sql,
                    f"Unable to check person_id referential integrity for {table_name}",
                    return_results=True
                )

                # Get count of violations
                violation_count = result[0][0] if result else 0

                # Get table concept_id from schema
                table_concept_id = int(schema[table_name].get('concept_id', 0))

                # Create report artifact
                artifact = report_artifact.ReportArtifact(
                    delivery_date=self.delivery_date,
                    artifact_bucket=self.bucket,
                    concept_id=table_concept_id,
                    name=f"Person_id referential integrity violation count: {table_name}",
                    value_as_string=table_name,
                    value_as_concept_id=table_concept_id,
                    value_as_number=float(violation_count)
                )
                artifact.save_artifact()

                if violation_count > 0:
                    utils.logger.warning(f"Found {violation_count} person_id referential integrity violations in {table_name}")
                else:
                    utils.logger.info(f"No person_id referential integrity violations in {table_name}")

            except Exception as e:
                utils.logger.error(f"Error checking person_id referential integrity for {table_name}: {e}")
                continue

        utils.logger.info("Created person_id referential integrity report artifacts")

    def _create_final_row_count_artifacts(self) -> None:
        """
        Create final row count artifacts for all OMOP CDM tables.

        Generates row count artifacts after vocabulary harmonization for every table
        defined in the target CDM schema (5.4). If a table doesn't exist as a parquet
        file, creates an artifact with count = 0.
        """
        utils.logger.info("Creating final row count report artifacts")

        # Get the target CDM schema (always 5.4)
        try:
            schema = utils.get_cdm_schema(self.target_cdm_version)
        except Exception as e:
            utils.logger.error(f"Unable to load CDM schema version {self.target_cdm_version}: {e}")
            return

        # Get all tables from the schema
        all_tables = sorted(schema.keys())

        # Process each table in the schema
        for table_name in all_tables:
            table_config: Any = constants.REPORTING_TABLE_CONFIG.get(table_name)

            # Skip if table not in config (shouldn't happen since we added all tables)
            if table_config is None:
                utils.logger.warning(f"Table {table_name} not in REPORTING_TABLE_CONFIG, skipping final row count")
                continue

            location = table_config["location"]

            # Get table concept_id from schema
            table_concept_id = int(schema[table_name].get('concept_id', 0))

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if utils.parquet_file_exists(table_path):
                # Table exists - count rows
                try:
                    sql = self.generate_row_count_sql(table_path)
                    result = utils.execute_duckdb_sql(
                        sql,
                        f"Unable to count rows for {table_name}",
                        return_results=True
                    )

                    row_count = result[0][0] if result else 0

                    # Create artifact with actual count
                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=table_concept_id,
                        name=f"Final row count: {table_name}",
                        value_as_string=table_name,
                        value_as_concept_id=table_concept_id,
                        value_as_number=float(row_count)
                    )
                    artifact.save_artifact()

                except Exception as e:
                    utils.logger.error(f"Error counting rows for {table_name}: {e}")
            else:
                # Table doesn't exist - create artifact with count = 0
                try:
                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=table_concept_id,
                        name=f"Final row count: {table_name}",
                        value_as_string=table_name,
                        value_as_concept_id=table_concept_id,
                        value_as_number=0.0
                    )
                    artifact.save_artifact()

                except Exception as e:
                    utils.logger.error(f"Error creating zero-count artifact for {table_name}: {e}")

        utils.logger.info("Created final row count report artifacts")

    def _create_time_series_row_count_artifacts(self) -> None:
        """
        Create time series row count artifacts showing rows per year for OMOP clinical tables (minus person and death).

        For each specified table with date fields, counts the number of rows per year from 1970-01-01 up to the delivery date. 
        Creates one report artifact per table-year combination with the row count for that year.
        """
        utils.logger.info("Creating time series row count report artifacts")

        # Time series range: 1970-01-01/default pipeline date to delivery_date
        start_date = constants.DEFAULT_DATE
        end_date = self.delivery_date

        # Get CDM schema for table concept_ids
        try:
            schema = utils.get_cdm_schema(self.target_cdm_version)
        except Exception as e:
            utils.logger.error(f"Unable to load CDM schema version {self.target_cdm_version}: {e}")
            return

        # Process each table
        for table_name, date_field in constants.TIME_SERIES_TABLES.items():
            # Get table configuration
            table_config: Any = constants.REPORTING_TABLE_CONFIG.get(table_name)

            if table_config is None:
                utils.logger.warning(f"Table {table_name} not in REPORTING_TABLE_CONFIG, skipping time series")
                continue

            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping time series row count")
                continue

            # Get table concept_id from schema
            table_concept_id = int(schema.get(table_name, {}).get('concept_id', 0))

            # Generate and execute SQL to get row counts by year
            try:
                sql = self.generate_time_series_row_count_sql(
                    table_path,
                    date_field,
                    start_date,
                    end_date
                )

                result = utils.execute_duckdb_sql(
                    sql,
                    f"Unable to query time series row count for {table_name}",
                    return_results=True
                )

                # Create one artifact per year
                for row in result:
                    year, row_count = row

                    artifact = report_artifact.ReportArtifact(
                        delivery_date=self.delivery_date,
                        artifact_bucket=self.bucket,
                        concept_id=table_concept_id,
                        name=f"Time series row count: {table_name}.{int(year)}",
                        value_as_string=f"{table_name}.{int(year)}",
                        value_as_concept_id=table_concept_id,
                        value_as_number=float(row_count)
                    )
                    artifact.save_artifact()

                utils.logger.info(f"Created {len(result)} time series artifacts for {table_name} ({start_date} to {end_date})")

            except Exception as e:
                utils.logger.error(f"Error creating time series row count for {table_name}: {e}")
                continue

        utils.logger.info("Created time series row count report artifacts")

    @staticmethod
    def generate_type_concept_breakdown_sql(table_uri: str, concept_uri: str, type_field: str) -> str:
        """
        Generate SQL to get type_concept_id breakdown for a table.

        Queries the specified type concept field, joins with the concept vocabulary
        table to get concept names, and returns counts grouped by type_concept_id.

        Args:
            table_uri: Full URI path to the table's parquet file
            concept_uri: Full URI path to the concept vocabulary parquet file
            type_field: Name of the type_concept_id field to analyze
        """
        return f"""
            SELECT
                COALESCE(t.{type_field}, 0) as type_concept_id,
                COALESCE(c.concept_name, 'No matching concept') as concept_name,
                COUNT(*) as record_count
            FROM read_parquet('{table_uri}') t
            LEFT JOIN read_parquet('{concept_uri}') c
                ON t.{type_field} = c.concept_id
            GROUP BY t.{type_field}, c.concept_name
            ORDER BY record_count DESC
        """

    @staticmethod
    def generate_vocabulary_breakdown_sql(table_uri: str, concept_uri: str, concept_field: str, is_source: bool) -> str:
        """
        Generate SQL to get vocabulary breakdown for a concept field.

        Queries the specified concept field, joins with the concept vocabulary
        table to get vocabulary IDs, and returns counts grouped by vocabulary.

        Args:
            table_uri: Full URI path to the table's parquet file
            concept_uri: Full URI path to the concept vocabulary parquet file
            concept_field: Name of the concept_id field to analyze
            is_source: Whether this is a source concept field (for logging purposes)
        """
        return f"""
            SELECT
                COALESCE(c.vocabulary_id, 'No matching concept') as vocabulary_id,
                COUNT(*) as record_count
            FROM read_parquet('{table_uri}') t
            LEFT JOIN read_parquet('{concept_uri}') c
                ON t.{concept_field} = c.concept_id
            GROUP BY c.vocabulary_id
            ORDER BY record_count DESC
        """

    @staticmethod
    def generate_row_count_sql(table_uri: str) -> str:
        """
        Generate SQL to count rows in a table.

        Args:
            table_uri: Full URI path to the table's parquet file
        """
        return f"SELECT COUNT(*) as row_count FROM read_parquet('{table_uri}')"

    @staticmethod
    def generate_date_datetime_default_count_sql(table_uri: str, field_name: str, default_value: str) -> str:
        """
        Generate SQL to count rows with default values in date/datetime fields.

        Args:
            table_uri: Full URI path to the table's parquet file
            field_name: Name of the date/datetime field to check
            default_value: Default value to search for (e.g., '1970-01-01' or '1970-01-01 00:00:00')

        Returns:
            SQL query that counts rows where the field equals the default value
        """
        return f"""
            SELECT COUNT(*) as default_count
            FROM read_parquet('{table_uri}')
            WHERE {field_name} = {default_value}
        """

    @staticmethod
    def generate_invalid_concept_id_sql(table_uri: str, concept_uri: str, concept_field: str) -> str:
        """
        Generate SQL to count rows with invalid concept_id values.

        Identifies concept_id values that are:
        - Not NULL
        - Not 0 (which represents "No matching concept" in OMOP)
        - Don't exist in the vocabulary concept table

        Args:
            table_uri: Full URI path to the table's parquet file
            concept_uri: Full URI path to the concept vocabulary parquet file
            concept_field: Name of the concept_id field to check

        Returns:
            SQL query that counts rows with invalid concept_ids
        """
        return f"""
            SELECT COUNT(*) as invalid_count
            FROM read_parquet('{table_uri}') t
            LEFT JOIN read_parquet('{concept_uri}') c
                ON t.{concept_field} = c.concept_id
            WHERE t.{concept_field} IS NOT NULL
              AND t.{concept_field} != 0
              AND c.concept_id IS NULL
        """

    @staticmethod
    def generate_person_id_referential_integrity_sql(table_uri: str, person_uri: str) -> str:
        """
        Generate SQL to count rows with person_id values that don't exist in the person table.

        Identifies person_id values that are:
        - Not NULL
        - Don't exist in the person table

        This checks referential integrity for the person_id foreign key relationship.

        Args:
            table_uri: Full URI path to the table's parquet file
            person_uri: Full URI path to the person table parquet file
        """
        return f"""
            SELECT COUNT(*) as violation_count
            FROM read_parquet('{table_uri}') t
            LEFT JOIN read_parquet('{person_uri}') p
                ON t.person_id = p.person_id
            WHERE t.person_id IS NOT NULL
              AND p.person_id IS NULL
        """

    @staticmethod
    def generate_time_series_row_count_sql(table_uri: str, date_field: str, start_date: str, end_date: str) -> str:
        """
        Generate SQL to count rows per year for a table's date field.

        Groups rows by year extracted from the specified date field, counting
        rows for each year between start_date and end_date (inclusive).

        Args:
            table_uri: Full URI path to the table's parquet file
            date_field: Name of the date field to use for yearly grouping
            start_date: Start date for time series (format: 'YYYY-MM-DD')
            end_date: End date for time series (format: 'YYYY-MM-DD')
        """
        return f"""
            SELECT
                EXTRACT(YEAR FROM {date_field}) as year,
                COUNT(*) as row_count
            FROM read_parquet('{table_uri}')
            WHERE {date_field} IS NOT NULL
              AND {date_field} >= '{start_date}'
              AND {date_field} <= '{end_date}'
            GROUP BY EXTRACT(YEAR FROM {date_field})
            ORDER BY year
        """

    @staticmethod
    def generate_report_consolidation_sql(select_statement: str, output_path: str) -> str:
        """
        Generate SQL to consolidate multiple report files into a single CSV.

        Args:
            select_statement: UNION ALL query joining multiple parquet files
            output_path: Full path where the consolidated CSV report should be written
        """
        consolidation_statement = f"""
            SET max_expression_depth TO 1000000;

            COPY (
                {select_statement}
            ) TO '{output_path}' (HEADER, DELIMITER ',');
        """

        return consolidation_statement
