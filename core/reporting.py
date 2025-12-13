import os
from datetime import datetime

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


class ReportGenerator:
    """
    Generates delivery reports for OMOP data processing.

    Consolidates temporary report artifacts into a final CSV report
    and creates metadata entries documenting the delivery and processing.
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
        Generate complete delivery report.

        Creates report artifacts with metadata and consolidates
        temporary report files into final CSV.
        """
        self._create_metadata_artifacts()
        self._create_type_concept_breakdown_artifacts()
        self._consolidate_report_files()

    def _create_metadata_artifacts(self) -> None:
        """Create report artifacts documenting delivery and processing metadata."""
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

    def _consolidate_report_files(self) -> None:
        """
        Consolidate temporary report files into final CSV.

        Discovers all temporary report parquet files and combines them into a single CSV file.
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
        Construct path to a table's Parquet file based on its location.

        Args:
            table_name: Name of the OMOP table
            location: ArtifactPaths enum value indicating where the file is stored

        Returns:
            Full path to the table's Parquet file (without URI scheme prefix)
        """
        if location == constants.ArtifactPaths.OMOP_ETL:
            # OMOP ETL tables are stored in subdirectories
            return f"{self.bucket}/{self.delivery_date}/{location.value}{table_name}/{table_name}{constants.PARQUET}"
        else:
            # Converted and derived files are stored directly in their directories
            return f"{self.bucket}/{self.delivery_date}/{location.value}{table_name}{constants.PARQUET}"

    def _create_type_concept_breakdown_artifacts(self) -> None:
        """
        Create report artifacts for type_concept_id breakdowns across all OMOP tables.

        For each table with a type_concept_id field, queries the breakdown of type concepts
        and their counts, joining with the concept vocabulary table to get concept names.
        Creates one report artifact per table-type_concept pair.
        """
        # Get target vocabulary version for this delivery
        target_vocab_version = self.target_vocabulary_version

        # Construct path to concept table in vocabulary
        concept_path = f"{constants.VOCAB_PATH}/{target_vocab_version}/optimized/concept{constants.PARQUET}"

        # Check if concept table exists
        if not utils.parquet_file_exists(concept_path):
            utils.logger.warning(f"Concept table not found at {concept_path}, skipping type concept breakdown")
            return

        concept_uri = storage.get_uri(concept_path)

        # Process each table with type_concept_id field
        for table_name, table_config in constants.TYPE_CONCEPT_TABLES.items():
            type_field = table_config["type_field"]
            location = table_config["location"]

            # Construct path to table
            table_path = self._get_table_path(table_name, location)

            # Check if table exists
            if not utils.parquet_file_exists(table_path):
                utils.logger.info(f"Table {table_name} not found, skipping type concept breakdown")
                continue

            table_uri = storage.get_uri(table_path)

            # Generate and execute SQL query for type_concept_id breakdown
            sql = self.generate_type_concept_breakdown_sql(table_uri, concept_uri, type_field)

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

        Returns:
            SQL statement that returns (type_concept_id, concept_name, record_count)
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
