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
                - site: Site identifier
                - site_bucket: Storage bucket for site data
                - delivery_date: Delivery date string (YYYY-MM-DD)
                - site_display_name: Human-readable site name
                - file_delivery_format: Format of delivered files (e.g., .csv, .parquet)
                - delivered_cdm_version: OMOP CDM version delivered by site
                - target_vocabulary_version: Target vocabulary version
                - target_cdm_version: Target OMOP CDM version
        """
        self.site = report_data["site"]
        self.bucket = report_data["site_bucket"]
        self.delivery_date = report_data["delivery_date"]
        self.site_display_name = report_data["site_display_name"]
        self.file_delivery_format = report_data["file_delivery_format"]
        self.delivered_cdm_version = report_data["delivered_cdm_version"]
        self.target_vocabulary_version = report_data["target_vocabulary_version"]
        self.target_cdm_version = report_data["target_cdm_version"]

        # Derived attributes
        self.tmp_artifacts_path = self._get_tmp_artifacts_path()
        self.output_path = self._get_output_path()

    def generate(self) -> None:
        """
        Generate complete delivery report.

        Creates report artifacts with metadata and consolidates
        temporary report files into final CSV.
        """
        self._create_metadata_artifacts()
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

        Discovers all temporary report parquet files and combines them
        into a single CSV output using DuckDB UNION ALL.
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
        return get_report_tmp_artifacts_path(self.bucket, self.delivery_date)

    def _get_output_path(self) -> str:
        """Get full URI path for final report CSV output."""
        return storage.get_uri(
            f"{self.bucket}/{self.delivery_date}/{constants.ArtifactPaths.REPORT.value}"
            f"delivery_report_{self.site}_{self.delivery_date}{constants.CSV}"
        )

    @staticmethod
    def generate_report_consolidation_sql(select_statement: str, output_path: str) -> str:
        """
        Generate SQL to consolidate multiple report files into a single CSV.

        Args:
            select_statement: UNION ALL query joining multiple parquet files
            output_path: Full path where the consolidated CSV report should be written

        Returns:
            SQL statement that sets max_expression_depth and exports consolidated data to CSV
        """
        return f"""
            SET max_expression_depth TO 1000000;

            COPY (
                {select_statement}
            ) TO '{output_path}' (HEADER, DELIMITER ',');
        """


def get_report_tmp_artifacts_path(bucket: str, delivery_date: str) -> str:
    """
    Returns the path to the temporary report artifacts directory.

    NOTE: This function returns the path WITHOUT storage scheme prefix,
    consistent with all other path utility functions. Callers should use storage.get_uri()
    if they need the full URI with scheme.

    Args:
        bucket: Storage bucket name
        delivery_date: Delivery date string

    Returns:
        Path to temporary report artifacts directory (without scheme prefix)
    """
    report_tmp_dir = f"{bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
    return report_tmp_dir
