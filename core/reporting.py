"""
Reporting module for OMOP file processor.

Handles consolidation of delivery report artifacts and metadata generation.
"""

import os
from datetime import datetime

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


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


def create_final_report_artifacts(report_data: dict) -> None:
    """Create report artifacts with delivery and processing metadata."""
    site_bucket = report_data["site_bucket"]
    delivery_date = report_data["delivery_date"]

    # Create tuples to represent a value to add to delivery report, and what that value describes
    delivery_date_value = (delivery_date, constants.DELIVERY_DATE_REPORT_NAME)
    site_display_name = (report_data["site_display_name"], constants.SITE_DISPLAY_NAME_REPORT_NAME)
    file_delivery_format = (report_data["file_delivery_format"], constants.FILE_DELIVERY_FORMAT_REPORT_NAME)
    delivered_cdm_version = (report_data["delivered_cdm_version"], constants.DELIVERED_CDM_VERSION_REPORT_NAME)
    delivered_vocab_version = (utils.get_delivery_vocabulary_version(site_bucket, delivery_date), constants.DELIVERED_VOCABULARY_VERSION_REPORT_NAME)
    target_vocabulary_version = (report_data["target_vocabulary_version"], constants.TARGET_VOCABULARY_VERSION_REPORT_NAME)
    target_cdm_version = (report_data["target_cdm_version"], constants.TARGET_CDM_VERSION_REPORT_NAME)
    target_cdm_version = (report_data["target_cdm_version"], constants.TARGET_CDM_VERSION_REPORT_NAME)
    file_processor_version = (os.getenv('COMMIT_SHA'), constants.FILE_PROCESSOR_VERSION_REPORT_NAME)
    processed_date = (datetime.today().strftime('%Y-%m-%d'), constants.PROCESSED_DATE_REPORT_NAME)

    # Create a list of the tuples
    report_data_points = [processed_date, file_processor_version, delivery_date_value, site_display_name, file_delivery_format, delivered_cdm_version, delivered_vocab_version, target_vocabulary_version, target_cdm_version]

    # Iterate over each tuple
    for report_data_point in report_data_points:
        # Seperate value and the thing it describes from tuple
        value, reporting_item = report_data_point
        value_as_concept_id: int = 0

        if reporting_item in [constants.DELIVERED_CDM_VERSION_REPORT_NAME, constants.TARGET_CDM_VERSION_REPORT_NAME]:
            value_as_concept_id = utils.get_cdm_version_concept_id(value)

        ra = report_artifact.ReportArtifact(
            delivery_date=delivery_date,
            artifact_bucket=site_bucket,
            concept_id=0,
            name=f"{reporting_item}",
            value_as_string=value,
            value_as_concept_id=value_as_concept_id,
            value_as_number=None
        )
        ra.save_artifact()


def generate_report(report_data: dict) -> None:
    """
    Generate final delivery report by consolidating all report artifacts into CSV.
    Combines temporary report files and creates metadata entries.
    """
    create_final_report_artifacts(report_data)

    site = report_data["site"]
    site_bucket = report_data["site_bucket"]
    delivery_date = report_data["delivery_date"]

    report_tmp_dir = f"{delivery_date}/{constants.ArtifactPaths.REPORT_TMP.value}"
    tmp_files = utils.list_files(site_bucket, report_tmp_dir, constants.PARQUET)

    if len(tmp_files) > 0:
        # Build UNION ALL SELECT statement to join together files
        file_paths = [storage.get_uri(f"{site_bucket}/{report_tmp_dir}{file}") for file in tmp_files]
        select_statement = " UNION ALL ".join([f"SELECT * FROM read_parquet('{path}')" for path in file_paths])

        output_path = storage.get_uri(f"{site_bucket}/{delivery_date}/{constants.ArtifactPaths.REPORT.value}delivery_report_{site}_{delivery_date}{constants.CSV}")

        # Generate SQL to consolidate report files
        join_files_query = generate_report_consolidation_sql(select_statement, output_path)

        utils.execute_duckdb_sql(join_files_query, "Unable to merge reporting artifacts")
