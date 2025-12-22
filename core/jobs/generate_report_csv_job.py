#!/usr/bin/env python3
"""
Cloud Run Job entry point for generating delivery report CSV files.

This job generates a delivery report CSV file containing information about
the data delivery. The CSV serves as a data source for visual reporting.

Required Environment Variables:
    SITE: Site identifier
    GCS_BUCKET: GCS bucket path for the site
    DELIVERY_DATE: Delivery date (YYYY-MM-DD format)
    SITE_DISPLAY_NAME: Human-readable site name
    FILE_DELIVERY_FORMAT: Format of delivered files (e.g., .csv, .parquet)
    DELIVERED_CDM_VERSION: OMOP CDM version delivered by site
    TARGET_VOCABULARY_VERSION: Target vocabulary version
    TARGET_CDM_VERSION: Target OMOP CDM version

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.reporting as reporting
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = [
        'SITE',
        'GCS_BUCKET',
        'DELIVERY_DATE',
        'SITE_DISPLAY_NAME',
        'FILE_DELIVERY_FORMAT',
        'DELIVERED_CDM_VERSION',
        'TARGET_VOCABULARY_VERSION',
        'TARGET_CDM_VERSION'
    ]

    env_values = {}
    missing_vars = []

    for var in required_vars:
        value = os.getenv(var)
        if not value:
            missing_vars.append(var)
        else:
            env_values[var] = value

    if missing_vars:
        utils.logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    return env_values


def main():
    """Main entry point for generate_report_csv job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Generate Report CSV - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"SITE: {env_values['SITE']}")
    utils.logger.info(f"GCS_BUCKET: {env_values['GCS_BUCKET']}")
    utils.logger.info(f"DELIVERY_DATE: {env_values['DELIVERY_DATE']}")
    utils.logger.info(f"SITE_DISPLAY_NAME: {env_values['SITE_DISPLAY_NAME']}")
    utils.logger.info(f"FILE_DELIVERY_FORMAT: {env_values['FILE_DELIVERY_FORMAT']}")
    utils.logger.info(f"DELIVERED_CDM_VERSION: {env_values['DELIVERED_CDM_VERSION']}")
    utils.logger.info(f"TARGET_VOCABULARY_VERSION: {env_values['TARGET_VOCABULARY_VERSION']}")
    utils.logger.info(f"TARGET_CDM_VERSION: {env_values['TARGET_CDM_VERSION']}")

    try:
        # Build report data dictionary
        report_data = {
            "site": env_values['SITE'],
            "bucket": env_values['GCS_BUCKET'],
            "delivery_date": env_values['DELIVERY_DATE'],
            "site_display_name": env_values['SITE_DISPLAY_NAME'],
            "file_delivery_format": env_values['FILE_DELIVERY_FORMAT'],
            "delivered_cdm_version": env_values['DELIVERED_CDM_VERSION'],
            "target_vocabulary_version": env_values['TARGET_VOCABULARY_VERSION'],
            "target_cdm_version": env_values['TARGET_CDM_VERSION']
        }

        # Execute report CSV generation
        utils.logger.info(f"Generating delivery report CSV for {env_values['SITE']} - {env_values['DELIVERY_DATE']}")
        generator = reporting.ReportGenerator(report_data)
        generator.generate()

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Generate Report CSV - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Generate Report CSV - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
