#!/usr/bin/env python3
"""
Cloud Run Job entry point for normalizing Parquet files.

This job standardizes data types, formats dates/datetimes, and ensures OMOP CDM compliance.

Required Environment Variables:
    FILE_PATH: Full GCS path to the original file (will be converted to parquet artifact path)
    OMOP_VERSION: OMOP CDM version (e.g., '5.4')
    DATE_FORMAT: Date format string (e.g., '%Y-%m-%d')
    DATETIME_FORMAT: Datetime format string (e.g., '%Y-%m-%d %H:%M:%S')

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.normalization as normalization
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['FILE_PATH', 'OMOP_VERSION', 'DATE_FORMAT', 'DATETIME_FORMAT']

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
    """Main entry point for normalize_parquet job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Normalize Parquet - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"FILE_PATH: {env_values['FILE_PATH']}")
    utils.logger.info(f"OMOP_VERSION: {env_values['OMOP_VERSION']}")
    utils.logger.info(f"DATE_FORMAT: {env_values['DATE_FORMAT']}")
    utils.logger.info(f"DATETIME_FORMAT: {env_values['DATETIME_FORMAT']}")

    try:
        # Get the parquet artifact location
        parquet_file_path = utils.get_parquet_artifact_location(env_values['FILE_PATH'])
        utils.logger.info(f"Parquet artifact path: {parquet_file_path}")

        # Execute normalization
        normalization.normalize_file(
            parquet_gcs_file_path=parquet_file_path,
            cdm_version=env_values['OMOP_VERSION'],
            date_format=env_values['DATE_FORMAT'],
            datetime_format=env_values['DATETIME_FORMAT']
        )

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Normalize Parquet - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Normalize Parquet - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
