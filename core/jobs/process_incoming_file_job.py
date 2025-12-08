#!/usr/bin/env python3
"""
Cloud Run Job entry point for processing incoming files (CSV/CSV.GZ to Parquet).

This job converts raw CSV or CSV.GZ files to standardized Parquet format.

Required Environment Variables:
    FILE_TYPE: Type of file being processed (e.g., 'person', 'condition_occurrence')
    FILE_PATH: Full GCS path to the file to process

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.file_processor as file_processor
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['FILE_TYPE', 'FILE_PATH']

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
    """Main entry point for process_incoming_file job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Process Incoming File - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"FILE_TYPE: {env_values['FILE_TYPE']}")
    utils.logger.info(f"FILE_PATH: {env_values['FILE_PATH']}")

    try:
        # Execute file processing
        utils.logger.info(f"Processing file: {env_values['FILE_PATH']}")
        file_processor.process_incoming_file(
            file_type=env_values['FILE_TYPE'],
            file_path=env_values['FILE_PATH']
        )

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Process Incoming File - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Process Incoming File - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
