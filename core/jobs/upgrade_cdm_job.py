#!/usr/bin/env python3
"""
Cloud Run Job entry point for upgrading CDM versions.

This job upgrades OMOP CDM files from one version to another (e.g., 5.3 to 5.4).

Required Environment Variables:
    FILE_PATH: Full GCS path to the file to upgrade
    OMOP_VERSION: Current OMOP CDM version
    TARGET_OMOP_VERSION: Target OMOP CDM version to upgrade to

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.omop_client as omop_client
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['FILE_PATH', 'OMOP_VERSION', 'TARGET_OMOP_VERSION']

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
    """Main entry point for upgrade_cdm job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Upgrade CDM - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"FILE_PATH: {env_values['FILE_PATH']}")
    utils.logger.info(f"OMOP_VERSION: {env_values['OMOP_VERSION']}")
    utils.logger.info(f"TARGET_OMOP_VERSION: {env_values['TARGET_OMOP_VERSION']}")

    try:
        # Execute CDM upgrade
        utils.logger.info(f"Upgrading file from CDM {env_values['OMOP_VERSION']} to {env_values['TARGET_OMOP_VERSION']}")
        omop_client.upgrade_file(
            gcs_file_path=env_values['FILE_PATH'],
            cdm_version=env_values['OMOP_VERSION'],
            target_omop_version=env_values['TARGET_OMOP_VERSION']
        )

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Upgrade CDM - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Upgrade CDM - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
