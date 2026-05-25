#!/usr/bin/env python3
"""
Cloud Run Job entry point for rewriting natural-key columns so they are
globally unique across sites.

This job runs after filter_connect_participants and before vocab harmonization.
It rewrites every in-scope natural-key column (PK or FK) in the file to a
site-salted hash, preserving referential integrity across the delivery.

Required Environment Variables:
    FILE_PATH: Full path to the original file (resolves to parquet artifact path)
    OMOP_VERSION: OMOP CDM version (e.g., '5.4')
    SITE: Site identifier used as the hash salt

Exit Codes:
    0: Success (including no-op skips)
    1: Failure
"""

import os
import sys
import traceback

import core.natural_keys as natural_keys
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['FILE_PATH', 'OMOP_VERSION', 'SITE']

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
    """Main entry point for unique_natural_keys job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Unique Natural Keys - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    env_values = validate_env_vars()

    utils.logger.info(f"FILE_PATH: {env_values['FILE_PATH']}")
    utils.logger.info(f"OMOP_VERSION: {env_values['OMOP_VERSION']}")
    utils.logger.info(f"SITE: {env_values['SITE']}")

    try:
        processor = natural_keys.NaturalKeyProcessor(
            file_path=env_values['FILE_PATH'],
            omop_version=env_values['OMOP_VERSION'],
            site=env_values['SITE'],
        )
        was_applied = processor.apply()

        if was_applied:
            utils.logger.info("Natural-key rewrite applied")
        else:
            utils.logger.info("Natural-key rewrite skipped (table not in scope)")

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Unique Natural Keys - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Unique Natural Keys - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
