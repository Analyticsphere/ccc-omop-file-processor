#!/usr/bin/env python3
"""
Cloud Run Job entry point for post-processing tasks.

This job applies one user-curated post-processing SQL task to the on-disk OMOP
artifacts. It runs after vocabulary harmonization (step
deduplicate_single_table) and before derived-table generation.

Required Environment Variables:
    SITE: Site identifier
    GCS_BUCKET: GCS bucket path for the site
    DELIVERY_DATE: Delivery date (YYYY-MM-DD format)
    CDM_VERSION: OMOP CDM version (e.g., '5.4')
    VOCAB_VERSION: Vocabulary version
    TASK_NAME: Name of the post-processing task (matches the SQL file stem under
               reference/sql/post_processing/<task_name>.sql)

Optional Environment Variables:
    OMOP_VOCAB_PATH: GCS path for vocabulary files (defaults to constants.VOCAB_PATH)

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.constants as constants
import core.post_processing as post_processing
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['SITE', 'GCS_BUCKET', 'DELIVERY_DATE', 'CDM_VERSION',
                     'VOCAB_VERSION', 'TASK_NAME']

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

    env_values['OMOP_VOCAB_PATH'] = os.getenv('OMOP_VOCAB_PATH', constants.VOCAB_PATH)

    return env_values


def main():
    """Main entry point for post_processing job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Post Processing - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    env_values = validate_env_vars()

    utils.logger.info(f"SITE: {env_values['SITE']}")
    utils.logger.info(f"GCS_BUCKET: {env_values['GCS_BUCKET']}")
    utils.logger.info(f"DELIVERY_DATE: {env_values['DELIVERY_DATE']}")
    utils.logger.info(f"CDM_VERSION: {env_values['CDM_VERSION']}")
    utils.logger.info(f"VOCAB_VERSION: {env_values['VOCAB_VERSION']}")
    utils.logger.info(f"TASK_NAME: {env_values['TASK_NAME']}")
    utils.logger.info(f"OMOP_VOCAB_PATH: {env_values['OMOP_VOCAB_PATH']}")

    try:
        processor = post_processing.PostProcessor(
            site=env_values['SITE'],
            bucket=env_values['GCS_BUCKET'],
            delivery_date=env_values['DELIVERY_DATE'],
            cdm_version=env_values['CDM_VERSION'],
            vocab_version=env_values['VOCAB_VERSION'],
            vocab_path=env_values['OMOP_VOCAB_PATH'],
            task_name=env_values['TASK_NAME'],
        )
        changes = processor.apply()

        utils.logger.info(
            f"Post-processing task '{env_values['TASK_NAME']}' affected {len(changes)} table(s)"
        )

        utils.logger.info("=" * 80)
        utils.logger.info(
            f"Cloud Run Job: Post Processing ({env_values['TASK_NAME']}) - SUCCESS"
        )
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error(
            f"Cloud Run Job: Post Processing ({env_values.get('TASK_NAME', 'UNKNOWN')}) - FAILED"
        )
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
