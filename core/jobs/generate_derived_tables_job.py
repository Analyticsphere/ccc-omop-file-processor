#!/usr/bin/env python3
"""
Cloud Run Job entry point for generating derived tables.

This job generates derived tables (observation_period, condition_era, drug_era)
from harmonized OMOP CDM data.

Required Environment Variables:
    SITE: Site identifier
    GCS_BUCKET: GCS bucket path for the site
    DELIVERY_DATE: Delivery date (YYYY-MM-DD format)
    TABLE_NAME: Name of derived table to generate (observation_period, condition_era, drug_era)
    VOCAB_VERSION: Vocabulary version

Optional Environment Variables:
    OMOP_VOCAB_PATH: GCS bucket for vocabulary files (defaults to constants.OMOP_VOCAB_PATH)

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.constants as constants
import core.omop_client as omop_client
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['SITE', 'GCS_BUCKET', 'DELIVERY_DATE', 'TABLE_NAME', 'VOCAB_VERSION']

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

    # Optional variables with defaults
    env_values['OMOP_VOCAB_PATH'] = os.getenv('OMOP_VOCAB_PATH', constants.OMOP_VOCAB_PATH)

    return env_values


def main():
    """Main entry point for generate_derived_tables job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Generate Derived Tables - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"SITE: {env_values['SITE']}")
    utils.logger.info(f"GCS_BUCKET: {env_values['GCS_BUCKET']}")
    utils.logger.info(f"DELIVERY_DATE: {env_values['DELIVERY_DATE']}")
    utils.logger.info(f"TABLE_NAME: {env_values['TABLE_NAME']}")
    utils.logger.info(f"VOCAB_VERSION: {env_values['VOCAB_VERSION']}")
    utils.logger.info(f"OMOP_VOCAB_PATH: {env_values['OMOP_VOCAB_PATH']}")

    try:
        # Execute derived table generation
        utils.logger.info(f"Generating derived table: {env_values['TABLE_NAME']}")
        omop_client.generate_derived_data_from_harmonized(
            site=env_values['SITE'],
            site_bucket=env_values['GCS_BUCKET'],
            delivery_date=env_values['DELIVERY_DATE'],
            table_name=env_values['TABLE_NAME'],
            vocab_version=env_values['VOCAB_VERSION'],
            vocab_path=env_values['OMOP_VOCAB_PATH']
        )

        utils.logger.info("=" * 80)
        utils.logger.info(f"Cloud Run Job: Generate Derived Tables ({env_values['TABLE_NAME']}) - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error(f"Cloud Run Job: Generate Derived Tables ({env_values.get('TABLE_NAME', 'UNKNOWN')}) - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
