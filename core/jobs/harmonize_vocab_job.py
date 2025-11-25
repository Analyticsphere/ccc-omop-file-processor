#!/usr/bin/env python3
"""
Cloud Run Job entry point for vocabulary harmonization.

This job performs one of 8 vocabulary harmonization steps on OMOP CDM files.

Required Environment Variables:
    FILE_PATH: Full GCS path to the file to harmonize
    VOCAB_VERSION: Vocabulary version to harmonize to
    OMOP_VERSION: OMOP CDM version
    SITE: Site identifier
    PROJECT_ID: GCP project ID
    DATASET_ID: BigQuery dataset ID
    STEP: Harmonization step name (source_target, target_remap, target_replacement,
          domain_check, omop_etl, consolidate_etl, discover_tables, deduplicate_single_table)

Optional Environment Variables:
    VOCAB_GCS_BUCKET: GCS bucket for vocabulary files (defaults to constants.VOCAB_GCS_PATH)
    OUTPUT_GCS_PATH: For discover_tables step, path to write table configs JSON

Exit Codes:
    0: Success
    1: Failure
"""

import json
import os
import sys
import traceback

import core.constants as constants
import core.gcp_services as gcp_services
import core.utils as utils
import core.vocab_harmonization as vocab_harmonization


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['FILE_PATH', 'VOCAB_VERSION', 'OMOP_VERSION', 'SITE',
                     'PROJECT_ID', 'DATASET_ID', 'STEP']

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
    env_values['VOCAB_GCS_BUCKET'] = os.getenv('VOCAB_GCS_BUCKET', constants.VOCAB_GCS_PATH)
    env_values['OUTPUT_GCS_PATH'] = os.getenv('OUTPUT_GCS_PATH', '')

    return env_values


def main():
    """Main entry point for harmonize_vocab job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Harmonize Vocabulary - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    # Validate environment variables
    env_values = validate_env_vars()

    utils.logger.info(f"FILE_PATH: {env_values['FILE_PATH']}")
    utils.logger.info(f"VOCAB_VERSION: {env_values['VOCAB_VERSION']}")
    utils.logger.info(f"VOCAB_GCS_BUCKET: {env_values['VOCAB_GCS_BUCKET']}")
    utils.logger.info(f"OMOP_VERSION: {env_values['OMOP_VERSION']}")
    utils.logger.info(f"SITE: {env_values['SITE']}")
    utils.logger.info(f"PROJECT_ID: {env_values['PROJECT_ID']}")
    utils.logger.info(f"DATASET_ID: {env_values['DATASET_ID']}")
    utils.logger.info(f"STEP: {env_values['STEP']}")

    try:
        # Initialize the VocabHarmonizer
        vocab_harmonizer = vocab_harmonization.VocabHarmonizer(
            file_path=env_values['FILE_PATH'],
            cdm_version=env_values['OMOP_VERSION'],
            site=env_values['SITE'],
            vocab_version=env_values['VOCAB_VERSION'],
            vocab_gcs_bucket=env_values['VOCAB_GCS_BUCKET'],
            project_id=env_values['PROJECT_ID'],
            dataset_id=env_values['DATASET_ID']
        )

        # Perform the requested harmonization step
        utils.logger.info(f"Executing harmonization step: {env_values['STEP']}")
        result = vocab_harmonizer.perform_harmonization(env_values['STEP'])

        # Special handling for discover_tables step - write results to GCS
        if env_values['STEP'] == constants.DISCOVER_TABLES_FOR_DEDUP:
            if result and env_values['OUTPUT_GCS_PATH']:
                utils.logger.info(f"Writing table configs to: {env_values['OUTPUT_GCS_PATH']}")

                # Write JSON to GCS
                result_json = json.dumps(result, indent=2)

                # Parse GCS path
                if env_values['OUTPUT_GCS_PATH'].startswith('gs://'):
                    gcs_path = env_values['OUTPUT_GCS_PATH'].replace('gs://', '')
                else:
                    gcs_path = env_values['OUTPUT_GCS_PATH']

                parts = gcs_path.split('/', 1)
                bucket_name = parts[0]
                blob_path = parts[1] if len(parts) > 1 else 'table_configs.json'

                # Upload to GCS
                storage_client = gcp_services.get_storage_client()
                bucket = storage_client.bucket(bucket_name)
                blob = bucket.blob(blob_path)
                blob.upload_from_string(result_json, content_type='application/json')

                utils.logger.info(f"Successfully wrote {len(result)} table configs to GCS")
            else:
                utils.logger.warning("discover_tables step completed but no OUTPUT_GCS_PATH provided")

        utils.logger.info("=" * 80)
        utils.logger.info(f"Cloud Run Job: Harmonize Vocabulary ({env_values['STEP']}) - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error(f"Cloud Run Job: Harmonize Vocabulary ({env_values.get('STEP', 'UNKNOWN')}) - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
