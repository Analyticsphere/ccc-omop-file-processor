#!/usr/bin/env python3
"""
Cloud Run Job entry point for reconciling per-table merge chunks into one merged table.

Unions every chunk file matching CHUNK_GLOB into a single merged parquet at OUTPUT_URI
(which lives in converted_files/, outside the chunk folder). union_by_name defends
against benign column-order/schema drift across sites.

Required Environment Variables:
    CHUNK_GLOB: Glob over the per-table staging folder (e.g. .../merge_chunks/<table>/*.parquet)
    OUTPUT_URI: Destination for the reconciled table (e.g. .../converted_files/<table>.parquet)

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.merge as merge
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['CHUNK_GLOB', 'OUTPUT_URI']

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
    """Main entry point for reconcile_chunks job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Reconcile Chunks - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    env_values = validate_env_vars()

    utils.logger.info(f"CHUNK_GLOB: {env_values['CHUNK_GLOB']}")
    utils.logger.info(f"OUTPUT_URI: {env_values['OUTPUT_URI']}")

    try:
        merge.MergeProcessor.reconcile_chunks(
            chunk_glob=env_values['CHUNK_GLOB'],
            output_uri=env_values['OUTPUT_URI'],
        )

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Reconcile Chunks - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Reconcile Chunks - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
