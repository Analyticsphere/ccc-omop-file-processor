#!/usr/bin/env python3
"""
Cloud Run Job entry point for extracting a (group x table) chunk from a source delivery.

Copies one source table into a provenance-named chunk file in the shared per-table
merge staging area. For v1 the participant scope is always "ALL" (whole table); a
participant-id subset (v2) is supported by pointing PARTICIPANT_SCOPE at a parquet of Connect IDs.

Required Environment Variables:
    SOURCE_URI: Path to the source delivery's table parquet (e.g. .../converted_files/<table>.parquet)
    CHUNK_URI: Destination chunk parquet path in the staging area
    PARTICIPANT_SCOPE: "ALL" for the whole table, otherwise a path to a parquet of participant ids

Optional Environment Variables:
    PERSON_ID_COLUMN: Column matched against the id set when scope != "ALL" (default "person_id")
    SITE_DISPLAY_NAME: Source site name; when set (person only), stamps care_site_id with its hash

Exit Codes:
    0: Success
    1: Failure
"""

import os
import sys
import traceback

import core.constants as constants
import core.merge as merge
import core.utils as utils


def validate_env_vars() -> dict[str, str]:
    """Validate and return required environment variables."""
    required_vars = ['SOURCE_URI', 'CHUNK_URI', 'PARTICIPANT_SCOPE']

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

    # Optional: person_id column used only when subsetting by participant scope
    env_values['PERSON_ID_COLUMN'] = os.getenv('PERSON_ID_COLUMN', constants.DEFAULT_PERSON_ID_COLUMN)

    # Optional: site name used to stamp care_site_id (person table only).
    env_values['SITE_DISPLAY_NAME'] = os.getenv('SITE_DISPLAY_NAME', '')

    return env_values


def main():
    """Main entry point for extract_participant_chunk job."""
    utils.logger.info("=" * 80)
    utils.logger.info("Cloud Run Job: Extract Participant Chunk - Starting")
    utils.logger.info("=" * 80)
    utils.logger.info(f"PID: {os.getpid()}")
    utils.logger.info(f"Working directory: {os.getcwd()}")

    env_values = validate_env_vars()

    utils.logger.info(f"SOURCE_URI: {env_values['SOURCE_URI']}")
    utils.logger.info(f"CHUNK_URI: {env_values['CHUNK_URI']}")
    utils.logger.info(f"PARTICIPANT_SCOPE: {env_values['PARTICIPANT_SCOPE']}")
    utils.logger.info(f"PERSON_ID_COLUMN: {env_values['PERSON_ID_COLUMN']}")
    utils.logger.info(f"SITE_DISPLAY_NAME: {env_values['SITE_DISPLAY_NAME']}")

    try:
        merge.MergeProcessor.extract_chunk(
            source_uri=env_values['SOURCE_URI'],
            chunk_uri=env_values['CHUNK_URI'],
            participant_scope=env_values['PARTICIPANT_SCOPE'],
            person_id_column=env_values['PERSON_ID_COLUMN'],
            # Empty env var -> None (no stamping); set only for person.
            site_display_name=env_values['SITE_DISPLAY_NAME'] or None,
        )

        utils.logger.info("=" * 80)
        utils.logger.info("Cloud Run Job: Extract Participant Chunk - SUCCESS")
        utils.logger.info("=" * 80)
        sys.exit(0)

    except Exception as e:
        utils.logger.error("=" * 80)
        utils.logger.error("Cloud Run Job: Extract Participant Chunk - FAILED")
        utils.logger.error("=" * 80)
        utils.logger.error(f"Error: {str(e)}")
        utils.logger.error(f"Traceback:\n{traceback.format_exc()}")
        sys.exit(1)


if __name__ == '__main__':
    main()
