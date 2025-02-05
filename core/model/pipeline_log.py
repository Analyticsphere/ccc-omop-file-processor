import core.constants as constants
import core.utils as utils
from typing import Optional
from datetime import datetime
from google.cloud import bigquery
import sys

class PipelineLog:
    def __init__(self, site_name: str, delivery_date: str, status: str, message: Optional[str], file_format: Optional[str], cdm_version: Optional[str], run_id: str):
        self.site_name = site_name
        self.delivery_date = delivery_date
        self.status = status
        self.message = message
        self.pipeline_start_datetime = datetime.now() if status == constants.PIPELINE_START_STRING else None
        self.pipeline_end_datetime = datetime.now() if status != constants.PIPELINE_START_STRING else None
        self.file_format = file_format
        self.cdm_version = cdm_version
        self.run_id = run_id

    def add_log_entry(self) -> None:
        if self.status == constants.PIPELINE_START_STRING:
            self.log_start()

    def log_start(self) -> None:
        """
        Log the start of the pipeline run, but only if a record for
        the given site_name and delivery_date doesn’t already exist.
        """

        try:
            # Construct a BigQuery client object
            client = bigquery.Client()

            # Build the MERGE statement to only insert new records
            query = f"""
                MERGE `{constants.PIPELINE_LOG_TABLE}` AS target
                USING (
                SELECT @site_name AS site_name, @delivery_date AS delivery_date
                ) AS source
                ON target.site_name = source.site_name
                AND target.delivery_date = source.delivery_date
                WHEN NOT MATCHED THEN
                INSERT (
                    site_name,
                    delivery_date,
                    status,
                    pipeline_start_datetime,
                    file_format,
                    cdm_version,
                    run_id
                )
                VALUES (
                    @site_name,
                    @delivery_date,
                    @status,
                    @pipeline_start_datetime,
                    @file_format,
                    @cdm_version,
                    @run_id
                )
            """

            # Set up the query parameters. For DATETIME, we format the Python datetime
            # object into a string that BigQuery expects (e.g., "YYYY-MM-DD HH:MM:SS").
            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                    bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                    bigquery.ScalarQueryParameter("status", "STRING", constants.PIPELINE_START_STRING),
                    bigquery.ScalarQueryParameter(
                        "pipeline_start_datetime",
                        "DATETIME",
                        self.pipeline_start_datetime.strftime("%Y-%m-%d %H:%M:%S")
                    ),
                    bigquery.ScalarQueryParameter("file_format", "STRING", self.file_format),
                    bigquery.ScalarQueryParameter("cdm_version", "STRING", self.cdm_version),
                    bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id),
                ]
            )

            # Run the query as a job and wait for it to complete.
            query_job = client.query(query, job_config=job_config)
            query_job.result()  # Wait for the job to complete.
        except Exception as e:
            utils.logger.error(f"Unable to add pipeline log record: {e}")
            sys.exit(1)

    def log_end(self) -> None:
        print()



    # TODO: If pipeline log table doesn't exist, create it
    # TODO: If status is constants.PIPELINE_START_STRING, (first check if needed and then) create new entry in table with start time
        # Can set - site, delivery_date, status, message, start_datetime, file_format, cdm_version, run_id
    # TODO: If status is constants.PIPELINE_END_STRING (first check if record exists), update existing record with end time
        # Can set - status, message, end_datetime
    # TODO: If status is constants.PIPELINE_ERROR_STRING (first check if record exists), update existing record with end time
        # Can set - status, message, end_datetime

    # If status != constants.PIPELINE_END_STRING, need to run the pipeline for that delivery