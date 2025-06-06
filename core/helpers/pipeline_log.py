from datetime import datetime
from typing import Optional

from google.cloud import bigquery  # type: ignore

import core.constants as constants
import core.gcp_services as gcp_services
import core.utils as utils


class PipelineLog:
    def __init__(self, logging_table: str, site_name: str, delivery_date: str, status: str, message: Optional[str], file_format: Optional[str], cdm_version: Optional[str], run_id: str):
        self.logging_table = logging_table
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
        elif self.status == constants.PIPELINE_RUNNING_STRING:
            self.log_running()
        elif self.status == constants.PIPELINE_ERROR_STRING:
            self.log_error()
        elif self.status == constants.PIPELINE_COMPLETE_STRING:
            self.log_complete()

    def log_start(self) -> None:
        """
        Log the start of the pipeline run, but only if a record for
        the given site_name and delivery_date doesn’t already exist.
        """

        try:
            # Build the MERGE statement to only insert new records
            query = f"""
                CREATE TABLE IF NOT EXISTS `{self.logging_table}`
                (
                    site_name STRING,
                    delivery_date DATE,
                    status STRING,
                    message STRING,
                    pipeline_start_datetime DATETIME,
                    pipeline_end_datetime DATETIME,
                    file_format STRING,
                    cdm_version STRING,
                    run_id STRING
                );

                MERGE `{self.logging_table}` AS target
                USING (
                SELECT @site_name AS site_name, @delivery_date AS delivery_date
                ) AS source
                ON target.site_name = source.site_name
                AND target.delivery_date = source.delivery_date
                WHEN MATCHED THEN
                    UPDATE SET
                        status = @status,
                        message = @message,
                        pipeline_start_datetime = @pipeline_start_datetime,
                        pipeline_end_datetime = NULL,
                        file_format = @file_format,
                        cdm_version = @cdm_version,
                        run_id = @run_id
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
            if self.pipeline_start_datetime:
                start_datetime_str = self.pipeline_start_datetime.strftime("%Y-%m-%d %H:%M:%S")

            job_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                    bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                    bigquery.ScalarQueryParameter("status", "STRING", constants.PIPELINE_START_STRING),
                    bigquery.ScalarQueryParameter("message", "STRING", self.message),
                    bigquery.ScalarQueryParameter(
                        "pipeline_start_datetime",
                        "DATETIME",
                        start_datetime_str
                    ),
                    bigquery.ScalarQueryParameter("file_format", "STRING", self.file_format),
                    bigquery.ScalarQueryParameter("cdm_version", "STRING", self.cdm_version),
                    bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id),
                ]
            )

            # Run the query as a job and wait for it to complete.
            gcp_services.execute_bq_sql(query, job_config)

        except Exception as e:
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'query_job_errors': None,
                'context': {
                    'site_name': self.site_name,
                    'delivery_date': self.delivery_date,
                    'status': self.status
                }
            }
            raise Exception(f"Unable to add pipeline log record: {error_details}") from e

    def log_complete(self) -> None:
        """
        Checks if a log entry exists in BigQuery for the given site and delivery date.
        If found, updates the record with the completed status and pipeline_end_datetime.
        """
        try:
            # First, check if a record exists for this site and delivery date.
            select_query = f"""
                SELECT 1
                FROM `{self.logging_table}`
                WHERE site_name = @site_name AND delivery_date = @delivery_date
                LIMIT 1
            """
            select_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                    bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                ]
            )

            exists = list(gcp_services.execute_bq_sql(select_query, select_config))

            if exists:
                # If the record exists, update it.
                update_query = f"""
                    UPDATE `{self.logging_table}`
                    SET status = @status,
                        pipeline_end_datetime = @pipeline_end_datetime,
                        message = NULL
                    WHERE site_name = @site_name AND delivery_date = @delivery_date
                """
                # Ensure that pipeline_end_datetime is formatted for BigQuery (YYYY-MM-DD HH:MM:SS).
                if self.pipeline_end_datetime:
                    end_datetime_str = self.pipeline_end_datetime.strftime("%Y-%m-%d %H:%M:%S")

                update_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("status", "STRING", self.status),
                        bigquery.ScalarQueryParameter("pipeline_end_datetime", "DATETIME", end_datetime_str),
                        bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                        bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                    ]
                )

                gcp_services.execute_bq_sql(update_query, update_config)
            else:
                utils.logger.warning(f"No record found for site {self.site_name} on {self.delivery_date}. Update skipped.")
        except Exception as e:
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'query_job_errors': None,
                'context': {
                    'site_name': self.site_name,
                    'delivery_date': self.delivery_date,
                    'status': self.status
                }
            }
            raise Exception(f"Unable to add pipeline log record: {error_details}") from e

    def log_running(self) -> None:
        """
        Checks if a log entry exists in BigQuery for the given site and delivery date.
        If found, updates the record with the running status and removes end date
        """
        try:
            # First, check if a record exists for this site and delivery date.
            select_query = f"""
                SELECT 1
                FROM `{self.logging_table}`
                WHERE site_name = @site_name AND delivery_date = @delivery_date
                LIMIT 1
            """
            select_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                    bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                ]
            )

            exists = list(gcp_services.execute_bq_sql(select_query, select_config))

            if exists:
                # If the record exists and isn't already set to running, update it.
                update_query = f"""
                    UPDATE `{self.logging_table}`
                    SET status = @status, pipeline_end_datetime = NULL, message = NULL
                    WHERE site_name = @site_name AND delivery_date = @delivery_date
                    AND status != @status
                """

                update_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("status", "STRING", self.status),
                        bigquery.ScalarQueryParameter("site_name", "STRING", self.site_name),
                        bigquery.ScalarQueryParameter("delivery_date", "DATE", self.delivery_date),
                    ]
                )

                gcp_services.execute_bq_sql(update_query, update_config)
            else:
                utils.logger.warning(f"No record found for site {self.site_name} on {self.delivery_date}. Update skipped.")
        except Exception as e:
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'query_job_errors': None,
                'context': {
                    'site_name': self.site_name,
                    'delivery_date': self.delivery_date,
                    'status': self.status
                }
            }
            raise Exception(f"Unable to add pipeline log record: {error_details}") from e

    def log_error(self) -> None:
        """
        Checks if a log entry exists in BigQuery for the given site and delivery date.
        If found, updates the record with the error status, message, and pipeline_end_datetime.
        """
        try:
            # First, check if a record exists for this site and delivery date.
            select_query = f"""
                SELECT 1
                FROM `{self.logging_table}`
                WHERE run_id = @run_id
                LIMIT 1
            """
            select_config = bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id),
                ]
            )

            exists = list(gcp_services.execute_bq_sql(select_query, select_config))

            if exists:
                # If the record exists, update it.
                update_query = f"""
                    UPDATE `{self.logging_table}`
                    SET 
                    status = @status,
                    pipeline_end_datetime = @pipeline_end_datetime,
                    message = CASE 
                                WHEN IFNULL(message, '') != '' 
                                    AND @message = '{constants.PIPELINE_DAG_FAIL_MESSAGE}' 
                                THEN message 
                                ELSE @message 
                                END
                    WHERE run_id = @run_id;
                """
                # Ensure that pipeline_end_datetime is formatted for BigQuery (YYYY-MM-DD HH:MM:SS).
                if self.pipeline_end_datetime:
                    end_datetime_str = self.pipeline_end_datetime.strftime("%Y-%m-%d %H:%M:%S")

                update_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("status", "STRING", self.status),
                        bigquery.ScalarQueryParameter("pipeline_end_datetime", "DATETIME", end_datetime_str),
                        bigquery.ScalarQueryParameter("message", "STRING", self.message),
                        bigquery.ScalarQueryParameter("run_id", "STRING", self.run_id)
                    ]
                )
                
                gcp_services.execute_bq_sql(update_query, update_config)
            else:
                utils.logger.warning(f"No record found for site {self.site_name} on {self.delivery_date}. Update skipped.")
        except Exception as e:
            error_details = {
                'error_type': type(e).__name__,
                'error_message': str(e),
                'query_job_errors': None,
                'context': {
                    'site_name': self.site_name,
                    'delivery_date': self.delivery_date,
                    'status': self.status
                }
            }
            raise Exception(f"Unable to add pipeline log record: {error_details}") from e
