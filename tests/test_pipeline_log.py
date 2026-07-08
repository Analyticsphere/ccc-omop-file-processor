"""
Unit tests for pipeline_log.get_latest_completed_delivery().

BigQuery is mocked; these tests assert the query shape (status filter, ordering,
limit) and the None-return behavior when nothing is completed or the table is absent.
"""

from datetime import date
from unittest.mock import MagicMock, patch

from google.cloud.exceptions import NotFound

import core.constants as constants
import core.helpers.pipeline_log as pipeline_log


def _make_client(rows=None, table_exists=True):
    """Build a mocked bigquery.Client whose query() returns the given rows."""
    mock_client = MagicMock()

    if table_exists:
        mock_client.get_table.return_value = MagicMock()
    else:
        mock_client.get_table.side_effect = NotFound("table not found")

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = rows if rows is not None else []
    mock_client.query.return_value = mock_query_job

    return mock_client


class TestGetLatestCompletedDelivery:
    """Tests for the happy-path query."""

    @patch('core.helpers.pipeline_log.bigquery.Client')
    def test_returns_latest_completed_date(self, mock_client_cls):
        mock_client = _make_client(rows=[{"delivery_date": date(2025, 3, 1)}])
        mock_client_cls.return_value = mock_client

        result = pipeline_log.get_latest_completed_delivery("siteA")

        assert result == "2025-03-01"

    @patch('core.helpers.pipeline_log.bigquery.Client')
    def test_query_filters_by_completed_status_ordered_desc_limit_one(self, mock_client_cls):
        mock_client = _make_client(rows=[{"delivery_date": date(2025, 3, 1)}])
        mock_client_cls.return_value = mock_client

        pipeline_log.get_latest_completed_delivery("siteA")

        query_text = mock_client.query.call_args[0][0]
        assert "status = @status" in query_text
        assert "ORDER BY delivery_date DESC" in query_text
        assert "LIMIT 1" in query_text

        # The status parameter binds to the 'completed' status constant.
        job_config = mock_client.query.call_args[1]["job_config"]
        status_params = [p for p in job_config.query_parameters if p.name == "status"]
        assert status_params and status_params[0].value == constants.PIPELINE_COMPLETE_STRING

    @patch('core.helpers.pipeline_log.bigquery.Client')
    def test_string_delivery_date_passthrough(self, mock_client_cls):
        """A delivery_date already returned as a string is passed through unchanged."""
        mock_client = _make_client(rows=[{"delivery_date": "2024-12-31"}])
        mock_client_cls.return_value = mock_client

        assert pipeline_log.get_latest_completed_delivery("siteA") == "2024-12-31"


class TestGetLatestCompletedNoCompleted:
    """Tests for the no-result and missing-table cases."""

    @patch('core.helpers.pipeline_log.bigquery.Client')
    def test_no_completed_delivery_returns_none(self, mock_client_cls):
        mock_client = _make_client(rows=[])
        mock_client_cls.return_value = mock_client

        assert pipeline_log.get_latest_completed_delivery("siteA") is None

    @patch('core.helpers.pipeline_log.bigquery.Client')
    def test_missing_logging_table_returns_none_without_query(self, mock_client_cls):
        mock_client = _make_client(table_exists=False)
        mock_client_cls.return_value = mock_client

        assert pipeline_log.get_latest_completed_delivery("siteA") is None
        mock_client.query.assert_not_called()
