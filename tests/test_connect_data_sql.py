"""
Unit tests for Connect data SQL generation.

Tests that the participant status SQL is correctly built with and without
a site_connect_id filter. Reference SQL files are stored in
tests/reference/sql/connect_data/
"""

from pathlib import Path

from core.gcp_services import build_connect_participant_status_sql

REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "connect_data"


def normalize_sql(sql: str) -> str:
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    filepath = REFERENCE_DIR / filename
    with open(filepath, 'r') as f:
        return f.read()


class TestBuildConnectParticipantStatusSql:
    """Tests for build_connect_participant_status_sql()."""

    def test_with_site_connect_id(self):
        """Test SQL includes WHERE filter when site_connect_id is provided."""
        result = build_connect_participant_status_sql(
            project_id="test-project",
            dataset_id="test_dataset",
            site_connect_id="452456345"
        )

        expected = load_reference_sql("participant_status_with_site_filter.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_without_site_connect_id(self):
        """Test SQL omits WHERE filter when site_connect_id is None."""
        result = build_connect_participant_status_sql(
            project_id="test-project",
            dataset_id="test_dataset",
            site_connect_id=None
        )

        expected = load_reference_sql("participant_status_all_sites.sql")
        assert normalize_sql(result) == normalize_sql(expected)
