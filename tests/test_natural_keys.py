"""
Unit tests for natural_keys.py NaturalKeyProcessor class.

Tests apply()-flow orchestration, table skip rules, and error handling.
SQL-generation tests live in test_natural_keys_sql.py.
"""

from unittest.mock import patch

import pytest

import core.constants as constants
from core.natural_keys import NaturalKeyProcessor


class TestNaturalKeyProcessorInit:
    """Tests for NaturalKeyProcessor initialization."""

    def test_init_stores_parameters(self):
        """Test that initialization stores all parameters."""
        processor = NaturalKeyProcessor(
            file_path="test-bucket/2025-01-15/visit_occurrence.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        assert processor.file_path == "test-bucket/2025-01-15/visit_occurrence.parquet"
        assert processor.omop_version == "5.4"
        assert processor.site == "site_alpha"

    def test_init_derives_paths(self):
        """Test that initialization derives table name and parquet artifact path."""
        processor = NaturalKeyProcessor(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        assert processor.table_name == "condition_occurrence"
        assert (
            processor.parquet_file_path
            == "test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet"
        )


class TestNaturalKeyProcessorApply:
    """Tests for the apply() orchestration method."""

    @pytest.mark.parametrize("table_name", ["person", "concept", "vocabulary", "domain"])
    @patch('core.natural_keys.utils.execute_duckdb_sql')
    @patch('core.natural_keys.utils.parquet_file_exists')
    def test_skips_excluded_tables(self, mock_exists, mock_execute, table_name):
        """Test that apply() returns False for excluded tables without touching files."""
        processor = NaturalKeyProcessor(
            file_path=f"test-bucket/2025-01-15/{table_name}.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        result = processor.apply()

        assert result is False
        # Early-exit: should not check file existence or run SQL
        mock_exists.assert_not_called()
        mock_execute.assert_not_called()

    @patch('core.natural_keys.utils.parquet_file_exists')
    def test_raises_when_parquet_missing(self, mock_exists):
        """Test that apply() raises when the normalized parquet file is missing."""
        mock_exists.return_value = False

        processor = NaturalKeyProcessor(
            file_path="test-bucket/2025-01-15/visit_occurrence.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        with pytest.raises(Exception, match="Normalized parquet file not found"):
            processor.apply()

    @patch('core.natural_keys.utils.execute_duckdb_sql')
    @patch('core.natural_keys.utils.get_columns_from_file')
    @patch('core.natural_keys.utils.parquet_file_exists')
    def test_skips_when_no_in_scope_columns(
        self, mock_exists, mock_get_columns, mock_execute
    ):
        """Test that apply() skips files with no natural-key columns."""
        mock_exists.return_value = True
        # No in-scope columns — just a vocabulary-style table
        mock_get_columns.return_value = [
            "concept_id", "concept_name", "vocabulary_id"
        ]

        processor = NaturalKeyProcessor(
            file_path="test-bucket/2025-01-15/cost.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        result = processor.apply()

        assert result is False
        mock_execute.assert_not_called()

    @patch('core.natural_keys.storage.get_uri')
    @patch('core.natural_keys.utils.execute_duckdb_sql')
    @patch('core.natural_keys.utils.get_columns_from_file')
    @patch('core.natural_keys.utils.parquet_file_exists')
    def test_runs_rewrite_when_in_scope_columns_present(
        self, mock_exists, mock_get_columns, mock_execute, mock_get_uri
    ):
        """Test that apply() executes the rewrite when natural-key columns are present."""
        mock_exists.return_value = True
        mock_get_columns.return_value = [
            "condition_occurrence_id",  # surrogate PK - not rewritten
            "person_id",  # never rewritten
            "visit_occurrence_id",  # rewritten
            "provider_id",  # rewritten
        ]
        mock_get_uri.side_effect = lambda path: f"gs://{path}"

        processor = NaturalKeyProcessor(
            file_path="test-bucket/2025-01-15/condition_occurrence.parquet",
            omop_version="5.4",
            site="site_alpha",
        )

        result = processor.apply()

        assert result is True
        mock_execute.assert_called_once()

        executed_sql = mock_execute.call_args[0][0]
        # Verify the SQL targets the in-scope columns and salts with the site
        assert "visit_occurrence_id" in executed_sql
        assert "provider_id" in executed_sql
        assert "'site_alpha'" in executed_sql
        # And does NOT touch person_id or the surrogate PK
        assert "person_id IS NOT NULL" not in executed_sql
        assert "condition_occurrence_id IS NOT NULL" not in executed_sql


class TestSkipTablesConstant:
    """Sanity tests on the constants — defensive checks against accidental
    rule violations in future refactors."""

    def test_person_is_skipped(self):
        """person must always be in the skip list (project rule)."""
        assert "person" in constants.NATURAL_KEY_REWRITE_SKIP_TABLES

    def test_vocab_tables_skipped(self):
        """All vocabulary tables must be in the skip list (project rule)."""
        for vocab_table in ["concept", "vocabulary", "domain", "concept_class", "relationship"]:
            assert vocab_table in constants.NATURAL_KEY_REWRITE_SKIP_TABLES

    def test_person_id_not_in_rewrite_list(self):
        """person_id must never be in the rewrite list (project rule)."""
        assert "person_id" not in constants.GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS

    def test_no_concept_ids_in_rewrite_list(self):
        """No *_concept_id column should be in the rewrite list (vocab references)."""
        for col in constants.GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS:
            assert not col.endswith("_concept_id"), (
                f"{col} ends with _concept_id — concept references are vocabulary and must not be rewritten"
            )
