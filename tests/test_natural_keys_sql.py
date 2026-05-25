"""
Unit tests for natural_keys.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/natural_keys/
"""

from pathlib import Path

from core.natural_keys import NaturalKeyProcessor

REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "natural_keys"


def normalize_sql(sql: str) -> str:
    """
    Normalize SQL for comparison by removing extra whitespace.
    Makes SQL comparison whitespace-insensitive.
    """
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    """Load reference SQL from file."""
    filepath = REFERENCE_DIR / filename
    with open(filepath, 'r') as f:
        return f.read()


class TestGenerateHashExpression:
    """Tests for generate_hash_expression()."""

    def test_visit_occurrence_id(self):
        """Test hash expression for visit_occurrence_id with site_alpha."""
        result = NaturalKeyProcessor.generate_hash_expression(
            column_name="visit_occurrence_id",
            site="site_alpha"
        )

        expected = load_reference_sql("generate_hash_expression_simple.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_provider_id_different_site(self):
        """Test hash expression with a different column and site."""
        result = NaturalKeyProcessor.generate_hash_expression(
            column_name="provider_id",
            site="my-site"
        )

        expected = load_reference_sql("generate_hash_expression_provider_id.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateRewriteSql:
    """Tests for generate_rewrite_sql()."""

    def test_visit_occurrence_multiple_columns(self):
        """Test SQL generation for visit_occurrence (PK + self-FK + FKs)."""
        table_uri = "gs://test-bucket/2025-01-15/artifacts/converted_files/visit_occurrence.parquet"
        columns_to_rewrite = [
            "visit_occurrence_id",
            "preceding_visit_occurrence_id",
            "provider_id",
            "care_site_id",
        ]

        result = NaturalKeyProcessor.generate_rewrite_sql(
            table_uri=table_uri,
            columns_to_rewrite=columns_to_rewrite,
            site="site_alpha"
        )

        expected = load_reference_sql("generate_rewrite_sql_visit_occurrence.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_condition_occurrence_fks_only(self):
        """Test SQL generation for condition_occurrence (FKs only, no PK rewrite)."""
        table_uri = "gs://test-bucket/2025-01-15/artifacts/converted_files/condition_occurrence.parquet"
        columns_to_rewrite = [
            "visit_occurrence_id",
            "visit_detail_id",
            "provider_id",
        ]

        result = NaturalKeyProcessor.generate_rewrite_sql(
            table_uri=table_uri,
            columns_to_rewrite=columns_to_rewrite,
            site="site_beta"
        )

        expected = load_reference_sql("generate_rewrite_sql_condition_occurrence.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_single_column(self):
        """Test SQL generation for the minimal single-column case."""
        table_uri = "gs://test-bucket/2025-01-15/artifacts/converted_files/care_site.parquet"
        columns_to_rewrite = ["location_id"]

        result = NaturalKeyProcessor.generate_rewrite_sql(
            table_uri=table_uri,
            columns_to_rewrite=columns_to_rewrite,
            site="site_gamma"
        )

        expected = load_reference_sql("generate_rewrite_sql_single_column.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestFindColumnsToRewrite:
    """Tests for find_columns_to_rewrite()."""

    def test_finds_all_in_scope_columns(self):
        """Test that all in-scope columns present in the file are returned."""
        actual_columns = [
            "visit_occurrence_id",
            "person_id",
            "visit_concept_id",
            "provider_id",
            "care_site_id",
            "visit_start_date",
        ]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        assert set(result) == {"visit_occurrence_id", "provider_id", "care_site_id"}

    def test_returns_empty_when_no_natural_keys(self):
        """Test that empty list returned when no in-scope columns present."""
        # Pure vocabulary-style columns — no natural keys
        actual_columns = ["concept_id", "concept_name", "vocabulary_id"]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        assert result == []

    def test_case_insensitive_match(self):
        """Test that column names match case-insensitively."""
        actual_columns = ["Visit_Occurrence_ID", "PROVIDER_ID"]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        assert set(result) == {"visit_occurrence_id", "provider_id"}

    def test_preserves_canonical_order(self):
        """Test that returned columns follow the order of the constants list."""
        # provider_id appears before care_site_id in GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS
        actual_columns = ["care_site_id", "provider_id", "visit_occurrence_id"]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        # Should match the constant order, not the input order
        assert result == ["visit_occurrence_id", "provider_id", "care_site_id"]

    def test_excludes_person_id(self):
        """Test that person_id is never in the rewrite list (project rule)."""
        actual_columns = ["person_id", "visit_occurrence_id"]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        assert "person_id" not in result
        assert result == ["visit_occurrence_id"]

    def test_excludes_concept_id_columns(self):
        """Test that *_concept_id columns are never rewritten."""
        actual_columns = [
            "visit_concept_id",
            "visit_source_concept_id",
            "condition_concept_id",
            "visit_occurrence_id",
        ]

        result = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        assert result == ["visit_occurrence_id"]
