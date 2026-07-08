"""
Unit tests for merge.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/merge/
"""

from pathlib import Path

from core.merge import MergeProcessor

# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "merge"


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


class TestExtractAllScopeSql:
    """Tests for generate_extract_chunk_sql() with participant_scope == 'ALL'."""

    def test_all_scope_has_no_where_clause(self):
        """ALL scope copies the whole source table with no WHERE clause."""
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/measurement.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/measurement__siteA__2025-01-01.parquet",
            participant_scope="ALL",
        )

        expected = load_reference_sql("extract_chunk_all_scope.sql")
        assert normalize_sql(result) == normalize_sql(expected)
        assert "WHERE" not in normalize_sql(result).upper()


class TestExtractIdScopeSql:
    """Tests for generate_extract_chunk_sql() with a participant-id subset (v2 path)."""

    def test_id_scope_filters_by_person_id_in_subquery(self):
        """A non-ALL scope subsets by person_id IN (SELECT id ...)."""
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/measurement.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/measurement__siteA__2025-01-01.parquet",
            participant_scope="ehr_merged/2026-06-24/artifacts/merge_chunks/_ids/siteA__2025-01-01.parquet",
            person_id_column="person_id",
        )

        expected = load_reference_sql("extract_chunk_id_scope.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_id_scope_respects_custom_person_id_column(self):
        """A custom person_id column name is used in the WHERE clause."""
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/measurement.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet",
            participant_scope="ehr_merged/ids.parquet",
            person_id_column="subject_id",
        )

        assert "WHERE subject_id IN (" in result


class TestReconcileGlobUnionByNameSql:
    """Tests for generate_reconcile_chunks_sql()."""

    def test_reconcile_globs_chunks_with_union_by_name(self):
        """Reconcile reads a glob of chunks with union_by_name and writes one output."""
        result = MergeProcessor.generate_reconcile_chunks_sql(
            chunk_glob="ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/*.parquet",
            output_uri="ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet",
        )

        expected = load_reference_sql("reconcile_chunks_union_by_name.sql")
        assert normalize_sql(result) == normalize_sql(expected)
        assert "union_by_name=true" in result
