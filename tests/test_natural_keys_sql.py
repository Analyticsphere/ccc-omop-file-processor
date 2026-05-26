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


class TestHashOutputStability:
    """
    Stability tests for the hash expression's output.

    DuckDB's hash() function is not guaranteed to be stable across major
    versions. Because we use it to assign globally-unique IDs that should
    join across deliveries, any drift in the hash output may silently break
    cross-site referential integrity.

    These tests pin the expected output to DuckDB 1.4.4. 
    If a future DuckDB upgrade changes the hash function, 
    OR if the hash expression itself is modified, these tests will fail
    """

    # Pinned to duckdb==1.4.4 (see requirements.txt). If this changes,
    # the hash output for the same input may change too.
    EXPECTED_HASHES = [
        # (input_value, site, expected_hash)
        (69575077305629080, "synthea53", 2555308237151760442),
        (1, "siteA", 1375360430257248328),
        (9999999999999, "a-different-site", 8438791239449742916),
        (-1, "synthea53", 4467886651043547291),
    ]

    def test_hash_output_matches_pinned_values(self):
        """
        Run the exact hash SQL the endpoint uses against live DuckDB and
        confirm output matches values pinned to DuckDB 1.4.4. If this test
        fails, DuckDB hash output has drifted — investigate before shipping.
        """
        import duckdb

        for value, site, expected in self.EXPECTED_HASHES:
            sql = (
                f"SELECT CAST((CAST(hash(CONCAT(CAST({value} AS VARCHAR), '{site}')) "
                f"AS UBIGINT) % 9223372036854775807) AS BIGINT)"
            )
            actual = duckdb.sql(sql).fetchone()[0]
            assert actual == expected, (
                f"DuckDB hash output drift detected for value={value}, site={site!r}: "
                f"expected {expected}, got {actual}. "
                f"DuckDB version: {duckdb.__version__}. "
                f"See NATURAL_KEY_FOLLOWUPS.md."
            )

    def test_hash_expression_generates_matching_sql(self):
        """
        Confirm generate_hash_expression() produces SQL whose DuckDB output
        matches the pinned values. This guards against changes to the
        Python-side expression as well as DuckDB drift.
        """
        import duckdb

        for value, site, expected in self.EXPECTED_HASHES:
            # Embed the hash expression in a SELECT against a single-row CTE
            hash_expr = NaturalKeyProcessor.generate_hash_expression(
                column_name="v", site=site
            )
            sql = f"WITH t AS (SELECT CAST({value} AS BIGINT) AS v) SELECT {hash_expr} FROM t"
            actual = duckdb.sql(sql).fetchone()[0]
            assert actual == expected, (
                f"Generated hash expression output drift for value={value}, site={site!r}: "
                f"expected {expected}, got {actual}. "
                f"Either the expression in generate_hash_expression() changed, "
                f"or DuckDB hash output drifted (version: {duckdb.__version__}). "
                f"See NATURAL_KEY_FOLLOWUPS.md."
            )

    def test_null_input_returns_null(self):
        """NULL inputs must pass through unchanged — never hashed."""
        import duckdb

        hash_expr = NaturalKeyProcessor.generate_hash_expression(
            column_name="v", site="synthea53"
        )
        sql = f"WITH t AS (SELECT CAST(NULL AS BIGINT) AS v) SELECT {hash_expr} FROM t"
        actual = duckdb.sql(sql).fetchone()[0]
        assert actual is None, (
            f"NULL input must produce NULL output (project rule), got {actual}"
        )


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
