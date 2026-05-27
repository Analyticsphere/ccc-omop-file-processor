"""
Unit tests for post_processing.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/post_processing/
"""

from pathlib import Path

import core.constants as constants
import core.utils as utils
from core.post_processing import PostProcessor

REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "post_processing"


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


TABLE_URI = "gs://test-bucket/2025-01-15/artifacts/converted_files/person.parquet"
SNAPSHOT_URI = "gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/person_pre.parquet"
DEATH_TABLE_URI = "gs://test-bucket/2025-01-15/artifacts/converted_files/death.parquet"
DEATH_SNAPSHOT_URI = "gs://test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/death_pre.parquet"


class TestGenerateSnapshotPkSql:
    """Tests for generate_snapshot_pk_sql()."""

    def test_person_pk_snapshot(self):
        result = PostProcessor.generate_snapshot_pk_sql(
            table_uri=TABLE_URI,
            pk_column="person_id",
            snapshot_uri=SNAPSHOT_URI,
        )

        expected = load_reference_sql("generate_snapshot_pk_sql_person.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateSnapshotRowHashSql:
    """Tests for generate_snapshot_row_hash_sql()."""

    def test_death_row_hash_snapshot(self):
        result = PostProcessor.generate_snapshot_row_hash_sql(
            table_uri=DEATH_TABLE_URI,
            columns=[
                "person_id",
                "death_date",
                "death_datetime",
                "death_type_concept_id",
                "cause_concept_id",
            ],
            snapshot_uri=DEATH_SNAPSHOT_URI,
        )

        expected = load_reference_sql("generate_snapshot_row_hash_sql_death.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGeneratePkDiffSql:
    """Tests for generate_pk_diff_sql()."""

    def test_person_pk_diff(self):
        result = PostProcessor.generate_pk_diff_sql(
            snapshot_uri=SNAPSHOT_URI,
            table_uri=TABLE_URI,
            pk_column="person_id",
        )

        expected = load_reference_sql("generate_pk_diff_sql_person.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateRowHashDiffSql:
    """Tests for generate_row_hash_diff_sql()."""

    def test_death_row_hash_diff(self):
        result = PostProcessor.generate_row_hash_diff_sql(
            snapshot_uri=DEATH_SNAPSHOT_URI,
            table_uri=DEATH_TABLE_URI,
            columns=[
                "person_id",
                "death_date",
                "death_datetime",
                "death_type_concept_id",
                "cause_concept_id",
            ],
        )

        expected = load_reference_sql("generate_row_hash_diff_sql_death.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateSnapshotRowCountSql:
    """Tests for generate_snapshot_row_count_sql()."""

    def test_count_snapshot_rows(self):
        result = PostProcessor.generate_snapshot_row_count_sql(SNAPSHOT_URI)
        assert "SELECT COUNT(*) FROM read_parquet" in result
        assert SNAPSHOT_URI in result


class TestPlaceholderToPostProcessingPath:
    """Tests for utils.placeholder_to_post_processing_path()."""

    def test_routes_harmonized_table_to_omop_etl(self):
        result = utils.placeholder_to_post_processing_path(
            site="site_alpha",
            bucket="test-bucket",
            delivery_date="2025-01-15",
            sql_script="SELECT * FROM read_parquet('@CONDITION_OCCURRENCE')",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="/vocab",
        )

        assert "artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet" in result
        assert "@CONDITION_OCCURRENCE" not in result

    def test_routes_non_harmonized_table_to_converted_files(self):
        result = utils.placeholder_to_post_processing_path(
            site="site_alpha",
            bucket="test-bucket",
            delivery_date="2025-01-15",
            sql_script="SELECT * FROM read_parquet('@PERSON')",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="/vocab",
        )

        assert "artifacts/converted_files/person.parquet" in result
        assert "@PERSON" not in result

    def test_routes_post_processing_only_placeholder(self):
        """Test that post-processing-only placeholders (e.g. @CARE_SITE) resolve correctly."""
        result = utils.placeholder_to_post_processing_path(
            site="site_alpha",
            bucket="test-bucket",
            delivery_date="2025-01-15",
            sql_script="SELECT * FROM read_parquet('@CARE_SITE')",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="/vocab",
        )

        assert "artifacts/converted_files/care_site.parquet" in result
        assert "@CARE_SITE" not in result

    def test_substitutes_site_and_current_date(self):
        result = utils.placeholder_to_post_processing_path(
            site="site_alpha",
            bucket="test-bucket",
            delivery_date="2025-01-15",
            sql_script="SELECT '@SITE' AS s, '@CURRENT_DATE' AS d",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="/vocab",
        )

        assert "'site_alpha'" in result
        assert "@SITE" not in result
        assert "@CURRENT_DATE" not in result

    def test_substitutes_vocab_placeholder(self):
        result = utils.placeholder_to_post_processing_path(
            site="site_alpha",
            bucket="test-bucket",
            delivery_date="2025-01-15",
            sql_script="SELECT * FROM read_parquet('@CONCEPT')",
            vocab_version="v5.0_24-JAN-25",
            vocab_path="/vocab",
        )

        assert "/vocab/v5.0_24-JAN-25/optimized/concept.parquet" in result
        assert "@CONCEPT" not in result


class TestPostProcessingExtraPlaceholdersConstant:
    """Sanity checks on the placeholder constant to catch accidental removals."""

    def test_all_new_placeholders_registered(self):
        expected_placeholders = {
            "@CARE_SITE", "@LOCATION", "@PROVIDER", "@VISIT_DETAIL", "@EPISODE",
            "@COST", "@PAYER_PLAN_PERIOD", "@METADATA", "@CDM_SOURCE",
            "@FACT_RELATIONSHIP", "@NOTE_NLP",
        }
        actual = set(constants.POST_PROCESSING_EXTRA_PATH_PLACEHOLDERS.keys())
        assert expected_placeholders == actual

    def test_no_overlap_with_clinical_data_placeholders(self):
        """The post-processing extras must not collide with existing placeholders."""
        overlap = (
            set(constants.POST_PROCESSING_EXTRA_PATH_PLACEHOLDERS.keys())
            & set(constants.CLINICAL_DATA_PATH_PLACEHOLDERS.keys())
        )
        assert overlap == set()
