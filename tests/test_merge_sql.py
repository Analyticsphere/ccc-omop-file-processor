"""
Unit tests for merge.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/merge/
"""

from pathlib import Path

import pytest

import core.constants as constants
from core.merge import MergeProcessor
from core.merge_reporting import MergeReporter

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


class TestMergeReportRowCountSql:
    """Tests for MergeReporter row-count SQL + chunk-glob builders."""

    def test_delivery_chunk_glob_matches_all_tables_for_one_delivery(self):
        """The glob spans every per-table subfolder for a single (site, delivery_date)."""
        glob = MergeReporter.delivery_chunk_glob(
            merge_bucket="ehr_merged", run_date="2026-06-24", site="siteA", delivery_date="2025-01-01"
        )
        assert glob == "ehr_merged/2026-06-24/artifacts/merge_chunks/*/*__siteA__2025-01-01.parquet"

    def test_row_count_globs_delivery_chunks_with_union_by_name(self):
        """Counts one delivery's rows across all its per-table chunks via union_by_name."""
        glob = MergeReporter.delivery_chunk_glob(
            merge_bucket="ehr_merged", run_date="2026-06-24", site="siteA", delivery_date="2025-01-01"
        )
        result = MergeReporter.generate_delivery_row_count_sql(glob)

        expected = load_reference_sql("merge_delivery_row_count.sql")
        assert normalize_sql(result) == normalize_sql(expected)
        assert "union_by_name=true" in result


class TestHashCareSiteId:
    """Tests for the site-name -> care_site_id hash."""

    def test_deterministic_and_in_positive_signed_64_bit_range(self):
        first = MergeProcessor.hash_care_site_id("Site A")
        assert first == MergeProcessor.hash_care_site_id("Site A")
        assert 1 <= first <= 0x7FFFFFFFFFFFFFFF

    def test_distinct_names_give_distinct_ids(self):
        assert MergeProcessor.hash_care_site_id("Site A") != MergeProcessor.hash_care_site_id("Site B")


class TestExtractStampsCareSiteId:
    """Tests that a site name stamps care_site_id via SELECT * REPLACE."""

    def test_all_scope_replaces_care_site_id_with_hash(self):
        care_site_id = MergeProcessor.hash_care_site_id("Site A")
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/person.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/person/person__siteA__2025-01-01.parquet",
            participant_scope="ALL",
            site_display_name="Site A",
        )
        assert f"* REPLACE (CAST({care_site_id} AS BIGINT) AS care_site_id)" in result

    def test_id_scope_replaces_care_site_id_with_hash(self):
        care_site_id = MergeProcessor.hash_care_site_id("Site A")
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/person.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/person/chunk.parquet",
            participant_scope="ehr_merged/ids.parquet",
            site_display_name="Site A",
        )
        assert f"* REPLACE (CAST({care_site_id} AS BIGINT) AS care_site_id)" in result
        assert "WHERE person_id IN (" in result

    def test_no_site_name_selects_star(self):
        result = MergeProcessor.generate_extract_chunk_sql(
            source_uri="siteA/2025-01-01/artifacts/converted_files/person.parquet",
            chunk_uri="ehr_merged/2026-06-24/artifacts/merge_chunks/person/chunk.parquet",
            participant_scope="ALL",
        )
        assert "REPLACE" not in result
        assert "SELECT * FROM read_parquet(" in normalize_sql(result)


class TestBuildCareSiteSql:
    """Tests for generate_build_care_site_sql()."""

    OUTPUT = "ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet"

    def test_one_row_per_site_with_hashed_id_and_null_padding(self):
        result = MergeProcessor.generate_build_care_site_sql(
            self.OUTPUT, ["Site A", "Site B"], "5.4"
        )
        id_a = MergeProcessor.hash_care_site_id("Site A")
        id_b = MergeProcessor.hash_care_site_id("Site B")
        # schema order: care_site_id, care_site_name, then 4 unset columns
        assert f"({id_a}, 'Site A', NULL, NULL, NULL, NULL)" in result
        assert f"({id_b}, 'Site B', NULL, NULL, NULL, NULL)" in result
        # projection/types come from the OMOP care_site schema
        assert "CAST(care_site_id AS BIGINT) AS care_site_id" in result
        assert "CAST(care_site_name AS VARCHAR) AS care_site_name" in result

    def test_escapes_single_quotes_in_name(self):
        result = MergeProcessor.generate_build_care_site_sql(self.OUTPUT, ["O'Neil Clinic"], "5.4")
        assert "'O''Neil Clinic'" in result

    def test_dedups_repeated_site_names(self):
        result = MergeProcessor.generate_build_care_site_sql(self.OUTPUT, ["Site A", "Site A"], "5.4")
        id_a = MergeProcessor.hash_care_site_id("Site A")
        assert result.count(f"({id_a}, 'Site A'") == 1

    def test_empty_site_list_raises(self):
        with pytest.raises(ValueError):
            MergeProcessor.generate_build_care_site_sql(self.OUTPUT, [], "5.4")


class TestBuildCdmSourceSql:
    """Tests for generate_build_cdm_source_sql()."""

    OUTPUT = "ehr_merged/2026-06-24/artifacts/converted_files/cdm_source.parquet"
    SOURCES = [
        "siteA/2025-01-01/artifacts/converted_files/cdm_source.parquet",
        "siteB/2025-02-01/artifacts/converted_files/cdm_source.parquet",
    ]

    def _sql(self, cdm_version="5.4"):
        return MergeProcessor.generate_build_cdm_source_sql(
            self.OUTPUT, self.SOURCES, site_count=2, cdm_version=cdm_version,
            vocabulary_version="v5.0 27-AUG-25", cdm_release_date="2026-06-24",
        )

    def test_fixed_metadata_and_site_count(self):
        result = self._sql()
        assert f"'{constants.MERGE_CDM_SOURCE_NAME}' AS cdm_source_name" in result
        assert f"'{constants.MERGE_CDM_SOURCE_ABBREVIATION}' AS cdm_source_abbreviation" in result
        assert f"'{constants.MERGE_CDM_HOLDER}' AS cdm_holder" in result
        assert constants.MERGE_CDM_SOURCE_DESCRIPTION.format(site_count=2) in result

    def test_source_release_date_is_latest_across_sites(self):
        result = self._sql()
        assert "MAX(source_release_date)" in result
        # union over both sites' cdm_source files
        assert "read_parquet([" in result and "union_by_name=true" in result
        for uri in self.SOURCES:
            assert uri in result

    def test_release_date_versions_and_concept_id(self):
        result = self._sql(cdm_version="5.4")
        assert "CAST('2026-06-24' AS DATE) AS cdm_release_date" in result
        assert "'5.4' AS cdm_version" in result
        assert "756265 AS cdm_version_concept_id" in result  # 5.4 concept id
        assert "'v5.0 27-AUG-25' AS vocabulary_version" in result

    def test_empty_sources_raises(self):
        with pytest.raises(ValueError):
            MergeProcessor.generate_build_cdm_source_sql(
                self.OUTPUT, [], site_count=0, cdm_version="5.4",
                vocabulary_version="v", cdm_release_date="2026-06-24",
            )
