"""
Unit tests for post_processing.py PostProcessor class.

Tests apply()-flow orchestration, artifact emission, dedup integration, and
error handling. SQL-generation tests live in test_post_processing_sql.py.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest

from core.post_processing import PostProcessor

DEFAULT_KWARGS = dict(
    site="site_alpha",
    bucket="test-bucket",
    delivery_date="2025-01-15",
    omop_version="5.4",
    vocab_version="v5.0_24-JAN-25",
    vocab_path="/vocab",
    task_name="example_task",
)


class TestPostProcessorInit:
    """Tests for PostProcessor initialization."""

    def test_init_stores_parameters(self):
        processor = PostProcessor(**DEFAULT_KWARGS)

        assert processor.site == "site_alpha"
        assert processor.bucket == "test-bucket"
        assert processor.delivery_date == "2025-01-15"
        assert processor.omop_version == "5.4"
        assert processor.vocab_version == "v5.0_24-JAN-25"
        assert processor.vocab_path == "/vocab"
        assert processor.task_name == "example_task"

    def test_init_derives_paths(self):
        processor = PostProcessor(**DEFAULT_KWARGS)

        assert processor.sql_script_path == (
            "reference/sql/post_processing/example_task.sql"
        )
        assert processor.tmp_dir == (
            "test-bucket/2025-01-15/artifacts/post_processing/example_task/tmp/"
        )


class TestResolveTableArtifactPath:
    """Tests for _resolve_table_artifact_path() routing."""

    def test_routes_harmonized_to_omop_etl(self):
        processor = PostProcessor(**DEFAULT_KWARGS)
        path = processor._resolve_table_artifact_path("condition_occurrence")
        assert path == (
            "test-bucket/2025-01-15/artifacts/omop_etl/"
            "condition_occurrence/condition_occurrence.parquet"
        )

    def test_routes_derived_to_derived_files(self):
        processor = PostProcessor(**DEFAULT_KWARGS)
        path = processor._resolve_table_artifact_path("drug_era")
        assert path == "test-bucket/2025-01-15/artifacts/derived_files/drug_era.parquet"

    def test_routes_default_to_converted_files(self):
        processor = PostProcessor(**DEFAULT_KWARGS)
        path = processor._resolve_table_artifact_path("person")
        assert path == "test-bucket/2025-01-15/artifacts/converted_files/person.parquet"


class TestPostProcessorApply:
    """Tests for the apply() orchestration method."""

    @patch("core.post_processing.os.path.isfile")
    def test_raises_when_script_missing(self, mock_isfile):
        mock_isfile.return_value = False
        processor = PostProcessor(**DEFAULT_KWARGS)

        with pytest.raises(FileNotFoundError, match="Post-processing task SQL script not found"):
            processor.apply()

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_no_changes_returns_empty_dict_and_no_artifacts(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """A task that produces zero diffs returns {} and writes no artifacts."""
        mock_isfile.return_value = True
        mock_render.return_value = "SELECT 1"
        mock_schema.return_value = {"person": {"columns": {"person_id": {"primary_key": "true", "type": "BIGINT"}}}}
        mock_exists.return_value = True
        # Snapshot copy returns None; diff queries return (added=0, removed=0)
        mock_execute.side_effect = _execute_side_effect(diff_result=(0, 0))

        with patch("builtins.open", mock_open(read_data="SELECT 1 -- task body")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            result = processor.apply()

        assert result == {}
        mock_artifact_class.assert_not_called()
        mock_dedupe.assert_not_called()

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_delete_only_emits_removed_artifact(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """A delete-only task emits removed-count + table-affected artifacts."""
        mock_isfile.return_value = True
        mock_render.return_value = "DELETE TEST"
        mock_schema.return_value = {
            "person": {"columns": {"person_id": {"primary_key": "true", "type": "BIGINT"}}}
        }
        mock_exists.return_value = True
        mock_execute.side_effect = _execute_side_effect(diff_result=(0, 5))

        with patch("builtins.open", mock_open(read_data="DELETE TEST")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            result = processor.apply()

        assert result == {"person": {"added": 0, "removed": 5}}
        # 3 artifacts per affected table: added/removed/affected
        assert mock_artifact_class.call_count == 3
        artifact_names = {call.kwargs["name"] for call in mock_artifact_class.call_args_list}
        assert any("rows removed from person" in n for n in artifact_names)
        assert any("rows added in person" in n for n in artifact_names)
        assert any("table affected" in n for n in artifact_names)

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_runs_dedupe_on_affected_surrogate_key_table(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """condition_occurrence is a SURROGATE_KEY_TABLE; if affected it gets dedupe."""
        mock_isfile.return_value = True
        mock_render.return_value = "INSERT TEST"
        mock_schema.return_value = {
            "condition_occurrence": {
                "columns": {"condition_occurrence_id": {"primary_key": "true", "type": "BIGINT"}}
            }
        }
        mock_exists.return_value = True
        mock_execute.side_effect = _execute_side_effect(diff_result=(3, 0))

        with patch("builtins.open", mock_open(read_data="INSERT TEST")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            processor.apply()

        mock_dedupe.assert_called_once()
        called_kwargs = mock_dedupe.call_args.kwargs
        assert called_kwargs["table_name"] == "condition_occurrence"
        assert called_kwargs["cdm_version"] == "5.4"

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_no_dedupe_on_natural_key_table(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """person is a natural-key table; dedupe should NOT run even if affected."""
        mock_isfile.return_value = True
        mock_render.return_value = "DELETE TEST"
        mock_schema.return_value = {
            "person": {"columns": {"person_id": {"primary_key": "true", "type": "BIGINT"}}}
        }
        mock_exists.return_value = True
        mock_execute.side_effect = _execute_side_effect(diff_result=(0, 2))

        with patch("builtins.open", mock_open(read_data="DELETE TEST")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            processor.apply()

        mock_dedupe.assert_not_called()

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.get_columns_from_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_no_pk_table_uses_row_hash_path(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_columns,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """death has no PK; snapshot/diff should go through the row-hash branch."""
        mock_isfile.return_value = True
        mock_render.return_value = "SELECT 1"
        # death has no primary_key column in its schema entry
        mock_schema.return_value = {
            "death": {"columns": {
                "person_id": {"type": "BIGINT"},
                "death_date": {"type": "DATE"},
            }}
        }
        mock_exists.return_value = True
        mock_columns.return_value = ["person_id", "death_date"]
        mock_execute.side_effect = _execute_side_effect(diff_result=(0, 0))

        with patch("builtins.open", mock_open(read_data="SELECT 1")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            processor.apply()

        # Verify the row-hash SQL was generated (look for hash(CONCAT(...)) in any call)
        executed_sqls = [c.args[0] for c in mock_execute.call_args_list]
        assert any("hash(CONCAT" in sql for sql in executed_sqls)
        mock_columns.assert_called_once()

    @patch("core.post_processing.report_artifact.ReportArtifact")
    @patch("core.post_processing.vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file")
    @patch("core.post_processing.storage.delete_file")
    @patch("core.post_processing.utils.execute_duckdb_sql")
    @patch("core.post_processing.utils.parquet_file_exists")
    @patch("core.post_processing.utils.get_cdm_schema")
    @patch("core.post_processing.utils.placeholder_to_post_processing_path")
    @patch("core.post_processing.os.path.isfile")
    def test_cleanup_runs_even_on_sql_failure(
        self,
        mock_isfile,
        mock_render,
        mock_schema,
        mock_exists,
        mock_execute,
        mock_delete,
        mock_dedupe,
        mock_artifact_class,
    ):
        """If the task SQL raises, snapshots must still be cleaned up."""
        mock_isfile.return_value = True
        mock_render.return_value = "BAD SQL"
        mock_schema.return_value = {
            "person": {"columns": {"person_id": {"primary_key": "true", "type": "BIGINT"}}}
        }
        mock_exists.return_value = True

        # First call: snapshot copy succeeds.
        # Second call: the rendered task SQL itself blows up.
        call_count = {"n": 0}

        def _side_effect(sql, *args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] == 1:
                # snapshot
                return None
            # task SQL
            raise Exception("boom")

        mock_execute.side_effect = _side_effect

        with patch("builtins.open", mock_open(read_data="BAD SQL")):
            processor = PostProcessor(**DEFAULT_KWARGS)
            with pytest.raises(Exception, match="boom"):
                processor.apply()

        mock_delete.assert_called()  # snapshot was cleaned up


def _execute_side_effect(diff_result):
    """
    Build a side_effect for utils.execute_duckdb_sql that:
      - returns None for COPY/snapshot statements (no return_results)
      - returns [diff_result] for diff queries (return_results=True)
    """
    def _side_effect(sql, *args, **kwargs):
        if kwargs.get("return_results"):
            # Diff queries return (added, removed); orphan queries return (count,)
            if " AS removed" in sql:
                return [diff_result]
            # row-count or orphan single-column result
            return [(0,)]
        return None
    return _side_effect
