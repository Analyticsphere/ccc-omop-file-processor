import os
from typing import Optional

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
import core.vocab_harmonization as vocab_harmonization
from core.storage_backend import storage


class PostProcessor:
    """
    Apply one user-curated post-processing SQL task to the on-disk OMOP artifacts
    and produce per-task report artifacts describing what changed.

    Flow:
      1. Load reference/sql/post_processing/<task_name>.sql
      2. Snapshot the row-identity set of every OMOP table on disk
      3. Execute the task SQL via DuckDB
      4. Diff snapshots to per-table added/removed row counts
      5. Emit ReportArtifact rows for affected tables
      6. Re-run vocab_harmonization dedup on affected surrogate-key tables
      7. Clean up the snapshot tmp directory
    """

    def __init__(
        self,
        site: str,
        bucket: str,
        delivery_date: str,
        omop_version: str,
        vocab_version: str,
        vocab_path: str,
        task_name: str,
    ):
        self.site = site
        self.bucket = bucket
        self.delivery_date = delivery_date
        self.omop_version = omop_version
        self.vocab_version = vocab_version
        self.vocab_path = vocab_path
        self.task_name = task_name
        self.sql_script_path = f"{constants.POST_PROCESSING_SCRIPT_PATH}{task_name}.sql"
        self.tmp_dir = (
            f"{bucket}/{delivery_date}/"
            f"{constants.ArtifactPaths.POST_PROCESSING_TMP.value}{task_name}/tmp/"
        )

    def apply(self) -> dict[str, dict[str, int]]:
        """
        Run the post-processing task.

        Returns:
            {table_name: {"added": N, "removed": N}, ...} for tables whose row
            identity set changed. Tables with no change are omitted.

        Raises:
            FileNotFoundError if the task SQL script does not exist.
        """
        if not os.path.isfile(self.sql_script_path):
            raise FileNotFoundError(
                f"Post-processing task SQL script not found at {self.sql_script_path}"
            )

        with open(self.sql_script_path, "r") as f:
            sql_script_raw = f.read()

        rendered_sql = utils.placeholder_to_post_processing_path(
            site=self.site,
            bucket=self.bucket,
            delivery_date=self.delivery_date,
            sql_script=sql_script_raw,
            vocab_version=self.vocab_version,
            vocab_path=self.vocab_path,
        )

        in_scope_tables = self._discover_in_scope_tables()
        utils.logger.info(
            f"Post-processing task '{self.task_name}': snapshotting {len(in_scope_tables)} tables"
        )

        snapshots = self._snapshot_tables(in_scope_tables)

        try:
            utils.execute_duckdb_sql(
                rendered_sql,
                f"Unable to execute post-processing task '{self.task_name}'",
            )

            changes = self._compute_changes(in_scope_tables, snapshots)

            self._emit_per_table_artifacts(changes)
            self._dedupe_surrogate_keys(changes, in_scope_tables)
        finally:
            self._cleanup_snapshots(snapshots)

        utils.logger.info(
            f"Post-processing task '{self.task_name}' applied: "
            f"{len(changes)} table(s) affected"
        )
        return changes

    # ---- table discovery ----------------------------------------------------

    def _discover_in_scope_tables(self) -> dict[str, str]:
        """
        Return {table_name: table_uri} for every OMOP table that has a parquet
        artifact on disk at its canonical location, excluding vocabulary tables.
        """
        in_scope: dict[str, str] = {}
        schema = utils.get_cdm_schema(self.omop_version)

        for table_name in schema.keys():
            if table_name in constants.VOCABULARY_TABLES:
                continue

            file_path = self._resolve_table_artifact_path(table_name)
            if utils.parquet_file_exists(file_path):
                in_scope[table_name] = storage.get_uri(file_path)

        return in_scope

    def _resolve_table_artifact_path(self, table_name: str) -> str:
        """Resolve where on disk an OMOP table lives at post-processing time."""
        if table_name in constants.VOCAB_HARMONIZED_TABLES:
            return (
                f"{self.bucket}/{self.delivery_date}/"
                f"{constants.ArtifactPaths.OMOP_ETL.value}{table_name}/"
                f"{table_name}{constants.PARQUET}"
            )
        if table_name in constants.DERIVED_DATA_TABLES_REQUIREMENTS:
            return (
                f"{self.bucket}/{self.delivery_date}/"
                f"{constants.ArtifactPaths.DERIVED_FILES.value}"
                f"{table_name}{constants.PARQUET}"
            )
        return (
            f"{self.bucket}/{self.delivery_date}/"
            f"{constants.ArtifactPaths.CONVERTED_FILES.value}"
            f"{table_name}{constants.PARQUET}"
        )

    # ---- snapshots ----------------------------------------------------------

    def _snapshot_tables(self, in_scope_tables: dict[str, str]) -> dict[str, dict]:
        """
        Write a small parquet snapshot per table capturing each row's identity
        (PK column, or row content hash if the table has no PK).
        """
        snapshots: dict[str, dict] = {}
        for table_name, table_uri in in_scope_tables.items():
            pk_column = utils.get_primary_key_column(table_name, self.omop_version)
            snapshot_uri = storage.get_uri(
                f"{self.tmp_dir}{table_name}_pre{constants.PARQUET}"
            )

            if pk_column:
                sql = PostProcessor.generate_snapshot_pk_sql(
                    table_uri=table_uri,
                    pk_column=pk_column,
                    snapshot_uri=snapshot_uri,
                )
                snapshots[table_name] = {
                    "snapshot_uri": snapshot_uri,
                    "pk_column": pk_column,
                    "columns": [],
                }
            else:
                file_path = self._resolve_table_artifact_path(table_name)
                columns = utils.get_columns_from_file(file_path)
                sql = PostProcessor.generate_snapshot_row_hash_sql(
                    table_uri=table_uri,
                    columns=columns,
                    snapshot_uri=snapshot_uri,
                )
                snapshots[table_name] = {
                    "snapshot_uri": snapshot_uri,
                    "pk_column": "",
                    "columns": columns,
                }

            utils.execute_duckdb_sql(
                sql,
                f"Unable to snapshot {table_name} for post-processing task '{self.task_name}'",
            )

        return snapshots

    # ---- diff ---------------------------------------------------------------

    def _compute_changes(
        self,
        in_scope_tables: dict[str, str],
        snapshots: dict[str, dict],
    ) -> dict[str, dict[str, int]]:
        """For each table, count rows added and removed since the snapshot."""
        changes: dict[str, dict[str, int]] = {}

        for table_name, table_uri in in_scope_tables.items():
            snapshot = snapshots[table_name]
            snapshot_uri = snapshot["snapshot_uri"]
            pk_column = snapshot["pk_column"]
            columns = snapshot["columns"]

            # The task SQL could have deleted the file itself; re-check.
            file_path = self._resolve_table_artifact_path(table_name)
            if not utils.parquet_file_exists(file_path):
                utils.logger.info(
                    f"Post-processing task '{self.task_name}': table {table_name} "
                    f"no longer present on disk; reporting as fully removed"
                )
                count_sql = PostProcessor.generate_snapshot_row_count_sql(snapshot_uri)
                result = utils.execute_duckdb_sql(
                    count_sql,
                    f"Unable to count snapshot rows for {table_name}",
                    return_results=True,
                )
                removed = int(result[0][0]) if result else 0
                if removed:
                    changes[table_name] = {"added": 0, "removed": removed}
                continue

            if pk_column:
                diff_sql = PostProcessor.generate_pk_diff_sql(
                    snapshot_uri=snapshot_uri,
                    table_uri=table_uri,
                    pk_column=pk_column,
                )
            else:
                diff_sql = PostProcessor.generate_row_hash_diff_sql(
                    snapshot_uri=snapshot_uri,
                    table_uri=table_uri,
                    columns=columns,
                )

            result = utils.execute_duckdb_sql(
                diff_sql,
                f"Unable to compute post-processing diff for {table_name}",
                return_results=True,
            )
            added = int(result[0][0]) if result else 0
            removed = int(result[0][1]) if result else 0

            if added or removed:
                changes[table_name] = {"added": added, "removed": removed}

        return changes

    # ---- artifacts ----------------------------------------------------------

    def _emit_per_table_artifacts(self, changes: dict[str, dict[str, int]]) -> None:
        """Emit added / removed / table-affected artifacts per changed table."""
        for table_name, counts in changes.items():
            self._save_artifact(
                name=f"Post-processing task '{self.task_name}': rows added in {table_name}",
                value_as_number=counts["added"],
            )
            self._save_artifact(
                name=f"Post-processing task '{self.task_name}': rows removed from {table_name}",
                value_as_number=counts["removed"],
            )
            self._save_artifact(
                name=f"Post-processing task '{self.task_name}': table affected",
                value_as_string=table_name,
            )

    def _save_artifact(
        self,
        name: str,
        value_as_string: Optional[str] = None,
        value_as_number: Optional[float] = None,
    ) -> None:
        artifact = report_artifact.ReportArtifact(
            delivery_date=self.delivery_date,
            artifact_bucket=self.bucket,
            concept_id=None,
            name=name,
            value_as_string=value_as_string,
            value_as_concept_id=None,
            value_as_number=value_as_number,
        )
        artifact.save_artifact()

    # ---- dedup --------------------------------------------------------------

    def _dedupe_surrogate_keys(
        self,
        changes: dict[str, dict[str, int]],
        in_scope_tables: dict[str, str],
    ) -> None:
        """Re-run dedup on surrogate-key tables touched by the task."""
        for table_name in changes:
            if table_name not in constants.SURROGATE_KEY_TABLES:
                continue
            if table_name not in in_scope_tables:
                continue

            vocab_harmonization.VocabHarmonizer.deduplicate_primary_keys_in_file(
                file_path=in_scope_tables[table_name],
                table_name=table_name,
                cdm_version=self.omop_version,
            )

    # ---- cleanup ------------------------------------------------------------

    def _cleanup_snapshots(self, snapshots: dict[str, dict]) -> None:
        for table_name, snapshot in snapshots.items():
            try:
                storage.delete_file(snapshot["snapshot_uri"])
            except Exception as e:
                utils.logger.warning(
                    f"Failed to clean up snapshot for {table_name}: {e}"
                )

    # ---- SQL generators -----------------------------------------------------

    @staticmethod
    def generate_snapshot_pk_sql(table_uri: str, pk_column: str, snapshot_uri: str) -> str:
        """Write a parquet file containing just the PK column of the table."""
        return f"""
        COPY (
            SELECT {pk_column}
            FROM read_parquet('{table_uri}')
        ) TO '{snapshot_uri}' {constants.DUCKDB_FORMAT_STRING}
        """.strip()

    @staticmethod
    def generate_snapshot_row_hash_sql(
        table_uri: str, columns: list[str], snapshot_uri: str
    ) -> str:
        """Write a parquet file containing a content hash per row."""
        concat_parts = ", ".join(f"CAST({col} AS VARCHAR)" for col in columns)
        return f"""
        COPY (
            SELECT hash(CONCAT({concat_parts})) AS row_hash
            FROM read_parquet('{table_uri}')
        ) TO '{snapshot_uri}' {constants.DUCKDB_FORMAT_STRING}
        """.strip()

    @staticmethod
    def generate_pk_diff_sql(
        snapshot_uri: str, table_uri: str, pk_column: str
    ) -> str:
        """Return (added, removed) row counts comparing current table to snapshot by PK."""
        return f"""
        SELECT
            (SELECT COUNT(*) FROM read_parquet('{table_uri}') c
             WHERE c.{pk_column} IS NOT NULL
               AND c.{pk_column} NOT IN (SELECT {pk_column} FROM read_parquet('{snapshot_uri}'))) AS added,
            (SELECT COUNT(*) FROM read_parquet('{snapshot_uri}') s
             WHERE s.{pk_column} IS NOT NULL
               AND s.{pk_column} NOT IN (SELECT {pk_column} FROM read_parquet('{table_uri}'))) AS removed
        """.strip()

    @staticmethod
    def generate_row_hash_diff_sql(
        snapshot_uri: str, table_uri: str, columns: list[str]
    ) -> str:
        """Return (added, removed) row counts comparing current table to snapshot by row hash."""
        concat_parts = ", ".join(f"CAST({col} AS VARCHAR)" for col in columns)
        return f"""
        SELECT
            (SELECT COUNT(*) FROM (
                SELECT hash(CONCAT({concat_parts})) AS row_hash FROM read_parquet('{table_uri}')
             ) c WHERE c.row_hash NOT IN (SELECT row_hash FROM read_parquet('{snapshot_uri}'))) AS added,
            (SELECT COUNT(*) FROM read_parquet('{snapshot_uri}') s
             WHERE s.row_hash NOT IN (
                SELECT hash(CONCAT({concat_parts})) FROM read_parquet('{table_uri}')
             )) AS removed
        """.strip()

    @staticmethod
    def generate_snapshot_row_count_sql(snapshot_uri: str) -> str:
        """Return total row count of a snapshot parquet file."""
        return f"SELECT COUNT(*) FROM read_parquet('{snapshot_uri}')"
