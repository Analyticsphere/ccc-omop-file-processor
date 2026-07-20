"""
EHR PR2 merge-pipeline helpers.

extract_chunk: copy one (group x table) slice from a source delivery into a
provenance-named chunk file. v1 scope is "ALL" (whole table); a participant-id
subset (v2) points PARTICIPANT_SCOPE at a parquet of ids. For person, stamps
care_site_id = hash_care_site_id(site) to record each patient's origin site.

reconcile_chunks: union a table's chunk files into converted_files/<table>.parquet
(union_by_name tolerates column-order/schema drift across sites).

build_care_site: write converted_files/care_site.parquet mapping each merged
site's hashed care_site_id to its name. Not carried from deliveries; built from
the merged site set to match the person.care_site_id stamps.
"""

import hashlib
from typing import Optional

import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


class MergeProcessor:
    """Extract per-group table chunks and reconcile them into merged per-table files."""

    @staticmethod
    def hash_care_site_id(site_display_name: str) -> int:
        """Stable, deterministic signed-64-bit care_site_id for a site name."""
        digest = hashlib.sha256(site_display_name.encode("utf-8")).digest()
        value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
        return value or 1

    @staticmethod
    def _extract_select_list(site_display_name: Optional[str]) -> str:
        """
        Extract SELECT list: "*", or "* REPLACE(...)" overwriting care_site_id with the
        site's hashed id when site_display_name is given (person only; care_site_id is a
        standard person column so REPLACE always resolves).
        """
        if site_display_name is None:
            return "*"
        care_site_id = MergeProcessor.hash_care_site_id(site_display_name)
        return f"* REPLACE (CAST({care_site_id} AS BIGINT) AS care_site_id)"

    @staticmethod
    def generate_extract_chunk_sql(
        source_uri: str,
        chunk_uri: str,
        participant_scope: str,
        person_id_column: str = constants.DEFAULT_PERSON_ID_COLUMN,
        site_display_name: Optional[str] = None,
    ) -> str:
        """
        Generate SQL to copy one source table (optionally subset by participant) into a chunk file.

        Args:
            source_uri: Path to the source delivery's table parquet (any/no scheme; normalized).
            chunk_uri: Path to the destination chunk parquet in the staging area.
            participant_scope: constants.PARTICIPANT_SCOPE_ALL for the whole table, otherwise a
                path to a parquet file with an `id` column of participant ids to keep.
            person_id_column: Column matched against the id set. Only used
                when participant_scope is not PARTICIPANT_SCOPE_ALL.
            site_display_name: Source site name; when set (person only), stamps care_site_id
                with its hash to record each patient's origin site.
        """
        source = storage.get_uri(source_uri)
        chunk = storage.get_uri(chunk_uri)
        select_list = MergeProcessor._extract_select_list(site_display_name)

        if participant_scope == constants.PARTICIPANT_SCOPE_ALL:
            return f"""
            COPY (
                SELECT {select_list} FROM read_parquet('{source}')
            ) TO '{chunk}' {constants.DUCKDB_FORMAT_STRING}
            """

        # v2 path will eventually subset to a pre-defined Connect ID list;
        # for v1, get all IDs and then filter later.
        ids = storage.get_uri(participant_scope)
        return f"""
        COPY (
            SELECT {select_list} FROM read_parquet('{source}')
            WHERE {person_id_column} IN (
                SELECT id FROM read_parquet('{ids}')
            )
        ) TO '{chunk}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def extract_chunk(
        source_uri: str,
        chunk_uri: str,
        participant_scope: str,
        person_id_column: str = constants.DEFAULT_PERSON_ID_COLUMN,
        site_display_name: Optional[str] = None,
    ) -> None:
        """Execute the chunk extraction. Writes chunk_uri outside the source directory."""
        sql = MergeProcessor.generate_extract_chunk_sql(
            source_uri, chunk_uri, participant_scope, person_id_column, site_display_name
        )
        utils.execute_duckdb_sql(sql, f"Unable to extract participant chunk to {chunk_uri}")

    @staticmethod
    def generate_build_care_site_sql(
        output_uri: str, site_display_names: list[str], cdm_version: str
    ) -> str:
        """
        Build care_site: one row per site, care_site_id = hash(name), care_site_name =
        name, other columns typed NULL. Column order/types from the OMOP care_site schema
        so the parquet loads to BQ cleanly. Duplicate names collapse to one row (PK).
        """
        # De-dup, preserving first-seen order (distinct sites -> distinct ids).
        unique_names = list(dict.fromkeys(site_display_names))
        if not unique_names:
            raise ValueError("Cannot build care_site table: no site names provided")

        columns = utils.get_table_schema("care_site", cdm_version)["care_site"]["columns"]
        output = storage.get_uri(output_uri)

        rows = []
        for name in unique_names:
            care_site_id = MergeProcessor.hash_care_site_id(name)
            escaped_name = name.replace("'", "''")
            value_exprs = []
            for column_name in columns:
                if column_name == "care_site_id":
                    value_exprs.append(str(care_site_id))
                elif column_name == "care_site_name":
                    value_exprs.append(f"'{escaped_name}'")
                else:
                    value_exprs.append("NULL")
            rows.append(f"({', '.join(value_exprs)})")

        projection = ",\n                ".join(
            f"CAST({column_name} AS {column_info['type']}) AS {column_name}"
            for column_name, column_info in columns.items()
        )
        column_list = ", ".join(columns.keys())
        values = ",\n                ".join(rows)

        return f"""
        COPY (
            SELECT
                {projection}
            FROM (VALUES
                {values}
            ) AS t({column_list})
        ) TO '{output}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def build_care_site(output_uri: str, site_display_names: list[str], cdm_version: str) -> None:
        """Write the merged instance's care_site table from the set of merged site names."""
        sql = MergeProcessor.generate_build_care_site_sql(output_uri, site_display_names, cdm_version)
        utils.execute_duckdb_sql(sql, f"Unable to build care_site table at {output_uri}")

    @staticmethod
    def generate_reconcile_chunks_sql(chunk_glob: str, output_uri: str) -> str:
        """
        Generate SQL to union all chunk files matching a glob into one merged parquet.

        Args:
            chunk_glob: Glob over the per-table staging folder (e.g. .../merge_chunks/<table>/*.parquet).
            output_uri: Destination for the reconciled table. MUST live outside the chunk folder
                (in converted_files/) so the read glob never re-reads its own output and downstream
                table-name parsing works.
        """
        chunks = storage.get_uri(chunk_glob)
        output = storage.get_uri(output_uri)
        return f"""
        COPY (
            SELECT * FROM read_parquet('{chunks}', union_by_name=true)
        ) TO '{output}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def reconcile_chunks(chunk_glob: str, output_uri: str) -> None:
        """Execute the reconciliation. Raises if the glob matches no chunk files."""
        sql = MergeProcessor.generate_reconcile_chunks_sql(chunk_glob, output_uri)
        utils.execute_duckdb_sql(sql, f"Unable to reconcile chunks into {output_uri}")
