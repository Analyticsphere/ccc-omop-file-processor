"""
EHR PR2 merge-pipeline helpers.

extract_chunk: pull one (group x table) slice out of a source delivery into a
provenance-named chunk file in the shared per-table staging area. For v1 the
participant scope is always "ALL" (whole table, no WHERE). A participant-id
subset (v2) is supported by pointing PARTICIPANT_SCOPE at a parquet of ids.

reconcile_chunks: union all chunk files for a table into a single merged
converted_files/<table>.parquet. union_by_name defends against benign
column-order/schema drift across sites (all were normalized to the same CDM).
"""

import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


class MergeProcessor:
    """Extract per-group table chunks and reconcile them into merged per-table files."""

    @staticmethod
    def generate_extract_chunk_sql(
        source_uri: str,
        chunk_uri: str,
        participant_scope: str,
        person_id_column: str = constants.DEFAULT_PERSON_ID_COLUMN,
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
        """
        source = storage.get_uri(source_uri)
        chunk = storage.get_uri(chunk_uri)

        if participant_scope == constants.PARTICIPANT_SCOPE_ALL:
            return f"""
            COPY (
                SELECT * FROM read_parquet('{source}')
            ) TO '{chunk}' {constants.DUCKDB_FORMAT_STRING}
            """

        # v2 path will eventually subset to a pre-defined Connect ID list;
        # for v1, get all IDs and then filter later.
        ids = storage.get_uri(participant_scope)
        return f"""
        COPY (
            SELECT * FROM read_parquet('{source}')
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
    ) -> None:
        """Execute the chunk extraction. Writes chunk_uri outside the source directory."""
        sql = MergeProcessor.generate_extract_chunk_sql(
            source_uri, chunk_uri, participant_scope, person_id_column
        )
        utils.execute_duckdb_sql(sql, f"Unable to extract participant chunk to {chunk_uri}")

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
