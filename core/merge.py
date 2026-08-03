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
        site_display_name: Optional[str] = None,
    ) -> str:
        """
        Generate SQL to copy one source table into a provenance-named chunk file.

        Args:
            source_uri: Path to the source delivery's table parquet (any/no scheme; normalized).
            chunk_uri: Path to the destination chunk parquet in the staging area.
            site_display_name: Source site name; when set (person only), stamps care_site_id
                with its hash to record each patient's origin site.
        """
        source = storage.get_uri(source_uri)
        chunk = storage.get_uri(chunk_uri)
        select_list = MergeProcessor._extract_select_list(site_display_name)

        return f"""
        COPY (
            SELECT {select_list} FROM read_parquet('{source}')
        ) TO '{chunk}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def extract_chunk(
        source_uri: str,
        chunk_uri: str,
        site_display_name: Optional[str] = None,
    ) -> None:
        """Execute the chunk extraction. Writes chunk_uri outside the source directory."""
        sql = MergeProcessor.generate_extract_chunk_sql(source_uri, chunk_uri, site_display_name)
        utils.execute_duckdb_sql(sql, f"Unable to extract chunk to {chunk_uri}")

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
    def generate_build_cdm_source_sql(
        output_uri: str,
        source_cdm_source_uris: list[str],
        site_count: int,
        cdm_version: str,
        vocabulary_version: str,
        cdm_release_date: str,
    ) -> str:
        """
        Generate SQL for the merged instance's de novo one-row cdm_source.

        Fixed Connect metadata (constants) + site_count in the description. source_release_date
        is the LATEST across the sites' cdm_source files (falling back to cdm_release_date if
        none). cdm_release_date is the merge run date; cdm_version_concept_id is derived from
        cdm_version. Column order/types mirror the single-site cdm_source.
        """
        if not source_cdm_source_uris:
            raise ValueError("Cannot build cdm_source: no source cdm_source files provided")

        output = storage.get_uri(output_uri)
        uri_list = ", ".join(f"'{storage.get_uri(uri)}'" for uri in source_cdm_source_uris)
        concept_id = utils.get_cdm_version_concept_id(cdm_version)
        description = constants.MERGE_CDM_SOURCE_DESCRIPTION.format(site_count=site_count)

        return f"""
        COPY (
            SELECT
                '{constants.MERGE_CDM_SOURCE_NAME}' AS cdm_source_name,
                '{constants.MERGE_CDM_SOURCE_ABBREVIATION}' AS cdm_source_abbreviation,
                '{constants.MERGE_CDM_HOLDER}' AS cdm_holder,
                '{description}' AS source_description,
                '{constants.MERGE_SOURCE_DOCUMENTATION_REFERENCE}' AS source_documentation_reference,
                '{constants.MERGE_CDM_ETL_REFERENCE}' AS cdm_etl_reference,
                COALESCE(
                    (SELECT MAX(source_release_date) FROM read_parquet([{uri_list}], union_by_name=true)),
                    CAST('{cdm_release_date}' AS DATE)
                ) AS source_release_date,
                CAST('{cdm_release_date}' AS DATE) AS cdm_release_date,
                '{cdm_version}' AS cdm_version,
                {concept_id} AS cdm_version_concept_id,
                '{vocabulary_version}' AS vocabulary_version
        ) TO '{output}' {constants.DUCKDB_FORMAT_STRING}
        """

    @staticmethod
    def build_cdm_source(
        output_uri: str,
        source_cdm_source_uris: list[str],
        site_count: int,
        cdm_version: str,
        vocabulary_version: str,
        cdm_release_date: str,
    ) -> None:
        """Write the merged instance's de novo cdm_source parquet."""
        sql = MergeProcessor.generate_build_cdm_source_sql(
            output_uri, source_cdm_source_uris, site_count, cdm_version, vocabulary_version, cdm_release_date
        )
        utils.execute_duckdb_sql(sql, f"Unable to build cdm_source at {output_uri}")

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
