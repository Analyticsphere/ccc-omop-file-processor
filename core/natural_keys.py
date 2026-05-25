from typing import Sequence

import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


class NaturalKeyProcessor:
    """
    Rewrites natural-key columns (PK and FK) in a normalized OMOP parquet file so values are globally unique.

    For every column listed in constants.GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS that
    is present in the file, the value is replaced with
    hash(CONCAT(value, site)) % 9223372036854775807, preserving NULLs.
    """

    def __init__(self, file_path: str, omop_version: str, site: str):
        """
        Args:
            file_path: Path to delivered OMOP file (any extension; resolved to
                the normalized parquet artifact).
            omop_version: OMOP CDM version, used for schema lookups by callers.
            site: Site identifier used as the hash salt.
        """
        self.file_path = file_path
        self.omop_version = omop_version
        self.site = site
        self.table_name = utils.get_table_name_from_path(file_path).lower()
        self.parquet_file_path = utils.get_parquet_artifact_location(file_path)

    def apply(self) -> bool:
        """
        Apply the natural-key rewrite to the table's parquet file.

        Returns:
            True when the file was rewritten.
            False when the table is skipped (vocab tables, person, or no
            in-scope columns present).
        """
        if self.table_name in constants.NATURAL_KEY_REWRITE_SKIP_TABLES:
            utils.logger.info(
                f"Skipping natural-key rewrite for {self.table_name}: table is excluded by policy"
            )
            return False

        if not utils.parquet_file_exists(self.parquet_file_path):
            raise Exception(f"Normalized parquet file not found at {self.parquet_file_path}")

        actual_columns = utils.get_columns_from_file(self.parquet_file_path)
        columns_to_rewrite = NaturalKeyProcessor.find_columns_to_rewrite(actual_columns)

        if not columns_to_rewrite:
            utils.logger.info(
                f"Skipping natural-key rewrite for {self.table_name}: no in-scope columns present"
            )
            return False

        table_uri = storage.get_uri(self.parquet_file_path)
        rewrite_sql = NaturalKeyProcessor.generate_rewrite_sql(
            table_uri=table_uri,
            columns_to_rewrite=columns_to_rewrite,
            site=self.site,
        )

        utils.execute_duckdb_sql(
            rewrite_sql,
            f"Unable to globalize natural keys in {self.parquet_file_path}",
        )

        utils.logger.info(
            f"Globalized natural keys in {self.parquet_file_path}: rewrote {columns_to_rewrite}"
        )
        return True

    @staticmethod
    def find_columns_to_rewrite(actual_columns: Sequence[str]) -> list[str]:
        """Return the subset of GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS present in the file."""
        actual_lower = {c.lower() for c in actual_columns}
        return [
            col for col in constants.GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS
            if col in actual_lower
        ]

    @staticmethod
    def generate_hash_expression(column_name: str, site: str) -> str:
        """
        Generate a NULL-preserving site-salted hash expression for a single column.

        Matches the hash shape used by transformer.py for surrogate keys:
        CAST(hash(CONCAT(value, site)) AS UBIGINT) % 9223372036854775807
        clamped into signed INT64 space.
        """

        hash_exp = f"""
            CASE WHEN {column_name} IS NOT NULL 
                THEN CAST((CAST(hash(CONCAT(CAST({column_name} AS VARCHAR), '{site}')) AS UBIGINT) % 9223372036854775807) AS BIGINT)
            ELSE NULL END AS {column_name}
        """

        return hash_exp

    @staticmethod
    def generate_rewrite_sql(
        table_uri: str,
        columns_to_rewrite: Sequence[str],
        site: str,
    ) -> str:
        """
        Generate SQL that rewrites the parquet file in place.

        Uses SELECT * REPLACE so each rewritten column stays in its original
        position in the schema — column order is preserved exactly.
        """
        replacement_exprs = ",\n                ".join(
            NaturalKeyProcessor.generate_hash_expression(col, site)
            for col in columns_to_rewrite
        )

        rewrite_sql = f"""
        COPY (
            SELECT * REPLACE (
                {replacement_exprs}
            )
            FROM read_parquet('{table_uri}')
        ) TO '{table_uri}' {constants.DUCKDB_FORMAT_STRING}
        """.strip()

        return rewrite_sql
