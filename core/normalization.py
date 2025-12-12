from typing import Any, Optional

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


class Normalizer:
    """
    Normalizes OMOP CDM parquet files to conform to schema requirements.

    Performs normalization including:
    - Data type conversions to OMOP CDM standard types
    - Default value population for required columns
    - Valid/invalid row separation
    - Deterministic composite primary key generation
    - Row count artifact creation
    - Connect ID handling
    """

    def __init__(self, file_path: str, cdm_version: str, date_format: str, datetime_format: str):
        """
        Initialize normalizer for a specific parquet file.

        Args:
            file_path: Path to parquet file to normalize
            cdm_version: OMOP CDM version (e.g., "5.4")
            date_format: Date format string (e.g., "%Y-%m-%d")
            datetime_format: Datetime format string (e.g., "%Y-%m-%d %H:%M:%S")
        """
        self.file_path = file_path
        self.cdm_version = cdm_version
        self.date_format = date_format
        self.datetime_format = datetime_format
        self.table_name = utils.get_table_name_from_path(file_path).lower()
        self.bucket, self.delivery_date = utils.get_bucket_and_delivery_date_from_path(file_path)
        # Loaded on demand
        self._schema: Optional[dict[Any, Any]] = None
        self._actual_columns: Optional[list[Any]] = None

    def normalize(self) -> None:
        """
        Execute complete file normalization.

        Generates and executes normalization SQL, then creates row count artifacts
        for valid and invalid rows.
        """
        sql = self._generate_normalization_sql()

        # Only run normalization if SQL exists (table is in OMOP CDM)
        if sql and len(sql) > 1:
            # Execute normalization SQL (writes files to disk)
            utils.execute_duckdb_sql(sql, f"Unable to normalize Parquet file {self.file_path}")

            # Create row count artifacts (reads files from disk)
            self._create_row_count_artifacts()

    def _generate_normalization_sql(self) -> str:
        """
        Generate SQL statement for file normalization.

        The generated SQL:
        - Converts data types to OMOP CDM standard
        - Creates invalid rows parquet file
        - Converts column names to lowercase
        - Ensures consistent column order
        - Sets deterministic composite keys for surrogate primary key tables
        """
        # Retrieve table schema
        schema = self._get_schema()
        if not schema or self.table_name not in schema:
            utils.logger.warning(f"No schema found for table {self.table_name}")
            return ""

        columns = schema[self.table_name]["columns"]
        ordered_omop_columns = list(columns.keys())

        # Get actual columns from file
        actual_columns = self._get_actual_columns()

        # Find Connect_ID column if it exists
        connect_id_column_name = self._find_connect_id_column(actual_columns)

        # Generate SQL expressions for each column
        coalesce_exprs, row_validity = self._generate_column_expressions(
            columns, ordered_omop_columns, actual_columns, connect_id_column_name
        )

        # Build SQL fragments
        coalesce_definitions_sql = ",\n                ".join(coalesce_exprs)

        # Ensure row_validity has at least one element
        if not row_validity:
            row_validity.append("''")
        row_validity_sql = ", ".join(row_validity)

        # Generate primary key replacement clause for surrogate key tables
        replace_clause = self._generate_primary_key_clause(ordered_omop_columns)

        # Build row hash statement
        row_hash_statement = ", ".join([
            f"COALESCE(CAST({column_name} AS VARCHAR), '')"
            for column_name in actual_columns
        ])

        # Generate final SQL script
        sql_script = f"""
        CREATE OR REPLACE TABLE row_check AS
            SELECT
                {coalesce_definitions_sql},
                CASE
                    WHEN COALESCE({row_validity_sql}) IS NULL THEN CAST((CAST(hash(CONCAT({row_hash_statement})) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('{storage.get_uri(self.file_path)}')
        ;

        COPY (
            SELECT *
            FROM read_parquet('{storage.get_uri(self.file_path)}')
            WHERE CAST((CAST(hash(CONCAT({row_hash_statement})) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO '{storage.get_uri(utils.get_invalid_rows_path_from_path(self.file_path))}' {constants.DUCKDB_FORMAT_STRING}
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) {replace_clause}
            FROM row_check
            WHERE row_hash IS NULL
        ) TO '{storage.get_uri(self.file_path)}' {constants.DUCKDB_FORMAT_STRING}
        ;

        """.strip()

        # Handle note_nlp 'offset' reserved keyword
        sql_script = sql_script.replace('offset', '"offset"')

        return sql_script

    def _generate_column_expressions(
        self,
        columns: dict,
        ordered_omop_columns: list,
        actual_columns: list,
        connect_id_column_name: str
    ) -> tuple[list, list]:
        """
        Generate SQL expressions for column normalization.

        Args:
            columns: Column definitions from schema
            ordered_omop_columns: Ordered list of OMOP column names
            actual_columns: Actual columns present in file
            connect_id_column_name: Name of Connect_ID column if present

        Returns:
            Tuple of (coalesce_expressions, row_validity_expressions)
        """
        coalesce_exprs = []
        row_validity = []

        for column_name in ordered_omop_columns:
            column_type = columns[column_name]["type"]
            is_required = columns[column_name]["required"].lower() == "true"

            # Special handling for person.birth_datetime
            if self.table_name == "person" and column_name == "birth_datetime":
                column_exists = column_name in actual_columns
                coalesce_exprs.append(
                    self.generate_birth_datetime_sql_expression(self.datetime_format, column_exists)
                )
                continue

            # Determine default value for required columns
            default_value = (
                utils.get_placeholder_value(column_name, column_type)
                if is_required or column_name.endswith("_concept_id")
                else "NULL"
            )

            # Special handling for person_id when connect_id exists in ANY table
            # Sites may send connect_id in any table (person, condition_occurrence, drug_exposure, etc.)
            if column_name == 'person_id' and connect_id_column_name:
                # Always use connect_id value for person_id when connect_id exists
                coalesce_exprs.append(
                    f"TRY_CAST(COALESCE({connect_id_column_name}, {default_value}) AS {column_type}) AS {column_name}"
                )

                # Add to row validity check if required
                if is_required:
                    row_validity.append(
                        f"CAST(TRY_CAST(COALESCE({connect_id_column_name}, {default_value}) AS {column_type}) AS VARCHAR)"
                    )

            # Column exists in file
            elif column_name in actual_columns:
                coalesce_exprs.append(
                    self._generate_column_cast_expression(column_name, column_type, default_value)
                )

                # Add to row validity check if required
                if is_required:
                    row_validity.append(
                        f"CAST(TRY_CAST(COALESCE({column_name}, {default_value}) AS {column_type}) AS VARCHAR)"
                    )

            # Column doesn't exist in file
            else:
                # Add placeholder column
                coalesce_exprs.append(
                    f"CAST({default_value} AS {column_type}) AS {column_name}"
                )

        return coalesce_exprs, row_validity

    def _generate_column_cast_expression(
        self,
        column_name: str,
        column_type: str,
        default_value: str
    ) -> str:
        """
        Generate SQL cast expression for a single column.

        Args:
            column_name: Name of column
            column_type: Target data type
            default_value: Default value to use if cast fails

        Returns:
            SQL expression string
        """
        # Special handling for DATE and DATETIME types
        if column_type in ["DATE", "TIMESTAMP", "DATETIME"]:
            format_to_try = self.date_format if column_type == "DATE" else self.datetime_format
            return f"""COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST({column_name} AS VARCHAR), '{format_to_try}') AS {column_type}),
                        TRY_CAST({column_name} AS {column_type}),
                        CAST({default_value} AS {column_type})
                    ) AS {column_name}"""

        # Required fields with default values
        elif default_value != "NULL":
            return f"TRY_CAST(COALESCE({column_name}, {default_value}) AS {column_type}) AS {column_name}"

        # Optional fields
        else:
            return f"TRY_CAST({column_name} AS {column_type}) AS {column_name}"

    def _generate_primary_key_clause(self, ordered_omop_columns: list) -> str:
        """
        Generate primary key replacement clause for surrogate key tables.

        Creates deterministic composite key by hashing concatenated column values.
        Uniqueness is not required at this stage (enforced later after vocab harmonization).

        Args:
            ordered_omop_columns: Ordered list of OMOP column names

        Returns:
            SQL REPLACE clause string, or empty string if not a surrogate key table
        """
        if self.table_name not in constants.SURROGATE_KEY_TABLES:
            return ""

        primary_key = utils.get_primary_key_column(self.table_name, self.cdm_version)

        # Create composite key from all columns except primary key
        primary_key_sql = ", ".join([
            f"COALESCE(CAST({column_name} AS VARCHAR), '')"
            for column_name in ordered_omop_columns
            if column_name != primary_key
        ])

        return f"""
            REPLACE(CAST((CAST(hash(CONCAT({primary_key_sql})) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS {primary_key})
        """

    def _create_row_count_artifacts(self) -> None:
        """
        Create report artifacts with row counts for valid and invalid rows.

        Reads the normalized parquet files and counts rows in each.
        Creates two report artifacts: one for valid rows, one for invalid rows.
        """
        table_concept_id = utils.get_cdm_schema(self.cdm_version)[self.table_name]['concept_id']

        valid_rows_file = (utils.get_parquet_artifact_location(self.file_path), 'Valid row count')
        invalid_rows_file = (utils.get_invalid_rows_path_from_path(self.file_path), 'Invalid row count')

        files = [valid_rows_file, invalid_rows_file]

        try:
            for file_path, count_type in files:
                # Generate and execute count query
                count_query = self.generate_row_count_sql(storage.get_uri(file_path))
                result = utils.execute_duckdb_sql(count_query, "Unable to count rows", return_results=True)
                row_count = result[0][0] if result else 0

                # Create report artifact
                artifact = report_artifact.ReportArtifact(
                    delivery_date=self.delivery_date,
                    artifact_bucket=self.bucket,
                    concept_id=table_concept_id,
                    name=f"{count_type}: {self.table_name}",
                    value_as_string=None,
                    value_as_concept_id=None,
                    value_as_number=row_count
                )
                artifact.save_artifact()
        except Exception as e:
            raise Exception(f"Unable to create row count artifacts: {e}") from e

    def _get_schema(self) -> dict[Any, Any]:
        """Get table schema for the specified OMOP version"""
        if self._schema is None:
            self._schema = utils.get_table_schema(self.table_name, self.cdm_version)
        return self._schema

    def _get_actual_columns(self) -> list[Any]:
        """Get actual columns from file"""
        if self._actual_columns is None:
            self._actual_columns = utils.get_columns_from_file(self.file_path)
        return self._actual_columns

    @staticmethod
    def _find_connect_id_column(actual_columns: list) -> str:
        """
        Find Connect_ID column name if it exists in file.

        Sites may send connect_id in any table (person, condition_occurrence, drug_exposure, etc.)
        to identify Connect study participants. When found, its value should be used for person_id.

        Args:
            actual_columns: List of actual column names in file

        Returns:
            Connect_ID column name, or empty string if not found
        """
        for column in actual_columns:
            if 'connectid' in column.lower() or 'connect_id' in column.lower():
                return column
        return ""

    @staticmethod
    def generate_birth_datetime_sql_expression(datetime_format: str, column_exists_in_file: bool) -> str:
        """
        Generate SQL expression to populate person.birth_datetime field.

        This field is required for downstream OHDSI tools like DataQualityDashboard and Achilles.

        Rules for calculating birth_datetime:
        1. If birth_datetime is already populated with valid DATETIME, use it
        2. Else if year/month/day are populated, concat them (YYYY-MM-DD 00:00:00)
        3. Else if year/month are populated, use YYYY-MM-01 00:00:00
        4. Else if year is populated, use YYYY-01-01 00:00:00
        5. Else use 1900-01-01 00:00:00

        Always uses midnight (00:00:00) as the time component in UTC.

        Args:
            datetime_format: Datetime format string for parsing
            column_exists_in_file: Whether birth_datetime column exists in source file

        Returns:
            SQL expression string for birth_datetime calculation
        """
        # Build calculation expression from component fields
        calculation_expr = """TRY_CAST(
                CONCAT(
                    LPAD(COALESCE(year_of_birth, '1900'), 4, '0'), '-',
                    LPAD(COALESCE(month_of_birth, '1'), 2, '0'), '-',
                    LPAD(COALESCE(day_of_birth, '1'), 2, '0'),
                    ' 00:00:00'
                ) AS DATETIME)"""

        # If birth_datetime exists, try to use it first, then fall back to calculation
        if column_exists_in_file:
            return f"""COALESCE(
                TRY_CAST(TRY_STRPTIME(birth_datetime, '{datetime_format}') AS DATETIME),
                TRY_CAST(birth_datetime AS DATETIME),
                {calculation_expr}
            ) AS birth_datetime"""
        else:
            # Calculate from year/month/day components
            return f"{calculation_expr} AS birth_datetime"

    @staticmethod
    def generate_row_count_sql(parquet_file_path: str) -> str:
        """
        Generate SQL to count rows in a parquet file.

        Args:
            parquet_file_path: Full URI path to parquet file

        Returns:
            SQL statement that counts all rows
        """
        return f"""
        SELECT COUNT(*) FROM read_parquet('{parquet_file_path}')
        """
