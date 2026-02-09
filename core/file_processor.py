import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


class FileProcessor:
    """
    Processes incoming OMOP data files to standardized Parquet format.

    Handles:
    - CSV to Parquet conversion (with retry logic for malformed data)
    - Parquet file processing (column name/type standardization)
    """

    def __init__(self, file_path: str, file_type: str):
        """
        Initialize file processor for a specific file.

        Args:
            file_path: Path to file to process
            file_type: Type of file (.csv, .parquet, etc.)
        """
        self.file_path = file_path
        self.file_type = file_type
        self.output_path = utils.get_parquet_artifact_location(file_path)
        self.table_name = utils.get_table_name_from_path(file_path)

    def process(self) -> str:
        """
        Process incoming file based on type.

        Returns:
            Path to processed output file
        """
        if self.file_type in [constants.CSV, constants.CSV_GZ]:
            return self._process_csv()
        elif self.file_type == constants.PARQUET:
            return self._process_parquet()
        else:
            raise Exception(f"Invalid source file format in file {self.file_path}: {self.file_type}")

    def _process_parquet(self) -> str:
        """
        Process incoming Parquet file.

        Validates and copies to artifact directory with:
        - Lowercase file name
        - Lowercase column names
        - VARCHAR column types
        """
        if not utils.valid_parquet_file(self.file_path):
            raise Exception(f"Invalid Parquet file at {self.file_path}")

        # Get columns from parquet file
        parquet_columns = utils.get_columns_from_file(self.file_path)

        # Generate and execute SQL
        copy_sql = self.generate_process_incoming_parquet_sql(self.file_path, parquet_columns)
        utils.execute_duckdb_sql(
            copy_sql,
            f"Unable to process incoming Parquet file {self.file_path}:"
        )

        return self.output_path

    def _process_csv(self, retry: bool = False, conversion_options: list = []) -> str:
        """
        Convert CSV file to Parquet format.

        On first attempt, uses strict parsing settings.
        On failure, retries with permissive settings for malformed rows.

        Args:
            retry: Whether this is a retry attempt
            conversion_options: Additional DuckDB CSV read options
        """
        # Get column names from CSV
        csv_column_names = utils.get_columns_from_file(self.file_path)

        # Generate SQL
        convert_statement = self.generate_csv_to_parquet_sql(
            self.file_path, csv_column_names, conversion_options
        )

        # Execute SQL with retry logic
        try:
            utils.execute_duckdb_sql(
                convert_statement,
                f"Unable to convert CSV file to Parquet {storage.get_uri(self.file_path)}"
            )
        except Exception as e:
            if not retry:
                # On error on first attempt, retry with more permissive settings
                utils.logger.info(f"Retrying {storage.get_uri(self.file_path)} CSV to Parquet conversion with more permissive settings")

                return self._process_csv(
                    retry=True,
                    conversion_options=[f"store_rejects=True, ignore_errors=True, parallel=False, encoding = '{utils.get_csv_file_encoding(storage.get_uri(self.file_path))}'"]
                )
            else:
                raise

        return self.output_path

    @staticmethod
    def generate_process_incoming_parquet_sql(file_path: str, parquet_columns: list[str]) -> str:
        """
        Generate SQL to process incoming Parquet file.

        Creates SQL that:
        - Converts all column names to lowercase
        - Casts all columns to VARCHAR
        - Handles special 'offset' column for note_nlp table

        Args:
            file_path: Path to the input Parquet file
            parquet_columns: List of column names from the Parquet file
        """
        select_list = []

        # Handle offset column in note_nlp
        # May come in as offset or "offset" and need different handling for each scenario
        for column in parquet_columns:
            # Always cast to VARCHAR, handle offset columns specially
            if column.lower() == '"offset"':
                select_list.append(f'CAST(""{column}"" AS VARCHAR) AS {column.lower()}')
            elif column.lower() == 'offset':
                select_list.append(f'CAST("{column}" AS VARCHAR) AS "{column.lower()}"')
            else:
                select_list.append(f"CAST({column} AS VARCHAR) AS {utils.clean_column_name_for_sql(column)}")

        select_clause = ", ".join(select_list)

        select_statement = f"""
        COPY (
            SELECT {select_clause}
            FROM read_parquet('{storage.get_uri(file_path)}')
        )
        TO '{storage.get_uri(utils.get_parquet_artifact_location(file_path))}' {constants.DUCKDB_FORMAT_STRING}
        """

        return select_statement

    @staticmethod
    def generate_csv_to_parquet_sql(file_path: str, csv_column_names: list[str], conversion_options: list = []) -> str:
        """
        Generate SQL to convert CSV file to Parquet format.

        Creates SQL that:
        - Reads CSV with configurable options
        - Cleans column names to lowercase
        - Handles special 'offset' column for note_nlp table

        Args:
            file_path: Path to the input CSV file
            csv_column_names: List of column names from the CSV file
            conversion_options: List of additional DuckDB CSV read options (e.g., ['ignore_errors=True'])
        """
        parquet_path = utils.get_parquet_artifact_location(file_path)

        select_list = []
        for column in csv_column_names:
            # Use the utility function to clean column names for the alias
            column_alias = utils.clean_column_name_for_sql(column)

            # Special handling for offset column in note_nlp
            if column.lower() not in ['offset', '"offset"', "'offset'"]:
                select_list.append(f"""
                "{column}" AS {column_alias}
                """)
            else:
                select_list.append(f"{column} AS {column_alias}")

        # Build final select statement
        select_clause = ", ".join(select_list)

        # note_nlp has column name 'offset' which is a reserved keyword in DuckDB
        # Special handling required to prevent parsing error
        # Re-add double quotes to offset column prevent DuckDB from returning parsing error
        select_clause = select_clause.replace('offset', '"offset"')

        # Generate CSV to Parquet conversion SQL
        select_statement = f"""
        COPY (
            SELECT {select_clause}
            FROM read_csv('{storage.get_uri(file_path)}',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False {FileProcessor.format_list(conversion_options)})
        ) TO '{storage.get_uri(parquet_path)}' {constants.DUCKDB_FORMAT_STRING}
        """
        
        return select_statement

    @staticmethod
    def format_list(items: list) -> str:
        """
        Format list as comma-separated string with leading comma.

        Args:
            items: List of items to format

        Returns:
            Empty string if list is empty, otherwise comma-prefixed string
        """
        if not items:
            return ''
        else:
            return ',' + ', '.join(items)
