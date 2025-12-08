import core.constants as constants
import core.utils as utils
from core.storage_backend import storage


def process_incoming_file(file_type: str, file_path: str) -> None:
    """
    Process incoming OMOP file by routing to appropriate converter based on file type.
    Converts .csv/.csv.gz/.parquet files to standardized Parquet, or processes existing Parquet files.
    """
    if file_type in [constants.CSV, constants.CSV_GZ]:
        csv_to_parquet(file_path)
    elif file_type == constants.PARQUET:
        process_incoming_parquet(file_path)
    else:
        raise Exception(f"Invalid source file format in file {file_path}: {file_type}")

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

    Returns:
        SQL string for processing the Parquet file
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

    return f"""
        COPY (
            SELECT {select_clause}
            FROM read_parquet('{storage.get_uri(file_path)}')
        )
        TO '{storage.get_uri(utils.get_parquet_artifact_location(file_path))}' {constants.DUCKDB_FORMAT_STRING}
    """


def process_incoming_parquet(file_path: str) -> None:
    """
    - Validates that the Parquet file at file_path is readable by DuckDB.
    - Copies incoming Parquet file to artifact directory, ensuring:
       - The output file name is all lowercase
       - All column names within the Parquet file are all lowercase.
       - All column types are converted to VARCHAR (if not alredy)
    """
    if utils.valid_parquet_file(file_path):
        # Get columns from parquet file
        parquet_columns = utils.get_columns_from_file(file_path)

        # Generate SQL
        copy_sql = generate_process_incoming_parquet_sql(file_path, parquet_columns)

        # Execute SQL
        utils.execute_duckdb_sql(copy_sql, f"Unable to process incoming Parquet file {file_path}:")
    else:
        raise Exception(f"Invalid Parquet file at {file_path}")

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

    Returns:
        SQL string for converting CSV to Parquet
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
    return f"""
        COPY (
            SELECT {select_clause}
            FROM read_csv('{storage.get_uri(file_path)}',
                null_padding=True, ALL_VARCHAR=True, strict_mode=False {format_list(conversion_options)})
        ) TO '{storage.get_uri(parquet_path)}' {constants.DUCKDB_FORMAT_STRING}
    """


def csv_to_parquet(file_path: str, retry: bool = False, conversion_options: list = []) -> None:
    """
    Converts a CSV file to Parquet format using DuckDB.
    On first attempt, uses strict and more performant parsing settings.
    On failure, retries with more permissive settings to handle malformed rows.
    On subsequent failure, an exception is raised.
    """
    # Get column names from CSV
    csv_column_names = utils.get_columns_from_file(file_path)

    # Generate SQL
    convert_statement = generate_csv_to_parquet_sql(file_path, csv_column_names, conversion_options)

    # Execute SQL
    try:
        utils.execute_duckdb_sql(convert_statement, f"Unable to convert CSV file to Parquet {storage.get_uri(file_path)}")
    except Exception as e:
        if not retry:
            retry_ignoring_errors(file_path)
        else:
            raise

def retry_ignoring_errors(file_path: str) -> None:
    """
    Retry converting CSV to Parquet with more permissive settings.
    """
    csv_to_parquet(file_path, True, ['store_rejects=True, ignore_errors=True, parallel=False'])

def format_list(items: list) -> str:
    """Format list as comma-separated string with leading comma, or empty string if list is empty."""
    if not items:  # Check if list is empty
        return ''
    else:
        return ',' + ', '.join(items)
