import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils
from core.storage_backend import storage


def get_birth_datetime_sql_expression(datetime_format: str, column_exists_in_file: bool) -> str:
    """
    Generates SQL expression to populate person.birth_datetime field.

    This field is required for downstream OHDSI tools like DataQualityDashboard and Achilles.

    Rules for calculating birth_datetime:
    1. If birth_datetime is already populated with valid DATETIME, use it
    2. Else if year/month/day are populated, concat them (YYYY-MM-DD 00:00:00)
    3. Else if year/month are populated, use YYYY-MM-01 00:00:00 (default missing day to 01)
    4. Else if year is populated, use YYYY-01-01 00:00:00 (default missing month/day to 01)
    5. Else use 1900-01-01 00:00:00 (default missing year to 1900)

    Always uses midnight (00:00:00) as the time component in UTC.

    Args:
        datetime_format: The datetime format string to use for parsing (e.g., '%Y-%m-%d %H:%M:%S')
        column_exists_in_file: Whether birth_datetime column exists in the source file

    Returns:
        SQL expression string for birth_datetime calculation
    """
    # Build calculation expression for birth_datetime from component fields
    # Note: At this stage, columns are still VARCHAR, so we use string literals in COALESCE
    calculation_expr = """TRY_CAST(
                CONCAT(
                    LPAD(COALESCE(year_of_birth, '1900'), 4, '0'), '-',
                    LPAD(COALESCE(month_of_birth, '1'), 2, '0'), '-',
                    LPAD(COALESCE(day_of_birth, '1'), 2, '0'),
                    ' 00:00:00'
                ) AS DATETIME)"""

    # If birth_datetime column exists in the file, try to use it first, then fall back to calculation
    if column_exists_in_file:
        return f"""COALESCE(
                TRY_CAST(TRY_STRPTIME(birth_datetime, '{datetime_format}') AS DATETIME),
                TRY_CAST(birth_datetime AS DATETIME),
                {calculation_expr}
            ) AS birth_datetime"""
    else:
        # If birth_datetime doesn't exist in file, calculate from year/month/day
        return f"{calculation_expr} AS birth_datetime"


def get_normalization_sql_statement(parquet_file_path: str, cdm_version: str, date_format: str, datetime_format: str) -> str:
    """
    Generates a SQL statement that, when executed:
        - Converts data types of columns within Parquet file to OMOP CDM standard
        - Creates a new Parquet file with the invalid rows from the original data file
        - Converts all column names to lower case
        - Ensures consistent column order within Parquet
        - Set (possibly non-unique) deterministric composite key for tables with surrogate primary keys

    This SQL has many functions, but it is far more efficient to do this all in one step,
    as compared to reading and writing the Parquet each time, for each piece of functionality.
    """

    # --------------------------------------------------------------------------
    # Parse out table name and bucket/subfolder info
    # --------------------------------------------------------------------------
    table_name = utils.get_table_name_from_path(parquet_file_path).lower()

    # --------------------------------------------------------------------------
    # Retrieve the table schema. If not found, return empty string
    # --------------------------------------------------------------------------
    schema = utils.get_table_schema(table_name, cdm_version)
    if not schema or table_name not in schema:
        utils.logger.warning(f"No schema found for table {table_name}")
        return ""

    columns = schema[table_name]["columns"]
    ordered_omop_columns = list(columns.keys())  # preserve column order

    # --------------------------------------------------------------------------
    # Identify which columns actually exist in the Parquet file 
    # --------------------------------------------------------------------------
    actual_columns = utils.get_columns_from_file(parquet_file_path)

    # Get Connect_ID column name, if it exists
    connect_id_column_name = ""
    for column in actual_columns:
        if 'connectid' in column.lower() or 'connect_id' in column.lower():
            connect_id_column_name = column
            break

    # --------------------------------------------------------------------------
    # Initialize lists to hold SQL expressions
    # --------------------------------------------------------------------------
    coalesce_exprs = []
    row_validity = []    # e.g. "cc.some_col IS NOT NULL AND ..."

    # --------------------------------------------------------------------------
    # Coalesce required columns if they're NULL, or generate a placeholder column if that column doesn't exist at all.
    # --------------------------------------------------------------------------
    for column_name in ordered_omop_columns:
        column_type = columns[column_name]["type"]
        is_required = columns[column_name]["required"].lower() == "true"

        # Special handling for person.birth_datetime
        # Calculate from year/month/day_of_birth if not already populated
        # This field is required for downstream OHDSI tools like DataQualityDashboard and Achilles
        if table_name == "person" and column_name == "birth_datetime":
            column_exists = column_name in actual_columns
            coalesce_exprs.append(get_birth_datetime_sql_expression(datetime_format, column_exists))
            continue

        # Determine default value if a required column is NULL
        default_value = utils.get_placeholder_value(column_name, column_type) if is_required or column_name.endswith("_concept_id") else "NULL"

        # If the site-delivered table contains an expected column... 
        if column_name in actual_columns:
            
            # Special handling for DATE and TIMESTAMP/DATETIME types using STRPTIME and user-supplied formats
            if column_type in ["DATE", "TIMESTAMP", "DATETIME"]:
                if column_type == "DATE":
                    format_to_try = date_format
                else:
                    format_to_try = datetime_format
                # Use STRPTIME with date_format, then cast to DATE
                # Cast column_name to VARCHAR to start; on retry, the field may already have been converted to DATE/DATETIME,
                # and TRY_STRPTIME() fails on non-string columns.
                coalesce_exprs.append(
                    f"""COALESCE(
                        TRY_CAST(TRY_STRPTIME(CAST({column_name} AS VARCHAR), '{format_to_try}') AS {column_type}), -- first try parsing with specified date format
                        TRY_CAST({column_name} AS {column_type}), -- then try just casting the value
                        CAST({default_value} AS {column_type}) -- finally, use default value
                    ) AS {column_name}"""
                )            

            # If the field *must* have a value...
            elif default_value != "NULL":
                coalesce_exprs.append(f"TRY_CAST(COALESCE({column_name}, {default_value}) AS {column_type}) AS {column_name}")
            # If default value is NULL, don't coalesce
            else:
                coalesce_exprs.append(f"TRY_CAST({column_name} AS {column_type}) AS {column_name}")
            
            # If the colum is provided, and it's required, add it to list of columns which must be of correct type
            if is_required:
                # In the final SQL statement, need to confirm that ALL required columns can be cast to their correct types
                # If any one of the columns cannot be cast to the correct type, the entire row fails
                # To do this check in one shot, perform a single COALESCE within the SQL statement
                # ALL columns in the final COALESCE must be of the same type; after TRY_CAST to expected type, cast to VARCHAR for the coalese
                row_validity.append(f"CAST(TRY_CAST(COALESCE({column_name}, {default_value}) AS {column_type}) AS VARCHAR)")
        else:
            # If the site provided a Connect_ID field and/or person_id, use Connect_ID in place of person_id
            if column_name == 'person_id' and connect_id_column_name and len(connect_id_column_name) > 1:
                coalesce_exprs.append(f"CAST({connect_id_column_name} AS {column_type}) AS {column_name}")

            # If the column doesn't exist in the delivery, add that column with a placeholder (NULL or a special default)
            # Still need to cast to ensure consist column types
            coalesce_exprs.append(f"CAST({default_value} AS {column_type}) AS {column_name}")
            # If a column is required but not provided, all rows should not fail validity check; just use a default value

    # Build coalesce statement
    coalesce_definitions_sql = ",\n                ".join(coalesce_exprs)

    # If row_validity list has no statements, add a string so SQL statement stays valid
    if not row_validity:
        row_validity.append("''")
    row_validity_sql = ", ".join(row_validity)

    # Create deterministic composite key for tables with surrogate primary keys
        # At this stage, uniqueness is *not* an expected, nor desired, property of the primary keys
        # Primary keys uniqueness will be enforced in later tasks, after vocabulary harmonization
    replace_clause = ""
    if table_name in constants.SURROGATE_KEY_TABLES:
        primary_key = utils.get_primary_key_column(table_name, cdm_version)

        # Create composite key by concatenting each column into a single value and taking its hash
        # Don't include the original primary key in the hash
        # % 9223372036854775807 to get a BIGINT
        primary_key_sql = ", ".join([f"COALESCE(CAST({column_name} AS VARCHAR), '')" for column_name in ordered_omop_columns if column_name != primary_key])
        replace_clause = f"""
            REPLACE(CAST((CAST(hash(CONCAT({primary_key_sql})) AS UBIGINT) % 9223372036854775807) AS BIGINT) AS {primary_key}) 
        """

    # Build concat statement that will eventually be hashed to identify valid/invalid rows
    # The row_hash involves ALL columns from incoming Parquet (whereas the primary key includes only columns in OMOP)
    row_hash_statement = ", ".join([f"COALESCE(CAST({column_name} AS VARCHAR), '')" for column_name in actual_columns])

    # Final normalization SQL statement
    # Step 1 - Identify invalid rows using output of COALESCE({row_validity_sql}) (NULL COALESCE result = invalid)
        # If invalid, set new column "row_hash" to unique hash that uniquely identifies the invalid row(s)
        # If valid, set new column "row_hash" to a NULL value
    # Step 2 - Create new, seperate invalid rows Parquet file containing all invalid rows
    # Step 3 - Resave over original Parquet file, saving only the valid rows; and setting deterministric primary key for surrogate key tables
    sql_script = f"""
        CREATE OR REPLACE TABLE row_check AS
            SELECT
                {coalesce_definitions_sql},
                CASE 
                    WHEN COALESCE({row_validity_sql}) IS NULL THEN CAST((CAST(hash(CONCAT({row_hash_statement})) AS UBIGINT) % 9223372036854775807) AS BIGINT)
                    ELSE NULL END AS row_hash
            FROM read_parquet('{storage.get_uri(parquet_file_path)}')
        ;

        COPY (
            SELECT *
            FROM read_parquet('{storage.get_uri(parquet_file_path)}')
            WHERE CAST((CAST(hash(CONCAT({row_hash_statement})) AS UBIGINT) % 9223372036854775807) AS BIGINT) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO '{storage.get_uri(utils.get_invalid_rows_path_from_path(parquet_file_path))}' {constants.DUCKDB_FORMAT_STRING}
        ;

        COPY (
            SELECT * EXCLUDE (row_hash) {replace_clause}
            FROM row_check
            WHERE row_hash IS NULL
        ) TO '{storage.get_uri(parquet_file_path)}' {constants.DUCKDB_FORMAT_STRING}
        ;

    """.strip()

    # note_nlp has column name 'offset' which is a reserved keyword in DuckDB
    # Need to add "" around offset column name to prevent parsing error
    sql_script = sql_script.replace('offset', '"offset"')

    return sql_script


def normalize_file(parquet_file_path: str, cdm_version: str, date_format: str, datetime_format: str) -> None:
    """
    Normalize Parquet file to conform to OMOP CDM schema.
    Applies data type conversions, supplies default values for missing required columns, and separates valid/invalid rows.
    """
    fix_sql = get_normalization_sql_statement(parquet_file_path, cdm_version, date_format, datetime_format)

    # Only run the fix SQL statement if it exists
    # Statement will exist only for tables/files in OMOP CDM
    if fix_sql and len(fix_sql) > 1:
        # Execute normalization SQL (writes files to disk)
        utils.execute_duckdb_sql(fix_sql, f"Unable to normalize Parquet file {parquet_file_path}")

        # Create row count artifacts (reads files from disk - independent operation)
        create_row_count_artifacts(parquet_file_path, cdm_version)


def generate_row_count_sql(parquet_file_path: str) -> str:
    """
    Generate SQL to count rows in a parquet file.

    Args:
        parquet_file_path: Full URI path to the parquet file

    Returns:
        SQL statement that counts all rows in the parquet file
    """
    return f"""
        SELECT COUNT(*) FROM read_parquet('{parquet_file_path}')
    """


def create_row_count_artifacts(file_path: str, cdm_version: str) -> None:
    """
    Create report artifacts with row counts for valid and invalid rows after normalization.
    Reads the normalized parquet files from disk and counts rows.
    """
    table_name = utils.get_table_name_from_path(file_path)
    table_concept_id = utils.get_cdm_schema(cdm_version)[table_name]['concept_id']
    bucket, delivery_date = utils.get_bucket_and_delivery_date_from_path(file_path)

    valid_rows_file = (utils.get_parquet_artifact_location(file_path), 'Valid row count')
    invalid_rows_file = (utils.get_invalid_rows_path_from_path(file_path),  'Invalid row count')

    files = [valid_rows_file, invalid_rows_file]

    # Count rows for each file and create report artifacts
    try:
        for file in files:
            file_path, count_type = file

            # Generate SQL to count rows
            count_query = generate_row_count_sql(storage.get_uri(file_path))
            result = utils.execute_duckdb_sql(count_query, "Unable to count rows", return_results=True)
            result_row = result.fetchone()
            row_count = result_row[0] if result_row else 0

            ra = report_artifact.ReportArtifact(
                delivery_date=delivery_date,
                artifact_bucket=bucket,
                concept_id=table_concept_id,
                name=f"{count_type}: {table_name}",
                value_as_string=None,
                value_as_concept_id=None,
                value_as_number=row_count
            )
            ra.save_artifact()
    except Exception as e:
        raise Exception(f"Unable to create row count artifacts: {e}") from e
