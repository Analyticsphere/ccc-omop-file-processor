import core.constants as constants
import core.utils as utils
import sys
import chardet
from io import StringIO
import csv
import codecs
from google.cloud import storage
import duckdb
import json

class StreamingCSVWriter:
    """Helper class to stream CSV data directly to and from GCS"""
    def __init__(self, target_blob):
        self.target_blob = target_blob
        self.buffer = StringIO()
        self.writer = csv.writer(self.buffer)
        self.upload_session = target_blob.create_resumable_upload_session(
            content_type='text/csv',
            timeout=None
        )
        self.total_bytes_uploaded = 0

    def writerow(self, row):
        """Write a row and upload if buffer reaches threshold"""
        self.writer.writerow(row)
        
        # If buffer gets too large, upload it
        if self.buffer.tell() > 1024 * 1024:  # 1MB threshold
            self._upload_buffer()
            

    def _upload_buffer(self):
        """Upload current buffer contents to GCS using resumable upload"""
        if self.buffer.tell() > 0:
            content = self.buffer.getvalue().encode('utf-8')
            
            # Calculate position for resumable upload
            end_byte = self.total_bytes_uploaded + len(content)
            
            # Upload the chunk using the resumable session
            self.target_blob.upload_from_string(
                content,
                content_type='text/csv',
                timeout=None,
                retry=None,
                if_generation_match=None
            )
            
            self.total_bytes_uploaded = end_byte
            
            # Clear the buffer
            self.buffer.seek(0)
            self.buffer.truncate()

    def close(self):
        """Upload any remaining data and close the writer"""
        self._upload_buffer()
        self.buffer.close()

def csv_to_parquet(gcs_file_path: str) -> None:
    conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

    try:
        with conn:
            utils.logger.info(f"Converting file gs://{gcs_file_path} to parquet...")

            # Get file name from GCS path
            file_name = gcs_file_path.split('/')[-1].lower()
            
            # Get the base directory path (everything before FIXED_FILES if present, or before the file name)
            if constants.ArtifactPaths.FIXED_FILES.value in gcs_file_path:
                base_directory = gcs_file_path.split(constants.ArtifactPaths.FIXED_FILES.value)[0]
            else:
                base_directory = '/'.join(gcs_file_path.split('/')[:-1])
            
            # Remove trailing slash if present
            base_directory = base_directory.rstrip('/')
            
            # Create the parquet file name (remove FIXED_FILE_TAG_STRING and change extension)
            parquet_file_name = (file_name
                               .replace(constants.FIXED_FILE_TAG_STRING, '')
                               .replace(constants.CSV, constants.PARQUET))
            
            # Construct the final parquet path
            parquet_path = f"{base_directory}/{constants.ArtifactPaths.CONVERTED_FILES.value}{parquet_file_name}"

            convert_statement = f"""
                COPY  (
                    SELECT
                        *
                    FROM read_csv('gs://{gcs_file_path}', null_padding=true,ALL_VARCHAR=True)
                ) TO 'gs://{parquet_path}' {constants.DUCKDB_FORMAT_STRING}
            """
            conn.execute(convert_statement)
            
    except duckdb.InvalidInputException as e:
        error_type = utils.parse_duckdb_csv_error(e)
        if error_type == "INVALID_UNICODE":
            utils.logger.warning(f"Non-UTF8 character found in file gs://{gcs_file_path}: {e}")
            convert_csv_file_encoding(gcs_file_path)

        elif error_type == "UNTERMINATED_QUOTE":
            utils.logger.warning(f"Unescaped quote found in file gs://{gcs_file_path}: {e}")
            
        elif error_type == "CSV_FORMAT_ERROR":
            utils.logger.error(f"CSV format error in file gs://{gcs_file_path}: {e}")
            sys.exit(1)
        else:
            utils.logger.error(f"Unknown CSV error in file gs://{gcs_file_path}: {e}")
            sys.exit(1)
    except Exception as e:
        utils.logger.error(f"Unable to convert CSV file to Parquet: {e}")
        sys.exit(1)
    finally:
        utils.close_duckdb_connection(conn, local_db_file, tmp_dir)
    
def convert_csv_file_encoding(gcs_file_path: str) -> None:
    """
    Creates a copy of non-UTF8 CSV files as a new CSV file with UTF8 encoding.
    File chunks are read and written in streaming mode; conversion done in memory 
    without downloading or saving files to VM.
    
    Args:
        gcs_file_path (str): Full GCS path including bucket (bucket/path/to/file.csv)
    """
    utils.logger.info("Attemping to converting file encoding to UTF8")

    # Split file path into bucket and object path
    path_parts = gcs_file_path.split('/')
    bucket_name = path_parts[0]
    file_path = '/'.join(path_parts[1:])

    try:
        # Initialize GCS client
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        
        # Source blob (aka file)
        source_blob = bucket.blob(file_path)
        
        # Verify the source file exists
        if not source_blob.exists():
            utils.logger.error(f"Source file does not exist: gs://{gcs_file_path}")
            sys.exit(1)

        # Create output filename
        file_name_parts = file_path.rsplit('.', 1)
        date_part = path_parts[1]
        file_name_part = file_name_parts[0].split('/')[1]
        file_ext = file_name_parts[1]

        new_file_path = f"{date_part}/{constants.ArtifactPaths.FIXED_FILES.value}{file_name_part}{constants.FIXED_FILE_TAG_STRING}.{file_ext}"
        target_blob = bucket.blob(new_file_path)

        utils.logger.info(f"Converting file gs://{gcs_file_path} to UTF-8 encoding...")

        # First pass: detect encoding from initial chunk
        with source_blob.open("rb") as source_file:
            initial_chunk = source_file.read(1024 * 1024)  # Read 1MB for encoding detection
            detected = chardet.detect(initial_chunk)
            detected_encoding = detected['encoding']

        if not detected_encoding:
            utils.logger.error(f"Could not detect encoding for file: {gcs_file_path}")
            sys.exit(1)

        utils.logger.info(f"Detected source encoding: {detected_encoding}")

        # Process and stream the file
        try:
            # Create streaming writer
            streaming_writer = StreamingCSVWriter(target_blob)

            # Stream from source and process
            with source_blob.open("rb") as source_file:
                # Map Windows encodings to their Python codec names
                codec_name = detected_encoding.replace('Windows-', 'cp')
                utils.logger.info(f"Using codec: {codec_name}")
                
                # Create a text wrapper that handles the encoding
                # If there's an issue with converting any of the non-UTF8 characters, replace them with a question mark symbol
                text_stream = codecs.getreader(codec_name)(source_file, errors='replace')

                csv_reader = csv.reader(text_stream)
                
                # Process CSV row by row, streaming directly to GCS
                for row in csv_reader:
                    streaming_writer.writerow(row)
            
            # Ensure all remaining data is uploaded
            streaming_writer.close()

            utils.logger.info(f"Successfully converted file to UTF-8. New file: gs://{bucket_name}/{new_file_path}")
            utils.logger.info(f"Total bytes processed: {streaming_writer.total_bytes_uploaded}\n")

            # After creating new file with UTF8 encoding, try converting it to Parquet
            csv_to_parquet(f"{bucket_name}/{new_file_path}")

        except UnicodeDecodeError as e:
            utils.logger.error(f"Failed to decode content with detected encoding {detected_encoding}: {str(e)}")
            sys.exit(1)
        except csv.Error as e:
            utils.logger.error(f"CSV parsing error: {str(e)}")
            sys.exit(1)

    except Exception as e:
        utils.logger.error(f"Unable to convert CSV to UTF8: {e}")
        sys.exit(1)

def get_table_name_from_path(gcs_file_path: str) -> str:
    # Extract file name from path and remove .parquet extension
    return gcs_file_path.split('/')[-1].replace(constants.PARQUET, '').lower()

def get_table_schema(table_name: str, cdm_version: str) -> dict:
    table_name = table_name.lower()
    schema_file = f"{constants.CDM_SCHEMA_PATH}{cdm_version}/{constants.CDM_SCHEMA_FILE_NAME}"

    try:
        with open(schema_file, 'r') as f:
            schema_json = f.read()
            schema = json.loads(schema_json)

        # Check if table exists in schema
        if table_name in schema:
            return {table_name: schema[table_name]}
        else:
            return {}
            
    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error getting table schema: {str(e)}")

def get_placeholder_value(field_name: str, field_type: str) -> str:
    # Return string representation of default value
    if field_name.endswith("_concept_id"):
        return "'0'"
    
    return {
        "VARCHAR": "''",
        "DATE": "'1970-01-01'",
        "BIGINT": "'-1'",
        "DOUBLE": "'-1.0'",
        "TIMESTAMP": "'1901-01-01 00:00:00'"
    }[field_type]

def get_fix_columns_sql_statement(gcs_file_path: str, cdm_version: str) -> str:
    """
    Generates a SQL statement that, when run:
        - Converts data types of columns within Parquet file to OMOP CDM standard
        - Creates a new Parquet file with the invalid rows from original data file
        - Converts all column names to lower case
        - Ensures consistent field order within Parquet

    This SQL has many functions, but it is far more efficient to do this all in one step,
    as compared to reading and writing the Parquet each time, for each piece of functionality.
    """
    # --------------------------------------------------------------------------
    # 1) Parse out table name and bucket/subfolder info for saving files later on
    # --------------------------------------------------------------------------
    table_name = get_table_name_from_path(gcs_file_path)
    bucket, subfolder = gcs_file_path.split('/')[:2]

    # --------------------------------------------------------------------------
    # 2) Retrieve the table schema. If not found, return empty string
    # --------------------------------------------------------------------------
    schema = get_table_schema(table_name, cdm_version)
    if not schema or table_name not in schema:
        utils.logger.warning(f"No schema found for table {table_name}")
        return ""

    fields = schema[table_name]["fields"]
    ordered_columns = list(fields.keys())  # preserve column order
    
    # --------------------------------------------------------------------------
    # 3) Initialize lists to build SQL expressions
    # --------------------------------------------------------------------------
    coalesce_exprs = []         # for "source_with_defaults" CTE
    cast_exprs = []             # for "conversion_check" CTE
    required_conditions = []    # e.g. "cc.some_col IS NOT NULL AND ..."
    
    # For invalid rows, we want everything as strings:
    invalid_select_exprs = []

    # We'll add a row ID to each row so we can join the casted data back to
    # the original data (for invalid rows). "ROW_NUMBER() OVER ()" will just
    # assign an incrementing integer to each row in the order they're read.
    row_id_col = "__rowid__"

    # --------------------------------------------------------------------------
    # 4) "source_with_defaults": Coalesce required fields if they're NULL
    # --------------------------------------------------------------------------
    for field_name in ordered_columns:
        field_type = fields[field_name]["type"]
        is_required = fields[field_name]["required"].lower() == "true"
        # Determine default value if a required field is NULL
        default_value = (
            get_placeholder_value(field_name, field_type) if is_required else "NULL"
        )
        coalesce_exprs.append(f"COALESCE({field_name}, {default_value}) AS {field_name}")
    # Add the row ID
    coalesce_exprs.append(f"ROW_NUMBER() OVER () AS {row_id_col}")

    # Combine into one SQL snippet for the first CTE
    coalesce_definitions_sql = ",\n                ".join(coalesce_exprs)

    # --------------------------------------------------------------------------
    # 5) "conversion_check": Overwrite each column with TRY_CAST(...) AS field_name
    # --------------------------------------------------------------------------
    #    - We assume field_type already matches DuckDB types (e.g., 'BIGINT', 'DATE', etc.).
    #    - If the cast fails, the column becomes NULL.
    #    - For required fields, we'll require cc.field_name IS NOT NULL.
    for field_name in ordered_columns:
        field_type = fields[field_name]["type"].upper()  # standardize to uppercase
        is_required = fields[field_name]["required"].lower() == "true"

        if field_type != "VARCHAR":
            # For non-VARCHAR fields, do an in-place cast
            cast_exprs.append(f"TRY_CAST({field_name} AS {field_type}) AS {field_name}")
            if is_required:
                required_conditions.append(f"cc.{field_name} IS NOT NULL")
        else:
            # For VARCHAR fields, no cast needed
            cast_exprs.append(field_name)
            if is_required:
                required_conditions.append(f"cc.{field_name} IS NOT NULL")

    # Also carry forward the row_id column so we can join back to the original data
    cast_exprs.append(row_id_col)
    cast_definitions_sql = ",\n                ".join(cast_exprs)

    # --------------------------------------------------------------------------
    # 6) Build the WHERE clauses for valid vs. invalid rows
    # --------------------------------------------------------------------------
    where_clause_valid = " AND ".join(required_conditions) if required_conditions else "TRUE"
    where_clause_invalid = f"NOT ({where_clause_valid})" if required_conditions else "FALSE"

    # --------------------------------------------------------------------------
    # 7) For invalid rows, keep everything as VARCHAR
    # --------------------------------------------------------------------------
    for field_name in ordered_columns:
        invalid_select_exprs.append(f"CAST(swd.{field_name} AS VARCHAR) AS {field_name}")
    invalid_select_sql = ",\n            ".join(invalid_select_exprs)

    # --------------------------------------------------------------------------
    # 8) Construct the final SQL statements
    # --------------------------------------------------------------------------
    create_valid_rows_sql = f"""
        CREATE OR REPLACE TEMP TABLE valid_rows AS
        WITH source_with_defaults AS (
            SELECT
                {coalesce_definitions_sql}
            FROM 'gs://{gcs_file_path}'
        ),
        conversion_check AS (
            SELECT
                {cast_definitions_sql}
            FROM source_with_defaults
        )
        SELECT
            cc.*
        FROM conversion_check cc
        WHERE {where_clause_valid};
    """.strip()

    create_invalid_rows_sql = f"""
        CREATE OR REPLACE TEMP TABLE invalid_rows AS
        WITH source_with_defaults AS (
            SELECT
                {coalesce_definitions_sql}
            FROM 'gs://{gcs_file_path}'
        ),
        conversion_check AS (
            SELECT
                {cast_definitions_sql}
            FROM source_with_defaults
        )
        SELECT
            {invalid_select_sql}
        FROM source_with_defaults swd
        JOIN conversion_check cc
          ON swd.{row_id_col} = cc.{row_id_col}
        WHERE {where_clause_invalid};
    """.strip()

    copy_valid_sql = f"""
        COPY valid_rows
        TO 'gs://{gcs_file_path}'
        {constants.DUCKDB_FORMAT_STRING};
    """.strip()

    copy_invalid_sql = f"""
        COPY invalid_rows
        TO 'gs://{bucket}/{subfolder}/{constants.ArtifactPaths.INVALID_ROWS.value}{table_name}{constants.PARQUET}'
        {constants.DUCKDB_FORMAT_STRING};
    """.strip()

    sql_script = f"""
        {create_valid_rows_sql};

        {create_invalid_rows_sql};

        {copy_valid_sql};

        {copy_invalid_sql};
    """.strip()
    
    return sql_script

def fix_columns(gcs_file_path: str, cdm_version: str) -> None:
    fix_sql = get_fix_columns_sql_statement(gcs_file_path, cdm_version)

    conn, local_db_file, tmp_dir = utils.create_duckdb_connection()
    
    # Only run the fix SQL statement if it exists
    # Statement will exist only for tables/files in OMOP CDM
    if fix_sql and len(fix_sql) > 1:
        try:
            with conn:
                conn.execute(fix_sql)
        except Exception as e:
            utils.logger.error(f"Unable to fix Parquet file: {e}")
            sys.exit(1)
        finally:
            utils.close_duckdb_connection(conn, local_db_file, tmp_dir)