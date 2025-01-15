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
        "string": "''",
        "date": "'1970-01-01'",
        "integer": "'-1'",
        "float": "'-1.0'",
        "datetime": "'1901-01-01 00:00:00'"
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

    utils.logger.warning("in get_fix_columns_sql_statement() function")
    utils.logger.warning(f"The file path is {gcs_file_path}")
    utils.logger.warning(f"OMOP version is {cdm_version}")

    # -------------------------
    # 0) Helper: map schema type to DuckDB type
    # -------------------------
    def _map_schema_type_to_duckdb_type(schema_type: str) -> str:
        type_map = {
            "string": "VARCHAR",
            "integer": "INTEGER",
            "float": "DOUBLE",
            "date": "DATE",
            "datetime": "TIMESTAMP",  # "datetime" => DuckDB TIMESTAMP
        }
        return type_map.get(schema_type.lower(), "VARCHAR")

    # Extract table name from file path
    table_name = get_table_name_from_path(gcs_file_path)

    # Split path for bucket/subfolder
    bucket, subfolder = gcs_file_path.split('/')[:2]
    utils.logger.warning(f"Bucket: {bucket}")
    utils.logger.warning(f"Subfolder: {subfolder}")
    
    # Get schema for this table
    schema = get_table_schema(table_name, cdm_version)
    if not schema or table_name not in schema:
        utils.logger.warning(f"No schema found for table {table_name}")
        return ""

    fields = schema[table_name]["fields"]
    ordered_columns = list(fields.keys())

    # Lists we'll build up
    coalesce_definitions = []
    cast_definitions = []
    required_conditions = []

    # -------------------------
    # 1) Build COALESCE for required columns
    # -------------------------
    #   "source_with_defaults": If required col is missing, fill w/ placeholder
    for field_name in ordered_columns:
        field_info = fields[field_name]
        field_type = field_info["type"].lower()
        is_required = field_info["required"].lower() == "true"

        # For required fields, set a default if NULL; else just keep it as is
        default_value = get_placeholder_value(field_name, field_type) if is_required else "NULL"

        coalesce_definitions.append(
            f"COALESCE({field_name}, {default_value}) AS {field_name}"
        )

    # -------------------------
    # 2) In the second CTE, we do the actual overwrite cast
    # -------------------------
    #   "conversion_check": Overwrite each column with TRY_CAST(...) 
    #                       for non-string fields 
    #                       (string fields remain as-is).
    #   If the cast fails, we get NULL in that column.
    for field_name in ordered_columns:
        field_info = fields[field_name]
        field_type = field_info["type"].lower()
        duckdb_type = _map_schema_type_to_duckdb_type(field_type)
        is_required = field_info["required"].lower() == "true"

        if field_type != "string":
            cast_definitions.append(f"TRY_CAST({field_name} AS {duckdb_type}) AS {field_name}")
            if is_required:
                # If required => must not be NULL after TRY_CAST
                required_conditions.append(f"{field_name} IS NOT NULL")
        else:
            # Keep the original as-is for strings
            cast_definitions.append(field_name)
            # if it's required => must not be NULL 
            if is_required:
                required_conditions.append(f"{field_name} IS NOT NULL")

    # Build the big WHERE clause for "valid" rows:
    # If you have no required fields, this might be empty -> use 'TRUE'
    where_clause_valid = " AND ".join(required_conditions) if required_conditions else "TRUE"

    # For invalid rows, we do the negation of the above
    # But the negation is simply `NOT (fieldA IS NOT NULL AND fieldB IS NOT NULL AND ...)`
    # which is `fieldA IS NULL OR fieldB IS NULL OR ...`
    # We'll just do `NOT ( <the same AND> )`
    where_clause_invalid = f"NOT ({where_clause_valid})" if required_conditions else "FALSE"

    # -------------------------
    # 3) Create TEMP TABLE valid_rows
    # -------------------------
    create_valid_rows_sql = f"""
        CREATE OR REPLACE TEMP TABLE valid_rows AS
        WITH source_with_defaults AS (
            SELECT
                {', '.join(coalesce_definitions)}
            FROM 'gs://{gcs_file_path}'
        ),
        conversion_check AS (
            SELECT
                {', '.join(cast_definitions)}
            FROM source_with_defaults
        )
        SELECT
            *
        FROM conversion_check
        WHERE {where_clause_valid};
    """.strip()

    # -------------------------
    # 4) Create TEMP TABLE invalid_rows
    #    i.e., any row that didn't meet the "required conditions"
    # -------------------------
    create_invalid_rows_sql = f"""
        CREATE OR REPLACE TEMP TABLE invalid_rows AS
        WITH source_with_defaults AS (
            SELECT
                {', '.join(coalesce_definitions)}
            FROM 'gs://{gcs_file_path}'
        ),
        conversion_check AS (
            SELECT
                {', '.join(cast_definitions)}
            FROM source_with_defaults
        )
        SELECT
            *
        FROM conversion_check
        WHERE {where_clause_invalid};
    """.strip()

    # -------------------------
    # 5) COPY valid_rows back to original Parquet (overwrite)
    # -------------------------
    copy_valid_sql = f"""
        COPY valid_rows
        TO 'gs://{gcs_file_path}'
        {constants.DUCKDB_FORMAT_STRING};
    """.strip()

    # -------------------------
    # 6) COPY invalid_rows to invalid_<table>.parquet
    # -------------------------
    copy_invalid_sql = f"""
        COPY invalid_rows
        TO 'gs://{bucket}/{subfolder}/{constants.ArtifactPaths.INVALID_ROWS.value}invalid_{table_name}{constants.PARQUET}'
        {constants.DUCKDB_FORMAT_STRING};
    """.strip()

    # -------------------------
    # 7) Return as one multi-statement script
    # -------------------------
    sql_script = f"""
        {create_valid_rows_sql};

        {create_invalid_rows_sql};

        {copy_valid_sql};

        {copy_invalid_sql};
    """.strip()

    return sql_script

def fix_columns(gcs_file_path: str, cdm_version: str) -> None:
    fix_sql = get_fix_columns_sql_statement(gcs_file_path, cdm_version)
    utils.logger.warning(f"RUNNING fix_columns ; gcs_file_path IS {gcs_file_path} AND cdm_version IS {cdm_version}")

    utils.logger.warning(f"!! The SQL to fix the Parquet for {gcs_file_path} is: ")
    utils.logger.warning(fix_sql)

    conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

    if fix_sql and len(fix_sql) > 1:
        utils.logger.warning("Will execute SQL")
        try:
            with conn:
                utils.logger.warning("About to run the SQL statement!")
                conn.execute(fix_sql)
        except Exception as e:
            utils.logger.error(f"Unable to fix Parquet file: {e}")
            sys.exit(1)
        finally:
            utils.close_duckdb_connection(conn, local_db_file, tmp_dir)
    else:
        utils.logger.warning("Will NOTTTTT execute SQL")