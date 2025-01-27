import core.constants as constants
import core.utils as utils
import sys
import chardet # type: ignore
from io import StringIO
import csv
import codecs
from google.cloud import storage # type: ignore
import duckdb # type: ignore


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

def process_incoming_file(file_type: str, gcs_file_path: str) -> None:
    if file_type == constants.CSV:
        csv_to_parquet(gcs_file_path)
    elif file_type == constants.PARQUET:
        process_incoming_parquet(gcs_file_path)
    else:
        utils.logger.info(f"Invalid source file format: {file_type}") 

def get_parquet_artifact_location(gcs_file_path: str) -> str:
    file_name = utils.get_table_name_from_gcs_path(gcs_file_path)
    base_directory, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
    
    # Remove trailing slash if present
    base_directory = base_directory.rstrip('/')
    
    # Create the parquet file name
    parquet_file_name = f"{file_name}{constants.PARQUET}"
    
    # Construct the final parquet path
    parquet_path = f"{base_directory}/{delivery_date}/{constants.ArtifactPaths.CONVERTED_FILES.value}{parquet_file_name}"

    return parquet_path

def process_incoming_parquet(gcs_file_path: str) -> None:
    """
    - Validates that the Parquet file at gcs_file_path in GCS is readable by DuckDB.
    - Copies incoming Parquet file to artifact GCS directory, ensuring:
       - The output file name is all lowercase
       - All column names within the Parquet file are all lowercase.
    """
    if utils.valid_parquet_file(gcs_file_path):
        # Built a SELECT statement which will copy original Parquet
        # to a new Parquet file, converting column names to lower case
        parquet_columns = utils.get_columns_from_parquet(gcs_file_path)

        select_list = []
        for column in parquet_columns:
            select_list.append(f"{column} AS {column.lower()}")
        select_clause = ", ".join(select_list)

        conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

        try:
            with conn:
                # Execute the SELECT statement to copy Parquet file
                copy_sql = f"""
                    COPY (
                        SELECT {select_clause}
                        FROM read_parquet('gs://{gcs_file_path}')
                    )
                    TO 'gs://{get_parquet_artifact_location(gcs_file_path)}' {constants.DUCKDB_FORMAT_STRING}
                """
                conn.execute(copy_sql)
        except Exception as e:
            utils.logger.error(f"Unable to processing incoming Parquet file: {e}")
            sys.exit(1)
        finally:
            utils.close_duckdb_connection(conn, local_db_file, tmp_dir)
    else:
        utils.logger.error(f"Invalid Parquet file")
        sys.exit(1)


def csv_to_parquet(gcs_file_path: str) -> None:
    conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

    try:
        with conn:
            parquet_path = get_parquet_artifact_location(gcs_file_path)

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
    utils.logger.info("Attemping to convert file encoding to UTF8")

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
                 #mypy doesn't recognize that the stream reader constructor accepts the errors parameter; skip checking in mypy
                text_stream = codecs.getreader(codec_name)(source_file, errors='replace') # type: ignore[call-arg]

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

def get_placeholder_value(field_name: str, field_type: str) -> str:
    # Return string representation of default value, based on field type

    # *All* fields that end in _concept_id must be populated
    # If a concept is unknown, OHDSI convention is to explicity populate field with concept_id 0
    if field_name.endswith("_concept_id"):
        return "'0'"

    default_value = constants.DEFAULT_FIELD_VALUES[field_type]
    
    return default_value

def get_fix_columns_sql_statement(gcs_file_path: str, cdm_version: str) -> str:
    """
    Generates a SQL statement that, when run:
        - Converts data types of columns within Parquet file to OMOP CDM standard
        - Creates a new Parquet file with the invalid rows from the original data file
        - Converts all column names to lower case
        - Ensures consistent field order within Parquet

    This SQL has many functions, but it is far more efficient to do this all in one step,
    as compared to reading and writing the Parquet each time, for each piece of functionality.
    """

    # --------------------------------------------------------------------------
    # Parse out table name and bucket/subfolder info
    # --------------------------------------------------------------------------
    table_name = utils.get_table_name_from_gcs_path(gcs_file_path).lower()
    bucket, subfolder = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)

    # --------------------------------------------------------------------------
    # Retrieve the table schema. If not found, return empty string
    # --------------------------------------------------------------------------
    schema = utils.get_table_schema(table_name, cdm_version)
    if not schema or table_name not in schema:
        utils.logger.warning(f"No schema found for table {table_name}")
        return ""

    fields = schema[table_name]["fields"]
    ordered_columns = list(fields.keys())  # preserve column order

    # --------------------------------------------------------------------------
    # Identify which columns actually exist in the Parquet file 
    #     using our new helper function
    # --------------------------------------------------------------------------
    actual_columns = utils.get_columns_from_parquet(gcs_file_path)

    # --------------------------------------------------------------------------
    # Initialize lists to build SQL expressions
    # --------------------------------------------------------------------------
    coalesce_exprs = []
    row_validity = []    # e.g. "cc.some_col IS NOT NULL AND ..."

    # --------------------------------------------------------------------------
    # "source_with_defaults": Coalesce required fields if they're NULL,
    #    or generate a placeholder column if that field doesn't exist at all.
    # --------------------------------------------------------------------------
    for field_name in ordered_columns:
        field_type = fields[field_name]["type"]
        is_required = fields[field_name]["required"].lower() == "true"

        # Determine default value if a required field is NULL
        default_value = get_placeholder_value(field_name, field_type) if is_required else "NULL"

        # Build concat statement that will eventually be hashed to identify rows
        row_hash_statement = ", ".join([f"COALESCE(CAST({field_name} AS VARCHAR), '')" for field_name in actual_columns])

        if field_name in actual_columns:           
            # If the column exists in the Parquet file, coalesce it with the default value, and try casting to expected type
            if default_value != "NULL":
                coalesce_exprs.append(f"TRY_CAST(COALESCE({field_name}, {default_value}) AS {field_type}) AS {field_name}")
            # If default value is NULL, don't coalesce
            else:
                coalesce_exprs.append(f"TRY_CAST({field_name} AS {field_type}) AS {field_name}")
            
            # If the field is provided, and it's required, add it to list of fields which must be of correct type
            if is_required:
                # In the final SQL statement, need to confirm that ALL required fields can be cast to their correct types
                # If any one of the fields cannot be cast to the correct type, the entire row fails
                # To do this check in one shot, perform a single COALESCE within the SQL statement
                # ALL fields in a COALESCE must be of the same type, so casting everything to VARCHAR *after* trying to cast it to its correct type
                row_validity.append(f"CAST(TRY_CAST(COALESCE({field_name}, {default_value}) AS {field_type}) AS VARCHAR)")
        else:
            # If the column doesn't exist, just produce a placeholder (NULL or a special default)
            # Still need to cast to ensure consist field types
            coalesce_exprs.append(f"CAST({default_value} AS {field_type}) AS {field_name}")

            # If the field IS NOT PROVIDED but it's still required - this is not a failed row; just use a default value
            # No need to add missing, required rows to row_validity check


    coalesce_definitions_sql = ",\n                ".join(coalesce_exprs)
    row_validity_sql = ", ".join(row_validity)

    # Build row_check table with row_validity column to indicate whether row is valid or not
    # Uniquely identify rows using hash generated from concatenting each column
        # If two rows have the same values, it will result in same hash, but that is okay for this use case
    # Use hash values to identify invalid rows from original Parquet, and save those rows to a seperate file
        # Need to save from original file because row_check will have TRY_CAST result, and will obscure original, invalid values
    # Resave over original parquet file, saving only the rows which are valid
    sql_script = f"""
        CREATE OR REPLACE TABLE row_check AS
            SELECT
                {coalesce_definitions_sql},
                CASE WHEN COALESCE({row_validity_sql}) IS NOT NULL THEN 'valid_row'
                ELSE 'invalid_row' END AS 'row_validity',
                md5(CONCAT({row_hash_statement})) AS rowhash
            FROM read_parquet('gs://{gcs_file_path}')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://{gcs_file_path}')
            WHERE md5(CONCAT({row_hash_statement})) IN (
                SELECT rowhash FROM row_check WHERE row_validity = 'invalid_row'
            )
        ) TO 'gs://{bucket}/{subfolder}/{constants.ArtifactPaths.INVALID_ROWS.value}{table_name}{constants.PARQUET}' {constants.DUCKDB_FORMAT_STRING}
        ;

        COPY (
            SELECT * EXCLUDE (row_validity,rowhash)
            FROM row_check
            WHERE row_validity = 'valid_row'
        ) TO 'gs://{gcs_file_path}' {constants.DUCKDB_FORMAT_STRING}
        ;

    """.strip()

    return sql_script

def fix_columns(gcs_file_path: str, cdm_version: str) -> None:
    fix_sql = get_fix_columns_sql_statement(gcs_file_path, cdm_version)

    utils.logger.warning(f"!!!SQL statement is: ")
    remove_new_line = fix_sql.replace("\n", "  ")
    utils.logger.warning(f"{remove_new_line}")

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