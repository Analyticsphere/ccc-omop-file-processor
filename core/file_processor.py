import codecs
import csv
import os
from io import StringIO

import chardet  # type: ignore
import duckdb  # type: ignore
from google.cloud import storage  # type: ignore

import core.constants as constants
import core.helpers.report_artifact as report_artifact
import core.utils as utils


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
        raise Exception(f"Invalid source file format: {file_type}")

def process_incoming_parquet(gcs_file_path: str) -> None:
    """
    - Validates that the Parquet file at gcs_file_path in GCS is readable by DuckDB.
    - Copies incoming Parquet file to artifact GCS directory, ensuring:
       - The output file name is all lowercase
       - All column names within the Parquet file are all lowercase.
    """
    if utils.valid_parquet_file(gcs_file_path):
        # Get columns from parquet file
        parquet_columns = utils.get_columns_from_file(gcs_file_path)
        select_list = []

        # Handle offset column in note_nlp
        # May come in as offset or "offset" and need different handling for each scenario
        for column in parquet_columns:
            if column == '"offset"':
                select_list.append(f'""{column}"" AS {column.lower()}')
            elif column == 'offset':
                select_list.append(f'"{column}" AS "{column.lower()}"')
            else:
                select_list.append(f"{column} AS {column.lower()}")
        select_clause = ", ".join(select_list)

        conn, local_db_file = utils.create_duckdb_connection()
        try:
            with conn:

                copy_sql = f"""
                    COPY (
                        SELECT {select_clause}
                        FROM read_parquet('gs://{gcs_file_path}')
                    )
                    TO 'gs://{utils.get_parquet_artifact_location(gcs_file_path)}' {constants.DUCKDB_FORMAT_STRING}
                """

                conn.execute(copy_sql)
        except Exception as e:
            utils.logger.error(f"Unable to process incoming Parquet file: {e}")
            raise Exception(f"Unable to process incoming Parquet file: {e}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)
    else:
        utils.logger.error(f"Invalid Parquet file")
        raise Exception(f"Invalid Parquet file")

def csv_to_parquet(gcs_file_path: str, retry: bool = False, conversion_options: list = []) -> None:
    conn, local_db_file = utils.create_duckdb_connection()

    try:
        with conn:
            parquet_path = utils.get_parquet_artifact_location(gcs_file_path)

            csv_column_names = utils.get_columns_from_file(gcs_file_path)

            select_list = []
            for column in csv_column_names:
                # get rid of " characters in column names to prevent double double quoting in offset handling
                column_alias = column.lower().replace('"', '')
                select_list.append(f"{column} AS {column_alias}")
            select_clause = ", ".join(select_list)

            # note_nlp has column name 'offset' which is a reserved keyword in DuckDB
            # Special handling required to prevent parsing error

            # Re-add double quotes to offset field prevent DuckDB from returning parsing error
            select_clause = select_clause.replace('offset', '"offset"')

            # Convert CSV to Parquet with lowercase column names
            convert_statement = f"""
                COPY (
                    SELECT {select_clause}
                    FROM read_csv('gs://{gcs_file_path}', 
                        null_padding=True, ALL_VARCHAR=True, strict_mode=False {format_list(conversion_options)})
                ) TO 'gs://{parquet_path}' {constants.DUCKDB_FORMAT_STRING}
            """

            conn.execute(convert_statement)
    except Exception as e:
        if not retry:
            # DuckDB doesn't have very specific exception types; this function allows us to catch and handle specific DuckDB errors
            error_type = utils.parse_duckdb_csv_error(e)
            if error_type == "INVALID_UNICODE":
                utils.logger.warning(f"Non-UTF8 character found in file gs://{gcs_file_path}: {e}")
                convert_csv_file_encoding(gcs_file_path)
            elif error_type == "UNTERMINATED_QUOTE":
                utils.logger.warning(f"Attempting to correct unescaped quote characters in file gs://{gcs_file_path}: {e}")
                fix_csv_quoting(gcs_file_path)
            elif error_type == "CSV_FORMAT_ERROR":
                utils.logger.error(f"CSV format error in file gs://{gcs_file_path}: {e}")
                raise Exception(f"CSV format error in file gs://{gcs_file_path}: {e}") from e
            else:
                utils.logger.error(f"Unable to convert CSV file to Parquet gs://{gcs_file_path}: {e}")
                raise Exception(f"Unable to convert CSV file to Parquet gs://{gcs_file_path}: {e}") from e
        else:
            utils.logger.error(f"Unable to convert CSV file to Parquet gs://{gcs_file_path}: {e}")
            raise Exception(f"Unable to convert CSV file to Parquet gs://{gcs_file_path}: {e}") from e    
    finally:
        utils.close_duckdb_connection(conn, local_db_file)
    
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
            raise Exception(f"Source file does not exist: gs://{gcs_file_path}")

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
            raise Exception(f"Could not detect encoding for file: {gcs_file_path}")

        utils.logger.info(f"Detected source encoding: {detected_encoding}")

        # Process and stream the file
        try:
            # Create streaming writer
            streaming_writer = StreamingCSVWriter(target_blob)

            # Stream from source and process
            with source_blob.open("rb") as source_file:
                # Map Windows encodings to their Python codec names
                codec_name = detected_encoding.replace('Windows-', 'cp')
                
                # Create a text wrapper that handles the encoding
                # If there's an issue with converting any of the non-UTF8 characters, replace them with a question mark symbol
                    # mypy doesn't recognize that the stream reader constructor accepts the errors parameter; skip checking in mypy
                text_stream = codecs.getreader(codec_name)(source_file, errors='replace') # type: ignore[call-arg]

                csv_reader = csv.reader(text_stream)
                
                # Process CSV row by row, streaming directly to GCS
                for row in csv_reader:
                    streaming_writer.writerow(row)
            
            # Ensure all remaining data is uploaded
            streaming_writer.close()

            utils.logger.info(f"Successfully converted file to UTF-8. New file: gs://{bucket_name}/{new_file_path}")

            # After creating new file with UTF8 encoding, try converting it to Parquet
            # store_rejects = True leads to more malformed rows getting included, but may add unexpected columns
            # Unexpected columns will be reported in data delivery report, and normalization step will remove them
            csv_to_parquet(f"{bucket_name}/{new_file_path}", True, ['store_rejects=True'])

        except UnicodeDecodeError as e:
            utils.logger.error(f"Failed to decode content with detected encoding {detected_encoding}: {str(e)}")
            raise Exception(f"Failed to decode content with detected encoding {detected_encoding}: {str(e)}") from e
        except csv.Error as e:
            utils.logger.error(f"CSV parsing error: {str(e)}")
            raise Exception(f"CSV parsing error: {str(e)}") from e

    except Exception as e:
        utils.logger.error(f"Unable to convert CSV to UTF8: {e}")
        raise Exception(f"Unable to convert CSV to UTF8: {e}") from e

def get_placeholder_value(field_name: str, field_type: str) -> str:
    # Return string representation of default value, based on field type

    # *All* fields that end in _concept_id must be populated
    # If a concept is unknown, OHDSI convention is to explicity populate field with concept_id 0
    if field_name.endswith("_concept_id"):
        return "'0'"

    default_value = constants.DEFAULT_FIELD_VALUES[field_type]
    
    return default_value

def get_normalization_sql_statement(gcs_file_path: str, cdm_version: str) -> str:
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
    # --------------------------------------------------------------------------
    actual_columns = utils.get_columns_from_file(gcs_file_path)

    # --------------------------------------------------------------------------
    # Initialize lists to build SQL expressions
    # --------------------------------------------------------------------------
    coalesce_exprs = []
    row_validity = []    # e.g. "cc.some_col IS NOT NULL AND ..."

    # --------------------------------------------------------------------------
    # Coalesce required fields if they're NULL, or generate a placeholder column if that field doesn't exist at all.
    # --------------------------------------------------------------------------
    for field_name in ordered_columns:
        field_type = fields[field_name]["type"]
        is_required = fields[field_name]["required"].lower() == "true"

        # Determine default value if a required field is NULL
        default_value = get_placeholder_value(field_name, field_type) if is_required or field_name.endswith("_concept_id") else "NULL"

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

    # If row_validity list has no statements, add a string so SQL statement stays valid
    if not row_validity:
        row_validity.append("'faketext'")
    row_validity_sql = ", ".join(row_validity)

    # Build row_check table with row_hash column
    # Uniquely identify invalid rows using hash generated from concatenting each column
        # If two rows have the same values, it will result in same hash, but that is okay for this use case
    # Use the hash values to identify invalid rows from original Parquet, and save those rows to a seperate file
        # Need to save from original file because row_check will have TRY_CAST result, and will obscure original, invalid values
    # Resave over original parquet file, saving only the rows which are valid
    sql_script = f"""
        CREATE OR REPLACE TABLE row_check AS
            SELECT
                {coalesce_definitions_sql},
                CASE 
                    WHEN COALESCE({row_validity_sql}) IS NULL THEN md5(CONCAT({row_hash_statement}))
                    ELSE NULL END AS row_hash
            FROM read_parquet('gs://{gcs_file_path}')
        ;

        COPY (
            SELECT *
            FROM read_parquet('gs://{gcs_file_path}')
            WHERE md5(CONCAT({row_hash_statement})) IN (
                SELECT row_hash FROM row_check WHERE row_hash IS NOT NULL
            )
        ) TO 'gs://{utils.get_invalid_rows_path_from_gcs_path(gcs_file_path)}' {constants.DUCKDB_FORMAT_STRING}
        ;

        COPY (
            SELECT * EXCLUDE (row_hash)
            FROM row_check
            WHERE row_hash IS NULL
        ) TO 'gs://{gcs_file_path}' {constants.DUCKDB_FORMAT_STRING}
        ;

    """.strip()

    # note_nlp has column name 'offset' which is a reserved keyword in DuckDB
    # Need to add "" around offset column name to prevent parsing error
    sql_script = sql_script.replace('offset', '"offset"')

    return sql_script

def normalize_file(gcs_file_path: str, cdm_version: str) -> None:
    fix_sql = get_normalization_sql_statement(gcs_file_path, cdm_version)
    
    # Only run the fix SQL statement if it exists
    # Statement will exist only for tables/files in OMOP CDM
    if fix_sql and len(fix_sql) > 1:
        conn, local_db_file = utils.create_duckdb_connection()

        try:
            with conn:
                conn.execute(fix_sql)
                # Get counts of valid/invalid rows for OMOP files
                create_row_count_artifacts(gcs_file_path, cdm_version, conn)
        except Exception as e:
            utils.logger.error(f"Unable to normalize Parquet file: {e}")
            raise Exception(f"Unable to normalize Parquet file: {e}") from e
        finally:
            utils.close_duckdb_connection(conn, local_db_file)

def create_row_count_artifacts(gcs_file_path: str, cdm_version: str, conn: duckdb.DuckDBPyConnection) -> None:
    table_name = utils.get_table_name_from_gcs_path(gcs_file_path)
    table_concept_id = utils.get_cdm_schema(cdm_version)[table_name]['concept_id']
    bucket, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)

    valid_rows_file = (utils.get_parquet_artifact_location(gcs_file_path), 'Valid row count')
    invalid_rows_file = (utils.get_invalid_rows_path_from_gcs_path(gcs_file_path),  'Invalid row count')

    files = [valid_rows_file, invalid_rows_file]

    for file in files:
        file_path, count_type = file

        count_query = f"""
            SELECT COUNT(*) FROM read_parquet('gs://{file_path}')
        """
        result = conn.execute(count_query).fetchone()[0]

        ra = report_artifact.ReportArtifact(
            delivery_date=delivery_date,
            artifact_bucket=bucket,
            concept_id=table_concept_id,
            name=f"{count_type}: {table_name}",
            value_as_string=None,
            value_as_concept_id=None,
            value_as_number=result
        )
        ra.save_artifact()

def fix_csv_quoting(gcs_file_path: str) -> None:
    """
    Handles some unquoted quote patterns in CSV files 
    Each CSV row is evaulated as a single string, and regex replacements are made to escape problematic characters
    
    File gets downloaded from GCS to VM executing Cloud Function.
    Streaming CSV file directly from GCS to VM resulted in unexpected behaviors, so the logic executes against a local file
    Ideally, we would read/write directly to/from GCS...
    """
    encoding: str = 'utf-8'
    batch_size: int = 1000

    # Download and get path to local CSV file
    broken_csv_path = utils.download_from_gcs(gcs_file_path)

    # Create output path, renaming original file
    filename = utils.get_table_name_from_gcs_path(gcs_file_path)
    output_csv_path = broken_csv_path.replace(filename, f"{filename}{constants.FIXED_FILE_TAG_STRING}")

    try:
        with open(broken_csv_path, 'r', encoding=encoding) as infile, \
             open(output_csv_path, 'w', encoding=encoding, newline='') as outfile:
            
            # Process header separately to preserve it exactly as is
            header = next(infile, None)
            if header is not None:
                outfile.write(header)
            
            # Process the rest of the file in batches
            batch = []
            for line in infile:
                batch.append(clean_csv_row(line.strip()))
                
                if len(batch) >= batch_size:
                    outfile.write('\n'.join(batch) + '\n')
                    batch = []
            
            # Write any remaining rows
            if batch:
                outfile.write('\n'.join(batch) + '\n')

            # Build GCS location for locally fixed file
            bucket, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_file_path)
            destination_blob = f"{delivery_date}/{constants.ArtifactPaths.FIXED_FILES.value}{filename}{constants.FIXED_FILE_TAG_STRING}{constants.CSV}"
            # Upload fixed file to GCS
            utils.upload_to_gcs(output_csv_path, bucket, destination_blob)

            # Delete local files
            os.remove(broken_csv_path)
            os.remove(output_csv_path)
            
            # After creating new file with fixed quoting, try converting it to Parquet
            # store_rejects = True leads to more malformed rows getting included, but may add unexpected columns
            # Unexpected columns will be reported in data delivery report, and normalization step will remove them
            csv_to_parquet(f"{bucket}/{destination_blob}", True, ['store_rejects=True'])
                
    except UnicodeDecodeError:
        raise ValueError(f"Failed to read the file with {encoding} encoding. Try a different encoding.")

def clean_csv_row(row: str) -> str:
    """
    Clean a CSV row by properly handling various quoting issues:
    1. Quotes within quoted fields need to be properly escaped (doubled)
    2. Fields containing single quotes need to be properly quoted
    3. Fields with consecutive single quotes need to be properly quoted
    """
    # First pass: Fix quotes within quoted fields
    result = []
    i = 0
    in_quotes = False
    
    while i < len(row):
        char = row[i]
        
        if char == '"':
            if not in_quotes:
                # Start of quoted field
                in_quotes = True
                result.append(char)
            else:
                # Could be end of quoted field or internal quote
                if i + 1 < len(row):
                    next_char = row[i + 1]
                    if next_char == '"':
                        # Already escaped quote
                        result.append('""')
                        i += 1
                    elif next_char != ',' and next_char != '\r' and next_char != '\n':
                        # Internal quote that needs escaping (Example 1)
                        result.append('""')
                    else:
                        # End of quoted field
                        in_quotes = False
                        result.append(char)
                else:
                    # End of quoted field at end of string
                    in_quotes = False
                    result.append(char)
        else:
            result.append(char)
        
        i += 1
    
    # Second pass: Split into fields and handle single quotes
    row_with_fixed_quotes = ''.join(result)
    fields = []
    current = []
    in_quotes = False
    i = 0
    
    while i < len(row_with_fixed_quotes):
        char = row_with_fixed_quotes[i]
        
        if char == '"':
            current.append(char)
            if not in_quotes:
                in_quotes = True
            else:
                # Check if it's an escaped quote
                if i + 1 < len(row_with_fixed_quotes) and row_with_fixed_quotes[i + 1] == '"':
                    current.append(row_with_fixed_quotes[i + 1])
                    i += 1
                else:
                    in_quotes = False
        elif char == ',' and not in_quotes:
            # End of field
            fields.append(''.join(current))
            current = []
        else:
            current.append(char)
        
        i += 1
    
    # Add the last field
    if current or not fields:
        fields.append(''.join(current))
    
    # Process fields with single quotes
    cleaned_fields = []
    for field in fields:
        # Handle single quotes (Examples 2 & 3)
        # If field contains single quotes and isn't already quoted
        if "'" in field and not (field.startswith('"') and field.endswith('"')):
            field = f'"{field}"'
        
        # Fields with single quotes aren't get read properly...
        if field == "\"'\"":
            field = "''"
        
        cleaned_fields.append(field)
    
    return ','.join(cleaned_fields)

def format_list(items: list) -> str:
    if not items:  # Check if list is empty
        return ''
    else:
        return ',' + ', '.join(items)
