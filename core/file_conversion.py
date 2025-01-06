import core.constants as constants
import core.utils as utils
import sys
import chardet
from io import StringIO
import csv
import codecs
from google.cloud import storage
import duckdb

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
                if_generation_match=None,
                num_retries=3,
                size=len(content),
                resumable=True,
                chunk_size=1024*1024  # 1MB chunks for upload
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

            file_name = gcs_file_path.split('/')[-1].lower()

            parquet_artifacts = gcs_file_path.replace(file_name, constants.ArtifactPaths.CONVERTED_FILES.value)

            # Replace CSV file extension with Parquet extension
            parquet_file_name = file_name.replace(constants.CSV,constants.PARQUET)
            # Remove FIXED_FILE_TAG_STRING string, if reprocessing file after it's been converted to UTF8
            parquet_file_name = file_name.replace(constants.FIXED_FILE_TAG_STRING,'')

            parquet_path = f"{parquet_artifacts}{parquet_file_name}"

            convert_statement = f"""
                COPY  (
                    SELECT
                        *
                    FROM read_csv('gs://{gcs_file_path}', null_padding=true,ALL_VARCHAR=True)
                ) TO 'gs://{parquet_path}' {constants.DUCKDB_FORMAT_STRING}
            """
            conn.execute(convert_statement)
            utils.logger.info("File successfully converted\n")
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
        new_file_path = f"{file_name_parts[0]}{constants.FIXED_FILE_TAG_STRING}.{file_name_parts[1]}"
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
                text_stream = codecs.getreader(codec_name)(source_file)
                csv_reader = csv.reader(text_stream)
                
                # Process CSV row by row, streaming directly to GCS
                for row in csv_reader:
                    streaming_writer.writerow(row)
            
            # Ensure all remaining data is uploaded
            streaming_writer.close()

            utils.logger.info(f"Successfully converted file to UTF-8. New file: gs://{bucket_name}/{new_file_path}")
            utils.logger.info(f"Total bytes processed: {streaming_writer.total_bytes_uploaded}\n")

            # After creating new file with UTF8 encoding, try converting it to Parquet
            csv_to_parquet(new_file_path)

        except UnicodeDecodeError as e:
            utils.logger.error(f"Failed to decode content with detected encoding {detected_encoding}: {str(e)}")
            sys.exit(1)
        except csv.Error as e:
            utils.logger.error(f"CSV parsing error: {str(e)}")
            sys.exit(1)

    except Exception as e:
        utils.logger.error(f"Unable to convert CSV to UTF8: {e}")
        sys.exit(1)
    
