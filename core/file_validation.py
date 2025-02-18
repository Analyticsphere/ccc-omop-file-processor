import core.utils as utils
import core.helpers.report_artifact as report_artifact
from google.cloud import storage
import os
import tempfile
import duckdb
    
def validate_cdm_table_name(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> bool:
    """
    Validates whether the filename (without extension) matches one of the
    OMOP CDM tables defined in the schema.json file.
    """
    
    try:
        schema = utils.get_cdm_schema(cdm_version=omop_version)
        
        # Extract the valid table names from the JSON spec
        valid_table_names = schema.keys()

        # Get the base name of the file (without extension)
        table_name = utils.get_table_name_from_gcs_path(file_path)

        # Check if the filename matches any of the table keys in the JSON
        is_valid_table_name = table_name in valid_table_names
        if is_valid_table_name:
            utils.logger.info(f"'{table_name}' IS a valid OMOP table name.")
            ra = report_artifact.ReportArtifact(
                concept_id=schema[table_name]['concept_id'],
                delivery_date=delivery_date,
                gcs_path=gcs_path,
                name=f"Valid table name: {table_name}",
                value_as_concept_id=None,
                value_as_number=None,
                value_as_string="valid table name"
            )
        else:
            utils.logger.info(f"'{table_name}' is NOT valid OMOP table name.")
            ra = report_artifact.ReportArtifact(
                concept_id=None,
                delivery_date=delivery_date,
                gcs_path=gcs_path,
                name=f"Invalid table name: {table_name}",
                value_as_concept_id=None,
                value_as_number=None,
                value_as_string="invalid table name"
            )
        utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
        ra.save_artifact()
            
        return is_valid_table_name

    except Exception as e:
        raise Exception(f"Unexpected error validating CDM file: {str(e)}")

def validate_cdm_table_columns(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> None:
    """
    Verify that column names in the parquet file are valid columns in the CDM schema
    and that there are no columns in the table schema that are absent in the parquet file.
    """
    
    try:
        table_name = utils.get_table_name_from_gcs_path(file_path)
        schema = utils.get_table_schema(table_name=table_name, cdm_version=omop_version)
        parquet_columns  = utils.get_columns_from_parquet(gcs_file_path=file_path)
        schema_columns = list(schema[table_name]['fields'].keys())

        # Check if parquet columns in the table schema
        for column in parquet_columns:
            if column in schema_columns:
                utils.logger.info(f"'{column}' is a valid column in schema for {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=schema['concept_id'],
                    delivery_date=delivery_date,
                    gcs_path=gcs_path,
                    name=f"Valid column name: {column}",
                    value_as_concept_id=None,
                    value_as_number=None,
                    value_as_string="valid column name"
                )
            else:
                utils.logger.warning(f"'{table_name}' is NOT a valid column in schema for {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=None,
                    delivery_date=delivery_date,
                    gcs_path=gcs_path,
                    name=f"Invalid column name: {column}",
                    value_as_concept_id=None,
                    value_as_number=None,
                    value_as_string="invalid column name"
                )
            utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
            ra.save_artifact()
        
        # Check if column in table schema is missing from parquet file   
        for column in schema_columns:
            if column not in parquet_columns:
                utils.logger.info(f"'{column}' is missing from {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=schema['concept_id'],
                    delivery_date=delivery_date,
                    gcs_path=gcs_path,
                    name=f"Missing column: {column}",
                    value_as_concept_id=None,
                    value_as_number=None,
                    value_as_string="missing column"
                )
            else:
                utils.logger.warning(f"'{table_name}' IS NOT a valid column in schema for {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=None,
                    delivery_date=delivery_date,
                    gcs_path=gcs_path,
                    name=f"Invalid column name: {table_name}",
                    value_as_concept_id=763780,
                    value_as_number=None,
                    value_as_string=None
                )
            utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
            ra.save_artifact()
    except Exception as e:
        raise Exception(f"Unexpected error validating columns for table {table_name}: {str(e)}")

def validate_file(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> None:
    """
    Validates a file's name and schema against the OMOP standard.

    Args:
        file_path (str): Path to the local file or GCS file to validate.
        omop_version (str): Version of the OMOP CDM standard.

    Returns:
        list[dict]: Validation results, including file path, file name validation, and schema validation status.
    """
    utils.logger.info(f"Validating schema of {file_path} against OMOP v{omop_version}")
    
    try:
        valid_table_name = validate_cdm_table_name(file_path, omop_version, delivery_date, gcs_path)
        # If it's not a valid table name, it does not have a schema to validate
        if valid_table_name:
            validate_cdm_table_columns(file_path, omop_version, delivery_date, gcs_path)
            
    except Exception as e:
        utils.logger.error(f"Error validating file {file_path}: {str(e)}")


def concatenate_report_artifacts(bucket_name: str, input_folder_prefix: str, output_folder_prifix: str) -> None:
    # TODO pass in parner name and delivery date
    # TODO get bucket name, output path, prefix, etc from constants
    """
    Downloads Parquet files from a GCS bucket into a temporary directory,
    concatenates them  and uploads the result back to GCS.
    """
    # Initialize the GCS client and bucket.
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    
    # List and filter files (i.e., blobs) that end with '.parquet'.
    blobs = utils.list_gcs_files(bucket_name, input_folder_prefix)
    
    parquet_blobs = [blob for blobs in blob if blob.name.endswith('.parquet')]
    if not parquet_blobs:
        utils.logger.info(f"No Parquet files found in the bucket named {bucket_name} with the folder_prefix {input_folder_prefix}.")

    utils.logger.info(f"Found {len(parquet_blobs)} Parquet files to process.")

    # Create a temporary directory for downloading the files (which are small).
    with tempfile.TemporaryDirectory() as tmpdir:
        # Download each Parquet file to the temporary directory.
        for blob in parquet_blobs:
            local_path = os.path.join(tmpdir, os.path.basename(blob.name))
            blob.download_to_filename(local_path) #TODO don't download! 
            print(f"Downloaded {blob.name} to {local_path}")

        # Define the output file path.
        merge_path = os.path.join(tmpdir, "merge.parquet")
        
        # Create a glob pattern to match all Parquet files in the temporary directory.
        parquet_pattern = os.path.join(tmpdir, "*.parquet")

        # Use DuckDB to read all Parquet files matching the glob and write out a merged file.
        # TODO COPY FROM BUCKET TO BUCKET
        # TODO SELECT * FROM (*.parquet)
        # create file list
        # * from gcs.. ,
        # generate union query with python
        # execute with duckdb
        duckdb.execute(f"""
            COPY (SELECT * FROM '{parquet_pattern}') 
            TO '{merge_path}' (FORMAT 'parquet'); 
        """)
        print(f"Concatenated Parquet file created at {merge_path}")

        # Upload the merged file back to GCS.
        output_blob_name = f"{output_folder_prifix}/concatinated_report_artifact"
        output_blob = bucket.blob(output_blob_name)
        output_blob.upload_from_filename(merge_path)
        print(f"Uploaded merged file to gs://{bucket_name}/{output_blob_name}")

