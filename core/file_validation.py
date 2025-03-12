import core.helpers.report_artifact as report_artifact
import core.utils as utils


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

def validate_cdm_table_columns(file_path: str, omop_version: str, delivery_date_REMOVE: str, gcs_path_REMOVE: str) -> None:
    """
    Verify that column names in the parquet file are valid columns in the CDM schema
    and that there are no columns in the table schema that are absent in the parquet file.
    """
    try:
        bucket_name, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)
        table_name = utils.get_table_name_from_gcs_path(file_path)
        schema = utils.get_table_schema(table_name=table_name, cdm_version=omop_version)
        
        parquet_artifact_location = utils.get_parquet_artifact_location(file_path)
        parquet_columns = set(utils.get_columns_from_file(parquet_artifact_location))

        # Get schema columns from the table schema and convert to set (for O(1) lookups)
        schema_columns = set(schema[table_name]['fields'].keys())

        # Identify valid and invalid columns from the parquet file
        valid_columns = parquet_columns & schema_columns  # Intersection of sets
        invalid_columns = parquet_columns - schema_columns  # Columns in parquet but not in schema

        # Identify missing columns: columns in the schema that are absent from the parquet file
        missing_columns = schema_columns - parquet_columns

        # Process valid columns
        for column in valid_columns:
            ra = report_artifact.ReportArtifact(
                concept_id=schema[table_name]['fields'][column]['concept_id'],
                delivery_date=delivery_date,
                gcs_path=bucket_name,
                name=f"Valid column name: {table_name}.{column}",
                value_as_concept_id=None,
                value_as_number=None,
                value_as_string="valid column name"
            )
            utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
            ra.save_artifact()

        # Process invalid columns (present in parquet but not in schema)
        for column in invalid_columns:
            ra = report_artifact.ReportArtifact(
                concept_id=None,
                delivery_date=delivery_date,
                gcs_path=bucket_name,
                name=f"Invalid column name: {table_name}.{column}",
                value_as_concept_id=None,
                value_as_number=None,
                value_as_string="invalid column name"
            )
            utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
            ra.save_artifact()

        for column in missing_columns:
            ra = report_artifact.ReportArtifact(
                concept_id=schema[table_name]['fields'][column]['concept_id'],
                delivery_date=delivery_date,
                gcs_path=bucket_name,
                name=f"Missing column: {table_name}.{column}",
                value_as_concept_id=None,
                value_as_number=None,
                value_as_string="missing column"
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



