import core.utils as utils
import core.helpers.report_artifact as report_artifact

    
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

def validate_cdm_table_columns(file_path: str, omop_version: str, delivery_date_REMOVE: str, gcs_path_REMOVE: str) -> None:
    """
    Verify that column names in the parquet file are valid columns in the CDM schema
    and that there are no columns in the table schema that are absent in the parquet file.
    """
    utils.logger.warning("Validating CDM table columns")
    try:
        utils.logger.warning(f"file_path is {file_path}")
        bucket_name, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)
        utils.logger.warning(f"bucket_name, delivery_date is {bucket_name}, {delivery_date}")
        table_name = utils.get_table_name_from_gcs_path(file_path)
        utils.logger.warning(f"table_name is {table_name}")
        schema = utils.get_table_schema(table_name=table_name, cdm_version=omop_version)
        utils.logger.warning(f"schema is {schema}")
        utils.logger.warning(f"parquet artifact is at {utils.get_parquet_artifact_location(file_path)}")
        parquet_columns  = utils.get_columns_from_parquet(utils.get_parquet_artifact_location(file_path))
        utils.logger.warning(f"parquet_columns is {parquet_columns}")
        schema_columns = list(schema[table_name]['fields'].keys())
        utils.logger.warning(f"schema_columns is {schema_columns}")

        # Check if parquet columns in the table schema
        for column in parquet_columns:
            if column in schema_columns:
                utils.logger.info(f"'{column}' is a valid column in schema for {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=schema['concept_id'],
                    delivery_date=delivery_date,
                    gcs_path=bucket_name,
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
                    gcs_path=bucket_name,
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
                    gcs_path=bucket_name,
                    name=f"Missing column: {column}",
                    value_as_concept_id=None,
                    value_as_number=None,
                    value_as_string="missing column"
                )
            # TODO: Evaulate if this else block is needed
            else:
                utils.logger.warning(f"'{table_name}' IS NOT a valid column in schema for {table_name}.")
                ra = report_artifact.ReportArtifact(
                    concept_id=None,
                    delivery_date=delivery_date,
                    gcs_path=bucket_name,
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
        utils.logger.warning(f"valid_table_name is {valid_table_name}")
        # If it's not a valid table name, it does not have a schema to validate
        if valid_table_name:
            validate_cdm_table_columns(file_path, omop_version, delivery_date, gcs_path)
            
    except Exception as e:
        utils.logger.error(f"Error validating file {file_path}: {str(e)}")
