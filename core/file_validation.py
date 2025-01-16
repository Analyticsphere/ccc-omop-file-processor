import json
import os
import core.constants as constants
import core.utils as utils
import core.model.report_artifact as report_artifact

def validate_cdm_table_name(file_path: str, omop_version: str, delivery_date: str, gcs_path: str) -> bool:
    """
    Validates whether the filename (without extension) matches one of the
    OMOP CDM tables defined in the schema.json file.
    """
    schema_file = f"{constants.CDM_SCHEMA_PATH}{omop_version}/{constants.CDM_SCHEMA_FILE_NAME}"

    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)

        # Extract the valid table names from the JSON spec
        valid_table_names = schema.keys()

        # Get the base name of the file (without extension)
        table_name_with_path = os.path.basename(file_path)
        table_name, _ = os.path.splitext(table_name_with_path) 

        # Check if the filename matches any of the table keys in the JSON
        is_valid_table_name = table_name in valid_table_names
        if is_valid_table_name:
            utils.logger.info(f"'{table_name}' IS a valid OMOP table name.")
        else:
            utils.logger.info(f"'{table_name}' IS NOT valid OMOP table name.")
            ra = report_artifact.ReportArtifact(
                metadata_concept_id=schema["person"]["concept_id"],
                delivery_date=delivery_date,
                gcs_path=gcs_path,
                name=f"Missing table: {table_name}",
                value_as_concept_id="9190",
                value_as_number=None,
                concept_id=None,
                value_as_string=None
            )
            utils.logger.info(f"ReportArtifact generated: {ra.to_json()}")
            ra.save_artifact()
            
        return is_valid_table_name

    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error validating CDM file: {str(e)}")


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
        validate_cdm_table_name(file_path, omop_version, delivery_date, gcs_path)
        # TODO validate_cdm_column_names(file_path, omop_version, delivery_date, gcs_path)

    except Exception as e:
        utils.logger.error(f"Error validating file {file_path}: {str(e)}")