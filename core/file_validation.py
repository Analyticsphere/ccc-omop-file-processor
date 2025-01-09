import json
import os
import core.constants as constants
import core.utils as utils
import sys

def validate_cdm_table_name(file_path: str, cdm_version: str) -> bool:
    """
    Validates whether the filename (without extension) matches one of the
    OMOP CDM tables defined in the schema.json file.
    """
    schema_file = f"{constants.CDM_SCHEMA_PATH}{cdm_version}/{constants.CDM_SCHEMA_FILE_NAME}"

    try:
        with open(schema_file, 'r') as f:
            schema = json.load(f)

        # Extract the valid table names from the JSON spec
        valid_table_names = schema.keys()

        # Get the base name of the file (without extension)
        table_name_with_path = os.path.basename(file_path)
        table_name, _ = os.path.splitext(table_name_with_path) 
        utils.logger.warning(f"In validate_cdm_table_name, table_name is {table_name}")

        # Check if the filename matches any of the table keys in the JSON
        return table_name in valid_table_names

    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error validating CDM file: {str(e)}")


def validate_file(file_path: str, omop_version: str = '5.3') -> list[dict]:
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
        # Validate the file name
        valid_file_name = validate_cdm_table_name(file_path, omop_version)

        if not valid_file_name:
            utils.logger.warning(f"File name validation failed for {file_path}")

        # Compile results
        results = [{
            "file": file_path,
            "valid_file_name": valid_file_name
        }]

        return results

    except Exception as e:
        utils.logger.error(f"Error validating file {file_path}: {str(e)}")