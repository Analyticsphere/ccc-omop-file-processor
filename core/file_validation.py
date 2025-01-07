import json
import os
import core.constants as constants

def validate_cdm_table_name(file_name: str, cdm_version='5.3') -> bool:
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
        table_name_with_path = os.path.basename(file_name)
        table_name, _ = os.path.splitext(table_name_with_path)

        # Check if the filename matches any of the table keys in the JSON
        return table_name in valid_table_names

    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error validating CDM file: {str(e)}")
