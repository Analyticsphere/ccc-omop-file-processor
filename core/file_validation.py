import json
import os
import core.constants as constants
import core.utils as utils

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

        # Check if the filename matches any of the table keys in the JSON
        return table_name in valid_table_names

    except FileNotFoundError:
        raise Exception(f"Schema file not found: {schema_file}")
    except json.JSONDecodeError:
        raise Exception(f"Invalid JSON format in schema file: {schema_file}")
    except Exception as e:
        raise Exception(f"Unexpected error validating CDM file: {str(e)}")

def validate_cdm_table_schema(file_path: str, cdm_version: str) -> dict:
    results = {
        "status": "success",
        "missing_columns": [],
        "type_mismatches": [],
        "unexpected_columns": []
    }

    try:
        if not file_path.startswith("gs://"):
            # Assume it's a local file
            local_file_path = file_path
        else:
            # Download the parquet file from GCS to a local temporary location
            local_file_path = f"/tmp/{os.path.basename(file_path)}"
            utils.download_gcs_file(file_path, local_file_path)

        # Load the schema file for the specified CDM version
        schema_file = f"{constants.CDM_SCHEMA_PATH}{cdm_version}/{constants.CDM_SCHEMA_FILE_NAME}"
        with open(schema_file, 'r') as f:
            schema_data = json.load(f)

        # Derive the table name from the file path
        table_name = os.path.splitext(os.path.basename(local_file_path))[0]

        # Establish a DuckDB connection
        conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

        # Load the parquet file into DuckDB
        conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM '{local_file_path}'")

        # Extract the schema of the loaded DuckDB table
        duckdb_schema_query = f"""
        SELECT 
            column_name, 
            data_type
        FROM information_schema.columns 
        WHERE table_name = '{table_name}';
        """
        duckdb_schema = conn.execute(duckdb_schema_query).fetchall()

        # Convert the schema to a dictionary for easier comparison
        duckdb_schema_dict = {
            row[0]: {"type": row[1]} for row in duckdb_schema
        }

        # Compare the extracted schema with the expected schema from the JSON file
        expected_schema = schema_data.get(table_name, {}).get("fields", {})
        for column_name, column_spec in expected_schema.items():
            if column_name not in duckdb_schema_dict:
                results["missing_columns"].append(column_name)
                continue

            # Check data type
            actual_type = duckdb_schema_dict[column_name]["type"].upper()
            expected_type = column_spec["type"].upper()
            if actual_type != expected_type:
                results["type_mismatches"].append({
                    "column": column_name,
                    "expected_type": expected_type,
                    "actual_type": actual_type
                })

        # Log extra columns in the DuckDB table
        for column_name in duckdb_schema_dict.keys():
            if column_name not in expected_schema:
                results["unexpected_columns"].append(column_name)

        # Cleanup
        os.remove(local_file_path)  # Delete the local copy of the downloaded parquet file
        utils.close_duckdb_connection(conn, local_db_file, tmp_dir)  # Close the DuckDB connection and clean up

        if results["missing_columns"] or results["type_mismatches"]:
            results["status"] = "failure"

        return results

    except Exception as e:
        utils.logger.error(f"Error in validate_cdm_table_schema: {e}")
        results["status"] = "error"
        results["error_message"] = str(e)
        return results

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
        schema_validation_results = None

        if not valid_file_name:
            utils.logger.warning(f"File name validation failed for {file_path}")
        else:
            schema_validation_results = validate_cdm_table_schema(file_path, omop_version)

        # Compile results
        results = [{
            "file": file_path,
            "valid_file_name": valid_file_name,
            "schema_validation_results": schema_validation_results,
        }]

        # Log the validation results
        if not valid_file_name or (schema_validation_results and schema_validation_results["status"] == "failure"):
            utils.logger.error(f"Validation failed for {file_path}")
        else:
            utils.logger.info(f"Validation successful for {file_path}")

        return results

    except Exception as e:
        utils.logger.error(f"Error validating file {file_path}: {str(e)}")
        return [{
            "file": file_path,
            "valid_file_name": False,
            "schema_validation_results": {
                "status": "error",
                "error_message": str(e)
            },
        }]