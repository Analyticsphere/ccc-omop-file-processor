import core.constants as constants
import core.utils as utils
import sys

def csv_to_parquet(gcs_file_path: str) -> None:
    conn, local_db_file, tmp_dir = utils.create_duckdb_connection()

    try:
        with conn:
            utils.logger.info(f"Converting file gs://{gcs_file_path} to parquet...")

            file_name = gcs_file_path.split('/')[-1]

            parquet_artifacts = gcs_file_path.replace(file_name, constants.ArtifactPaths.CONVERTED_FILES.value)
            parquet_file_name = file_name.replace(constants.CSV,constants.PARQUET)
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
    except Exception as e:
        utils.logger.error(f"Unable to convert CSV file to Parquet: {e}")
        sys.exit(1)
    finally:
        utils.close_duckdb_connection(conn, local_db_file, tmp_dir)