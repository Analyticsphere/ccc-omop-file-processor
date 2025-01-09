from flask import Flask, jsonify, request
from datetime import datetime
import core.utils as utils
import core.constants as constants
import core.file_conversion as file_conversion
import core.file_validation as file_validation
import os

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    utils.logger.info("API status check called")
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200

@app.route('/get_file_list', methods=['GET'])
def get_files():
    # Get parameters from query string
    bucket = request.args.get('bucket')
    folder = request.args.get('folder')

    utils.logger.info(f"Obtaining files from {folder} folder in {bucket} bucket")
    
    file_list = utils.list_gcs_files(bucket, folder)

    return jsonify({
        'status': 'healthy',
        'file_list': file_list,
        'service': constants.SERVICE_NAME
    }), 200
    
@app.route('/validate_cdm_file', methods=['GET'])
def validate_file(file_config: dict) -> list[dict]:
    """
    Validates a file's name and schema against the OMOP standard.

    Args:
        file_config (dict): Configuration for the file, including GCS path, OMOP version, etc.

    Returns:
        list[dict]: Validation results, including file path, file name validation, and schema validation status.
    """
    # Construct GCS path
    gcs_file_path = (
        f"gs://{file_config[constants.FileConfig.GCS_PATH.value]}/"
        f"{file_config[constants.FileConfig.DELIVERY_DATE.value]}/"
        f"{file_config[constants.FileConfig.FILE_NAME.value]}"
    )
    omop_version = file_config[constants.FileConfig.OMOP_VERSION.value]
    
    utils.logger.info(f"Validating schema of {gcs_file_path} against OMOP v{omop_version}")
    result = file_validation.validate_file(gcs_file_path, omop_version)
    utils.logger.info(f"Validation successful for {gcs_file_path}")

    return jsonify({
        'status': 'success',
        'result': result,
        'service': constants.SERVICE_NAME
    }), 200
    
    # TODO Write to BQ meta_data, but this will just show in Airflow logs.

    
@app.route('/create_artifact_buckets', methods=['GET'])
def create_artifact_buckets():
    parent_bucket = request.args.get('parent_bucket')

    utils.logger.info(f"Creating artifact buckets in gs://{parent_bucket}")

    artifact_dir = f"{parent_bucket}/{constants.ArtifactPaths.ARTIFACTS.value}"
    files_dir = f"{parent_bucket}/{constants.ArtifactPaths.CONVERTED_FILES.value}"
    report_dir = f"{parent_bucket}/{constants.ArtifactPaths.REPORT.value}"
    dqd_dir = f"{parent_bucket}/{constants.ArtifactPaths.DQD.value}"

    directories = [artifact_dir, files_dir, report_dir, dqd_dir]

    for directory in directories:
        utils.create_gcs_directory(directory)
    
    return "Directories created successfully", 200

@app.route('/convert_to_parquet', methods=['GET'])
def convert_to_parquet():
    file_path: str = request.args.get('file_path')

    try:
        if file_path.endswith(constants.CSV):
            utils.logger.info(f"Converting CSV file {file_path} to Parquet format")
            file_conversion.csv_to_parquet(file_path)
        elif file_path.endswith(constants.PARQUET):
            utils.logger.info(f"Received parquet file from site: {file_path}")
        else:
            utils.logger.info(f"Invalid source file format: {file_path}")
    
        return "Converted file to Parquet", 200
    except:
        return "Unable to convert files to Parquet", 500


if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)