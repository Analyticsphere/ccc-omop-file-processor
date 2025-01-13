from flask import Flask, jsonify, request
from datetime import datetime
import core.utils as utils
import core.constants as constants
import core.file_conversion as file_conversion
import os
import core.model.report_artifact as report_artifact

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
    
    try:
        file_list = utils.list_gcs_files(bucket, folder)

        return jsonify({
            'status': 'healthy',
            'file_list': file_list,
            'service': constants.SERVICE_NAME
        }), 200
    except:
        return "Unable to get list of files to process", 500

@app.route('/create_artifact_buckets', methods=['GET'])
def create_artifact_buckets():
    parent_bucket = request.args.get('parent_bucket')

    utils.logger.info(f"Creating artifact buckets in gs://{parent_bucket}")

    directories = []

    try:
        # Create fully qualified paths for each artifact directory
        for path in constants.ArtifactPaths:
            full_path = f"{parent_bucket}/{path.value}"
            directories.append(full_path)
        
        # Create the actual GCS directories
        for directory in directories:
            utils.create_gcs_directory(directory)
        
        return "Directories created successfully", 200
    except:
        return "Unable to create artifact buckets", 500

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

@app.route('/fix_parquet', methods=['GET'])
def fix_parquet_file():
    file_path: str = request.args.get('file_path')
    omop_version: str = request.args.get('omop_version')

    try:
        utils.logger.info(f"Attempt to fix Parquet file {file_path}")
        file_conversion.fix_columns(file_path, omop_version)

        return "Fixed Parquet file", 200
    except:
        "Unable to fix Parquet file", 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)