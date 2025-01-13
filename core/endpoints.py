from flask import Flask, jsonify, request
from datetime import datetime
import core.utils as utils
import core.constants as constants
import core.file_conversion as file_conversion
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

@app.route('/create_artifact_buckets', methods=['GET'])
def create_artifact_buckets():
    parent_bucket = request.args.get('parent_bucket')

    utils.logger.info(f"Creating artifact buckets in gs://{parent_bucket}")

    directories = []

    # Create fully qualified paths for each artifact directory
    for path in constants.ArtifactPaths:
        full_path = f"{parent_bucket}/{path.value}"
        directories.append(full_path)
    
    # Create the actual GCS directories
    for directory in directories:
        utils.create_gcs_directory(directory)
    
    return "Directories created successfully", 200

@app.route('/delete_artifact_files', methods=['GET'])
def delete_artifact_files():
    parent_bucket = request.args.get('parent_bucket')
    
    utils.logger.info(f"Deleting files from artifact buckets in gs://{parent_bucket}")
    
    deleted_count = 0
    failed_deletions = []

    # Process each artifact directory
    for path in constants.ArtifactPaths:
        full_path = f"{parent_bucket}/{path.value}"
        
        try:
            # Check if directory exists before attempting deletion
            if utils.gcs_path_exists(full_path):
                # Get list of files in the directory
                files = utils.list_gcs_files(full_path)
                
                # Delete each file in the directory
                for file in files:
                    try:
                        utils.delete_gcs_file(file)
                        deleted_count += 1
                        utils.logger.info(f"Deleted file: {file}")
                    except Exception as e:
                        failed_deletions.append({"file": file, "error": str(e)})
                        utils.logger.error(f"Failed to delete file {file}: {str(e)}")
            else:
                utils.logger.info(f"Directory does not exist: {full_path}")
                
        except Exception as e:
            utils.logger.error(f"Error processing directory {full_path}: {str(e)}")
            failed_deletions.append({"directory": full_path, "error": str(e)})
    
    # Prepare response
    response = {
        "deleted_files_count": deleted_count,
        "failed_deletions": failed_deletions
    }
    
    if failed_deletions:
        return response, 207  # Return 207 Multi-Status if some deletions failed
    return response, 200

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