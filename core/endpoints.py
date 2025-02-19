import os
from datetime import datetime

from flask import Flask, jsonify, request  # type: ignore

import core.bq_client as bq_client
import core.constants as constants
import core.file_processor as file_processor
import core.file_validation as file_validation
import core.helpers.pipeline_log as pipeline_log
import core.utils as utils

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
    file_format = request.args.get('file_format')

    utils.logger.info(f"Obtaining files from {folder} folder in {bucket} bucket")
    
    try:
        file_list = utils.list_gcs_files(bucket, folder, file_format)

        return jsonify({
            'status': 'healthy',
            'file_list': file_list,
            'service': constants.SERVICE_NAME
        }), 200
    except:
        return "Unable to get list of files to process", 500

@app.route('/validate_file', methods=['GET'])
def validate_file():
    """
    Validates a file's name and schema against the OMOP standard.
    """
    try:
        # Get parameters from query string
        file_path = request.args.get('file_path')
        omop_version = request.args.get('omop_version')
        delivery_date = request.args.get('delivery_date')
        gcs_path = request.args.get('gcs_path')
        
        result = file_validation.validate_file(file_path=file_path, omop_version=omop_version, delivery_date=delivery_date, gcs_path=gcs_path)
        utils.logger.info(f"Validation successful for {file_path}")

        return jsonify({
            'status': 'success',
            'result': result,
            'service': constants.SERVICE_NAME
        }), 200
        
    except Exception as e:
        return f"Unable to run file validation: {e}", 500
    
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
    file_type: str = request.args.get('file_type')
    file_path: str = request.args.get('file_path')

    try:
        file_processor.process_incoming_file(file_type, file_path)    
        return "Converted file to Parquet", 200
    except:
        return "Unable to convert files to Parquet", 500

@app.route('/normalize_parquet', methods=['GET'])
def normalize_parquet_file():
    file_path: str = request.args.get('file_path')
    omop_version: str = request.args.get('omop_version')

    parquet_file_path = utils.get_parquet_artifact_location(file_path)

    try:
        utils.logger.info(f"Attempting to fix Parquet file {parquet_file_path}")
        file_processor.normalize_file(parquet_file_path, omop_version)

        return "Fixed Parquet file", 200
    except:
        return "Unable to fix Parquet file", 500

@app.route('/upgrade_cdm', methods=['GET'])
def cdm_upgrade():
    file_path: str = request.args.get('file_path')
    omop_version: str = request.args.get('omop_version')

    try:
        utils.logger.info(f"Attempting to upgrade file {file_path}")
        file_processor.upgrade_file(file_path, omop_version)

        return "Upgraded file", 200
    except:
        return "Unable to upgrade file", 500

@app.route('/parquet_to_bq', methods=['GET'])
def parquet_gcs_to_bq():
    file_path: str = request.args.get('file_path')
    project_id: str = request.args.get('project_id')
    dataset_id: str = request.args.get('dataset_id')

    try:
        utils.logger.info(f"Attempting to load file {file_path} to {project_id}.{dataset_id}")
        bq_client.load_parquet_to_bigquery(file_path, project_id, dataset_id)

        return "Loaded Parquet file to BigQuery", 200
    except:
        return "Unable to load Parquet file", 500

@app.route('/clear_bq_dataset', methods=['GET'])
def clear_bq_tables():
    project_id: str = request.args.get('project_id')
    dataset_id: str = request.args.get('dataset_id')

    try:
        utils.logger.info(f"Removing all tables from {project_id}.{dataset_id}")
        bq_client.remove_all_tables(project_id, dataset_id)

        return "Removed all tables", 200
    except:
        return "Unable to delete tables within dataset", 500

@app.route('/pipeline_log', methods=['GET'])
def log_pipeline_state():
    logging_table: str = request.args.get('logging_table')
    site_name: str = request.args.get('site_name')
    delivery_date: str = request.args.get('delivery_date')
    status: str = request.args.get('status')
    message: str = request.args.get('message')
    file_type: str = request.args.get('file_type')
    omop_version: str = request.args.get('omop_version')
    run_id: str = request.args.get('run_id')

    try:
        if status:
            pipeline_logger = pipeline_log.PipelineLog(
                logging_table,
                site_name,
                delivery_date,
                status,
                message,
                file_type,
                omop_version,
                run_id
            )
            pipeline_logger.add_log_entry()
        else:
            return "Log status not provided", 400

        return "Complete BigQuery table write", 200
    except:
        return "Unable to write to BigQuery table", 400    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)
