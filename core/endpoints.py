import os
from datetime import datetime

from flask import Flask, jsonify, request  # type: ignore

import core.bq_client as bq_client
import core.constants as constants
import core.file_processor as file_processor
import core.file_validation as file_validation
import core.helpers.pipeline_log as pipeline_log
import core.utils as utils
import core.omop_client as omop_client

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat():
    utils.logger.info("API status check called")
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200

@app.route('/create_optimized_vocab', methods=['POST'])
def create_optimized_vocab():
    data = request.get_json()
    vocab_version = data.get('vocab_version')
    vocab_gcs_bucket = data.get('vocab_gcs_bucket')

    try:
        omop_client.create_optimized_vocab_file(vocab_version, vocab_gcs_bucket)

        return "Created optimized vocabulary file", 200
    except Exception as e:
        utils.logger.error(f"Error creating optimized vocabulary: {str(e)}")
        return f"Error creating optimized vocabulary: {str(e)}", 500

@app.route('/get_file_list', methods=['GET'])
def get_files():
    # Keep this as GET since your client code is still using GET
    bucket = request.args.get('bucket')
    folder = request.args.get('folder')
    file_format = request.args.get('file_format')
   
    try:
        file_list = utils.list_gcs_files(bucket, folder, file_format)

        return jsonify({
            'status': 'healthy',
            'file_list': file_list,
            'service': constants.SERVICE_NAME
        }), 200
    except Exception as e:
        utils.logger.error(f"Unable to get list of files to process: {str(e)}")
        return f"Unable to get list of files to process: {str(e)}", 500

@app.route('/validate_file', methods=['POST'])
def validate_file():
    """
    Validates a file's name and schema against the OMOP standard.
    """
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        omop_version = data.get('omop_version')
        delivery_date = data.get('delivery_date')
        gcs_path = data.get('gcs_path')
        
        result = file_validation.validate_file(file_path=file_path, omop_version=omop_version, delivery_date=delivery_date, gcs_path=gcs_path)
        utils.logger.info(f"Validation successful for {file_path}")

        return jsonify({
            'status': 'success',
            'result': result,
            'service': constants.SERVICE_NAME
        }), 200
        
    except Exception as e:
        utils.logger.error(f"Unable to run file validation: {str(e)}")
        return f"Unable to run file validation: {str(e)}", 500
    
@app.route('/create_artifact_buckets', methods=['POST'])
def create_artifact_buckets():
    data = request.get_json()
    parent_bucket = data.get('parent_bucket')

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
    except Exception as e:
        utils.logger.error(f"Unable to create artifact buckets: {str(e)}")
        return f"Unable to create artifact buckets: {str(e)}", 500

@app.route('/convert_to_parquet', methods=['POST'])
def convert_to_parquet():
    data = request.get_json()
    file_type = data.get('file_type')
    file_path = data.get('file_path')

    try:
        file_processor.process_incoming_file(file_type, file_path)    
        return "Converted file to Parquet", 200
    except Exception as e:
        utils.logger.error(f"Unable to convert files to Parquet: {str(e)}")
        return f"Unable to convert files to Parquet: {str(e)}", 500

@app.route('/normalize_parquet', methods=['POST'])
def normalize_parquet_file():
    data = request.get_json()
    file_path = data.get('file_path')
    omop_version = data.get('omop_version')

    parquet_file_path = utils.get_parquet_artifact_location(file_path)

    try:
        utils.logger.info(f"Attempting to fix Parquet file {parquet_file_path}")
        file_processor.normalize_file(parquet_file_path, omop_version)

        return "Fixed Parquet file", 200
    except Exception as e:
        utils.logger.error(f"Unable to fix Parquet file: {str(e)}")
        return f"Unable to fix Parquet file: {str(e)}", 500

@app.route('/upgrade_cdm', methods=['POST'])
def cdm_upgrade():
    data = request.get_json()
    file_path = data.get('file_path')
    omop_version = data.get('omop_version')
    target_omop_version = data.get('target_omop_version')

    try:
        utils.logger.info(f"Attempting to upgrade file {file_path}")
        file_processor.upgrade_file(file_path, omop_version, target_omop_version)

        return "Upgraded file", 200
    except Exception as e:
        utils.logger.error(f"Unable to upgrade file: {str(e)}")
        return f"Unable to upgrade file: {str(e)}", 500

@app.route('/harmonize_vocab', methods=['POST'])
def vocab_harmonization():
    data = request.get_json()
    file_path = data.get('file_path')
    vocab_version = data.get('vocab_version')
    vocab_gcs_bucket = data.get('vocab_gcs_bucket')

    try:
        
        utils.logger.info(f"Harmonizing vocabulary for {file_path} to version {vocab_version}")
        # TODO: Function implementation missing; add implementation here

        return f"Vocabulary harmonized to {vocab_version}", 200
    except Exception as e:
        utils.logger.error(f"Unable to harmonize vocabulary: {str(e)}")
        return f"Unable to harmonize vocabulary: {str(e)}", 500

@app.route('/parquet_to_bq', methods=['POST'])
def parquet_gcs_to_bq():
    data = request.get_json()
    file_path = data.get('file_path')
    project_id = data.get('project_id')
    dataset_id = data.get('dataset_id')

    try:
        utils.logger.info(f"Attempting to load file {file_path} to {project_id}.{dataset_id}")
        bq_client.load_parquet_to_bigquery(file_path, project_id, dataset_id)

        return "Loaded Parquet file to BigQuery", 200
    except Exception as e:
        utils.logger.error(f"Unable to load Parquet file: {str(e)}")
        return f"Unable to load Parquet file: {str(e)}", 500

@app.route('/clear_bq_dataset', methods=['POST'])
def clear_bq_tables():
    data = request.get_json()
    project_id = data.get('project_id')
    dataset_id = data.get('dataset_id')

    try:
        utils.logger.info(f"Removing all tables from {project_id}.{dataset_id}")
        bq_client.remove_all_tables(project_id, dataset_id)

        return "Removed all tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to delete tables within dataset: {str(e)}")
        return f"Unable to delete tables within dataset: {str(e)}", 500

@app.route('/generate_delivery_report', methods=['POST'])
def generate_final_delivery_report():
    report_data = request.get_json()

    try:
        utils.logger.info(f"Generating final delivery report for {report_data['delivery_date']} delivery from {report_data['site']}")
        utils.generate_report(report_data)

        return "Generated delivery report file", 200
    except Exception as e:
        utils.logger.error(f"Unable to generate delivery report: {str(e)}")
        return f"Unable to generate delivery report: {str(e)}", 500

@app.route('/create_missing_tables', methods=['POST'])
def create_missing_omop_tables():
    data = request.get_json()
    project_id = data.get('project_id')
    dataset_id = data.get('dataset_id')
    omop_version = data.get('omop_version')

    try:
        utils.logger.info(f"Creating any missing v{omop_version} tables in {project_id}.{dataset_id}")
        omop_client.create_missing_tables(project_id, dataset_id, omop_version)

        return "Created missing tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to create missing tables: {str(e)}")
        return f"Unable to create missing tables: {str(e)}", 500

@app.route('/populate_cdm_source', methods=['POST'])
def add_cdm_source_record():
    cdm_source_data = request.get_json()

    try:
        utils.logger.info(f"If empty, populating cdm_source table for {cdm_source_data['source_release_date']} delivery from {cdm_source_data['cdm_source_abbreviation']}")
        omop_client.populate_cdm_source(cdm_source_data)

        return "cdm_source table populated", 200
    except Exception as e:
        utils.logger.error(f"Unable to populate cdm_source table: {str(e)}")
        return f"Unable to populate cdm_source table: {str(e)}", 500 

@app.route('/pipeline_log', methods=['POST'])
def log_pipeline_state():
    data = request.get_json()
    logging_table = data.get('logging_table')
    site_name = data.get('site_name')
    delivery_date = data.get('delivery_date')
    status = data.get('status')
    message = data.get('message')
    file_type = data.get('file_type')
    omop_version = data.get('omop_version')
    run_id = data.get('run_id')

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
    except Exception as e:
        utils.logger.error(f"Unable to write to BigQuery table: {str(e)}")
        return f"Unable to write to BigQuery table: {str(e)}", 400    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)