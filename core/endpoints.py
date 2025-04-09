import os
from datetime import datetime
from typing import Any, Optional, cast

from flask import Flask, jsonify, request  # type: ignore

import core.constants as constants
import core.file_processor as file_processor
import core.file_validation as file_validation
import core.gcp_services as gcp_services
import core.helpers.pipeline_log as pipeline_log
import core.omop_client as omop_client
import core.utils as utils
import core.vocab_harmonization as vh
import uuid
import json
from concurrent.futures import ThreadPoolExecutor
from google.cloud import storage  # type: ignore

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat() -> tuple[Any, int]:
    utils.logger.info("API status check called")
    
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200


@app.route('/create_optimized_vocab', methods=['POST'])
def create_optimized_vocab() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    vocab_version: str = data.get('vocab_version', '')
    #vocab_gcs_bucket: str = data.get('vocab_gcs_bucket', '')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH

    if not all([vocab_version, vocab_gcs_bucket]):
        return "Missing a required parameter to 'create_optimized_vocab' endpoint. Required: vocab_version, vocab_gcs_bucket", 400

    try:
        omop_client.convert_vocab_to_parquet(vocab_version, vocab_gcs_bucket)
        omop_client.create_optimized_vocab_file(vocab_version, vocab_gcs_bucket)

        return "Created optimized vocabulary files", 200
    except Exception as e:
        utils.logger.error(f"Error creating optimized vocabulary: {str(e)}")
        return f"Error creating optimized vocabulary: {str(e)}", 500


@app.route('/create_artifact_buckets', methods=['POST'])
def create_artifact_buckets() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    delivery_bucket: Optional[str] = data.get('delivery_bucket')

    if not delivery_bucket:
        return "Missing required parameter to 'create_artifact_buckets' endpoint: delivery_bucket", 400

    utils.logger.info(f"Creating artifact buckets in gs://{delivery_bucket}")

    directories: list[str] = []

    try:
        # Create fully qualified paths for each artifact directory
        for path in constants.ArtifactPaths:
            full_path = f"{delivery_bucket}/{path.value}"
            directories.append(full_path)
        
        # Create the actual GCS directories
        for directory in directories:
            gcp_services.create_gcs_directory(directory)
        
        return "Directories created successfully", 200
    except Exception as e:
        utils.logger.error(f"Unable to create artifact buckets: {str(e)}")
        return f"Unable to create artifact buckets: {str(e)}", 500


@app.route('/get_log_row', methods=['GET'])
def get_log_row() -> tuple[Any, int]:
    site: Optional[str] = request.args.get('site')
    delivery_date: Optional[str] = request.args.get('delivery_date')

    if not all([site, delivery_date]):
        return "Missing a required parameter to 'get_log_row' endpoint. Required: site, delivery_date", 400
    
    try:
        log_row: list[str] = gcp_services.get_bq_log_row(site, delivery_date)
        return jsonify({
            'status': 'healthy',
            'log_row': log_row,
            'service': constants.SERVICE_NAME
        }), 200   
    except Exception as e:
        utils.logger.error(f"Unable to get get BigQuery log row: {str(e)}")
        return f"Unable to get get BigQuery log row: {str(e)}", 500


@app.route('/get_file_list', methods=['GET'])
def get_files() -> tuple[Any, int]:
    bucket: Optional[str] = request.args.get('bucket')
    folder: Optional[str] = request.args.get('folder')
    file_format: Optional[str] = request.args.get('file_format')
   
    # Validate required parameters
    if not all([bucket, folder, file_format]):
        return "Missing a required parameter to 'get_file_list' endpoint. Required: bucket, folder, file_format", 400

    try:
        file_list: list[str] = utils.list_gcs_files(bucket, folder, file_format)

        return jsonify({
            'status': 'healthy',
            'file_list': file_list,
            'service': constants.SERVICE_NAME
        }), 200
    except Exception as e:
        utils.logger.error(f"Unable to get list of files to process: {str(e)}")
        return f"Unable to get list of files to process: {str(e)}", 500


@app.route('/process_incoming_file', methods=['POST'])
def process_file() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_type: Optional[str] = data.get('file_type')
    file_path: Optional[str] = data.get('file_path')

    if not all([file_type, file_path]):
        return "Missing a required parameter to 'process_incoming_file' endpoint. Required: file_type, file_path", 400

    try:
        file_processor.process_incoming_file(file_type, file_path)    
        return "Converted file to Parquet", 200
    except Exception as e:
        utils.logger.error(f"Unable to convert files to Parquet: {str(e)}")
        return f"Unable to convert files to Parquet: {str(e)}", 500


@app.route('/validate_file', methods=['POST'])
def validate_file() -> tuple[str, int]:
    """
    Validates a file's name and schema against the OMOP standard.
    """
    try:
        data: dict[str, Any] = request.get_json() or {}
        file_path: Optional[str] = data.get('file_path')
        omop_version: Optional[str] = data.get('omop_version')
        delivery_date: Optional[str] = data.get('delivery_date')
        gcs_path: Optional[str] = data.get('gcs_path')
        
        # Validate required parameters
        if not all([file_path, omop_version, delivery_date, gcs_path]):
            return "Missing a required parameter to 'validate_file' endpoint. Required: file_path, omop_version, delivery_date, gcs_path", 400

        # Use empty string as default for optional params
        file_validation.validate_file(
            file_path=file_path, 
            omop_version=omop_version, 
            delivery_date=delivery_date, 
            gcs_path=gcs_path
        )
        utils.logger.info(f"Validation successful for {file_path}")

        return "File successfully validated", 200
        
    except Exception as e:
        utils.logger.error(f"Unable to run file validation: {str(e)}")
        return f"Unable to run file validation: {str(e)}", 500
    

@app.route('/normalize_parquet', methods=['POST'])
def normalize_parquet_file() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    omop_version: Optional[str] = data.get('omop_version')

    if not all([file_path, omop_version]):
        return "Missing a required parameter to 'normalize_parquet' endpoint. Required: file_path, omop_version", 400

    parquet_file_path: str = utils.get_parquet_artifact_location(file_path)

    try:
        utils.logger.info(f"Attempting to normalize Parquet file {parquet_file_path}")
        file_processor.normalize_file(parquet_file_path, omop_version)

        return "Normalized Parquet file", 200
    except Exception as e:
        utils.logger.error(f"Unable to normalize Parquet file: {str(e)}")
        return f"Unable to normalize Parquet file: {str(e)}", 500


@app.route('/upgrade_cdm', methods=['POST'])
def cdm_upgrade() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    omop_version: Optional[str] = data.get('omop_version')
    target_omop_version: Optional[str] = data.get('target_omop_version')

    if not all([file_path, omop_version, target_omop_version]):
        return "Missing a required parameter to 'upgrade_cdm' endpoint. Required: file_path, omop_version, target_omop_version", 400

    try:
        utils.logger.info(f"Attempting to upgrade file {file_path}")
        omop_client.upgrade_file(file_path, omop_version, target_omop_version)

        return "Upgraded file", 200
    except Exception as e:
        utils.logger.error(f"Unable to upgrade file: {str(e)}")
        return f"Unable to upgrade file: {str(e)}", 500


@app.route('/clear_bq_dataset', methods=['POST'])
def clear_bq_tables() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([project_id, dataset_id]):
        return "Missing a required parameter to 'clear_bq_dataset' endpoint. Required: project_id, dataset_id", 400

    try:
        utils.logger.info(f"Removing all tables from {project_id}.{dataset_id}")
        gcp_services.remove_all_tables(project_id, dataset_id)

        return "Removed all tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to delete tables within dataset: {str(e)}")
        return f"Unable to delete tables within dataset: {str(e)}", 500



# @app.route('/harmonize_vocab', methods=['POST'])
# def harmonize_vocab() -> tuple[str, int]:
#     data: dict[str, Any] = request.get_json() or {}
#     file_path: Optional[str] = data.get('file_path')
#     vocab_version: Optional[str] = data.get('vocab_version')
#     vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
#     omop_version: Optional[str] = data.get('omop_version')
#     site: Optional[str] = data.get('site')
#     project_id: Optional[str] = data.get('project_id')
#     dataset_id: Optional[str] = data.get('dataset_id')

#     if not all([file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id]):
#         return "Missing a required parameter to 'harmonize_vocab' endpoint. Required: file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id", 400

#     try:
#         utils.logger.info(f"Harmonizing vocabulary for {file_path} to version {vocab_version}")

#         vocab_harmonizer = vh.VocabHarmonizer(
#             file_path=file_path,
#             cdm_version=omop_version,
#             site=site,
#             vocab_version=vocab_version,
#             vocab_gcs_bucket=vocab_gcs_bucket,
#             project_id=project_id,
#             dataset_id=dataset_id
#         )
#         vocab_harmonizer.harmonize()
        
#         utils.logger.warning(f"About to return 200 response from harmonize_vocab() endpoint")
#         return f"Vocabulary harmonized to {vocab_version}", 200
#     except Exception as e:
#         utils.logger.error(f"Unable to harmonize vocabulary of {file_path}: {str(e)}")
#         return f"Unable to harmonize vocabulary of {file_path}: {str(e)}", 500



# Create thread pool for harmonization jobs
harmonization_executor = ThreadPoolExecutor(max_workers=8)
@app.route('/harmonize_vocab', methods=['POST'])
def harmonize_vocab() -> tuple[Any, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
    omop_version: Optional[str] = data.get('omop_version')
    site: Optional[str] = data.get('site')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id]):
        return "Missing required parameters", 400

    try:
        # Generate job ID
        job_id = str(uuid.uuid4())
        bucket_name, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)
        
        # Create status directory
        status_dir = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status"
        gcp_services.create_gcs_directory(f"{bucket_name}/{status_dir}")
        
        # Create initial status file with all parameters needed for processing
        status_file_path = f"{status_dir}/{job_id}.json"
        status_data = {
            "job_id": job_id,
            "status": "queued",
            "file_path": file_path,
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket,
            "omop_version": omop_version,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "start_time": datetime.utcnow().isoformat(),
            "current_step": "init",
            "current_step_index": 0,
            "steps": [
                constants.SOURCE_TARGET,
                constants.TARGET_REMAP,
                constants.TARGET_REPLACEMENT, 
                constants.DOMAIN_CHECK,
                "omop_etl"
            ],
            "error": None
        }
        
        # Save job data
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(status_file_path) 
        blob.upload_from_string(json.dumps(status_data), content_type="application/json")
        
        # Return immediately with job ID for polling
        return jsonify({
            "job_id": job_id,
            "status": "queued",
            "bucket": bucket_name,
            "delivery_date": delivery_date,
            "message": f"Vocabulary harmonization job queued for {file_path}"
        }), 202
        
    except Exception as e:
        utils.logger.error(f"Error setting up harmonization job: {str(e)}")
        return f"Error: {str(e)}", 500


@app.route('/harmonize_vocab_process_step', methods=['POST'])
def harmonize_vocab_process_step() -> tuple[Any, int]:
    data = request.get_json() or {}
    job_id = data.get('job_id')
    bucket_name = data.get('bucket')
    delivery_date = data.get('delivery_date')
    
    if not all([job_id, bucket_name, delivery_date]):
        return "Missing required parameters", 400
        
    try:
        # Get job status
        status_path = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status/{job_id}.json"
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(status_path)
        
        if not blob.exists():
            return f"Job {job_id} not found", 404
            
        # Load job data
        job_data = json.loads(blob.download_as_string())
        
        # If job already completed or errored, return status
        if job_data['status'] in ['completed', 'error']:
            return jsonify(job_data), 200
            
        # Update status to running if it was queued
        if job_data['status'] == 'queued':
            job_data['status'] = 'running'
            
        # Get current step and index
        current_step_index = job_data['current_step_index']
        steps = job_data['steps']
        
        # Check if all steps are complete
        if current_step_index >= len(steps):
            job_data['status'] = 'completed'
            job_data['end_time'] = datetime.utcnow().isoformat()
            blob.upload_from_string(json.dumps(job_data), content_type="application/json")
            return jsonify(job_data), 200
            
        # Get the current step to execute
        current_step = steps[current_step_index]
        job_data['current_step'] = current_step
        
        # Initialize harmonizer with job parameters
        utils.logger.info(f"Job {job_id}: Processing step {current_step_index+1}/{len(steps)}: {current_step}")
        vocab_harmonizer = vh.VocabHarmonizer(
            file_path=job_data['file_path'],
            cdm_version=job_data['omop_version'],
            site=job_data['site'],
            vocab_version=job_data['vocab_version'],
            vocab_gcs_bucket=job_data['vocab_gcs_bucket'],
            project_id=job_data['project_id'],
            dataset_id=job_data['dataset_id']
        )
        
        # Execute step based on its type
        try:
            if current_step_index == 0:
                # Clean up existing files on first step
                gcs_path = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}{vocab_harmonizer.source_table_name}"
                existing_files = utils.list_gcs_files(bucket_name, gcs_path, constants.PARQUET)
                for file in existing_files:
                    gcp_services.delete_gcs_file(f"{gcs_path}/{file}")
            
            # Process the current step
            if current_step == constants.SOURCE_TARGET:
                vocab_harmonizer.source_target_remapping()
            elif current_step == constants.DOMAIN_CHECK:
                vocab_harmonizer.domain_table_check()
            elif current_step == constants.TARGET_REMAP:
                vocab_harmonizer.check_new_targets(constants.TARGET_REMAP)
            elif current_step == constants.TARGET_REPLACEMENT:
                vocab_harmonizer.check_new_targets(constants.TARGET_REPLACEMENT)
            elif current_step == "omop_etl":
                vocab_harmonizer.omop_etl()
            else:
                raise Exception(f"Unknown step: {current_step}")
                
            # Update job progress
            job_data['current_step_index'] = current_step_index + 1
            job_data['last_step_completed'] = current_step
            job_data['last_updated'] = datetime.utcnow().isoformat()
            blob.upload_from_string(json.dumps(job_data), content_type="application/json")
            
            return jsonify({
                "job_id": job_id,
                "status": "running",
                "current_step": current_step,
                "progress": f"{current_step_index + 1}/{len(steps)}"
            }), 200
            
        except Exception as e:
            # Log and save error
            utils.logger.error(f"Job {job_id} failed at step {current_step}: {str(e)}")
            job_data['status'] = 'error'
            job_data['error'] = f"Failed at step {current_step}: {str(e)}"
            job_data['end_time'] = datetime.utcnow().isoformat()
            blob.upload_from_string(json.dumps(job_data), content_type="application/json")
            
            return jsonify({
                "job_id": job_id,
                "status": "error",
                "step": current_step,
                "error": str(e)
            }), 500
    
    except Exception as e:
        return f"Error processing step: {str(e)}", 500


@app.route('/harmonize_vocab_status', methods=['GET'])
def harmonize_vocab_status() -> tuple[Any, int]:
    job_id: Optional[str] = request.args.get('job_id')
    bucket_name: Optional[str] = request.args.get('bucket')
    delivery_date: Optional[str] = request.args.get('delivery_date')

    if not all([job_id, bucket_name, delivery_date]):
        return "Missing a required parameter to 'harmonize_vocab_status' endpoint. Required: job_id, bucket, delivery_date", 400

    try:
        # Get status file from GCS
        status_file_path = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status/{job_id}.json"
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(status_file_path)
        
        if not blob.exists():
            return jsonify({
                "error": f"Job {job_id} not found"
            }), 404
        
        # Read status
        status_data = json.loads(blob.download_as_string())
        
        # Return appropriate response based on status
        if status_data["status"] == "completed":
            return jsonify({
                "job_id": job_id,
                "status": "completed",
                "file_path": status_data["file_path"],
                "start_time": status_data["start_time"],
                "end_time": status_data["end_time"]
            }), 200
        elif status_data["status"] == "error":
            return jsonify({
                "job_id": job_id,
                "status": "error",
                "file_path": status_data["file_path"],
                "error": status_data["error"],
                "start_time": status_data["start_time"],
                "end_time": status_data["end_time"]
            }), 500
        else:  # running
            return jsonify({
                "job_id": job_id,
                "status": "running",
                "file_path": status_data["file_path"],
                "start_time": status_data["start_time"]
            }), 202
            
    except Exception as e:
        utils.logger.error(f"Error checking status for job {job_id}: {str(e)}")
        return jsonify({
            "error": f"Error checking status: {str(e)}"
        }), 500



@app.route('/populate_derived_data', methods=['POST'])
def populate_dervied_data_table() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    site: Optional[str] = data.get('site')
    site_bucket: Optional[str] = data.get('gcs_bucket')
    delivery_date: Optional[str] = data.get('delivery_date')
    table_name: Optional[str] = data.get('table_name')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH

    if not all([site, delivery_date, table_name, project_id, dataset_id, vocab_version, vocab_gcs_bucket, site_bucket]):
        return "Missing a required parameter to 'populate_derived_data' endpoint. Required: site, delivery_date, table_name, project_id, dataset_id, vocab_version, vocab_gcs_bucket, site_bucket", 400

    try:
        utils.logger.info(f"Generating derived table {table_name} for {delivery_date} delivery from {site}")
        omop_client.generate_derived_data(site, site_bucket, delivery_date, table_name, project_id, dataset_id, vocab_version, vocab_gcs_bucket)
        return "Created derived table", 200
    except Exception as e:
        utils.logger.error(f"Unable to create dervied table: {str(e)}")
        return f"Unable to create derived tables: {str(e)}", 500


@app.route('/load_target_vocab', methods=['POST'])
def target_vocab_to_bq() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    table_file_name: Optional[str] = data.get('table_file_name')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([vocab_version, vocab_gcs_bucket, project_id, dataset_id, table_file_name]):
        return "Missing a required parameter to 'load_target_vocab' endpoint. Required: vocab_version, vocab_gcs_bucket, project_id, dataset_id, table_file_name", 400
    try:
        omop_client.load_vocabulary_table(vocab_version, vocab_gcs_bucket, table_file_name,project_id,dataset_id)

        return f"Successfully loaded vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}", 200
    except Exception as e:
        utils.logger.error(f"Unable to load vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}: {str(e)}")
        return f"Unable to load vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}: {str(e)}", 500        


@app.route('/parquet_to_bq', methods=['POST'])
def parquet_gcs_to_bq() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    table_name: Optional[str] = data.get('table_name')
    write_type: Optional[str] = data.get('write_type')

    if not all([file_path, project_id, dataset_id, write_type, table_name]):
        return "Missing a required parameter to 'parquet_to_bq' endpoint. Required: file_path, project_id, dataset_id, write_type, table_name", 400
    
    try:
        write_type = constants.BQWriteTypes(write_type)
    except ValueError:
        return f"Invalid write_disposwrite_typeition: {write_type}. Valid values are: {[e.value for e in constants.BQWriteTypes]}", 400

    try:
        utils.logger.info(f"Attempting to load file {file_path} to {project_id}.{dataset_id} using {write_type.value} method")
        gcp_services.load_parquet_to_bigquery(file_path, project_id, dataset_id, table_name, write_type)

        return "Loaded Parquet file to BigQuery", 200
    except Exception as e:
        utils.logger.error(f"Unable to load Parquet file: {str(e)}")
        return f"Unable to load Parquet file: {str(e)}", 500


@app.route('/generate_delivery_report', methods=['POST'])
def generate_final_delivery_report() -> tuple[str, int]:
    report_data: dict[str, Any] = request.get_json() or {}
    
    # Validate required columns for report
    if not report_data.get('delivery_date') or not report_data.get('site'):
        return "Missing required parameters to 'generate_delivery_report' endpoint JSON: delivery_date and site", 400

    try:
        utils.logger.info(f"Generating final delivery report for {report_data['delivery_date']} delivery from {report_data['site']}")
        utils.generate_report(report_data)

        return "Generated delivery report file", 200
    except Exception as e:
        utils.logger.error(f"Unable to generate delivery report: {str(e)}")
        return f"Unable to generate delivery report: {str(e)}", 500


@app.route('/create_missing_tables', methods=['POST'])
def create_missing_omop_tables() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    omop_version: Optional[str] = data.get('omop_version')

    if not all([project_id, dataset_id, omop_version]):
        return "Missing a required parameter to 'create_missing_tables' endpoint. Required: project_id, dataset_id, omop_version", 400

    try:
        utils.logger.info(f"Creating any missing v{omop_version} tables in {project_id}.{dataset_id}")
        omop_client.create_missing_tables(project_id, dataset_id, omop_version)

        return "Created missing tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to create missing tables: {str(e)}")
        return f"Unable to create missing tables: {str(e)}", 500


@app.route('/populate_cdm_source', methods=['POST'])
def add_cdm_source_record() -> tuple[str, int]:
    cdm_source_data: dict[str, Any] = request.get_json() or {}
    
    # Validate required columns
    if not cdm_source_data.get('source_release_date') or not cdm_source_data.get('cdm_source_abbreviation'):
        return "Missing required parameters to 'populate_cdm_source' endpoint JSON: source_release_date and cdm_source_abbreviation", 400

    try:
        utils.logger.info(f"If empty, populating cdm_source table for {cdm_source_data['source_release_date']} delivery from {cdm_source_data['cdm_source_abbreviation']}")
        omop_client.populate_cdm_source(cdm_source_data)

        return "cdm_source table populated", 200
    except Exception as e:
        utils.logger.error(f"Unable to populate cdm_source table: {str(e)}")
        return f"Unable to populate cdm_source table: {str(e)}", 500 
 

@app.route('/pipeline_log', methods=['POST'])
def log_pipeline_state() -> tuple:
    data: dict = request.get_json()
    logging_table: str = constants.BQ_LOGGING_TABLE
    site_name: Optional[str] = data.get('site_name')
    delivery_date: Optional[str] = data.get('delivery_date')
    status: Optional[str] = data.get('status')
    message: Optional[str] = data.get('message')
    file_type: Optional[str] = data.get('file_type')
    omop_version: Optional[str] = data.get('omop_version')
    run_id: Optional[str] = data.get('run_id')

    try:
        # Check if required columns are present
        if not all([site_name, delivery_date, status, run_id]):
            return "Missing a required parameter to 'pipeline_log' endpoint. Required: site_name, delivery_date, status, run_id", 400

        pipeline_logger = pipeline_log.PipelineLog(
            logging_table,
            cast(str, site_name),
            cast(str, delivery_date),
            cast(str, status),
            message,  # Optional parameters don't need casting
            file_type,
            omop_version,
            cast(str, run_id)
        )
        pipeline_logger.add_log_entry()

        return "Successfully logged to BigQuery", 200

    except Exception as e:
        utils.logger.error(f"Unable to save logging information to BigQuery table: {str(e)}")
        return f"Unable to save logging information to BigQuery table: {str(e)}", 500    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)