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
import core.transformer as transformer
import core.utils as utils
import core.vocab_harmonization as vh

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

    if not vocab_version or not vocab_gcs_bucket:
        return "Missing required parameters: vocab_version and vocab_gcs_bucket", 400

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
        return "Missing required parameter: delivery_bucket", 400

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

    if not site or not delivery_date:
        return "Missing required parameter: site and delivery_date", 400
    
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
    if not bucket or not folder or not file_format:
        return "Missing required parameter: bucket, folder, file_format", 400

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

    if not file_type or not file_path:
        return "Missing required parameters: file_type and file_path", 400

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
        if not file_path or not omop_version or not delivery_date or not gcs_path:
            return "Missing required parameters: file_path, omop_version, delivery_date, gcs_path", 400

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

    if not file_path or not omop_version:
        return "Missing required parameters: file_path and omop_version", 400

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

    if not file_path or not omop_version or not target_omop_version:
        return "Missing required parameters: file_path, omop_version, and target_omop_version", 400

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

    if not project_id or not dataset_id:
        return "Missing required parameters: project_id and dataset_id", 400

    try:
        utils.logger.info(f"Removing all tables from {project_id}.{dataset_id}")
        gcp_services.remove_all_tables(project_id, dataset_id)

        return "Removed all tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to delete tables within dataset: {str(e)}")
        return f"Unable to delete tables within dataset: {str(e)}", 500


@app.route('/harmonize_vocab', methods=['POST'])
def harmonize_vocab() -> tuple[str, int]:
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
    omop_version: Optional[str] = data.get('omop_version')
    site: Optional[str] = data.get('site')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not file_path or not vocab_version or not vocab_gcs_bucket or not omop_version or not site or not project_id or not dataset_id:
        return "Missing required parameters: file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id", 400

    try:
        utils.logger.info(f"Harmonizing vocabulary for {file_path} to version {vocab_version}")

        vocab_harmonizer = vh.VocabHarmonizer(
            file_path=file_path,
            cdm_version=omop_version,
            site=site,
            vocab_version=vocab_version,
            vocab_gcs_bucket=vocab_gcs_bucket,
            project_id=project_id,
            dataset_id=dataset_id
        )
        vocab_harmonizer.harmonize()
        
        return f"Vocabulary harmonized to {vocab_version}", 200
    except Exception as e:
        utils.logger.error(f"Unable to harmonize vocabulary: {str(e)}")
        return f"Unable to harmonize vocabulary: {str(e)}", 500


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

    if not site or not delivery_date or not table_name or not project_id or not dataset_id or not vocab_version or not vocab_gcs_bucket or not site_bucket:
        return "Missing required parameters: site, delivery_date, table_name, project_id, dataset_id, vocab_version, vocab_gcs_bucket, site_bucket", 400

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

    if not vocab_version or not vocab_gcs_bucket or not project_id or not dataset_id or not table_file_name:
        return "Missing required parameters: vocab_version, vocab_gcs_bucket, project_id, dataset_id, table_file_name", 400
    
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

    if not file_path or not project_id or not dataset_id or not write_type or not table_name:
        return "Missing required parameters: file_path, project_id, dataset_id, write_type, table_name", 400
    
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
        return "Missing required parameters in report data: delivery_date and site", 400

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

    if not project_id or not dataset_id or not omop_version:
        return "Missing required parameters: project_id, dataset_id, and omop_version", 400

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
        return "Missing required parameters in cdm_source_data: source_release_date and cdm_source_abbreviation", 400

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
            return "Missing required columns for BigQuery logging", 400

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