import os
from datetime import datetime
from typing import Any, Optional

from flask import Flask, jsonify, request  # type: ignore

import core.constants as constants
import core.file_processor as file_processor
import core.file_validation as file_validation
import core.gcp_services as gcp_services
import core.helpers.pipeline_log as pipeline_log
import core.normalization as normalization
import core.omop_client as omop_client
import core.utils as utils
import core.vocab_harmonization as vocab_harmonization
from core.storage_backend import storage

app = Flask(__name__)

@app.route('/heartbeat', methods=['GET'])
def heartbeat() -> tuple[Any, int]:
    """API health check endpoint."""
    utils.logger.info("API status check called")

    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.utcnow().isoformat(),
        'service': constants.SERVICE_NAME
    }), 200


@app.route('/create_optimized_vocab', methods=['POST'])
def create_optimized_vocab() -> tuple[str, int]:
    """Convert vocabulary CSV files to Parquet and create optimized vocabulary lookup file."""
    data: dict[str, Any] = request.get_json() or {}
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_path: str = constants.VOCAB_PATH

    if not all([vocab_version, vocab_path]):
        return "Missing a required parameter to 'create_optimized_vocab' endpoint. Required: vocab_version, vocab_path", 400

    try:
        # At this point, we know vocab_version is not None
        assert vocab_version is not None

        omop_client.convert_vocab_to_parquet(vocab_version, vocab_path)
        omop_client.create_optimized_vocab_file(vocab_version, vocab_path)

        return "Created optimized vocabulary files", 200
    except Exception as e:
        utils.logger.error(f"Error creating optimized vocabulary: {str(e)}")
        return f"Error creating optimized vocabulary: {str(e)}", 500


@app.route('/create_artifact_directories', methods=['POST'])
def create_artifact_directories() -> tuple[str, int]:
    """Create directories for storing processing artifacts (converted files, reports, etc.)."""
    data: dict[str, Any] = request.get_json() or {}
    delivery_bucket: Optional[str] = data.get('delivery_bucket')

    if not delivery_bucket:
        return "Missing required parameter to 'create_artifact_directories' endpoint: delivery_bucket", 400

    utils.logger.info(f"Creating artifact directories in {storage.get_uri(delivery_bucket)}")

    directories: list[str] = []

    try:
        # Create fully qualified paths for each artifact directory
        for path in constants.ArtifactPaths:
            full_path = f"{delivery_bucket}/{path.value}"
            directories.append(full_path)

        # Create the artifact directories
        for directory in directories:
            storage.create_directory(directory)

        return "Directories created successfully", 200
    except Exception as e:
        utils.logger.error(f"Unable to create artifact directories: {str(e)}")
        return f"Unable to create artifact directories: {str(e)}", 500


@app.route('/get_log_row', methods=['GET'])
def get_log_row() -> tuple[Any, int]:
    """Retrieve pipeline log entries from BigQuery for specified site and delivery date."""
    site: Optional[str] = request.args.get('site')
    delivery_date: Optional[str] = request.args.get('delivery_date')

    if not all([site, delivery_date]):
        return "Missing a required parameter to 'get_log_row' endpoint. Required: site, delivery_date", 400
    
    try:
        # At this point, we know site and delivery_date are not None
        assert site is not None
        assert delivery_date is not None
        
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
    """List files in directory matching specified format."""
    bucket: Optional[str] = request.args.get('bucket')
    folder: Optional[str] = request.args.get('folder')
    file_format: Optional[str] = request.args.get('file_format')
   
    # Validate required parameters
    if not all([bucket, folder, file_format]):
        return "Missing a required parameter to 'get_file_list' endpoint. Required: bucket, folder, file_format", 400

    try:
        # At this point we know these are not None
        assert bucket is not None
        assert folder is not None
        assert file_format is not None
        
        file_list: list[str] = utils.list_files(bucket, folder, file_format)

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
    """Convert incoming OMOP file (.csv/.csv.gz/.parquet) to standardized Parquet format."""
    data: dict[str, Any] = request.get_json() or {}
    file_type: Optional[str] = data.get('file_type')
    file_path: Optional[str] = data.get('file_path')

    if not all([file_type, file_path]):
        return "Missing a required parameter to 'process_incoming_file' endpoint. Required: file_type, file_path", 400

    try:
        # At this point we know these are not None
        assert file_type is not None
        assert file_path is not None
        
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
        storage_path: Optional[str] = data.get('storage_path')

        # Validate required parameters
        if not all([file_path, omop_version, delivery_date, storage_path]):
            return "Missing a required parameter to 'validate_file' endpoint. Required: file_path, omop_version, delivery_date, storage_path", 400

        # At this point we know these are not None
        assert file_path is not None
        assert omop_version is not None
        assert delivery_date is not None
        assert storage_path is not None

        file_validation.validate_file(
            file_path=file_path,
            omop_version=omop_version,
            delivery_date=delivery_date,
            storage_path=storage_path
        )
        utils.logger.info(f"Validation successful for {file_path}")

        return "File successfully validated", 200
        
    except Exception as e:
        utils.logger.error(f"Unable to run file validation: {str(e)}")
        return f"Unable to run file validation: {str(e)}", 500
    

@app.route('/normalize_parquet', methods=['POST'])
def normalize_parquet_file() -> tuple[str, int]:
    """Normalize Parquet file to conform to OMOP CDM schema with type conversions and constraints."""
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    omop_version: Optional[str] = data.get('omop_version')
    date_format: Optional[str] = data.get('date_format')
    datetime_format: Optional[str] = data.get('datetime_format')

    if not all([file_path, omop_version, date_format, datetime_format]):
        return "Missing a required parameter to 'normalize_parquet' endpoint. Required: file_path, omop_version, date_format, datetime_format", 400

    try:
        # At this point we know these are not None
        assert file_path is not None
        assert omop_version is not None
        assert date_format is not None
        assert datetime_format is not None
        
        parquet_file_path: str = utils.get_parquet_artifact_location(file_path)
        utils.logger.info(f"Attempting to normalize Parquet file {parquet_file_path}")
        normalization.normalize_file(parquet_file_path, omop_version, date_format, datetime_format)

        return "Normalized Parquet file", 200
    except Exception as e:
        utils.logger.error(f"Unable to normalize Parquet file: {str(e)}")
        return f"Unable to normalize Parquet file: {str(e)}", 500


@app.route('/upgrade_cdm', methods=['POST'])
def cdm_upgrade() -> tuple[str, int]:
    """Upgrade OMOP CDM file from one version to another (e.g., 5.3 to 5.4)."""
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    omop_version: Optional[str] = data.get('omop_version')
    target_omop_version: Optional[str] = data.get('target_omop_version')

    if not all([file_path, omop_version, target_omop_version]):
        return "Missing a required parameter to 'upgrade_cdm' endpoint. Required: file_path, omop_version, target_omop_version", 400

    try:
        # At this point we know these are not None
        assert file_path is not None
        assert omop_version is not None
        assert target_omop_version is not None
        
        utils.logger.info(f"Attempting to upgrade file {file_path}")
        omop_client.upgrade_file(file_path, omop_version, target_omop_version)

        return "Upgraded file", 200
    except Exception as e:
        utils.logger.error(f"Unable to upgrade file: {str(e)}")
        return f"Unable to upgrade file: {str(e)}", 500


@app.route('/clear_bq_dataset', methods=['POST'])
def clear_bq_tables() -> tuple[str, int]:
    """Delete all tables from BigQuery dataset."""
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([project_id, dataset_id]):
        return "Missing a required parameter to 'clear_bq_dataset' endpoint. Required: project_id, dataset_id", 400

    try:
        # At this point we know these are not None
        assert project_id is not None
        assert dataset_id is not None
        
        utils.logger.info(f"Removing all tables from {project_id}.{dataset_id}")
        gcp_services.remove_all_tables(project_id, dataset_id)

        return "Removed all tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to delete tables within dataset: {str(e)}")
        return f"Unable to delete tables within dataset: {str(e)}", 500


@app.route('/harmonize_vocab', methods=['POST'])
def harmonize_vocab() -> tuple[Any, int]:
    """
    Execute vocabulary harmonization step on OMOP file.
    Harmonizes concepts to target vocabulary version through multi-step process.
    """
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
    omop_version: Optional[str] = data.get('omop_version')
    site: Optional[str] = data.get('site')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    step: Optional[str] = data.get('step')

    if not all([file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id, step]):
        return "Missing a required parameter to 'harmonize_vocab' endpoint. Required: file_path, vocab_version, vocab_gcs_bucket, omop_version, site, project_id, dataset_id, step", 400

    try:
        utils.logger.info(f"Harmonizing vocabulary for {file_path} to version {vocab_version}, step: {step}")

        # At this point we know these are not None
        assert file_path is not None
        assert vocab_version is not None
        assert omop_version is not None
        assert site is not None
        assert project_id is not None
        assert dataset_id is not None
        assert step is not None

        # Initialize the VocabHarmonizer
        vocab_harmonizer = vocab_harmonization.VocabHarmonizer(
            file_path=file_path,
            cdm_version=omop_version,
            site=site,
            vocab_version=vocab_version,
            vocab_gcs_bucket=vocab_gcs_bucket,
            project_id=project_id,
            dataset_id=dataset_id
        )

        # Perform the requested harmonization step
        result = vocab_harmonizer.perform_harmonization(step)

        # If this is the discovery step, return the list of tables
        if step == constants.DISCOVER_TABLES_FOR_DEDUP:
            return jsonify({
                'status': 'success',
                'message': f'Successfully discovered tables for deduplication',
                'table_configs': result,
                'step': step
            }), 200

        # For all other steps, return standard success message
        return jsonify({
            'status': 'success',
            'message': f'Successfully completed {step} for {file_path}',
            'file_path': file_path,
            'step': step
        }), 200

    except Exception as e:
        utils.logger.error(f"Unable to harmonize vocabulary of {file_path} at step {step}: {str(e)}")
        return f"Unable to harmonize vocabulary of {file_path} at step {step}: {str(e)}", 500


@app.route('/generate_derived_tables_from_harmonized', methods=['POST'])
def generate_derived_tables_from_harmonized() -> tuple[str, int]:
    """
    Generate derived tables from HARMONIZED data (post-vocabulary harmonization).

    This endpoint should be called AFTER vocabulary harmonization is complete but
    BEFORE loading to BigQuery. It reads from harmonized Parquet files in the
    omop_etl directory and writes to the derived_files directory.

    The derived tables will be loaded to BigQuery in a separate step.
    """
    data: dict[str, Any] = request.get_json() or {}
    site: Optional[str] = data.get('site')
    site_bucket: Optional[str] = data.get('gcs_bucket')
    delivery_date: Optional[str] = data.get('delivery_date')
    table_name: Optional[str] = data.get('table_name')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH

    if not all([site, delivery_date, table_name, vocab_version, vocab_gcs_bucket, site_bucket]):
        return "Missing a required parameter to 'generate_derived_tables_from_harmonized' endpoint. Required: site, delivery_date, table_name, vocab_version, vocab_gcs_bucket, site_bucket", 400

    try:
        # At this point we know these are not None
        assert site is not None
        assert site_bucket is not None
        assert delivery_date is not None
        assert table_name is not None
        assert vocab_version is not None

        utils.logger.info(f"Generating derived table {table_name} from harmonized data for {delivery_date} delivery from {site}")
        omop_client.generate_derived_data_from_harmonized(site, site_bucket, delivery_date, table_name, vocab_version, vocab_gcs_bucket)
        return "Created derived table from harmonized data", 200
    except Exception as e:
        utils.logger.error(f"Unable to create derived table from harmonized data: {str(e)}")
        return f"Unable to create derived table from harmonized data: {str(e)}", 500


@app.route('/load_target_vocab', methods=['POST'])
def target_vocab_to_bq() -> tuple[str, int]:
    """Load target vocabulary Parquet files to BigQuery tables."""
    data: dict[str, Any] = request.get_json() or {}
    table_file_name: Optional[str] = data.get('table_file_name')
    vocab_version: Optional[str] = data.get('vocab_version')
    vocab_gcs_bucket: str = constants.VOCAB_GCS_PATH
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([vocab_version, vocab_gcs_bucket, project_id, dataset_id, table_file_name]):
        return "Missing a required parameter to 'load_target_vocab' endpoint. Required: vocab_version, vocab_gcs_bucket, project_id, dataset_id, table_file_name", 400
    try:
        # At this point we know these are not None
        assert vocab_version is not None
        assert table_file_name is not None
        assert project_id is not None
        assert dataset_id is not None
        
        omop_client.load_vocabulary_table(vocab_version, vocab_gcs_bucket, table_file_name, project_id, dataset_id)

        return f"Successfully loaded vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}", 200
    except Exception as e:
        utils.logger.error(f"Unable to load vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}: {str(e)}")
        return f"Unable to load vocabulary {vocab_version} file {table_file_name} to {project_id}.{dataset_id}: {str(e)}", 500        


@app.route('/parquet_to_bq', methods=['POST'])
def parquet_gcs_to_bq() -> tuple[str, int]:
    """Load Parquet file from GCS to BigQuery table."""
    data: dict[str, Any] = request.get_json() or {}
    file_path: Optional[str] = data.get('file_path')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    table_name: Optional[str] = data.get('table_name')
    write_type: Optional[str] = data.get('write_type')

    if not all([file_path, project_id, dataset_id, write_type, table_name]):
        return "Missing a required parameter to 'parquet_to_bq' endpoint. Required: file_path, project_id, dataset_id, write_type, table_name", 400
    
    try:
        # At this point we know these are not None
        assert file_path is not None
        assert project_id is not None
        assert dataset_id is not None
        assert table_name is not None
        assert write_type is not None
        
        write_disposition = constants.BQWriteTypes(write_type)
    except ValueError:
        return f"Invalid write_disposition: {write_type}. Valid values are: {[e.value for e in constants.BQWriteTypes]}", 400

    try:
        utils.logger.info(f"Attempting to load file {file_path} to {project_id}.{dataset_id} using {write_disposition.value} method")
        gcp_services.load_parquet_to_bigquery(file_path, project_id, dataset_id, table_name, write_disposition)

        return "Loaded Parquet file to BigQuery", 200
    except Exception as e:
        utils.logger.error(f"Unable to load Parquet file: {str(e)}")
        return f"Unable to load Parquet file: {str(e)}", 500


@app.route('/generate_delivery_report', methods=['POST'])
def generate_final_delivery_report() -> tuple[str, int]:
    """Generate final delivery report CSV by consolidating all report artifacts."""
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
    """Create missing OMOP CDM tables in BigQuery dataset using DDL scripts."""
    data: dict[str, Any] = request.get_json() or {}
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')
    omop_version: Optional[str] = data.get('omop_version')

    if not all([project_id, dataset_id, omop_version]):
        return "Missing a required parameter to 'create_missing_tables' endpoint. Required: project_id, dataset_id, omop_version", 400

    try:
        # At this point we know these are not None
        assert project_id is not None
        assert dataset_id is not None
        assert omop_version is not None
        
        utils.logger.info(f"Creating any missing v{omop_version} tables in {project_id}.{dataset_id}")
        omop_client.create_missing_tables(project_id, dataset_id, omop_version)

        return "Created missing tables", 200
    except Exception as e:
        utils.logger.error(f"Unable to create missing tables: {str(e)}")
        return f"Unable to create missing tables: {str(e)}", 500


@app.route('/populate_cdm_source', methods=['POST'])
def add_cdm_source_record() -> tuple[str, int]:
    """Populate cdm_source table with metadata about the CDM instance and delivery."""
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
 

@app.route('/harmonized_parquets_to_bq', methods=['POST'])
def harmonized_parquets_to_bq() -> tuple[str, int]:
    """
    Load consolidated OMOP ETL parquet files from GCS to BigQuery.
    
    This endpoint discovers all consolidated parquet files in the OMOP_ETL artifacts directory
    and loads each one to its corresponding BigQuery table.
    """
    data: dict[str, Any] = request.get_json() or {}
    gcs_bucket: Optional[str] = data.get('gcs_bucket')
    delivery_date: Optional[str] = data.get('delivery_date')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([gcs_bucket, delivery_date, project_id, dataset_id]):
        return "Missing a required parameter to 'harmonized_parquets_to_bq' endpoint. Required: gcs_bucket, delivery_date, project_id, dataset_id", 400
    
    try:
        # At this point we know these are not None
        assert gcs_bucket is not None
        assert delivery_date is not None
        assert project_id is not None
        assert dataset_id is not None
        
        # Call the GCP service function to handle the heavy lifting
        results = gcp_services.load_harmonized_parquets_to_bq(
            gcs_bucket,
            delivery_date,
            project_id,
            dataset_id
        )
        
        # Prepare response message
        loaded_tables = results['loaded']
        skipped_tables = results['skipped']
        
        response_parts = []
        if loaded_tables:
            response_parts.append(f"Successfully loaded {len(loaded_tables)} table(s): {', '.join(loaded_tables)}")
        if skipped_tables:
            response_parts.append(f"Skipped {len(skipped_tables)} table(s): {', '.join(skipped_tables)}")
        
        response_message = ". ".join(response_parts)
        utils.logger.info(response_message)
        
        return response_message, 200
        
    except Exception as e:
        utils.logger.error(f"Error loading harmonized parquets to BigQuery: {str(e)}")
        return f"Error loading harmonized parquets to BigQuery: {str(e)}", 500


@app.route('/load_derived_tables_to_bq', methods=['POST'])
def load_derived_tables_to_bq() -> tuple[str, int]:
    """
    Load derived table parquet files from GCS to BigQuery.

    This endpoint discovers all derived table parquet files in the DERIVED_FILES artifacts directory
    and loads each one to its corresponding BigQuery table.
    """
    data: dict[str, Any] = request.get_json() or {}
    gcs_bucket: Optional[str] = data.get('gcs_bucket')
    delivery_date: Optional[str] = data.get('delivery_date')
    project_id: Optional[str] = data.get('project_id')
    dataset_id: Optional[str] = data.get('dataset_id')

    if not all([gcs_bucket, delivery_date, project_id, dataset_id]):
        return "Missing a required parameter to 'load_derived_tables_to_bq' endpoint. Required: gcs_bucket, delivery_date, project_id, dataset_id", 400

    try:
        # At this point we know these are not None
        assert gcs_bucket is not None
        assert delivery_date is not None
        assert project_id is not None
        assert dataset_id is not None

        # Call the GCP service function to handle the heavy lifting
        results = gcp_services.load_derived_tables_to_bq(
            gcs_bucket,
            delivery_date,
            project_id,
            dataset_id
        )

        # Prepare response message
        loaded_tables = results['loaded']
        skipped_tables = results['skipped']

        response_parts = []
        if loaded_tables:
            response_parts.append(f"Successfully loaded {len(loaded_tables)} derived table(s): {', '.join(loaded_tables)}")
        if skipped_tables:
            response_parts.append(f"Skipped {len(skipped_tables)} derived table(s): {', '.join(skipped_tables)}")

        if not response_parts:
            response_message = "No derived tables found to load"
        else:
            response_message = ". ".join(response_parts)

        utils.logger.info(response_message)

        return response_message, 200

    except Exception as e:
        utils.logger.error(f"Error loading derived tables to BigQuery: {str(e)}")
        return f"Error loading derived tables to BigQuery: {str(e)}", 500


@app.route('/pipeline_log', methods=['POST'])
def log_pipeline_state() -> tuple:
    """Log pipeline execution state to BigQuery logging table."""
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

        # At this point we know these are not None
        assert site_name is not None
        assert delivery_date is not None
        assert status is not None
        assert run_id is not None
        
        pipeline_logger = pipeline_log.PipelineLog(
            logging_table,
            site_name,
            delivery_date,
            status,
            message,  # Optional parameters don't need casting
            file_type,
            omop_version,
            run_id
        )
        pipeline_logger.add_log_entry()

        return "Successfully logged to BigQuery", 200

    except Exception as e:
        utils.logger.error(f"Unable to save logging information to BigQuery table: {str(e)}")
        return f"Unable to save logging information to BigQuery table: {str(e)}", 500    

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 8080))
    app.run(host='0.0.0.0', port=port)