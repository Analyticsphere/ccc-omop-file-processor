"""
Module for managing long-running asynchronous jobs
"""
import json
import uuid
from datetime import datetime
from typing import Any

from google.cloud import storage

import core.constants as constants
import core.gcp_services as gcp_services
import core.utils as utils
import core.vocab_harmonization as vh


class HarmonizationJobManager:
    """
    Manages vocabulary harmonization jobs that run asynchronously
    """
    
    @staticmethod
    def create_job(file_path: str, cdm_version: str, site: str, 
                  vocab_version: str, vocab_gcs_bucket: str, 
                  project_id: str, dataset_id: str) -> dict[str, Any]:
        """
        Creates a new harmonization job and returns the job info
        """
        # Generate job ID
        job_id = str(uuid.uuid4())
        bucket_name, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(file_path)
        
        # Create status directory
        status_dir = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status"
        gcp_services.create_gcs_directory(f"{bucket_name}/{status_dir}")
        
        # Define harmonization steps
        steps = [
            constants.SOURCE_TARGET,
            constants.TARGET_REMAP,
            constants.TARGET_REPLACEMENT, 
            constants.DOMAIN_CHECK,
            "omop_etl"
        ]
        
        # Create job status file
        status_file_path = f"{status_dir}/{job_id}.json"
        status_data = {
            "job_id": job_id,
            "status": "queued",
            "file_path": file_path,
            "vocab_version": vocab_version,
            "vocab_gcs_bucket": vocab_gcs_bucket,
            "cdm_version": cdm_version,
            "site": site,
            "project_id": project_id,
            "dataset_id": dataset_id,
            "start_time": datetime.utcnow().isoformat(),
            "current_step": "init",
            "current_step_index": 0,
            "steps": steps,
            "bucket": bucket_name,
            "delivery_date": delivery_date,
            "error": None
        }
        
        # Save job data
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(status_file_path) 
        blob.upload_from_string(json.dumps(status_data), content_type="application/json")
        
        utils.logger.info(f"Created harmonization job {job_id} for {file_path}")
        
        # Return job info
        return {
            "job_id": job_id,
            "status": "queued",
            "bucket": bucket_name,
            "delivery_date": delivery_date,
            "message": f"Vocabulary harmonization job queued for {file_path}"
        }
    
    @staticmethod
    def get_job_status(job_id: str, bucket_name: str, delivery_date: str) -> dict[str, Any]:
        """
        Gets the current status of a harmonization job
        """
        status_path = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status/{job_id}.json"
        
        try:
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(status_path)
            
            if not blob.exists():
                return {"error": f"Job {job_id} not found", "status": "not_found"}
                
            # Load job data
            job_data = json.loads(blob.download_as_string())
            return job_data
            
        except Exception as e:
            utils.logger.error(f"Error getting job status for {job_id}: {str(e)}")
            return {"error": str(e), "status": "error"}
    
    @staticmethod
    def process_job_step(job_id: str, bucket_name: str, delivery_date: str) -> dict[str, Any]:
        """
        Processes a single step of a harmonization job
        """
        status_path = f"{delivery_date}/{constants.ArtifactPaths.HARMONIZED_FILES.value}job_status/{job_id}.json"
        
        try:
            # Get job data
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            blob = bucket.blob(status_path)
            
            if not blob.exists():
                return {"error": f"Job {job_id} not found", "status": "not_found"}
                
            # Load job data
            job_data = json.loads(blob.download_as_string())
            
            # If job already completed or errored, return status
            if job_data['status'] in ['completed', 'error']:
                return job_data
                
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
                return job_data
                
            # Get the current step to execute
            current_step = steps[current_step_index]
            job_data['current_step'] = current_step
            
            # Initialize harmonizer with job parameters
            utils.logger.info(f"Job {job_id}: Processing step {current_step_index+1}/{len(steps)}: {current_step}")
            
            file_path = job_data['file_path']
            vocab_harmonizer = vh.VocabHarmonizer(
                file_path=file_path,
                cdm_version=job_data['cdm_version'],
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
                job_data['progress'] = f"{current_step_index + 1}/{len(steps)}"
                
                # Check if all steps are complete after incrementing
                if job_data['current_step_index'] >= len(steps):
                    job_data['status'] = 'completed'
                    job_data['end_time'] = datetime.utcnow().isoformat()
                
                blob.upload_from_string(json.dumps(job_data), content_type="application/json")
                return job_data
                
            except Exception as e:
                # Log and save error
                utils.logger.error(f"Job {job_id} failed at step {current_step}: {str(e)}")
                job_data['status'] = 'error'
                job_data['error'] = f"Failed at step {current_step}: {str(e)}"
                job_data['end_time'] = datetime.utcnow().isoformat()
                blob.upload_from_string(json.dumps(job_data), content_type="application/json")
                
                return {
                    "job_id": job_id,
                    "status": "error",
                    "step": current_step,
                    "error": str(e)
                }
                
        except Exception as e:
            utils.logger.error(f"Error processing job step for {job_id}: {str(e)}")
            return {"error": str(e), "status": "error"}