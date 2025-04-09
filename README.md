# OMOP File Processor - Quick Start Guide

Additional documentation, including a [comprehensive user guide](https://github.com/Analyticsphere/ehr-pipeline-documentation/wiki/OMOP-Pipeline-User-Guide), can be found in the [`ehr-pipeline-documentation`](https://github.com/Analyticsphere/ehr-pipeline-documentation) repository.

## Overview

The OMOP File Processor is an API-driven service that processes healthcare data files conforming to the OMOP Common Data Model (CDM). It performs these key functions:

- Validates, cleans, and normalizes OMOP CSV/Parquet files
- Converts files between formats and CDM versions
- Harmonizes vocabularies to a common version
- Loads processed data to BigQuery tables

The service is deployed as a Docker container in Google Cloud Run and is typically integrated with Airflow DAGs from the `ccc-orchestrator` repository. 

More information is available in [API documentation](#omop-data-processing-api-documentation) and the complete OMOP Data Pipeline user guide.

## Prerequisites

- Google Cloud Platform access with appropriate permissions
- Access to the target GCS buckets and BigQuery datasets
- Service account with required permissions

## File Storage Convention

Files processed by this service are stored in GCS buckets following this structure:
```
gs://{bucket-name}/{YYYY-MM-DD}/{filename}
```
where `YYYY-MM-DD` corresponds to the date the files were received.

## Configuration

### 1. Cloud Build Configuration (`cloudbuild.yml`)

Resources allocation is configured in the `cloudbuild.yml` file of the omop-file-processor.

- **CPU**: Number of CPU cores allocated to the Cloud Run service
  ```
  '--cpu=4'  # Default is 4 cores
  ```
- **Memory**: RAM allocated to the Cloud Run service
  ```
  '--memory=16Gi'  # Default is 16GB
  ```

### 2. Cloud Build Web UI Substitution Variables

Set these variables in the Cloud Build trigger configuration:

- **`_IMAGE_NAME`**: Container image name (default: `ccc-omop-file-processor`)
- **`_SERVICE_ACCOUNT`**: Service account email (format: `service-account@project-id.iam.gserviceaccount.com`)
- **`_TMP_GCS_BUCKET`**: GCS bucket used for temporarily processing files
- **`_BQ_LOGGING_TABLE`**: Table used to store pipeline execution status in BigQuery. Specify a fully qualified table path (i.e. `project_name.dataset_name.table_name`). The pipeline will create the specified table if it does not already exist.
- **`_VOCAB_GCS_PATH`**: GCS bucket containing OMOP vocabulary files downloaded from Athena

### 3. Application Constants (`core/constants.py`)

Adjust these settings in the `constants.py` file to match resouce allocations:

- **`DUCKDB_MEMORY_LIMIT`**: Maximum memory for DuckDB (should be set to Cloud Run memory minus 2GB-4GB)
  ```
  DUCKDB_MEMORY_LIMIT = "12GB"  # For a 16GB Cloud Run instance
  ```
- **`DUCKDB_THREADS`**: Number of concurrent threads (should never exceed CPU count). Lower the number of threads to reduce memory utilization and prevent out-of-memory errors.
  ```
  DUCKDB_THREADS = "2"  # For a 4-core Cloud Run instance
  ```

## Setup Process

1. **Set up GCS buckets** for your data files
   - Create a main bucket for data files
   - Ensure the `_TMP_GCS_BUCKET` exists for temporary processing
   - Download vocabulary files from [Athena](https://athena.ohdsi.org/search-terms/start) and upload them to a folder in the GCS bucket `_VOCAB_GCS_PATH`.

2. **Deploy the service**
   - Create a trigger to build the omop-file-processor as a Cloud Run function
   - Note the resulting Cloud Run URL for API calls. This URL will be used in the Airflow DAG, and when manually making API calls.


## Verifying Setup

To verify your service is properly configured:

1. Test the heartbeat endpoint:
   ```bash
   curl -X GET https://your-service-url/heartbeat
   ```
   
   Expected response:
   ```json
   {
     "status": "healthy",
     "timestamp": "2025-04-01T12:34:56.789Z",
     "service": "omop-file-processor"
   }
   ```

2. Check Cloud Run logs to ensure the service is running without errors

## Troubleshooting

Common issues and solutions:

- **Memory errors**: Review Cloud Run logs, and if needed, increase Cloud Run memory allocation and the `DUCKDB_MEMORY_LIMIT` parameter.
- **Permission denied errors**: Verify the service account has appropriate access to GCS buckets and BigQuery datasets
- **Timeout errors**: The pipeline is built to performantely handle large workloads. Timeout errors may indicate an error in the source data files, a bug in the pipeline, or a need to increase resource allocation.

#

# OMOP Data Processing API Documentation

## Table of Contents

- [Introduction](#introduction)
- [Common Response Codes](#common-response-codes)
- [Environment Configuration](#environment-configuration)
- [API Endpoints](#api-endpoints)
  - [Heartbeat](#heartbeat)
  - [Create artifact buckets](#create-artifact-buckets)
  - [Create optimized vocabulary files](#create-optimized-vocab)
  - [Get BigQuery log row](#get-log-row)
  - [Get list of files to process](#get-file-list)
  - [Process incoming file](#process-incoming-file)
  - [Validate OMOP data file](#validate-file)
  - [Normalize OMOP data file](#normalize-parquet)
  - [Upgrade CDM version](#upgrade-cdm)
  - [Harmonize vocabulary version](#harmonize-vocab)
  - [Get vocabulary harmonization status](#harmonize-vocab-status)
  - [Perform vocabulary harmonization steps](#harmonize-vocab-process-step)
  - [Populate derived data](#populate-derived-data)
  - [Clear BigQuery dataset](#clear-bq-dataset)
  - [Load vocabulary data](#load-target-vocab)
  - [Load Parquet to BigQuery](#parquet-to-bq)
  - [Create missing tables](#create-missing-tables)
  - [Populate cdm_source table](#populate-cdm-source)
  - [Generate delivery report](#generate-delivery-report)
  - [BigQuery logging](#pipeline-log)

## Introduction

The omop-file-processor API provides a set of endpoints for working with healthcare data structured according to the Observational Medical Outcomes Partnership (OMOP) Common Data Model (CDM). It is currently deployed as a Google Cloud Run service in the NCI's Connect GCP environment.

The API operates on files stored in Google Cloud Storage (GCS) buckets. Each endpoint performs specific operations on individual data files within an OMOP delivery, with users providing GCS file paths and configuration parameters to initiate processing. This file-centric approach supports performant parallel processing of large healthcare datasets.

This API facilitates:

- **Validation and Quality Control**: Ensures data files conform to OMOP CDM specifications
- **Format Conversion and Normalization**: Transforms files between formats and normalizes data to meet OMOP standards
- **Vocabulary Management**: Harmonizes clinical terminologies and concept mappings across different vocabulary versions
- **ETL Operations**: Performs extract, transform, and load operations between different OMOP structures
- **BigQuery Integration**: Loads processed data into Google BigQuery for analysis
- **Process Logging**: Tracks processing steps and outcomes for auditing and troubleshooting

The API is implemented using Flask, providing a RESTful interface. The data processing logic uses DuckDB for manipulation of CSV and Parquet files. Although the underlying technology is designed to be platform-agnostic, the current implementation requires files to be stored in GCS buckets.

## Common Response Codes

All POST endpoints in this API return the following standard response codes:

| Status Code | Description |
|-------------|-------------|
| 200 | Operation completed successfully |
| 400 | Missing or invalid required parameters |
| 500 | Server error occurred during operation execution |

## Environment Configuration

Several values used across multiple endpoints are configured through environment variables or constants in the application rather than being passed with each request. This includes:

| Configuration | Variable | Description |
|---------------|----------|-------------|
| Vocabulary GCS Path | `VOCAB_GCS_PATH` | GCS bucket path containing vocabulary files |
| BigQuery Logging Table | `BQ_LOGGING_TABLE` | Fully qualified table ID for pipeline logging |
| Service Name | `SERVICE_NAME` | Name of the service for identification in logs |

These values must be properly configured in the environment or application constants file before using the API.

## API Endpoints

### Heartbeat

**Endpoint:** `GET /heartbeat`

**Description:** Provides a status check to verify the API is running properly.

**Response:**

| Status Code | Description |
|-------------|-------------|
| 200 | API is running properly |

**Response Format:**
```json
{
    "status": "healthy",
    "timestamp": "2023-05-01T12:34:56.789012",
    "service": "omop-file-processor"
}
```

---

### Create Artifact Buckets

**Endpoint:** `POST /create_artifact_buckets`

**Description:** Creates the necessary buckets in Google Cloud Storage for the pipeline to store artifacts generated during data processing.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| delivery_bucket | string | Yes | GCS path to data delivery |

**Example Request:**
```json
{
    "delivery_bucket": "delivery_site/2023-05-01"
}
```

---

### Create Optimized Vocab

**Endpoint:** `POST /create_optimized_vocab`

**Description:** Converts CSV files downloaded from [Athena](https://athena.ohdsi.org/search-terms/start) to Parquet format and creates an "optimized_vocabulary" file which is used in the vocabulary harmonization processes.

Vocabulary files must be stored in folders in a GCS bucket dedicated to maintaining OMOP vocabulary data. The folder must be named the same as the version (i.e. `gs://vocab_bucket/v5.0 29-FEB-24/`).

OMOP vocabulary files are updated twice a year. Users will need to manually download new vocabulary files from Athena when they become available and upload them to GCS for use in the pipeline.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| vocab_version | string | Yes | The version of the OMOP vocabulary to use |

**Note:** The vocabulary GCS bucket is configured via the `VOCAB_GCS_PATH` constant and does not need to be passed in the request.

**Example Request:**
```json
{
    "vocab_version": "v5.0 29-FEB-24"
}
```

---

### Get Log Row

**Endpoint:** `GET /get_log_row`

**Description:** Retrieves log information for a specific site and delivery date from the BigQuery logging table.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site | string | Yes | Site identifier |
| delivery_date | string | Yes | Delivery date |

**Response:**

| Status Code | Description |
|-------------|-------------|
| 200 | Log row retrieved successfully |
| 400 | Missing required parameters |
| 500 | Unable to get BigQuery log row |

**Response Format:**
```json
{
    "status": "healthy",
    "log_row": [
        {
            "site_name": "hospital-a",
            "delivery_date": "2023-05-01",
            "status": "completed",
            "message": null,
            "pipeline_start_datetime": "2023-05-01T12:00:00",
            "pipeline_end_datetime": "2023-05-01T14:30:00",
            "file_type": ".csv",
            "omop_version": "5.4",
            "run_id": "run-123456"
        }
    ],
    "service": "omop-file-processor"
}
```

---

### Get File List

**Endpoint:** `GET /get_file_list`

**Description:** Retrieves a list of OMOP data files that need to be processed. 

When OMOP files are stored in the format `gs://delivery_site/YYYY-MM-DD/file1.csv`, the bucket is *delivery_site* and the folder is *YYYY-MM-DD*. 

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| bucket | string | Yes | Google Cloud Storage bucket name |
| folder | string | Yes | Folder path within the bucket |
| file_format | string | Yes | File format to filter by (".csv" or ".parquet") |

**Response:**

| Status Code | Description |
|-------------|-------------|
| 200 | List of files retrieved successfully |
| 400 | Missing required parameters |
| 500 | Unable to get list of files |

**Response Format:**
```json
{
    "status": "healthy",
    "file_list": ["file1.csv", "file2.csv", "file3.csv"],
    "service": "omop-file-processor"
}
```

---

### Process Incoming File

**Endpoint:** `POST /process_incoming_file`

**Description:** Processes an incoming file, typically converting it to Parquet format for more efficient processing.

The pipeline supports incoming CSV and Parquet files.

The pipeline will attempt to automatically correct invalid text formatting, unescaped quotes, and other common CSV issues when converting CSV files to Parquet.

Incoming Parquet files are copied to the pipeline artifacts bucket, during which all fields are converted to string type.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_type | string | Yes | Type of file being processed (".csv" or ".parquet") |
| file_path | string | Yes | Path to the file to process |

**Example Request:**
```json
{
    "file_type": ".csv",
    "file_path": "delivery_site/2023-05-01/person.csv"
}
```

---

### Validate File

**Endpoint:** `POST /validate_file`

**Description:** Validates a file's name and schema against the OMOP standard for a given OMOP CDM version. Generates report artifacts which are used in the final delivery report file.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to the file to validate |
| omop_version | string | Yes | OMOP CDM version to validate against |
| delivery_date | string | Yes | Delivery date of the data |
| gcs_path | string | Yes | Google Cloud Storage path |

**Example Request:**
```json
{
    "file_path": "delivery_site/2023-05-01/person.csv",
    "omop_version": "5.4",
    "delivery_date": "2023-05-01",
    "gcs_path": "delivery_site/2023-05-01/"
}
```

---

### Normalize Parquet

**Endpoint:** `POST /normalize_parquet`

**Description:** Normalizes a Parquet file according to OMOP standards.

- Converts data types of columns within Parquet file to types specified in OMOP CDM
- Creates a new Parquet file with the invalid rows from the original data file in `artifacts/invalid_rows/`
- Ensures consistent column order within Parquet
- Set (possibly non-unique) deterministic composite key for tables with surrogate primary keys
- Adds missing columns and removes unexpected columns from Parquet files

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to the original data file |
| omop_version | string | Yes | OMOP CDM version to normalize against |

**Example Request:**
```json
{
    "file_path": "delivery_site/2023-05-01/person.csv",
    "omop_version": "5.4"
}
```

---

### Upgrade CDM

**Endpoint:** `POST /upgrade_cdm`

**Description:** Upgrades a file from one OMOP CDM version to another. Currently, the pipeline only supports upgrading from 5.3 to 5.4.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to the original data file |
| omop_version | string | Yes | Current OMOP CDM version |
| target_omop_version | string | Yes | Target OMOP CDM version |

**Example Request:**
```json
{
    "file_path": "delivery_site/2023-05-01/person.csv",
    "omop_version": "5.3",
    "target_omop_version": "5.4"
}
```

---

# Vocabulary Harmonization API Documentation

### Why Polling for Vocabulary Harmonization?

The vocabulary harmonization API endpoints use a polling-based approach, as this functionality is particularly resource-intensive and time-consuming. Several constraints led to implementing a polling-based approach:

1. **Airflow Task Management**: The Airflow DAG coordinates API calls and waits for their completion. Long-running synchronous API calls can cause Airflow tasks to remain in a "running" state even after the Cloud Run service has succeeded and returned a response, eventually triggering timeout errors in Airflow.

2. **Resource Efficiency**: Breaking the harmonization into discrete steps allows each step to be processed with full resources, rather than trying to process everything in a single request where resources might be constrained over time.

3. **Resilience to Failures**: If a step fails, only that step needs to be retried rather than the entire process.

4. **Progress Tracking**: The polling mechanism provides detailed progress information, making it easier to monitor and debug the harmonization process.

This approach ensures that both the Cloud Run service and the Airflow DAG operate within their respective constraints while still enabling processing of large datasets.

---

## Harmonize Vocab

**Endpoint:** `POST /harmonize_vocab`

**Description:** Initiates an asynchronous vocabulary harmonization job. This endpoint returns immediately with a job ID that can be used to check status and process steps.

The endpoint uses an asynchronous polling pattern due to the resource-intensive nature of vocabulary harmonization, which can exceed Cloud Run or Airflow task request timeouts with large datasets.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to the original data file |
| vocab_version | string | Yes | Target vocabulary version |
| omop_version | string | Yes | OMOP CDM version |
| site | string | Yes | Site identifier |
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Note:** The vocabulary GCS bucket is configured via the `VOCAB_GCS_PATH` constant and does not need to be passed in the request.

**Response:**

| Status Code | Description |
|-------------|-------------|
| 202 | Job accepted for processing |
| 400 | Missing required parameters |
| 500 | Unable to initiate harmonization job |

**Response Format:**
```json
{
    "job_id": "12345678-1234-5678-1234-567812345678",
    "status": "queued",
    "bucket": "my-site-bucket",
    "delivery_date": "2023-05-01",
    "message": "Vocabulary harmonization job queued for delivery_site/2023-05-01/condition_occurrence.csv"
}
```

**Example Request:**
```json
{
    "file_path": "delivery_site/2023-05-01/condition_occurrence.csv",
    "vocab_version": "v5.0 29-FEB-24",
    "omop_version": "5.4",
    "site": "hospital-a",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm"
}
```

## Harmonize Vocab Status

**Endpoint:** `GET /harmonize_vocab_status`

**Description:** Checks the current status of a previously initiated harmonization job. This endpoint is used by the Airflow DAG to poll for job completion.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| job_id | string | Yes | Unique identifier for the harmonization job |
| bucket | string | Yes | Google Cloud Storage bucket name |
| delivery_date | string | Yes | Delivery date of the data |

**Response:**

| Status Code | Description |
|-------------|-------------|
| 200 | Job completed successfully |
| 202 | Job is still in progress |
| 404 | Job not found |
| 500 | Error retrieving job status or job failed |

**Response Format (in progress):**
```json
{
    "job_id": "12345678-1234-5678-1234-567812345678",
    "status": "running",
    "current_step": "SOURCE_TARGET",
    "current_step_index": 1,
    "steps": ["SOURCE_TARGET", "TARGET_REMAP", "TARGET_REPLACEMENT", "DOMAIN_CHECK", "omop_etl"],
    "progress": "1/5"
}
```

**Response Format (completed):**
```json
{
    "job_id": "12345678-1234-5678-1234-567812345678",
    "status": "completed",
    "start_time": "2023-05-01T12:34:56.789Z",
    "end_time": "2023-05-01T12:35:56.789Z",
    "file_path": "delivery_site/2023-05-01/condition_occurrence.csv"
}
```

## Harmonize Vocab Process Step

**Endpoint:** `POST /harmonize_vocab_process_step`

**Description:** Processes a single step of a vocabulary harmonization job. This endpoint is called by the Airflow DAG to advance the job one step at a time.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| job_id | string | Yes | Unique identifier for the harmonization job |
| bucket | string | Yes | Google Cloud Storage bucket name |
| delivery_date | string | Yes | Delivery date of the data |

**Response:**

| Status Code | Description |
|-------------|-------------|
| 200 | Step processed successfully or job completed |
| 404 | Job not found |
| 500 | Error processing step |

**Response Format:**
```json
{
    "job_id": "12345678-1234-5678-1234-567812345678",
    "status": "running",
    "current_step": "TARGET_REMAP",
    "progress": "2/5"
}
```

**Example Request:**
```json
{
    "job_id": "12345678-1234-5678-1234-567812345678",
    "bucket": "my-site-bucket",
    "delivery_date": "2023-05-01"
}
```

---


### Populate Derived Data

**Endpoint:** `POST /populate_derived_data`

**Description:** Generates and populates derived data tables based on OMOP data.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site | string | Yes | Site identifier |
| gcs_bucket | string | Yes | Google Cloud Storage bucket |
| delivery_date | string | Yes | Delivery date |
| table_name | string | Yes | Name of the derived table to create |
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| vocab_version | string | Yes | Vocabulary version |

**Note:** The vocabulary GCS bucket is configured via the `VOCAB_GCS_PATH` constant and does not need to be passed in the request.

**Example Request:**
```json
{
    "site": "hospital-a",
    "gcs_bucket": "my-site-bucket",
    "delivery_date": "2023-05-01",
    "table_name": "drug_era",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm",
    "vocab_version": "v5.0 29-FEB-24"
}
```

---

### Clear BQ Dataset

**Endpoint:** `POST /clear_bq_dataset`

**Description:** Removes *all* tables from a specified BigQuery dataset.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Example Request:**
```json
{
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm"
}
```

---

### Load Target Vocab

**Endpoint:** `POST /load_target_vocab`

**Description:** Loads Parquet vocabulary files as tables in BigQuery.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| table_file_name | string | Yes | Vocabulary table file name |
| vocab_version | string | Yes | Vocabulary version |
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Note:** The vocabulary GCS bucket is configured via the `VOCAB_GCS_PATH` constant and does not need to be passed in the request.

**Example Request:**
```json
{
    "table_file_name": "concept",
    "vocab_version": "v5.0 29-FEB-24",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm"
}
```

---

### Parquet to BQ

**Endpoint:** `POST /parquet_to_bq`

**Description:** Loads Parquet data from Google Cloud Storage to BigQuery.

This endpoint requires the `write_type` parameter, which the file processor API uses to determine where the Parquet data can be found in GCS, and the appropriate write method (i.e. append vs. truncate).

`write_type` is a member of the custom BQWriteTypes Enum class, represented as a string in the API call.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to the Parquet assets or original file |
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| table_name | string | Yes | BigQuery table name |
| write_type | string | Yes | BQWriteTypes enum type |

**Example Request:**
```json
{
    "file_path": "delivery_site/2023-05-01/person.csv",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm",
    "table_name": "person",
    "write_type": "processed_file"
}
```

**Additional Example Requests:**
```json
{
    "file_path": "delivery_site/2023-05-01/artifacts/converted_files/person.parquet",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm",
    "table_name": "person",
    "write_type": "specific_file"
}
```

```json
{
    "file_path": "delivery_site/2023-05-01/artifacts/harmonized_files/note/transformed",
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm",
    "table_name": "device_exposure",
    "write_type": "ETLed_file"
}
```

---

### Create Missing Tables

**Endpoint:** `POST /create_missing_tables`

**Description:** Creates any missing OMOP tables in a BigQuery dataset based on the specified CDM version.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| project_id | string | Yes | Google Cloud project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| omop_version | string | Yes | OMOP CDM version |

**Example Request:**
```json
{
    "project_id": "my-gcp-project",
    "dataset_id": "omop_cdm",
    "omop_version": "5.4"
}
```

---

### Populate CDM Source

**Endpoint:** `POST /populate_cdm_source`

**Description:** Populates the CDM_SOURCE table with metadata about the data source.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| source_release_date | string | Yes | Release date of the source data |
| cdm_source_abbreviation | string | Yes | Abbreviation for the CDM source |
| [additional parameters] | various | No | Additional source metadata as needed |

**Example Request:**
```json
{
    "source_release_date": "2023-05-01",
    "cdm_source_abbreviation": "HOSP_A",
    "cdm_version": "5.4",
    "vocabulary_version": "v5.0 29-FEB-24",
    "cdm_release_date": "2023-05-05"
}
```

---

### Generate Delivery Report

**Endpoint:** `POST /generate_delivery_report`

**Description:** Generates a final report for a site's data delivery.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| delivery_date | string | Yes | Date of the delivery |
| site | string | Yes | Site identifier |
| [additional parameters] | various | No | Additional report data as needed |

**Example Request:**
```json
{
    "delivery_date": "2023-05-01",
    "site": "hospital-a",
    "site_display_name": "Hospital A",
    "file_delivery_format": ".csv",
    "delivered_cdm_version": "5.3",
    "target_cdm_version": "5.4",
    "target_vocabulary_version": "v5.0 29-FEB-24"
}
```

---

### Pipeline Log

**Endpoint:** `POST /pipeline_log`

**Description:** Logs pipeline execution state to a BigQuery table for tracking and monitoring.

**Request Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site_name | string | Yes | Site identifier |
| delivery_date | string | Yes | Delivery date |
| status | string | Yes | Status of the pipeline step |
| run_id | string | Yes | Unique identifier for the pipeline run |
| message | string | No | Detailed message about the step |
| file_type | string | No | Type of file being processed |
| omop_version | string | No | OMOP CDM version |

**Note:** The logging table is configured via the `BQ_LOGGING_TABLE` constant and does not need to be passed in the request.

**Example Request:**
```json
{
    "site_name": "hospital-a",
    "delivery_date": "2023-05-01",
    "status": "COMPLETED",
    "run_id": "run-123456",
    "message": "Successfully processed person table",
    "file_type": "person",
    "omop_version": "5.4"
}
```