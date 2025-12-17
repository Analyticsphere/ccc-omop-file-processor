# OMOP File Processor

## Overview

The OMOP File Processor is a REST API service for processing healthcare data conforming to the OMOP Common Data Model (CDM). The service validates, normalizes, and transforms OMOP data files, harmonizes vocabularies, and loads processed data to BigQuery. The file processor is one of three components to the OMOP pipeline.

The service is deployed as a Docker container on Google Cloud Run and integrates with Airflow DAGs for orchestration. Comprehensive documentation is available in the [`ehr-pipeline-documentation`](https://github.com/Analyticsphere/ehr-pipeline-documentation) repository, including a [user guide](https://github.com/Analyticsphere/ehr-pipeline-documentation/wiki/OMOP-Pipeline-User-Guide).

## Prerequisites

- Google Cloud Platform project with permissions for:
  - Cloud Storage (read/write to GCS buckets)
  - BigQuery (create/modify datasets and tables)
  - Cloud Run (deploy services)
- Service account with required IAM roles
- OMOP vocabulary files from [Athena](https://athena.ohdsi.org/search-terms/start)

## Storage Configuration

The service supports two storage backends, GCS and local environments, controlled by the `STORAGE_BACKEND` environmental variable. Unless otherwise specified, GCS is used by default.

### File Path Convention

Files in GCS must follow this structure:
```
{bucket-or-directory}/{YYYY-MM-DD}/{filename}
```

**Examples:**
- GCS: `gs://site-bucket/2024-01-15/person.csv`

## Deployment Configuration

### Cloud Run Resource Allocation

Configure CPU and memory in the cloudbuild.yml file.

```yaml
--cpu=4           # Number of CPU cores
--memory=16Gi     # RAM allocation
```

### Cloud Build Substitution Variables

Set these in the Cloud Build trigger configuration:

| Variable | Description | Example |
|----------|-------------|---------|
| `_IMAGE_NAME` | Container image name | `ccc-omop-file-processor` |
| `_SERVICE_ACCOUNT` | Service account email | `sa@project.iam.gserviceaccount.com` |
| `_TMP_DIRECTORY` | GCS bucket for temporary files | `processing-temp` |
| `_BQ_LOGGING_TABLE` | BigQuery logging table | `project.dataset.pipeline_log` |
| `_OMOP_VOCAB_PATH` | GCS path to vocabulary files | `vocab-bucket` |

### Application Constants (`core/constants.py`)

Configure DuckDB resource limits to match Cloud Run allocation:

```python
DUCKDB_MEMORY_LIMIT = "12GB"  # Cloud Run memory minus 2-4GB overhead
DUCKDB_THREADS = "2"           # Should not exceed CPU count
```

**Tuning guidelines:**
- Memory limit: Cloud Run allocation minus 2-4GB for system overhead
- Thread count: Lower values reduce memory pressure and prevent OOM errors
- Monitor Cloud Run logs to optimize settings

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Memory errors | Increase Cloud Run memory and `DUCKDB_MEMORY_LIMIT` |
| Timeout errors | Check source data quality; increase resources if needed |
| Permission errors | Verify service account IAM roles for GCS and BigQuery |
| Vocabulary errors | Ensure vocabulary files are in correct GCS path structure |

---

# API Documentation

## Response Codes

All POST endpoints return standard HTTP status codes:

| Code | Description |
|------|-------------|
| 200 | Success |
| 400 | Missing or invalid parameters |
| 500 | Internal server error |

## Environment Configuration

These values are configured via environment variables:

| Variable | Description |
|----------|-------------|
| `OMOP_VOCAB_PATH` | GCS bucket path for vocabulary files |
| `BQ_LOGGING_TABLE` | Fully qualified BigQuery table ID for logging |
| `STORAGE_BACKEND` | Storage backend type (`gcs` or `local`) |
| `DATA_ROOT` | Base directory for local storage (local backend only) |

## API Endpoints

### Heartbeat

**Endpoint:** `GET /heartbeat`

**Description:** Health check endpoint.

**Response:**
```json
{
  "status": "healthy",
  "timestamp": "2024-01-15T12:00:00.000Z",
  "service": "omop-file-processor"
}
```

---

### Create Artifact Directories

**Endpoint:** `POST /create_artifact_directories`

**Description:** Creates directory structure for processing artifacts in storage.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| delivery_bucket | string | Yes | Storage path to data delivery (e.g., `site/2024-01-15`) |

**Example:**
```json
{
  "delivery_bucket": "site/2024-01-15"
}
```

---

### Create Optimized Vocab

**Endpoint:** `POST /create_optimized_vocab`

**Description:** Converts vocabulary CSV files to Parquet and creates optimized lookup table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| vocab_version | string | Yes | Vocabulary version (e.g., `v5.0 29-FEB-24`) |

**Note:** Vocabulary files must exist at `{OMOP_VOCAB_PATH}/{vocab_version}/` before calling this endpoint.

**Example:**
```json
{
  "vocab_version": "v5.0 29-FEB-24"
}
```

---

### Get Log Row

**Endpoint:** `GET /get_log_row`

**Description:** Retrieves pipeline log entries from BigQuery.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site | string | Yes | Site identifier |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |

**Response:**
```json
{
  "status": "healthy",
  "log_row": [
    {
      "site_name": "hospital-a",
      "delivery_date": "2024-01-15",
      "status": "completed",
      "message": null,
      "pipeline_start_datetime": "2024-01-15T10:00:00",
      "pipeline_end_datetime": "2024-01-15T12:30:00",
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

**Description:** Lists OMOP data files in a storage location.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| bucket | string | Yes | Bucket or directory name |
| folder | string | Yes | Folder path within bucket |
| file_format | string | Yes | File extension (`.csv` or `.parquet`) |

**Response:**
```json
{
  "status": "healthy",
  "file_list": ["person.csv", "condition_occurrence.csv", "drug_exposure.csv"],
  "service": "omop-file-processor"
}
```

---

### Process Incoming File

**Endpoint:** `POST /process_incoming_file`

**Description:** Processes incoming files. Converts CSV to Parquet with automatic error correction for malformed CSV data. Copies Parquet files and standardizes all fields to string type.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_type | string | Yes | File extension (`.csv` or `.parquet`) |
| file_path | string | Yes | Full path to file |

**Example:**
```json
{
  "file_type": ".csv",
  "file_path": "site/2024-01-15/person.csv"
}
```

---

### Validate File

**Endpoint:** `POST /validate_file`

**Description:** Validates file name and schema against OMOP CDM specification. Generates validation report artifacts.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to file |
| omop_version | string | Yes | OMOP CDM version (e.g., `5.4`) |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| gcs_path | string | Yes | Storage path prefix |

**Example:**
```json
{
  "file_path": "site/2024-01-15/person.csv",
  "omop_version": "5.4",
  "delivery_date": "2024-01-15",
  "gcs_path": "site/2024-01-15/"
}
```

---

### Normalize Parquet

**Endpoint:** `POST /normalize_parquet`

**Description:** Normalizes Parquet file to OMOP CDM standards:
- Converts column data types to CDM specification
- Adds missing columns with appropriate defaults
- Removes unexpected columns
- Standardizes column order
- Generates deterministic composite keys for surrogate key tables
- Writes invalid rows to separate file (`artifacts/invalid_rows/`)

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to file |
| omop_version | string | Yes | OMOP CDM version |

**Example:**
```json
{
  "file_path": "site/2024-01-15/person.csv",
  "omop_version": "5.4"
}
```

---

### Upgrade CDM

**Endpoint:** `POST /upgrade_cdm`

**Description:** Upgrades file from OMOP CDM 5.3 to 5.4. Applies schema changes and removes deprecated tables.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to file |
| omop_version | string | Yes | Current CDM version (e.g., `5.3`) |
| target_omop_version | string | Yes | Target CDM version (e.g., `5.4`) |

**Example:**
```json
{
  "file_path": "site/2024-01-15/person.csv",
  "omop_version": "5.3",
  "target_omop_version": "5.4"
}
```

---

## Vocabulary Harmonization

Vocabulary harmonization is an 8-step process that updates concept mappings to a target vocabulary version and migrates records between domain tables when concept domains change.

### Execution Model

**Steps 1-5:** Execute per file for vocabulary-harmonized tables only.

**Vocabulary-harmonized tables:**
- condition_occurrence
- drug_exposure
- procedure_occurrence
- measurement
- observation
- device_exposure
- specimen
- note

**Step 6:** Execute once per site after all files complete steps 1-5.

**Step 7:** Execute once per site. Returns table configurations for parallel processing.

**Step 8:** Execute per table in parallel using configurations from step 7.

### Harmonization Steps

Execute in order using the `/harmonize_vocab` endpoint with the `step` parameter:

| Step | Value | Description | Execution |
|------|-------|-------------|-----------|
| 1 | `source_target` | Update source concept mappings to target vocabulary | Per file |
| 2 | `target_remap` | Remap non-standard target concepts using "Maps to" relationships | Per file |
| 3 | `target_replacement` | Replace deprecated concepts using "Concept replaced by" relationships | Per file |
| 4 | `domain_check` | Update concept domains and identify domain table changes | Per file |
| 5 | `omop_etl` | Perform domain-based ETL when concepts migrate between tables | Per file |
| 6 | `consolidate_etl` | Merge all ETL outputs per table | Once per site |
| 7 | `discover_tables_for_dedup` | Identify tables requiring deduplication | Once per site |
| 8 | `deduplicate_single_table` | Deduplicate primary keys for a single table | Per table (parallel) |

---

### Harmonize Vocab

**Endpoint:** `POST /harmonize_vocab`

**Description:** Executes a single vocabulary harmonization step.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to file (or JSON table config for step 8) |
| vocab_version | string | Yes | Target vocabulary version |
| omop_version | string | Yes | OMOP CDM version |
| site | string | Yes | Site identifier |
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| step | string | Yes | Step value from table above |

**Example (Step 1):**
```json
{
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "vocab_version": "v5.0 29-FEB-24",
  "omop_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "source_target"
}
```

**Response (Steps 1-6):**
```json
{
  "status": "success",
  "message": "Successfully completed source_target for site/2024-01-15/condition_occurrence.csv",
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "step": "source_target"
}
```

**Example (Step 7 - Discovery):**
```json
{
  "file_path": "site/2024-01-15/dummy.csv",
  "vocab_version": "v5.0 29-FEB-24",
  "omop_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "discover_tables_for_dedup"
}
```

**Response (Step 7 - Discovery):**
```json
{
  "status": "success",
  "message": "Successfully discovered tables for deduplication",
  "table_configs": [
    {
      "site": "hospital-a",
      "delivery_date": "2024-01-15",
      "table_name": "condition_occurrence",
      "bucket_name": "site",
      "etl_folder": "2024-01-15/artifacts/omop_etl/",
      "file_path": "gs://site/2024-01-15/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet",
      "cdm_version": "5.4",
      "project_id": "my-project",
      "dataset_id": "omop_cdm"
    }
  ],
  "step": "discover_tables_for_dedup"
}
```

**Example (Step 8 - Deduplicate Single Table):**

The `file_path` parameter contains a JSON-encoded table configuration from step 7.

```json
{
  "file_path": "{\"site\":\"hospital-a\",\"delivery_date\":\"2024-01-15\",\"table_name\":\"condition_occurrence\",\"bucket_name\":\"site\",\"etl_folder\":\"2024-01-15/artifacts/omop_etl/\",\"file_path\":\"gs://site/2024-01-15/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet\",\"cdm_version\":\"5.4\",\"project_id\":\"my-project\",\"dataset_id\":\"omop_cdm\"}",
  "vocab_version": "v5.0 29-FEB-24",
  "omop_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "deduplicate_single_table"
}
```

---

### Load Harmonized Parquets to BigQuery

**Endpoint:** `POST /harmonized_parquets_to_bq`

**Description:** Loads all consolidated harmonized Parquet files from `omop_etl/` directory to BigQuery. Automatically discovers and loads all tables.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| bucket | string | Yes | Bucket name |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Example:**
```json
{
  "bucket": "site",
  "delivery_date": "2024-01-15",
  "project_id": "my-project",
  "dataset_id": "omop_cdm"
}
```

**Response:**
```
Successfully loaded 3 table(s): condition_occurrence, drug_exposure, measurement
```

---

### Generate Derived Tables From Harmonized

**Endpoint:** `POST /generate_derived_tables_from_harmonized`

**Description:** Generates derived tables from harmonized data. Reads from `omop_etl/` directory and writes to `derived_files/` directory.

**Supported derived tables:**
- drug_era
- dose_era
- condition_era
- observation_period

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site | string | Yes | Site identifier |
| bucket | string | Yes | Bucket name |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| table_name | string | Yes | Derived table name |
| vocab_version | string | Yes | Vocabulary version |

**Example:**
```json
{
  "site": "hospital-a",
  "bucket": "site",
  "delivery_date": "2024-01-15",
  "table_name": "drug_era",
  "vocab_version": "v5.0 29-FEB-24"
}
```

---

### Load Derived Tables to BQ

**Endpoint:** `POST /load_derived_tables_to_bq`

**Description:** Loads all derived table Parquet files to BigQuery. Automatically discovers and loads all tables from `derived_files/` directory.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| bucket | string | Yes | Bucket name |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Example:**
```json
{
  "bucket": "site",
  "delivery_date": "2024-01-15",
  "project_id": "my-project",
  "dataset_id": "omop_cdm"
}
```

**Response:**
```
Successfully loaded 2 derived table(s): drug_era, condition_era
```

---

### Clear BQ Dataset

**Endpoint:** `POST /clear_bq_dataset`

**Description:** Deletes all tables from a BigQuery dataset.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Example:**
```json
{
  "project_id": "my-project",
  "dataset_id": "omop_cdm"
}
```

---

### Load Target Vocab

**Endpoint:** `POST /load_target_vocab`

**Description:** Loads vocabulary Parquet file to BigQuery table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| table_file_name | string | Yes | Vocabulary table name (e.g., `concept`) |
| vocab_version | string | Yes | Vocabulary version |
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |

**Example:**
```json
{
  "table_file_name": "concept",
  "vocab_version": "v5.0 29-FEB-24",
  "project_id": "my-project",
  "dataset_id": "omop_cdm"
}
```

---

### Parquet to BQ

**Endpoint:** `POST /parquet_to_bq`

**Description:** Loads Parquet file to BigQuery table. Write behavior depends on `write_type` parameter.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| file_path | string | Yes | Path to file or Parquet artifact |
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| table_name | string | Yes | BigQuery table name |
| write_type | string | Yes | Write mode: `processed_file` or `specific_file` |

**Write Types:**
- `processed_file`: Loads pipeline-processed file from `artifacts/converted_files/`
- `specific_file`: Loads file from exact path specified in `file_path`

**Example:**
```json
{
  "file_path": "site/2024-01-15/person.csv",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "table_name": "person",
  "write_type": "processed_file"
}
```

---

### Create Missing Tables

**Endpoint:** `POST /create_missing_tables`

**Description:** Creates any missing OMOP tables in BigQuery dataset using CDM DDL.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| project_id | string | Yes | GCP project ID |
| dataset_id | string | Yes | BigQuery dataset ID |
| omop_version | string | Yes | OMOP CDM version |

**Example:**
```json
{
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "omop_version": "5.4"
}
```

---

### Populate CDM Source File

**Endpoint:** `POST /populate_cdm_source_file`

**Description:** Creates or updates cdm_source Parquet file with data source metadata.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| source_release_date | string | Yes | Source data release date |
| cdm_source_abbreviation | string | Yes | Source abbreviation |
| cdm_version | string | No | CDM version |
| vocabulary_version | string | No | Vocabulary version |
| cdm_release_date | string | No | CDM release date |

**Example:**
```json
{
  "source_release_date": "2024-01-15",
  "cdm_source_abbreviation": "HOSP_A",
  "cdm_version": "5.4",
  "vocabulary_version": "v5.0 29-FEB-24",
  "cdm_release_date": "2024-01-20"
}
```

---

### Generate Delivery Report

**Endpoint:** `POST /generate_delivery_report`

**Description:** Generates comprehensive data delivery report CSV file.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| site | string | Yes | Site identifier |
| site_display_name | string | Yes | Human-readable site name |
| bucket | string | Yes | Bucket name |
| file_delivery_format | string | Yes | File format (`.csv` or `.parquet`) |
| delivered_cdm_version | string | Yes | Delivered CDM version |
| target_cdm_version | string | Yes | Target CDM version |
| target_vocabulary_version | string | Yes | Target vocabulary version |

**Example:**
```json
{
  "delivery_date": "2024-01-15",
  "site": "hospital-a",
  "site_display_name": "Hospital A",
  "bucket": "site",
  "file_delivery_format": ".csv",
  "delivered_cdm_version": "5.3",
  "target_cdm_version": "5.4",
  "target_vocabulary_version": "v5.0 29-FEB-24"
}
```

---

### Pipeline Log

**Endpoint:** `POST /pipeline_log`

**Description:** Writes pipeline execution state to BigQuery logging table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| site_name | string | Yes | Site identifier |
| delivery_date | string | Yes | Delivery date (YYYY-MM-DD) |
| status | string | Yes | Pipeline status |
| run_id | string | Yes | Unique run identifier |
| message | string | No | Status message |
| file_type | string | No | File type |
| omop_version | string | No | OMOP CDM version |

**Example:**
```json
{
  "site_name": "hospital-a",
  "delivery_date": "2024-01-15",
  "status": "COMPLETED",
  "run_id": "run-123456",
  "message": "Successfully processed person table",
  "file_type": "person",
  "omop_version": "5.4"
}
```
