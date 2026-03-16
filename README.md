# OMOP File Processor

## Overview

The OMOP File Processor is a REST API service for processing healthcare data conforming to the OMOP Common Data Model (CDM). The service validates, normalizes, upgrades, and transforms OMOP data files, harmonizes vocabularies, filters Connect participants, generates derived tables and reporting artifacts, and loads processed data to BigQuery. The file processor is one of three components of the OMOP pipeline.

The service is deployed as a Docker container on Google Cloud Run and integrates with Airflow DAGs for orchestration. The repository also includes Cloud Run job entry points for long-running processing stages that the orchestrator runs directly. Additional pipeline documentation is available in the [`ehr-pipeline-documentation`](https://github.com/Analyticsphere/ehr-pipeline-documentation) repository, including the [OMOP pipeline user guide](https://github.com/Analyticsphere/ehr-pipeline-documentation/wiki/OMOP-Pipeline-User-Guide).

### Current Support

- Input file types: `.csv`, `.csv.gz`, `.parquet`
- OMOP CDM versions: `5.3`, `5.4`
- CDM upgrade path: `5.3 -> 5.4` only
- Storage backends: `gcs` (default), `local`

## Prerequisites

- Google Cloud Platform project with permissions for:
  - Cloud Storage
  - BigQuery
  - Cloud Run
- Service account with the required IAM roles
- OMOP vocabulary files from [Athena](https://athena.ohdsi.org/search-terms/start)

The local storage backend covers file-based processing paths. BigQuery operations and the GCS-specific discovery and load helpers still require Google Cloud services.

## Storage Configuration

Input paths are expected to follow this pattern:

```text
{bucket-or-directory}/{YYYY-MM-DD}/{filename}
```

Examples:

- `site-a/2025-01-15/person.csv`
- `site-a/2025-01-15/condition_occurrence.csv.gz`
- `site-a/2025-01-15/visit_occurrence.parquet`

When `STORAGE_BACKEND=local`, relative paths are resolved under `DATA_ROOT` and converted to `file://...` URIs internally. When `STORAGE_BACKEND=gcs` (the default), paths are converted to `gs://...` URIs.

### Artifact Layout

Artifacts are created under `{bucket}/{delivery_date}/artifacts/`.

| Path | Purpose |
|------|---------|
| `artifacts/converted_files/` | Standardized Parquet files created from the delivery inputs |
| `artifacts/harmonized_files/` | Intermediate vocabulary harmonization outputs, grouped by source table |
| `artifacts/omop_etl/` | OMOP-to-OMOP ETL outputs, grouped by target table |
| `artifacts/derived_files/` | Derived OMOP tables generated after harmonization |
| `artifacts/delivery_report/tmp/` | Temporary report artifact Parquet files |
| `artifacts/delivery_report/` | Final consolidated delivery report CSV |
| `artifacts/invalid_rows/` | Invalid rows removed during normalization |
| `artifacts/connect_data/` | Exported Connect participant status Parquet file |
| `artifacts/dqd/` | Reserved artifact directory |
| `artifacts/achilles/` | Reserved artifact directory |
| `artifacts/pass/` | Reserved artifact directory |

Important behavior:

- `POST /create_artifact_directories` creates all of these directories.
- Directory creation clears existing files in those directories before reuse.
- Several endpoints accept the original delivery `file_path` and internally resolve the processed artifact path in `artifacts/converted_files/`.

## Deployment Configuration

### Cloud Run Resource Allocation

`cloudbuild.yaml` currently deploys the service and Cloud Run jobs with:

- CPU: `4`
- Memory: `16Gi`
- Service concurrency: `1`
- Service timeout: `3600s`
- Harmonization job timeout: `7200s`
- Other job timeouts: `3600s`
- DuckDB temp volume mounted at `/mnt/data`

### Cloud Build Variables

The Cloud Build configuration uses these trigger-time values:

| Variable | Description |
|----------|-------------|
| `_IMAGE_NAME` | Container image name |
| `_BQ_LOGGING_TABLE` | BigQuery logging table |
| `_OMOP_VOCAB_PATH` | Root path to delivered vocabulary files |
| `_TMP_DIRECTORY` | Cloud Storage bucket mounted for DuckDB temp files |
| `SERVICE_ACCOUNT_EMAIL` | Service account used by the service and jobs |

The build also uses standard Cloud Build values such as `PROJECT_ID` and `COMMIT_SHA`.

DuckDB supports workloads that exceed available memory by spilling data to disk. Because this service runs as a serverless endpoint, it does not have access to persistent local storage for DuckDB to write these temporary files. To address this, the GCS bucket specified by the `_TMP_DIRECTORY` environment variable is mounted as a local filesystem at `/mnt/data/`. DuckDB uses this location to store temporary spill files during pipeline execution.

## Environment Variables

| Variable | Required | Purpose |
|----------|----------|---------|
| `OMOP_VOCAB_PATH` | Yes for vocabulary work | Root path for delivered vocabulary files |
| `BQ_LOGGING_TABLE` | Yes for pipeline logging | Fully qualified BigQuery table used by `/pipeline_log` and `/get_log_row` |
| `STORAGE_BACKEND` | No | `gcs` or `local`. Defaults to `gcs` |
| `DATA_ROOT` | Local backend only | Root directory used when `STORAGE_BACKEND=local` |
| `DUCKDB_TEMP_DIR` | No | DuckDB temp directory. Defaults to `/mnt/data/` |
| `COMMIT_SHA` | No | Written into delivery report metadata when present |
| `PORT` | No | Flask/gunicorn port. Defaults to `8080` |

The service also relies on standard Google Cloud credentials for GCS and BigQuery access.

## Typical Pipeline Order

A typical run in the current orchestrator DAG follows this order:

1. `GET /heartbeat`
2. `POST /create_optimized_vocab`
3. `GET /get_log_row`
4. `POST /pipeline_log` with `status=started`
5. `POST /create_artifact_directories`
6. `GET /get_file_list`
7. `POST /process_incoming_file` through `core.jobs.process_incoming_file_job`
8. `POST /validate_file`
9. `POST /normalize_parquet` through `core.jobs.normalize_parquet_job`
10. `POST /upgrade_cdm` through `core.jobs.upgrade_cdm_job` when the delivery CDM version is below the target
11. `POST /get_connect_data`
12. `POST /filter_connect_participants`
13. `POST /populate_cdm_source_file`
14. `POST /harmonize_vocab` with `step=source_target`
15. `POST /harmonize_vocab` with `step=target_remap`
16. `POST /harmonize_vocab` with `step=target_replacement`
17. `POST /harmonize_vocab` with `step=domain_check`
18. `POST /harmonize_vocab` with `step=omop_etl`
19. `POST /harmonize_vocab` with `step=consolidate_etl`
20. `POST /harmonize_vocab` with `step=discover_tables_for_dedup`
21. `POST /harmonize_vocab` with `step=deduplicate_single_table`
22. `POST /generate_derived_tables_from_harmonized` through `core.jobs.generate_derived_tables_job`
23. `POST /clear_bq_dataset`
24. `POST /harmonized_parquets_to_bq`
25. `POST /load_target_vocab` when the site configuration requests standard target vocabulary tables
26. `POST /parquet_to_bq` for remaining non-harmonized delivered tables
27. `POST /load_derived_tables_to_bq`
28. `POST /pipeline_log` with `status=running`
29. `POST /create_missing_tables`
30. `POST /parquet_to_bq` for `cdm_source`
31. `POST /generate_delivery_report_csv`
32. `POST /pipeline_log` with `status=completed`

If any stage fails, the DAG also uses `POST /pipeline_log` with `status=error`.

## HTTP API

All POST endpoints accept JSON request bodies. Missing route-level required fields return `400`. Unhandled errors return `500`.

### Response Codes

| Code | Meaning |
|------|---------|
| `200` | Request completed successfully |
| `400` | Required parameters were missing or invalid |
| `500` | The service raised an internal error |

The endpoint details below are listed in the order each endpoint first appears in the current `ehr_pipeline.py` DAG. The headings are intentionally unnumbered so this section is easier to maintain when endpoints are added or reordered.

### Heartbeat

**Endpoint:** `GET /heartbeat`

**DAG usage:** First API call in the DAG. Used by the health-check task before any site processing begins.

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

### Create Optimized Vocab

**Endpoint:** `POST /create_optimized_vocab`

**DAG usage:** Called once near the start of the DAG before site discovery.

**Description:** Converts delivered Athena vocabulary CSV files to Parquet and builds the optimized vocabulary lookup file used during harmonization.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `vocab_version` | string | Yes | Vocabulary version folder name under `{OMOP_VOCAB_PATH}` |

**Notes:**

- Vocabulary files must already exist at `{OMOP_VOCAB_PATH}/{vocab_version}/`.
- The service converts all vocabulary CSV files it finds in that version folder.
- The optimized lookup output is written to `optimized/optimized_vocab_file.parquet`.

**Example:**

```json
{
  "vocab_version": "v5.0 29-FEB-24"
}
```

---

### Get Log Row

**Endpoint:** `GET /get_log_row`

**DAG usage:** Called during site discovery to determine whether a delivery has already been processed or ended in error.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site` | string | Yes | Site identifier |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |

**Description:** Retrieves matching pipeline log rows from the BigQuery logging table.

**Notes:**

- If the BigQuery logging table does not exist yet, the endpoint returns an empty `log_row` list.

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
      "file_format": ".csv",
      "cdm_version": "5.4",
      "run_id": "run-123456"
    }
  ],
  "service": "omop-file-processor"
}
```

---

### Pipeline Log

**Endpoint:** `POST /pipeline_log`

**DAG usage:** Called multiple times during the run. The DAG uses it with `status=started`, `status=running`, `status=completed`, and `status=error`.

**Description:** Writes pipeline execution state to the BigQuery logging table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site_name` | string | Yes | Site identifier |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `status` | string | Yes | Pipeline status |
| `run_id` | string | Yes | Pipeline run identifier |
| `message` | string | No | Error or status message |
| `file_type` | string | No | Delivery file format or file identifier, depending on caller |
| `omop_version` | string | No | OMOP CDM version |

**Notes:**

- Supported status values in the current implementation are `started`, `running`, `error`, and `completed`.
- The endpoint creates the BigQuery table if it does not already exist.
- In the BigQuery logging table and `GET /get_log_row` output, these values are stored under the column names `file_format` and `cdm_version`.

**Example:**

```json
{
  "site_name": "hospital-a",
  "delivery_date": "2024-01-15",
  "status": "started",
  "run_id": "run-123456",
  "file_type": ".csv",
  "omop_version": "5.3"
}
```

---

### Create Artifact Directories

**Endpoint:** `POST /create_artifact_directories`

**DAG usage:** Called before `GET /get_file_list`. The orchestrator helper triggers this automatically as part of file discovery.

**Description:** Creates the artifact directory structure for a site delivery.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `delivery_bucket` | string | Yes | Delivery root path, for example `site/2024-01-15` |

**Notes:**

- This call creates all directories listed in the artifact layout section above.
- Existing files under those directories are removed before reuse.

**Example:**

```json
{
  "delivery_bucket": "site/2024-01-15"
}
```

---

### Get File List

**Endpoint:** `GET /get_file_list`

**DAG usage:** Called during file discovery after artifact directory creation.

**Description:** Lists delivery files matching the requested suffix within a single folder.

**Query Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Bucket or root directory name |
| `folder` | string | Yes | Folder path within the bucket, usually the delivery date |
| `file_format` | string | Yes | Filename suffix filter such as `.csv`, `.csv.gz`, or `.parquet` |

**Notes:**

- Listing is non-recursive.
- The endpoint returns only filenames, not full paths.

**Response:**

```json
{
  "status": "healthy",
  "file_list": [
    "person.csv",
    "condition_occurrence.csv",
    "drug_exposure.csv"
  ],
  "service": "omop-file-processor"
}
```

---

### Process Incoming File

**Endpoint:** `POST /process_incoming_file`

**DAG usage:** Implemented in the DAG through `core.jobs.process_incoming_file_job`.

**Description:** Converts incoming delivery files into the standardized Parquet artifact format used by the rest of the pipeline.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_type` | string | Yes | Input file type: `.csv`, `.csv.gz`, or `.parquet` |
| `file_path` | string | Yes | Original delivery file path |

**Behavior:**

- `.csv` and `.csv.gz` files are converted to Parquet.
- CSV encoding is auto-detected.
- CSV conversion retries once with more permissive DuckDB settings if the first read fails.
- `.parquet` files are rewritten into the converted-files artifact location with cleaned lowercase column names and `VARCHAR` column types.

**Example:**

```json
{
  "file_type": ".csv.gz",
  "file_path": "site/2024-01-15/person.csv.gz"
}
```

---

### Validate File

**Endpoint:** `POST /validate_file`

**DAG usage:** Called once per file after conversion.

**Description:** Validates the table name and column names of the processed Parquet artifact against the OMOP schema reference for the requested CDM version.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path |
| `omop_version` | string | Yes | OMOP CDM version, for example `5.4` |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `storage_path` | string | Yes | Storage path prefix supplied by the caller |

**Notes:**

- The validator reads the processed Parquet artifact derived from `file_path`, not the original source file.
- `storage_path` is part of the route contract but is not used by the current validator implementation.
- Validation creates report artifacts for valid table names, invalid table names, valid columns, invalid columns, and missing columns.

**Example:**

```json
{
  "file_path": "site/2024-01-15/person.csv",
  "omop_version": "5.4",
  "delivery_date": "2024-01-15",
  "storage_path": "site"
}
```

---

### Normalize Parquet

**Endpoint:** `POST /normalize_parquet`

**DAG usage:** Implemented in the DAG through `core.jobs.normalize_parquet_job`.

**Description:** Normalizes the processed Parquet artifact to the OMOP schema for the requested CDM version.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path |
| `omop_version` | string | Yes | OMOP CDM version |
| `date_format` | string | Yes | Site-specific date format used for `DATE` parsing |
| `datetime_format` | string | Yes | Site-specific datetime format used for `TIMESTAMP` and `DATETIME` parsing |

**Behavior:**

- Casts values to OMOP field types
- Adds missing columns
- Writes invalid rows to `artifacts/invalid_rows/`
- Rewrites valid rows to the converted-files artifact
- Generates deterministic surrogate keys for surrogate-key tables
- Uses `connect_id` values for `person_id` when present

**Example:**

```json
{
  "file_path": "site/2024-01-15/person.csv",
  "omop_version": "5.4",
  "date_format": "%Y-%m-%d",
  "datetime_format": "%Y-%m-%d %H:%M:%S"
}
```

---

### Upgrade CDM

**Endpoint:** `POST /upgrade_cdm`

**DAG usage:** Implemented in the DAG through `core.jobs.upgrade_cdm_job`. The DAG skips this stage when the delivered CDM version already matches the target version.

**Description:** Upgrades the processed Parquet artifact from one OMOP CDM version to another.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path |
| `omop_version` | string | Yes | Delivered OMOP CDM version |
| `target_omop_version` | string | Yes | Target OMOP CDM version |

**Notes:**

- Only `5.3 -> 5.4` is supported by the current implementation.
- Some tables are removed during the upgrade path. In those cases, the processed artifact is deleted.

**Example:**

```json
{
  "file_path": "site/2024-01-15/measurement.csv",
  "omop_version": "5.3",
  "target_omop_version": "5.4"
}
```

---

### Get Connect Data

**Endpoint:** `POST /get_connect_data`

**DAG usage:** Called once per site delivery after file-level CDM upgrade and before file-level Connect filtering.

**Description:** Exports Connect participant-status data from BigQuery into a Parquet artifact and creates Connect eligibility report artifacts.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | string | Yes | BigQuery project ID |
| `dataset_id` | string | Yes | BigQuery dataset containing the Connect participant table |
| `delivery_bucket` | string | Yes | Delivery root path, for example `site/2024-01-15` |
| `site_connect_id` | string or integer | Yes | Site-specific Connect identifier |

**Notes:**

- The exported Parquet file is written to `artifacts/connect_data/participant_status.parquet`.
- A processed `person` Parquet artifact must already exist because the report-artifact step compares Connect identifiers to delivered `person_id` values.

**Example:**

```json
{
  "project_id": "my-project",
  "dataset_id": "connect_dataset",
  "delivery_bucket": "site/2024-01-15",
  "site_connect_id": "12345"
}
```

---

### Filter Connect Participants

**Endpoint:** `POST /filter_connect_participants`

**DAG usage:** Called once per file after Connect data export.

**Description:** Rewrites the processed Parquet artifact to keep only eligible Connect participants.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path |
| `omop_version` | string | Yes | OMOP CDM version used to resolve table metadata |

**Rows are removed when:**

- `person_id` is missing, non-numeric, or `-1`
- the identifier is not present in the exported Connect data
- the participant is not verified
- consent is withdrawn
- HIPAA is revoked
- data destruction is requested

**Notes:**

- Tables without a `person_id` column are skipped and return a success response with a skip message.

**Example:**

```json
{
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "omop_version": "5.4"
}
```

---

### Populate CDM Source File

**Endpoint:** `POST /populate_cdm_source_file`

**DAG usage:** Called once per site delivery after file-level Connect filtering and before vocabulary harmonization.

**Description:** Creates or populates `cdm_source.parquet` if the file does not exist or exists but is empty.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Site bucket or root directory |
| `source_release_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `cdm_source_name` | string | Yes | Source name written to `cdm_source` |
| `cdm_source_abbreviation` | string | Yes | Source abbreviation |
| `cdm_holder` | string | Yes | Organization holding the data |
| `source_description` | string | Yes | Source description |
| `cdm_version` | string | Yes | Target OMOP CDM version |
| `cdm_release_date` | string | Yes | CDM release date |
| `source_documentation_reference` | string | No | Source documentation reference |
| `cdm_etl_reference` | string | No | ETL documentation reference |

**Notes:**

- If `cdm_source.parquet` already exists and contains rows, the endpoint does nothing.
- The service derives `vocabulary_version` from the delivered `vocabulary.parquet` file rather than taking it as a request field.

**Example:**

```json
{
  "bucket": "site",
  "source_release_date": "2024-01-15",
  "cdm_source_name": "Hospital A EHR",
  "cdm_source_abbreviation": "HOSP_A",
  "cdm_holder": "Hospital A",
  "source_description": "OMOP delivery for Hospital A",
  "cdm_version": "5.4",
  "cdm_release_date": "2024-01-20"
}
```

---

### Harmonize Vocab

**Endpoint:** `POST /harmonize_vocab`

**DAG usage:** Called in eight ordered steps. Steps 1 to 5 run per eligible file. Steps 6 and 7 run once per site. Step 8 runs once per discovered target table.

**Description:** Executes one step of the vocabulary harmonization process.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path, a dummy path for site-level steps, or a JSON-encoded table configuration for step 8 |
| `vocab_version` | string | Yes | Target vocabulary version |
| `omop_version` | string | Yes | Target OMOP CDM version |
| `site` | string | Yes | Site identifier |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |
| `step` | string | Yes | Harmonization step name |

**Step order in the DAG:**

| Order | `step` value | Execution model |
|------|--------------|-----------------|
| 1 | `source_target` | Per eligible file |
| 2 | `target_remap` | Per eligible file |
| 3 | `target_replacement` | Per eligible file |
| 4 | `domain_check` | Per eligible file |
| 5 | `omop_etl` | Per eligible file |
| 6 | `consolidate_etl` | Once per site |
| 7 | `discover_tables_for_dedup` | Once per site |
| 8 | `deduplicate_single_table` | Once per discovered table |

**Notes:**

- The orchestrator skips tables outside the harmonized-table set before calling the endpoint.
- `discover_tables_for_dedup` returns `table_configs` in the response.
- For `deduplicate_single_table`, the `file_path` field must contain the JSON-encoded configuration returned by the discovery step.

**Example: step 1**

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

**Response for steps 1 to 6 and 8:**

```json
{
  "status": "success",
  "message": "Successfully completed source_target for site/2024-01-15/condition_occurrence.csv",
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "step": "source_target"
}
```

**Example: step 7**

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

**Response for step 7:**

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

**Example: step 8**

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

### Generate Derived Tables From Harmonized

**Endpoint:** `POST /generate_derived_tables_from_harmonized`

**DAG usage:** Implemented in the DAG through `core.jobs.generate_derived_tables_job` after vocabulary harmonization finishes.

**Description:** Generates derived OMOP tables from harmonized data.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site` | string | Yes | Site identifier |
| `bucket` | string | Yes | Site bucket or root directory |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `table_name` | string | Yes | Derived table name |
| `vocab_version` | string | Yes | Vocabulary version used for harmonized lookups |

**Supported `table_name` values:**

- `condition_era`
- `drug_era`
- `observation_period`

**Notes:**

- The service reads harmonized data from `artifacts/omop_etl/`.
- Outputs are written to `artifacts/derived_files/`.
- `dose_era` appears in DDL and reporting configuration, but the current endpoint implementation does not support generating it.

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

### Clear BQ Dataset

**Endpoint:** `POST /clear_bq_dataset`

**DAG usage:** First step in the BigQuery load task group.

**Description:** Deletes all tables from the target BigQuery dataset.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |

**Example:**

```json
{
  "project_id": "my-project",
  "dataset_id": "omop_cdm"
}
```

---

### Load Harmonized Parquets to BigQuery

**Endpoint:** `POST /harmonized_parquets_to_bq`

**DAG usage:** Called after the dataset is cleared and before target vocabulary loading.

**Description:** Loads all consolidated harmonized Parquet files from `artifacts/omop_etl/` into BigQuery.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Site bucket or root directory |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |

**Notes:**

- The endpoint discovers target tables automatically from subdirectories under `artifacts/omop_etl/`.
- In the DAG, this step may be skipped when no harmonized tables exist for the delivery.

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

```text
Successfully loaded 3 table(s): condition_occurrence, drug_exposure, measurement
```

---

### Load Target Vocab

**Endpoint:** `POST /load_target_vocab`

**DAG usage:** Called after harmonized-table loading when the site configuration requests standard target vocabulary tables.

**Description:** Loads a target vocabulary Parquet file into a BigQuery table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `table_file_name` | string | Yes | Vocabulary table file stem, for example `concept` |
| `vocab_version` | string | Yes | Vocabulary version |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |

**Notes:**

- `table_file_name` should be the file stem, not `concept.parquet`.

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

**DAG usage:** Called twice in the current DAG:

- first for remaining non-harmonized delivered tables
- later again to load `cdm_source`

**Description:** Loads one Parquet file into a BigQuery table.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path or exact Parquet path, depending on `write_type` |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |
| `table_name` | string | Yes | BigQuery table name |
| `write_type` | string | Yes | `processed_file` or `specific_file` |

**Write types:**

- `processed_file`: resolves `file_path` to `artifacts/converted_files/<table>.parquet`
- `specific_file`: loads the exact `file_path` provided

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

### Load Derived Tables to BQ

**Endpoint:** `POST /load_derived_tables_to_bq`

**DAG usage:** Called after remaining delivered-table loads complete.

**Description:** Loads all derived Parquet files from `artifacts/derived_files/` into BigQuery.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Site bucket or root directory |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |

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

```text
Successfully loaded 2 derived table(s): drug_era, condition_era
```

---

### Create Missing Tables

**Endpoint:** `POST /create_missing_tables`

**DAG usage:** Called during the cleanup/setup stage after the main BigQuery load group finishes.

**Description:** Executes the OMOP DDL for the requested version against the target BigQuery dataset.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |
| `omop_version` | string | Yes | OMOP CDM version |

**Notes:**

- The route name says "create missing tables", but the DDL files include `CREATE OR REPLACE TABLE` statements for some tables.
- Existing tables are rewritten in place to cast date and datetime fields to the expected BigQuery types while preserving the rows selected from the existing table.
- This endpoint is therefore not limited to creating only missing tables.

**Example:**

```json
{
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "omop_version": "5.4"
}
```

---

### Generate Delivery Report CSV

**Endpoint:** `POST /generate_delivery_report_csv`

**DAG usage:** Implemented in the DAG through `core.jobs.generate_report_csv_job` after cleanup completes.

**Description:** Generates the consolidated delivery report CSV used by downstream reporting and dashboards.

**Parameters required for successful generation:**

| Parameter | Type | Required by route | Required by generator | Description |
|-----------|------|-------------------|-----------------------|-------------|
| `delivery_date` | string | Yes | Yes | Delivery date in `YYYY-MM-DD` format |
| `site` | string | Yes | Yes | Site identifier |
| `bucket` | string | No | Yes | Site bucket or root directory |
| `site_display_name` | string | No | Yes | Human-readable site name |
| `file_delivery_format` | string | No | Yes | Delivery file format |
| `delivered_cdm_version` | string | No | Yes | Delivered OMOP CDM version |
| `target_vocabulary_version` | string | No | Yes | Target vocabulary version |
| `target_cdm_version` | string | No | Yes | Target OMOP CDM version |

**Notes:**

- The route validates only `delivery_date` and `site`.
- The report generator itself expects the full set of fields above, so callers should always send all of them.

**Example:**

```json
{
  "delivery_date": "2024-01-15",
  "site": "hospital-a",
  "bucket": "site",
  "site_display_name": "Hospital A",
  "file_delivery_format": ".csv",
  "delivered_cdm_version": "5.3",
  "target_cdm_version": "5.4",
  "target_vocabulary_version": "v5.0 29-FEB-24"
}
```

## Cloud Run Jobs

The repository also exposes direct job entry points under `core/jobs/`.

| Job module | Required environment variables | Equivalent API stage |
|-----------|--------------------------------|----------------------|
| `core.jobs.process_incoming_file_job` | `FILE_TYPE`, `FILE_PATH` | `POST /process_incoming_file` |
| `core.jobs.normalize_parquet_job` | `FILE_PATH`, `OMOP_VERSION`, `DATE_FORMAT`, `DATETIME_FORMAT` | `POST /normalize_parquet` |
| `core.jobs.upgrade_cdm_job` | `FILE_PATH`, `OMOP_VERSION`, `TARGET_OMOP_VERSION` | `POST /upgrade_cdm` |
| `core.jobs.harmonize_vocab_job` | `FILE_PATH`, `VOCAB_VERSION`, `OMOP_VERSION`, `SITE`, `PROJECT_ID`, `DATASET_ID`, `STEP` | `POST /harmonize_vocab` |
| `core.jobs.generate_derived_tables_job` | `SITE`, `GCS_BUCKET`, `DELIVERY_DATE`, `TABLE_NAME`, `VOCAB_VERSION` | `POST /generate_derived_tables_from_harmonized` |
| `core.jobs.generate_report_csv_job` | `SITE`, `GCS_BUCKET`, `DELIVERY_DATE`, `SITE_DISPLAY_NAME`, `FILE_DELIVERY_FORMAT`, `DELIVERED_CDM_VERSION`, `TARGET_VOCABULARY_VERSION`, `TARGET_CDM_VERSION` | `POST /generate_delivery_report_csv` |

## Running Tests

Run the unit test suite with:

```bash
pytest
```
