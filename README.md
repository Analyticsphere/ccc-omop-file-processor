# OMOP File Processor

## Overview

The OMOP File Processor is a REST API service for processing healthcare data conforming to the OMOP Common Data Model (CDM). The service validates, normalizes, upgrades, and transforms OMOP data files, harmonizes vocabularies, filters Connect participants, generates derived tables and reporting artifacts, and loads processed data to BigQuery. The file processor is one of three components of the OMOP pipeline.

The service also merges per-site deliveries into a single OMOP instance, combining each site's most recent `completed` delivery into one merged dataset under a separate merge DAG.

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
| `artifacts/post_processing/` | Temporary per-task snapshots used by post-processing diff reporting |
| `artifacts/merge_chunks/` | Provenance-named per-table chunks staged during a merge run (`<table>__<site>__<delivery_date>.parquet`, grouped by table) |
| `artifacts/dqd/` | Reserved artifact directory |
| `artifacts/achilles/` | Reserved artifact directory |
| `artifacts/pass/` | Reserved artifact directory |

Important behavior:

- `POST /create_artifact_directories` creates all of these directories.
- Directory creation clears existing files in those directories before reuse.
- Several endpoints accept the original delivery `file_path` and internally resolve the processed artifact path in `artifacts/converted_files/`.

## Local Development

The service can run end-to-end against the local filesystem instead of GCS/BigQuery. This is the recommended setup for developing or debugging file-processing logic without touching cloud resources.

### Layout under `local-data/`

The `local-data/` directory at the repo root is mounted into the container at `/data`. Deliveries and vocabularies go here:

```text
local-data/
├── <site>/<YYYY-MM-DD>/...     # delivery files (csv, csv.gz, parquet)
├── vocabulary/<vocab_version>/ # Athena vocabulary CSVs
└── temp/                       # DuckDB spill directory (must exist before first run)
```

### Build and run

```bash
docker build -t omop-processor:local .
mkdir -p local-data/temp

docker run -d \
  --name omop-processor-local \
  -p 8080:8080 \
  -v "$(pwd)/local-data:/data" \
  -e STORAGE_BACKEND=local \
  -e DATA_ROOT=/data \
  -e OMOP_VOCAB_PATH=/data/vocabulary \
  -e BQ_LOGGING_TABLE=local.test.pipeline_log \
  -e DUCKDB_TEMP_DIR=/data/temp/ \
  -e PORT=8080 \
  omop-processor:local
```

`local-data/restart-local.sh` wraps the stop/remove/run cycle for iteration.

### Examples of calling local endpoints
Check service heartbeat.
```bash
curl http://localhost:8080/heartbeat
```

Create the directory structure for processing artifacts.
```bash
curl -X POST http://localhost:8080/create_artifact_directories \
  -H "Content-Type: application/json" \
  -d '{"delivery_bucket": "synthea53/2025-01-01"}'
```

Convert CSV to Parquet.
```bash
curl -X POST http://localhost:8080/process_incoming_file \
  -H "Content-Type: application/json" \
  -d '{"file_type": ".csv", "file_path": "synthea53/2025-01-01/person.csv"}'
```

Source to Target vocabulary harmonization.
```bash
curl -X POST http://localhost:8080/harmonize_vocab \
  -H "Content-Type: application/json" \
  -d '{
    "file_path": "synthea53/2025-01-01/procedure_occurrence.csv",
    "vocab_version": "v5.0 29-FEB-24",
    "cdm_version": "5.4",
    "site": "synthea53",
    "project_id": "local-project",
    "dataset_id": "omop_cdm",
    "step": "source_target"
  }'
```

### File paths in API requests

Paths in request bodies are relative to `DATA_ROOT`. Do not include the `/data/` prefix or any storage scheme.

- Host file: `./local-data/synthea53/2025-01-01/person.csv`
- API field: `"file_path": "synthea53/2025-01-01/person.csv"`

The storage backend converts these to `file:///data/...` URIs internally.

### Endpoints that still need cloud access

BigQuery-backed endpoints (`/pipeline_log`, `/get_log_row`, `/clear_bq_dataset`, `/parquet_to_bq`, `/harmonized_parquets_to_bq`, `/load_derived_tables_to_bq`, `/load_target_vocab`, `/create_missing_tables`, `/get_connect_data`) still require Google Cloud credentials. The local backend covers file-based stages only.

### Rebuilding after code changes

```bash
docker stop omop-processor-local && docker rm omop-processor-local
docker build -t omop-processor:local .
./local-data/restart-local.sh
```

## Deployment Configuration

### Cloud Run Resource Allocation

`cloudbuild.yaml` currently deploys the service and Cloud Run jobs with:

- CPU: `4`
- Memory: `16Gi`
- Service concurrency: `1`
- Service timeout: `3600s`
- Harmonization job timeout: `7200s`
- Merge job timeouts: `3600s`
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
11. `POST /populate_cdm_source_file`
12. `POST /unique_natural_keys` through `core.jobs.unique_natural_keys_job`
13. `POST /get_connect_data`
14. `POST /filter_connect_participants`
15. `POST /harmonize_vocab` with `step=source_target`
16. `POST /harmonize_vocab` with `step=target_remap`
17. `POST /harmonize_vocab` with `step=source_concept_backfill`
18. `POST /harmonize_vocab` with `step=domain_check`
19. `POST /harmonize_vocab` with `step=omop_etl`
20. `POST /harmonize_vocab` with `step=consolidate_etl`
21. `POST /harmonize_vocab` with `step=discover_tables_for_dedup`
22. `POST /harmonize_vocab` with `step=deduplicate_single_table`
23. `POST /post_processing` through `core.jobs.post_processing_job` for each task configured by the site
24. `POST /generate_derived_tables_from_harmonized` through `core.jobs.generate_derived_tables_job`
25. `POST /clear_bq_dataset`
26. `POST /harmonized_parquets_to_bq`
27. `POST /load_target_vocab` when the site configuration requests standard target vocabulary tables
28. `POST /parquet_to_bq` for remaining non-harmonized delivered tables
29. `POST /load_derived_tables_to_bq`
30. `POST /pipeline_log` with `status=running`
31. `POST /create_missing_tables`
32. `POST /parquet_to_bq` for `cdm_source`
33. `POST /generate_delivery_report_csv` through `core.jobs.generate_report_csv_job` (the DAG invokes the job once per artifact type in parallel, then a final consolidation pass)
34. `POST /pipeline_log` with `status=completed`

If any stage fails, the DAG also uses `POST /pipeline_log` with `status=error`.

Steps 7 through 12 run as a single per-file group (`process_delivery`), followed by per-site `populate_cdm_source_file` and per-file `unique_natural_keys`. The `filtering` group (steps 13–14) runs as a separate sequential group after `process_delivery` completes, not interleaved with it. Vocabulary harmonization steps `target_replacement` (logical step 3) and `secondary_concept_backfill` (logical step 6) remain available as endpoints but are intentionally excluded from the current DAG chain.

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
| `cdm_version` | string | No | OMOP CDM version |

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
  "cdm_version": "5.3"
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
| `cdm_version` | string | Yes | OMOP CDM version, for example `5.4` |
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
  "cdm_version": "5.4",
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
| `cdm_version` | string | Yes | OMOP CDM version |
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
  "cdm_version": "5.4",
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
| `cdm_version` | string | Yes | Delivered OMOP CDM version |
| `target_cdm_version` | string | Yes | Target OMOP CDM version |

**Notes:**

- Only `5.3 -> 5.4` is supported by the current implementation.
- Some tables are removed during the upgrade path. In those cases, the processed artifact is deleted.

**Example:**

```json
{
  "file_path": "site/2024-01-15/measurement.csv",
  "cdm_version": "5.3",
  "target_cdm_version": "5.4"
}
```

---

### Populate CDM Source File

**Endpoint:** `POST /populate_cdm_source_file`

**DAG usage:** Called once per site delivery after the per-file CDM upgrade completes and before `unique_natural_keys` runs.

**Description:** Creates or populates `cdm_source.parquet` if the file does not exist, exists but is empty, or contains more than one row.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Site bucket or root directory |
| `delivery_date` | string | Yes | Delivery date directory component in `YYYY-MM-DD` format |
| `source_release_date` | string | Yes | Source release date written to the `source_release_date` column |
| `cdm_source_name` | string | Yes | Source name written to `cdm_source` |
| `cdm_source_abbreviation` | string | Yes | Source abbreviation |
| `cdm_holder` | string | Yes | Organization holding the data |
| `source_description` | string | Yes | Source description |
| `target_cdm_version` | string | Yes | Target OMOP CDM version (written to `cdm_version`; drives `cdm_version_concept_id`) |
| `target_vocab_version` | string | Yes | Target vocabulary version (written to `vocabulary_version`) |
| `cdm_release_date` | string | Yes | CDM release date |
| `date_format` | string | Yes | strptime format used to parse `source_release_date` / `cdm_release_date` |
| `source_documentation_reference` | string | No | Source documentation reference |
| `cdm_etl_reference` | string | No | ETL documentation reference |

**Notes:**

- If `cdm_source.parquet` exists with exactly one row, the endpoint keeps every site-delivered descriptive column but overwrites the pipeline-controlled fields:
    - `source_release_date` and `cdm_release_date` keep the site's value if it parses as a valid date; otherwise they fall back to `delivery_date`.
    - `cdm_version`, `cdm_version_concept_id`, and `vocabulary_version` are overwritten with the target values the pipeline standardized to (`target_cdm_version` / `target_vocab_version`); the site's self-reported values are discarded.
- If the file does not exist, exists with zero rows, or exists with more than one row, it is populated from the request payload (all rows overwritten).
- On the populate path: `source_release_date` is wrapped in a date-cast fallback chain (falls back to `delivery_date` if unparseable). `cdm_release_date` is unconditionally set to `delivery_date`. Although the request's `cdm_release_date` field is still required, its value is ignored.
- The "Source system extraction date" report artifact is always written; if the cdm_source `source_release_date` value cannot be parsed, the artifact falls back to `delivery_date`.

**Example:**

```json
{
  "bucket": "site",
  "delivery_date": "2024-01-15",
  "source_release_date": "2024-01-15",
  "cdm_source_name": "Hospital A EHR",
  "cdm_source_abbreviation": "HOSP_A",
  "cdm_holder": "Hospital A",
  "source_description": "OMOP delivery for Hospital A",
  "target_cdm_version": "5.4",
  "target_vocab_version": "v5.0_24-JAN-25",
  "cdm_release_date": "2024-01-20",
  "date_format": "%Y-%m-%d"
}
```

---

### Unique Natural Keys

**Endpoint:** `POST /unique_natural_keys`

**DAG usage:** Implemented in the DAG through `core.jobs.unique_natural_keys_job`. Runs per file as part of the `process_delivery` task group, immediately after `populate_cdm_source_file` and before the parallel `filtering` group.

**Description:** Rewrites natural-key columns (PK and FK) in the processed Parquet artifact so values are globally unique across sites. Each in-scope column value is replaced with `hash(CONCAT(value, site)) % 9223372036854775807`, preserving NULLs.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path |
| `cdm_version` | string | Yes | OMOP CDM version |
| `site` | string | Yes | Site identifier used as the hash salt |

**Notes:**

- Vocabulary tables are excluded by policy (`NATURAL_KEY_REWRITE_SKIP_TABLES`). The endpoint returns a 200 success with a skip message in those cases.
- The `person` table IS rewritten — its `location_id`, `provider_id`, and `care_site_id` FK columns are globalized so they continue to reference the rewritten parent rows. `person_id` is protected from rewrite by its exclusion from `GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS`, which holds across every table where `person_id` appears.
- Tables with none of the in-scope columns present are also skipped with a 200 success.
- Columns rewritten when present (`GLOBALLY_UNIQUE_NATURAL_KEY_COLUMNS`):
    - `visit_occurrence_id`, `preceding_visit_occurrence_id`
    - `visit_detail_id`, `preceding_visit_detail_id`, `parent_visit_detail_id`
    - `provider_id`, `care_site_id`, `location_id`
    - `episode_id`
- The hash shape matches the surrogate-key hash used by `transformer.py`, so cross-table FK joins continue to work after rewrite.

**Example:**

```json
{
  "file_path": "site/2024-01-15/visit_occurrence.csv",
  "cdm_version": "5.4",
  "site": "hospital-a"
}
```

---

### Get Connect Data

**Endpoint:** `POST /get_connect_data`

**DAG usage:** Called once per site delivery as part of the `filtering` task group, which runs after the per-file `process_delivery` group completes and before vocabulary harmonization.

**Description:** Exports Connect participant-status data from BigQuery into a Parquet file. When `delivery_bucket` is provided, also creates Connect eligibility report artifacts.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `project_id` | string | Yes | BigQuery project ID |
| `dataset_id` | string | Yes | BigQuery dataset containing the Connect participant table |
| `delivery_bucket` | string | One of `delivery_bucket` or `parquet_destination` required | Delivery root path, for example `site/2024-01-15`. Used to infer the Parquet output location and to generate eligibility report artifacts. |
| `parquet_destination` | string | One of `delivery_bucket` or `parquet_destination` required | Explicit output path for the Parquet file (must end in `.parquet`). Takes precedence over `delivery_bucket` for determining where to write. |
| `site_connect_id` | string or integer | No | Site-specific Connect identifier. If omitted, data for all sites is returned. |

**Notes:**

- At least one of `delivery_bucket` or `parquet_destination` must be provided. If both are given, `parquet_destination` determines the output path and `delivery_bucket` is used for report artifact generation.
- When only `parquet_destination` is provided, eligibility report artifacts are not generated.
- When only `delivery_bucket` is provided, the Parquet file is written to `artifacts/connect_data/participant_status.parquet` under the delivery path.
- A processed `person` Parquet artifact must already exist when `delivery_bucket` is provided, because the report-artifact step compares Connect identifiers to delivered `person_id` values.

**Example (delivery bucket):**

```json
{
  "project_id": "my-project",
  "dataset_id": "connect_dataset",
  "delivery_bucket": "site/2024-01-15",
  "site_connect_id": "12345"
}
```

**Example (explicit destination, all sites):**

```json
{
  "project_id": "my-project",
  "dataset_id": "connect_dataset",
  "parquet_destination": "output-bucket/participant_status.parquet"
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
| `cdm_version` | string | Yes | OMOP CDM version used to resolve table metadata |

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
  "cdm_version": "5.4"
}
```

---

### Harmonize Vocab

**Endpoint:** `POST /harmonize_vocab`

**DAG usage:** Called in ten ordered steps. Steps 1 to 7 run per eligible file. Steps 8 and 9 run once per site. Step 10 runs once per discovered target table.

**Description:** Executes one step of the vocabulary harmonization process.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file_path` | string | Yes | Original delivery file path, a dummy path for site-level steps, or a JSON-encoded table configuration for step 10 |
| `vocab_version` | string | Yes | Target vocabulary version |
| `cdm_version` | string | Yes | Target OMOP CDM version |
| `site` | string | Yes | Site identifier |
| `project_id` | string | Yes | Google Cloud project ID |
| `dataset_id` | string | Yes | BigQuery dataset ID |
| `step` | string | Yes | Harmonization step name |

**Defined harmonization steps:**

The table below lists every step the endpoint accepts. Two steps (marked *not in DAG*) remain available as endpoints but are intentionally excluded from the current DAG chain — see the [Typical Pipeline Order](#typical-pipeline-order) for the active execution order.

| `step` value | Execution model | DAG status |
|--------------|-----------------|------------|
| `source_target` | Per eligible file | Active |
| `target_remap` | Per eligible file | Active |
| `target_replacement` | Per eligible file | **Not in DAG** |
| `source_concept_backfill` | Per eligible file | Active |
| `domain_check` | Per eligible file | Active |
| `secondary_concept_backfill` | Per eligible file | **Not in DAG** |
| `omop_etl` | Per eligible file | Active |
| `consolidate_etl` | Once per site | Active |
| `discover_tables_for_dedup` | Once per site | Active |
| `deduplicate_single_table` | Once per discovered table | Active |

**Notes:**

- The orchestrator skips tables outside the harmonized-table set before calling the endpoint.
- `source_concept_backfill` sets the primary `_concept_id` to `_source_concept_id` when the concept ID is zero, the source concept ID is non-zero, and the source concept exists in the vocabulary.
- `secondary_concept_backfill` applies the same backfill logic to non-primary concept ID columns (e.g., `unit_concept_id`) across all harmonized files produced by prior steps. Even though this step is not invoked by the current DAG, the endpoint still accepts it.
- `discover_tables_for_dedup` returns `table_configs` in the response.
- For `deduplicate_single_table`, the `file_path` field must contain the JSON-encoded configuration returned by the discovery step.

**Example: step 1**

```json
{
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "vocab_version": "v5.0 29-FEB-24",
  "cdm_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "source_target"
}
```

**Response for steps 1 to 8 and 10:**

```json
{
  "status": "success",
  "message": "Successfully completed source_target for site/2024-01-15/condition_occurrence.csv",
  "file_path": "site/2024-01-15/condition_occurrence.csv",
  "step": "source_target"
}
```

**Example: step 9**

```json
{
  "file_path": "site/2024-01-15/dummy.csv",
  "vocab_version": "v5.0 29-FEB-24",
  "cdm_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "discover_tables_for_dedup"
}
```

**Response for step 9:**

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

**Example: step 10**

```json
{
  "file_path": "{\"site\":\"hospital-a\",\"delivery_date\":\"2024-01-15\",\"table_name\":\"condition_occurrence\",\"bucket_name\":\"site\",\"etl_folder\":\"2024-01-15/artifacts/omop_etl/\",\"file_path\":\"gs://site/2024-01-15/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet\",\"cdm_version\":\"5.4\",\"project_id\":\"my-project\",\"dataset_id\":\"omop_cdm\"}",
  "vocab_version": "v5.0 29-FEB-24",
  "cdm_version": "5.4",
  "site": "hospital-a",
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "step": "deduplicate_single_table"
}
```

---

### Post Processing

**Endpoint:** `POST /post_processing`

**DAG usage:** Implemented in the DAG through `core.jobs.post_processing_job`. Called once per post-processing task configured for the site, after `harmonize_vocab` step `deduplicate_single_table` and before derived-table generation.

**Description:** Applies one user-curated post-processing SQL task to the on-disk OMOP artifacts. Tasks can delete rows from and insert rows into any non-vocabulary OMOP table. Per-task report artifacts capture rows added, rows removed, and tables affected. After the task runs, surrogate-key tables that were affected are passed through the same primary-key deduplication step used after vocabulary harmonization.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site` | string | Yes | Site identifier |
| `bucket` | string | Yes | Site bucket or root directory |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |
| `cdm_version` | string | Yes | OMOP CDM version, for example `5.4` |
| `vocab_version` | string | Yes | Vocabulary version (used to resolve `@CONCEPT`-style placeholders) |
| `task_name` | string | Yes | Task name. Must match a SQL file at `reference/sql/post_processing/<task_name>.sql` |

**Behavior:**

- Loads `reference/sql/post_processing/<task_name>.sql`. Returns 400 if the script does not exist.
- Snapshots the row-identity set of every non-vocabulary OMOP table on disk (PK column when present, otherwise a row content hash).
- Executes the task SQL via DuckDB after substituting placeholders (see below).
- Diffs the post-task table state against each snapshot to count added and removed rows per table.
- Emits three report artifacts per affected table: rows added, rows removed, and table affected.
- Re-runs primary-key deduplication on every affected surrogate-key table.
- Cleans up snapshot files at `artifacts/post_processing/<task_name>/tmp/`.

**Available placeholders in task SQL:**

| Placeholder | Routes to |
|-------------|-----------|
| `@CONDITION_OCCURRENCE`, `@DRUG_EXPOSURE`, `@VISIT_OCCURRENCE`, `@PROCEDURE_OCCURRENCE`, `@DEVICE_EXPOSURE`, `@MEASUREMENT`, `@OBSERVATION`, `@NOTE`, `@SPECIMEN` | `artifacts/omop_etl/<table>/<table>.parquet` |
| `@PERSON`, `@DEATH`, `@CARE_SITE`, `@LOCATION`, `@PROVIDER`, `@VISIT_DETAIL`, `@EPISODE`, `@COST`, `@PAYER_PLAN_PERIOD`, `@METADATA`, `@CDM_SOURCE`, `@FACT_RELATIONSHIP`, `@NOTE_NLP` | `artifacts/converted_files/<table>.parquet` |
| `@CONCEPT`, `@CONCEPT_ANCESTOR`, `@OPTIMIZED_VOCABULARY` | Vocabulary parquet files for the requested `vocab_version` |
| `@SITE` | Site identifier (used as a hash salt for inserted surrogate keys) |
| `@CURRENT_DATE` | Today's date in `YYYY-MM-DD` format |

Derived tables (`condition_era`, `drug_era`, `observation_period`) are not exposed as placeholders, because they are regenerated immediately after post-processing.

**Example:**

```json
{
  "site": "hospital-a",
  "bucket": "site",
  "delivery_date": "2024-01-15",
  "cdm_version": "5.4",
  "vocab_version": "v5.0 29-FEB-24",
  "task_name": "remove_text-to-concept_measurements"
}
```

**Response (success):**

```text
Post-processing task 'remove_text-to-concept_measurements' applied: 1 table(s) affected (measurement: +0/-128)
```

### Authoring a post-processing task

1. Add a SQL file at `reference/sql/post_processing/<task_name>.sql`. Use one or more `COPY (...) TO '...'` statements separated by semicolons; the same read-and-overwrite pattern used by natural-key globalization and the CDM upgrade flow.
2. Use the placeholders documented above to refer to OMOP table artifacts. Inline the parquet format string (`(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)`) in your `COPY` targets.
3. For inserts into surrogate-key tables, mint the primary key with the canonical hash formula so the new row keeps the same identity contract as the rest of the pipeline:

    ```sql
    -- Example: minting a condition_occurrence_id
    CAST(
      hash(CONCAT(
          CAST(person_id            AS VARCHAR),
          CAST(condition_concept_id AS VARCHAR),
          CAST(condition_start_date AS VARCHAR),
          /* ... every other non-PK column ... */
          '@SITE'
      )) % 9223372036854775807 AS BIGINT
    ) AS condition_occurrence_id
    ```

    After the task runs, the pipeline automatically re-runs primary-key deduplication on every affected surrogate-key table, so accidental hash collisions are corrected deterministically. Natural-key and derived tables are *not* auto-deduplicated.

4. Treat updates as delete-plus-insert. Because primary keys depend on row content, an "updated" row will have a different primary key from the row it replaces. The post-processing diff correctly reports this as one added and one removed row.

5. Foreign-key referential integrity across tables is your responsibility. The pipeline does not cascade deletes or repair orphaned references.

6. **Vocabulary files are read-only.** A task that tries to write to any OMOP vocabulary parquet (`concept`, `vocabulary`, `domain`, `concept_class`, `relationship`, `concept_relationship`, `concept_synonym`, `concept_ancestor`, `drug_strength`) or the optimized vocab lookup is rejected with a 400 before any DuckDB execution happens. The guard inspects the rendered SQL for `COPY ... TO '...<vocab>.parquet'` patterns and catches placeholder-resolved paths AND hard-coded paths. Reading vocabulary via `read_parquet('@CONCEPT')` etc. inside a SELECT is unaffected — only writes are blocked. Because all modifications to parquet artifacts in this pipeline go through a `COPY (SELECT …) TO '<file>'` overwrite, the single check covers updates, deletions, and insertions equally.

---

### Generate Derived Tables From Harmonized

**Endpoint:** `POST /generate_derived_tables_from_harmonized`

**DAG usage:** Implemented in the DAG through `core.jobs.generate_derived_tables_job`. Runs after the `post_processing` task group, so derived tables reflect the final post-processed state of the harmonized clinical tables.

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
| `cdm_version` | string | Yes | OMOP CDM version |

**Notes:**

- The route name says "create missing tables", but the DDL files include `CREATE OR REPLACE TABLE` statements for some tables.
- Existing tables are rewritten in place to cast date and datetime fields to the expected BigQuery types while preserving the rows selected from the existing table.
- This endpoint is therefore not limited to creating only missing tables.

**Example:**

```json
{
  "project_id": "my-project",
  "dataset_id": "omop_cdm",
  "cdm_version": "5.4"
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

## Merge Pipeline Endpoints

The endpoints below implement the EHR PR2 merge pipeline, which combines each site's most recent `completed` delivery into a single merged OMOP instance. They are driven by a separate merge DAG, not the single-site `ehr_pipeline.py` DAG documented above. A merge run discovers each site's latest completed delivery, extracts every source table into provenance-named chunks under `artifacts/merge_chunks/`, reconciles those chunks into merged `converted_files/` tables, builds the merged instance's `care_site` and `cdm_source`, and records merge provenance.

---

### Get Latest Completed Delivery

**Endpoint:** `POST /get_latest_completed_delivery`

**Description:** Returns the `delivery_date` of a site's most recent `completed` delivery via a server-side `MAX(delivery_date)` query against the BigQuery logging table, or `null` if the site has no completed delivery (or the logging table does not exist yet).

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `site` | string | Yes | Site identifier (`site_name` in the logging table) |

**Response:**

```json
{
  "status": "healthy",
  "delivery_date": "2024-01-15",
  "service": "omop-file-processor"
}
```

---

### Get Delivery CDM Version

**Endpoint:** `POST /get_delivery_cdm_version`

**Description:** Reads the CDM and vocabulary versions a processed delivery was standardized to from its `artifacts/converted_files/cdm_source.parquet`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `bucket` | string | Yes | Bucket or root directory of the source delivery |
| `delivery_date` | string | Yes | Delivery date in `YYYY-MM-DD` format |

**Notes:**

- This is the standardized version for a processed delivery; the delivered version recorded in site config and the BigQuery log is not the same thing.
- Both fields are `null` if the `cdm_source` file has no rows.

**Response:**

```json
{
  "status": "healthy",
  "cdm_version": "5.4",
  "vocabulary_version": "v5.0 29-FEB-24",
  "service": "omop-file-processor"
}
```

---

### Extract Participant Chunk

**Endpoint:** `POST /extract_participant_chunk`

**DAG usage:** Implemented in the merge DAG through `core.jobs.extract_participant_chunk_job`.

**Description:** Copies one source-delivery table into a provenance-named chunk file (`<table>__<site>__<delivery_date>.parquet`) under `artifacts/merge_chunks/<table>/`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `source_uri` | string | Yes | Path to the source delivery's table Parquet |
| `chunk_uri` | string | Yes | Destination chunk Parquet path in the staging area |
| `site_display_name` | string | No | Source site name. When set (`person` only), stamps `care_site_id` with a deterministic hash of the name so each merged patient records its origin site |

**Example:**

```json
{
  "source_uri": "site-a/2024-01-15/artifacts/converted_files/person.parquet",
  "chunk_uri": "merged/2024-08-03/artifacts/merge_chunks/person/person__site-a__2024-01-15.parquet",
  "site_display_name": "Hospital A"
}
```

---

### Reconcile Chunks

**Endpoint:** `POST /reconcile_chunks`

**DAG usage:** Implemented in the merge DAG through `core.jobs.reconcile_chunks_job`.

**Description:** Unions a table's chunk files into a single merged `converted_files/<table>.parquet`.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `chunk_glob` | string | Yes | Glob over the per-table staging folder, e.g. `.../merge_chunks/<table>/*.parquet` |
| `output_uri` | string | Yes | Destination for the reconciled table |

**Notes:**

- `output_uri` must live outside the chunk folder (in `converted_files/`) so the read glob never re-reads its own output.
- Chunks are unioned with `union_by_name=true`.

**Example:**

```json
{
  "chunk_glob": "merged/2024-08-03/artifacts/merge_chunks/person/*.parquet",
  "output_uri": "merged/2024-08-03/artifacts/converted_files/person.parquet"
}
```

---

### Build Care Site

**Endpoint:** `POST /build_care_site`

**Description:** Writes the merged instance's `care_site` table: one row per merged site mapping its hashed `care_site_id` to `care_site_name`, all other columns `NULL`. `care_site` rows are not carried over from the individual deliveries.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `output_uri` | string | Yes | Destination for the `care_site` Parquet |
| `site_display_names` | list | Yes | Display names of the merged sites (de-duplicated, first-seen order preserved) |
| `cdm_version` | string | Yes | OMOP CDM version used to resolve the `care_site` schema |

**Example:**

```json
{
  "output_uri": "merged/2024-08-03/artifacts/converted_files/care_site.parquet",
  "site_display_names": ["Hospital A", "Hospital B"],
  "cdm_version": "5.4"
}
```

---

### Build Merge CDM Source

**Endpoint:** `POST /build_merge_cdm_source`

**Description:** Writes a de novo one-row `cdm_source` for the merged instance using fixed Connect metadata plus the merged site count.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `output_uri` | string | Yes | Destination for the `cdm_source` Parquet |
| `source_cdm_source_uris` | list | Yes | Source deliveries' `cdm_source` Parquet paths, used to derive `source_release_date` |
| `site_count` | integer | Yes | Number of merged sites (written into the source description) |
| `cdm_version` | string | Yes | Target OMOP CDM version (drives `cdm_version_concept_id`) |
| `vocabulary_version` | string | Yes | Target vocabulary version |
| `cdm_release_date` | string | Yes | Merge run date, written to `cdm_release_date` |

**Notes:**

- `source_release_date` is the latest across the source deliveries' `cdm_source` files, falling back to `cdm_release_date` if none is present.

**Example:**

```json
{
  "output_uri": "merged/2024-08-03/artifacts/converted_files/cdm_source.parquet",
  "source_cdm_source_uris": [
    "site-a/2024-01-15/artifacts/converted_files/cdm_source.parquet",
    "site-b/2024-02-01/artifacts/converted_files/cdm_source.parquet"
  ],
  "site_count": 2,
  "cdm_version": "5.4",
  "vocabulary_version": "v5.0 29-FEB-24",
  "cdm_release_date": "2024-08-03"
}
```

---

### Generate Merge Report

**Endpoint:** `POST /generate_merge_report`

**Description:** Records merge provenance — the distinct sites merged, each `(site, delivery_date)` delivery and the row count it contributed, and the merge-wide total — as report artifacts consolidated into a delivery report CSV.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `merge_bucket` | string | Yes | The merged instance's bucket |
| `run_date` | string | Yes | The merged instance's delivery-date slot in `YYYY-MM-DD` format |
| `site` | string | Yes | Synthetic merge `site_name` used for the report CSV file name |
| `deliveries` | list | Yes | Source deliveries in the run, each `{"site", "delivery_date"}` |

**Example:**

```json
{
  "merge_bucket": "merged",
  "run_date": "2024-08-03",
  "site": "connect_ehr",
  "deliveries": [
    {"site": "site-a", "delivery_date": "2024-01-15"},
    {"site": "site-b", "delivery_date": "2024-02-01"}
  ]
}
```

## Cloud Run Jobs

The repository also exposes direct job entry points under `core/jobs/`.

| Job module | Required environment variables | Equivalent API stage |
|-----------|--------------------------------|----------------------|
| `core.jobs.process_incoming_file_job` | `FILE_TYPE`, `FILE_PATH` | `POST /process_incoming_file` |
| `core.jobs.normalize_parquet_job` | `FILE_PATH`, `CDM_VERSION`, `DATE_FORMAT`, `DATETIME_FORMAT` | `POST /normalize_parquet` |
| `core.jobs.upgrade_cdm_job` | `FILE_PATH`, `CDM_VERSION`, `TARGET_CDM_VERSION` | `POST /upgrade_cdm` |
| `core.jobs.unique_natural_keys_job` | `FILE_PATH`, `CDM_VERSION`, `SITE` | `POST /unique_natural_keys` |
| `core.jobs.harmonize_vocab_job` | `FILE_PATH`, `VOCAB_VERSION`, `CDM_VERSION`, `SITE`, `PROJECT_ID`, `DATASET_ID`, `STEP` | `POST /harmonize_vocab` |
| `core.jobs.post_processing_job` | `SITE`, `GCS_BUCKET`, `DELIVERY_DATE`, `CDM_VERSION`, `VOCAB_VERSION`, `TASK_NAME` | `POST /post_processing` |
| `core.jobs.generate_derived_tables_job` | `SITE`, `GCS_BUCKET`, `DELIVERY_DATE`, `TABLE_NAME`, `VOCAB_VERSION` | `POST /generate_derived_tables_from_harmonized` |
| `core.jobs.generate_report_csv_job` | `SITE`, `GCS_BUCKET`, `DELIVERY_DATE`, `SITE_DISPLAY_NAME`, `FILE_DELIVERY_FORMAT`, `DELIVERED_CDM_VERSION`, `TARGET_VOCABULARY_VERSION`, `TARGET_CDM_VERSION`; optional `ARTIFACT_TYPE` to generate one artifact type or run the final consolidation | `POST /generate_delivery_report_csv` |
| `core.jobs.extract_participant_chunk_job` | `SOURCE_URI`, `CHUNK_URI`; optional `SITE_DISPLAY_NAME` | `POST /extract_participant_chunk` |
| `core.jobs.reconcile_chunks_job` | `CHUNK_GLOB`, `OUTPUT_URI` | `POST /reconcile_chunks` |

## Running Tests

Run the unit test suite with:

```bash
pytest
```
