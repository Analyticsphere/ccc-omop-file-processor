# Architecture Notes - Local Development

## Path Resolution Strategy

### The Challenge
The API receives relative paths like `synthea_53/2025-01-01/person.csv`, but the actual files in the Docker container are at absolute paths like `/data/synthea_53/2025-01-01/person.csv`.

### The Solution
Use a configurable `DATA_ROOT` environment variable to resolve relative paths:

```python
# In storage_backend.py
if self.backend == 'local' and not path.startswith('/'):
    data_root = os.getenv('DATA_ROOT', '/data')  # Default: /data
    path = f"{data_root}/{path}"
```

### Why This Works

**For Docker:**
- Volumes mounted to `/data/`
- Set `DATA_ROOT=/data` (or use default)
- Relative paths automatically resolve correctly

**For Other Setups:**
- Can mount to different location
- Set `DATA_ROOT=/my/custom/path`
- Works without code changes

**For Native Python:**
- No Docker involved
- Set `DATA_ROOT=/Users/me/omop-data`
- Same code works everywhere

### Example Flow

**Input (API call):**
```json
{
  "file_path": "synthea_53/2025-01-01/person.csv"
}
```

**Storage Backend Processing:**
1. Strip any existing scheme: `synthea_53/2025-01-01/person.csv`
2. Check if relative (doesn't start with `/`): Yes
3. Get DATA_ROOT from env: `/data` (default)
4. Prepend DATA_ROOT: `/data/synthea_53/2025-01-01/person.csv`
5. Add scheme: `file:///data/synthea_53/2025-01-01/person.csv`

**DuckDB receives:**
```sql
SELECT * FROM read_csv('file:///data/synthea_53/2025-01-01/person.csv')
```

### Configuration

**Docker (default):**
```bash
docker run -d \
  -v /path/to/data:/data \
  -e STORAGE_BACKEND=local \
  -e DATA_ROOT=/data \
  ...
```

**Docker (custom mount):**
```bash
docker run -d \
  -v /path/to/data:/mydata \
  -e STORAGE_BACKEND=local \
  -e DATA_ROOT=/mydata \
  ...
```

**Native Python:**
```bash
export STORAGE_BACKEND=local
export DATA_ROOT=/Users/me/omop-data
python -m flask run
```

### Benefits

1. ✅ **Flexible** - Works with any mount point
2. ✅ **Configurable** - No hardcoded paths
3. ✅ **Portable** - Same code works Docker, native, containers
4. ✅ **Sensible Default** - `/data` is standard for containers
5. ✅ **No Code Changes** - Change behavior via env vars only

## Environment Variables

### Storage Configuration
- `STORAGE_BACKEND` - Backend type (`gcs`, `local`)
- `DATA_ROOT` - Root directory for local storage (default: `/data`)

### DuckDB Configuration
- `DUCKDB_TEMP_DIR` - Temp files location (default: `/mnt/data/`)
- `DUCKDB_MEMORY_LIMIT` - Memory limit (default: `12GB`)
- `DUCKDB_THREADS` - Thread count (default: `2`)

### Other Configuration
- `VOCAB_GCS_PATH` - Vocabulary files location
- `BQ_LOGGING_TABLE` - BigQuery logging table (mocked in local mode)
- `PORT` - API port (default: `8080`)

## Design Principles

1. **Configuration over Convention** - Use env vars, not hardcoded paths
2. **Sensible Defaults** - Works out-of-box for common cases
3. **Cloud-Agnostic** - Same patterns for local and cloud
4. **Single Source of Truth** - Storage operations in StorageBackend only
5. **No Duplication** - Utils functions use StorageBackend primitives
