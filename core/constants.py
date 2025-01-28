from enum import Enum

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "5GB"
DUCKDB_MAX_SIZE = "500GB"
#DUCKDB_THREADS = "6"
DUCKDB_THREADS = "6"

SERVICE_NAME = "omop-file-processor"

CSV = ".csv"
PARQUET = ".parquet"

CDM_SCHEMA_PATH = "reference/schemas/"
CDM_SCHEMA_FILE_NAME = "schema.json"

FIXED_FILE_TAG_STRING = "_pipeline_fix_formatting"
VALID_ROW_STRING = "valid_row"
INVALID_ROW_STRING = "invalid_row"
ROW_VALIDITY_COLUMN_STRING = "row_validity"
ROW_HASH_COLUMN_STRING = "row_hash"

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    FIXED_FILES = f"{ARTIFACTS}fixed_files/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"

# Using -1 as place/holder default value for numeric fields 
#   as these are uncommon values in real data
# Using date 1970-01-01 because it's the Unix epoch, and it's
#   unlikely that this date will appear in real data
DEFAULT_FIELD_VALUES = {
        "VARCHAR": "''",
        "DATE": "'1970-01-01'",
        "BIGINT": "'-1'",
        "DOUBLE": "'-1.0'",
        "TIMESTAMP": "'1901-01-01 00:00:00'"
    }