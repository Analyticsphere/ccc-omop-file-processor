from enum import Enum

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "6GB"
DUCKDB_MAX_SIZE = "500GB"

SERVICE_NAME = "omop-file-processor"

CSV = ".csv"
PARQUET = ".parquet"

FIXED_FILE_TAG_STRING = "_pipeline_fix_formatting"

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    FIXED_FILES = f"{ARTIFACTS}fixed_files/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"