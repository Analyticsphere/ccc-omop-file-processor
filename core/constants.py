from enum import Enum

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "6GB"
DUCKDB_MAX_SIZE = "500GB"

SERVICE_NAME = "omop-file-processor"

CSV = ".csv"
PARQUET = ".parquet"

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    DQD = f"{ARTIFACTS}dqd/"