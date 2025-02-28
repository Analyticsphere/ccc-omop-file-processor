from enum import Enum

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "8GB"
DUCKDB_MAX_SIZE = "500GB"
DUCKDB_THREADS = "4"

SERVICE_NAME = "omop-file-processor"

CSV = ".csv"
PARQUET = ".parquet"

CDM_v53 = "5.3"
CDM_v53_CONCEPT_ID = 1147543
CDM_v54 = "5.4"
CDM_v54_CONCEPT_ID = 756265

CDM_SCHEMA_PATH = "reference/schemas/"
CDM_SCHEMA_FILE_NAME = "schema.json"

DDL_SQL_PATH = "reference/sql/ddl/"
DDL_FILE_NAME = "ddl.sql"
DDL_PLACEHOLDER_STRING = "@cdmDatabaseSchema"

CONDITION_OCCURRENCE_PLACEHOLDER_STRING = "@CONDITION_OCCURRENCE_PATH"

SQL_PATH = "reference/sql/"
CDM_UPGRADE_SCRIPT_PATH = f"{SQL_PATH}cdm_upgrade/"

OPTIMIZED_VOCAB_FOLDER = "optimized"
OPTIMIZED_VOCAB_FILE_NAME = f"optimized_vocab_file{PARQUET}"
MAPPING_RELATIONSHIPS = "'Maps to','Maps to value','Maps to unit'"
REPLACEMENT_RELATIONSHIPS = "'Concept replaced by','Concept was_a to','Concept poss_eq to','Concept same_as to','Concept alt_to to'"

PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"

FIXED_FILE_TAG_STRING = "_pipeline_fix_formatting"

CONDITION_ERA = "condition_era",
DRUG_ERA = "drug_era",
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES = set(CONDITION_ERA, DRUG_ERA, OBSERVATION_PERIOD)

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

CHANGED = "changed"
REMOVED = "removed"
ADDED = "added"
CDM_53_TO_54 = {
    "attribute_definition": REMOVED,
    "visit_occurrence": CHANGED,
    "visit_detail": CHANGED,
    "procedure_occurrence": CHANGED,
    "device_exposure": CHANGED,
    "measurement": CHANGED,
    "observation": CHANGED,
    "note": CHANGED,
    "location": CHANGED,
    "metadata": CHANGED,
    "cdm_source": CHANGED,
    "episode": ADDED,
    "episode_event": ADDED,
    "cohort": ADDED
}

TABLES_WITHOUT_SOURCE_ID = ["note", "specimen"]

SOURCE_TARGET_FIELDS = {
    "visit_occurrence": {
        "source_concept_id": "visit_source_concept_id",
        "target_concept_id": "visit_concept_id"
    },
    "visit_detail": {
        "source_concept_id": "visit_detail_source_concept_id",
        "target_concept_id": "visit_detail_concept_id"
    },
    "condition_occurrence": {
        "source_concept_id": "condition_source_concept_id",
        "target_concept_id": "condition_concept_id"
    }, 
    "drug_exposure": {
        "source_concept_id": "drug_source_concept_id",
        "target_concept_id": "drug_concept_id"
    },
    "procedure_occurrence": {
        "source_concept_id": "procedure_source_concept_id",
        "target_concept_id": "procedure_concept_id"
    },
    "device_exposure": {
        "source_concept_id": "device_source_concept_id",
        "target_concept_id": "device_concept_id"
    },
    "measurement": {
        "source_concept_id": "measurement_source_concept_id",
        "target_concept_id": "measurement_concept_id"
    },
    "observation": {
        "source_concept_id": "observation_source_concept_id",
        "target_concept_id": "observation_concept_id"
    },
    "note": {
        "source_concept_id": "",
        "target_concept_id": "note_class_concept_id"
    },
    "specimen": {
        "source_concept_id": "",
        "target_concept_id": "specimen_concept_id"
    }
}

DELIVERY_DATE_REPORT_NAME = "Delivery date"
SITE_DISPLAY_NAME_REPORT_NAME = "Site"
FILE_DELIVERY_FORMAT_REPORT_NAME = "File delivery format"
DELIVERED_CDM_VERSION_REPORT_NAME = "Delivered CDM version"
DELIVERED_VOCABULARY_VERSION_REPORT_NAME = "Delivered vocabulary version"
TARGET_VOCABULARY_VERSION_REPORT_NAME = "Standardized to vocabulary version"
TARGET_CDM_VERSION_REPORT_NAME = "Standardized to CDM version"
FILE_PROCESSOR_VERSION_REPORT_NAME = "Pipeline file processor version"
PROCESSED_DATE_REPORT_NAME = "Delivery processing date"