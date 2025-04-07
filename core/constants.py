from enum import Enum
import os

DUCKDB_FORMAT_STRING = "(FORMAT 'parquet', COMPRESSION 'zstd')"
DUCKDB_MEMORY_LIMIT = "10GB"
DUCKDB_MAX_SIZE = "5000GB"
DUCKDB_THREADS = "4"

SERVICE_NAME = "omop-file-processor"
BQ_LOGGING_TABLE = (os.getenv('BQ_LOGGING_TABLE'), '')
VOCAB_GCS_PATH = (os.getenv('VOCAB_GCS_PATH'), '')

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

SQL_PATH = "reference/sql/"
CDM_UPGRADE_SCRIPT_PATH = f"{SQL_PATH}cdm_upgrade/"

DERIVED_TABLE_PATH = f"{SQL_PATH}derived_tables/"

OMOP_ETL_PATH = f"{SQL_PATH}omop_etl/"

OPTIMIZED_VOCAB_FOLDER = "optimized"
OPTIMIZED_VOCAB_FILE = "optimized_vocab_file"
OPTIMIZED_VOCAB_FILE_NAME = f"{OPTIMIZED_VOCAB_FILE}{PARQUET}"
MAPPING_RELATIONSHIPS = "'Maps to','Maps to value','Maps to unit'"
REPLACEMENT_RELATIONSHIPS = "'Concept replaced by','Concept was_a to','Concept poss_eq to','Concept same_as to','Concept alt_to to'"

PIPELINE_START_STRING = "started"
PIPELINE_RUNNING_STRING = "running"
PIPELINE_COMPLETE_STRING = "completed"
PIPELINE_ERROR_STRING = "error"
PIPELINE_DAG_FAIL_MESSAGE = "DAG failed"

FIXED_FILE_TAG_STRING = "_pipeline_fix_formatting"

class BQWriteTypes(str, Enum):
    SPECIFIC_FILE = "specific_file"
    ETLed_FILE = "ETLed_file"
    PROCESSED_FILE = "processed_file"



CONDITION_ERA = "condition_era"
DRUG_ERA = "drug_era"
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES_REQUIREMENTS = {
    CONDITION_ERA: ["condition_occurrence"],
    DRUG_ERA: ["drug_exposure"],
    OBSERVATION_PERIOD: ["person"]
}

SITE_PLACEHOLDER_STRING = "@SITE"
CURRENT_DATE_PLACEHOLDER_STRING = "@CURRENT_DATE"
CONDITION_OCCURRENCE_PLACEHOLDER_STRING = "@CONDITION_OCCURRENCE"
MEASUREMENT_PLACEHOLDER_STRING = "@MEASUREMENT"
OBSERVATION_PLACEHOLDER_STRING = "@OBSERVATION"
DRUG_EXPOSURE_PLACEHOLDER_STRING = "@DRUG_EXPOSURE"
VISIT_OCCURRENCE_PLACEHOLDER_STRING = "@VISIT_OCCURRENCE"
DEVICE_EXPOSURE_PLACEHOLDER_STRING = "@DEVICE_EXPOSURE"
NOTE_PLACEHOLDER_STRING = "@NOTE"
PROCEDURE_OCCURRENCE_PLACEHOLDER_STRING = "@PROCEDURE_OCCURRENCE"
SPECIMEN_PLACEHOLDER_STRING = "@SPECIMEN"
DEATH_PLACEHOLDER_STRING = "@DEATH"
PERSON_PLACEHOLDER_STRING = "@PERSON"
CONCEPT_ANCESTOR_PLACEHOLDER_STRING = "@CONCEPT_ANCESTOR"
CONCEPT_PLACEHOLDER_STRING = "@CONCEPT"
OPTIMIZED_VOCAB_PLACEHOLDER_STRING = "@OPTIMIZED_VOCABULARY"

CLINICAL_DATA_PATH_PLACEHOLDERS = {
    CONDITION_OCCURRENCE_PLACEHOLDER_STRING: "condition_occurrence",
    DRUG_EXPOSURE_PLACEHOLDER_STRING: "drug_exposure",
    VISIT_OCCURRENCE_PLACEHOLDER_STRING: "visit_occurrence",
    DEATH_PLACEHOLDER_STRING: "death",
    PERSON_PLACEHOLDER_STRING: "person",
    MEASUREMENT_PLACEHOLDER_STRING: "measurement",
    OBSERVATION_PLACEHOLDER_STRING: "observation",
    DEVICE_EXPOSURE_PLACEHOLDER_STRING: "device_exposure",
    NOTE_PLACEHOLDER_STRING: "note",
    PROCEDURE_OCCURRENCE_PLACEHOLDER_STRING: "procedure_occurrence",
    SPECIMEN_PLACEHOLDER_STRING: "specimen"
}

VOCAB_PATH_PLACEHOLDERS = {
    CONCEPT_ANCESTOR_PLACEHOLDER_STRING: "concept_ancestor",
    CONCEPT_PLACEHOLDER_STRING: "concept",
    OPTIMIZED_VOCAB_PLACEHOLDER_STRING: "optimized_vocab_file"
}

class ArtifactPaths(str, Enum):
    ARTIFACTS = "artifacts/"
    FIXED_FILES = f"{ARTIFACTS}fixed_files/"
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    HARMONIZED_FILES = f"{ARTIFACTS}harmonized_files/"
    CREATED_FILES = f"{ARTIFACTS}created_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    ACHILLES = f"{ARTIFACTS}achilles/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"

# Using -1 as place/holder default value for numeric columns 
#   as these are uncommon values in real data
# Using date 1970-01-01 because it's the Unix epoch, and it's
#   unlikely that this date will appear in real data
DEFAULT_COLUMN_VALUES = {
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

SOURCE_TARGET_COLUMNS = {
    "visit_occurrence": {
        "source_concept_id": "visit_source_concept_id",
        "target_concept_id": "visit_concept_id"
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

SOURCE_TARGET = "Map source concepts to a updated target codes"
DOMAIN_CHECK = "Check for latest domain and update if needed"
TARGET_REMAP = "Remap non-standard targets to new standard targets"
TARGET_REPLACEMENT = "Replace non-standard targets with new standard targets"

# Primary key column can be found in schema.json file
NATURAL_KEY_TABLES = {
    "person",
    "location",
    "care_site",
    "provider",
    "episode",
    "concept",
    "vocabulary",
    "domain",
    "concept_class",
    "relationship",
    "visit_occurrence",
    "visit_detail"
}

SURROGATE_KEY_TABLES = {
    "observation_period",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "note",
    "note_nlp",
    "specimen",
    "payer_plan_period",
    "cost",
    "drug_era",
    "dose_era",
    "condition_era"
}

NO_PRIMARY_KEY_TABLES = {
    "death",
    "fact_relationship",
    "episode_event",
    "cdm_source",
    "concept_relationship",
    "concept_synonym",
    "concept_ancestor",
    "source_to_concept_map",
    "drug_strength",
    "cohort",
    "cohort_definition",
    "attribute_definition"
}