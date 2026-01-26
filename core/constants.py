import os
from enum import Enum

DUCKDB_FORMAT_STRING = "(FORMAT parquet, COMPRESSION zstd, COMPRESSION_LEVEL 1)"
DUCKDB_MEMORY_LIMIT = "12GB"
DUCKDB_MAX_SIZE = "5000GB"
DUCKDB_THREADS = "2"

SERVICE_NAME = "omop-file-processor"
GCS_BACKEND = "gcs"
LOCAL_BACKEND = "local"
BACKENDS = {
    GCS_BACKEND: 'gs://',
    LOCAL_BACKEND: 'file://',
}
BQ_LOGGING_TABLE = os.getenv('BQ_LOGGING_TABLE', 'NO BQ_LOGGING_TABLE DEFINED')
VOCAB_PATH = os.getenv('OMOP_VOCAB_PATH', 'NO _OMOP_VOCAB_PATH DEFINED')
STORAGE_BACKEND = os.getenv('STORAGE_BACKEND', GCS_BACKEND)

PARQUET = ".parquet"
CSV_GZ = ".csv.gz"
CSV = ".csv"
FILE_EXTENSIONS = [PARQUET, CSV_GZ, CSV]

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
DERIVED_TABLE_SCRIPT_PATH = f"{SQL_PATH}derived_tables/"
OMOP_ETL_SCRIPT_PATH = f"{SQL_PATH}omop_etl/"

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

class BQWriteTypes(str, Enum):
    """
    BigQuery write operation types.

    SPECIFIC_FILE: Overwrite table with the exact Parquet file specified
    PROCESSED_FILE: Overwrite table with pipeline-processed version of the file
    """
    SPECIFIC_FILE = "specific_file"
    PROCESSED_FILE = "processed_file"

CONDITION_ERA = "condition_era"
DRUG_ERA = "drug_era"
OBSERVATION_PERIOD = "observation_period"
DERIVED_DATA_TABLES_REQUIREMENTS = {
    CONDITION_ERA: ["condition_occurrence"],
    DRUG_ERA: ["drug_exposure"],
    OBSERVATION_PERIOD: ["person"]
}

# Tables that undergo vocabulary harmonization
# These are available in the omop_etl/ directory after harmonization
VOCAB_HARMONIZED_TABLES = [
    "visit_occurrence",
    "condition_occurrence",
    "drug_exposure",
    "procedure_occurrence",
    "device_exposure",
    "measurement",
    "observation",
    "note",
    "specimen"
]

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
    CONVERTED_FILES = f"{ARTIFACTS}converted_files/"
    HARMONIZED_FILES = f"{ARTIFACTS}harmonized_files/"
    OMOP_ETL = f"{ARTIFACTS}omop_etl/"
    DERIVED_FILES = f"{ARTIFACTS}derived_files/"
    REPORT = f"{ARTIFACTS}delivery_report/"
    REPORT_TMP = f"{ARTIFACTS}delivery_report/tmp/"
    DQD = f"{ARTIFACTS}dqd/"
    ACHILLES = f"{ARTIFACTS}achilles/"
    PASS_ANALYSIS = f"{ARTIFACTS}pass/"
    INVALID_ROWS = f"{ARTIFACTS}invalid_rows/"

# Using -1 as place/holder default value for numeric columns 
#   as these are uncommon values in real data
# Using date 1970-01-01 because it's the Unix epoch, and it's
#   unlikely that this date will appear in real data
DEFAULT_DATE = "1970-01-01"
DEFAULT_COLUMN_VALUES = {
        "VARCHAR": "''",
        "DATE": f"'{DEFAULT_DATE}'",
        "BIGINT": "'-1'",
        "DOUBLE": "'-1.0'",
        "TIMESTAMP": f"'{DEFAULT_DATE} 00:00:00'",
        "DATETIME": f"'{DEFAULT_DATE} 00:00:00'"
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

# Configuration for reporting on OMOP tables
# Maps table name to its location, type_concept_id field, and vocabulary concept fields
REPORTING_TABLE_CONFIG = {
    "person": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": [
            {"concept_id": "gender_concept_id", "source_concept_id": "gender_source_concept_id"},
            {"concept_id": "race_concept_id", "source_concept_id": "race_source_concept_id"},
            {"concept_id": "ethnicity_concept_id", "source_concept_id": "ethnicity_source_concept_id"}
        ]
    },
    "visit_occurrence": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "visit_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "visit_concept_id", "source_concept_id": "visit_source_concept_id"},
            {"concept_id": "admitted_from_concept_id", "source_concept_id": None},
            {"concept_id": "discharged_to_concept_id", "source_concept_id": None}
        ]
    },
    "visit_detail": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": "visit_detail_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "visit_detail_concept_id", "source_concept_id": "visit_detail_source_concept_id"},
            {"concept_id": "admitted_from_concept_id", "source_concept_id": None},
            {"concept_id": "discharged_to_concept_id", "source_concept_id": None}
        ]
    },
    "condition_occurrence": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "condition_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "condition_concept_id", "source_concept_id": "condition_source_concept_id"},
            {"concept_id": "condition_status_concept_id", "source_concept_id": None}
        ]
    },
    "drug_exposure": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "drug_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "drug_concept_id", "source_concept_id": "drug_source_concept_id"},
            {"concept_id": "route_concept_id", "source_concept_id": None}
        ]
    },
    "procedure_occurrence": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "procedure_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "procedure_concept_id", "source_concept_id": "procedure_source_concept_id"},
            {"concept_id": "modifier_concept_id", "source_concept_id": None}
        ]
    },
    "device_exposure": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "device_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "device_concept_id", "source_concept_id": "device_source_concept_id"},
            {"concept_id": "unit_concept_id", "source_concept_id": "unit_source_concept_id"}
        ]
    },
    "measurement": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "measurement_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "measurement_concept_id", "source_concept_id": "measurement_source_concept_id"},
            {"concept_id": "operator_concept_id", "source_concept_id": None},
            {"concept_id": "value_as_concept_id", "source_concept_id": None},
            {"concept_id": "unit_concept_id", "source_concept_id": "unit_source_concept_id"}
        ]
    },
    "observation": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "observation_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "observation_concept_id", "source_concept_id": "observation_source_concept_id"},
            {"concept_id": "value_as_concept_id", "source_concept_id": None},
            {"concept_id": "qualifier_concept_id", "source_concept_id": None},
            {"concept_id": "unit_concept_id", "source_concept_id": None}
        ]
    },
    "death": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": "death_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "cause_concept_id", "source_concept_id": "cause_source_concept_id"}
        ]
    },
    "note": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "note_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "note_class_concept_id", "source_concept_id": None},
            {"concept_id": "encoding_concept_id", "source_concept_id": None},
            {"concept_id": "language_concept_id", "source_concept_id": None}
        ]
    },
    "specimen": {
        "location": ArtifactPaths.OMOP_ETL,
        "type_field": "specimen_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "specimen_concept_id", "source_concept_id": None},
            {"concept_id": "unit_concept_id", "source_concept_id": None},
            {"concept_id": "anatomic_site_concept_id", "source_concept_id": None},
            {"concept_id": "disease_status_concept_id", "source_concept_id": None}
        ]
    },
    "care_site": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": [
            {"concept_id": "place_of_service_concept_id", "source_concept_id": None}
        ]
    },
    "provider": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": [
            {"concept_id": "specialty_concept_id", "source_concept_id": "specialty_source_concept_id"},
            {"concept_id": "gender_concept_id", "source_concept_id": "gender_source_concept_id"}
        ]
    },
    "payer_plan_period": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": [
            {"concept_id": "payer_concept_id", "source_concept_id": "payer_source_concept_id"},
            {"concept_id": "plan_concept_id", "source_concept_id": "plan_source_concept_id"},
            {"concept_id": "sponsor_concept_id", "source_concept_id": "sponsor_source_concept_id"},
            {"concept_id": "stop_reason_concept_id", "source_concept_id": "stop_reason_source_concept_id"}
        ]
    },
    "cost": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": "cost_type_concept_id",
        "vocabulary_fields": [
            {"concept_id": "currency_concept_id", "source_concept_id": None},
            {"concept_id": "revenue_code_concept_id", "source_concept_id": None},
            {"concept_id": "drg_concept_id", "source_concept_id": None}
        ]
    },
    "episode": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": "episode_type_concept_id",
        "vocabulary_fields": []
    },
    "observation_period": {
        "location": ArtifactPaths.DERIVED_FILES,
        "type_field": "period_type_concept_id",
        "vocabulary_fields": []
    },
    "condition_era": {
        "location": ArtifactPaths.DERIVED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "drug_era": {
        "location": ArtifactPaths.DERIVED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "dose_era": {
        "location": ArtifactPaths.DERIVED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "episode_event": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "location": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "cdm_source": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "metadata": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "fact_relationship": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "note_nlp": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "source_to_concept_map": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "cohort": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "cohort_definition": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "concept": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "vocabulary": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "concept_ancestor": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "concept_class": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "concept_relationship": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "concept_synonym": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "domain": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "drug_strength": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    },
    "relationship": {
        "location": ArtifactPaths.CONVERTED_FILES,
        "type_field": None,
        "vocabulary_fields": []
    }
}

# Tables and their primary date fields for time series analysis
# Using start_date fields for tables with both start and end dates
TIME_SERIES_TABLES = {
    "visit_occurrence": "visit_start_date",
    "visit_detail": "visit_detail_start_date",
    "condition_occurrence": "condition_start_date",
    "drug_exposure": "drug_exposure_start_date",
    "procedure_occurrence": "procedure_date",
    "device_exposure": "device_exposure_start_date",
    "measurement": "measurement_date",
    "observation": "observation_date",
    "note": "note_date",
    "specimen": "specimen_date"
}

SOURCE_TARGET = "source_target"
DOMAIN_CHECK = "domain_check"
TARGET_REMAP = "target_remap"
TARGET_REPLACEMENT = "target_replacement"
OMOP_ETL = "omop_etl"
CONSOLIDATE_ETL = "consolidate_etl"
DISCOVER_TABLES_FOR_DEDUP = "discover_tables_for_dedup"
DEDUPLICATE_SINGLE_TABLE = "deduplicate_single_table"

# Primary key column can be found in schema.json file
NATURAL_KEY_TABLES = [
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
]

SURROGATE_KEY_TABLES = [
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
]

NO_PRIMARY_KEY_TABLES = [
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
]
