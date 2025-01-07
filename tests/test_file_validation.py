import pytest
import os
import json
from unittest.mock import patch, mock_open
from core.file_validation import validate_cdm_table_name  

# Mock constants
mock_constants = {
    "CDM_SCHEMA_PATH": "/mock/path/",
    "CDM_SCHEMA_FILE_NAME": "schema.json"
}

@pytest.fixture
def mock_schema_file():
    # Mock schema JSON data
    return json.dumps({
        "person": {},
        "visit_occurrence": {},
        "drug_exposure": {}
    })

def test_validate_cdm_table_name_valid_file(mock_schema_file):
    file_name = "/mock/path/person.csv"
    cdm_version = "5.3"

    with patch("builtins.open", mock_open(read_data=mock_schema_file)), \
         patch("core.constants.CDM_SCHEMA_PATH", mock_constants["CDM_SCHEMA_PATH"]), \
         patch("core.constants.CDM_SCHEMA_FILE_NAME", mock_constants["CDM_SCHEMA_FILE_NAME"]):
        
        assert validate_cdm_table_name(file_name, cdm_version) is True

def test_validate_cdm_table_name_invalid_file(mock_schema_file):
    file_name = "/mock/path/invalid_table.csv"
    cdm_version = "5.3"

    with patch("builtins.open", mock_open(read_data=mock_schema_file)), \
         patch("core.constants.CDM_SCHEMA_PATH", mock_constants["CDM_SCHEMA_PATH"]), \
         patch("core.constants.CDM_SCHEMA_FILE_NAME", mock_constants["CDM_SCHEMA_FILE_NAME"]):
        
        assert validate_cdm_table_name(file_name, cdm_version) is False

def test_validate_cdm_table_name_invalid_json():
    file_name = "/mock/path/person.csv"
    cdm_version = "5.3"

    with patch("builtins.open", mock_open(read_data="Invalid JSON")), \
         patch("core.constants.CDM_SCHEMA_PATH", mock_constants["CDM_SCHEMA_PATH"]), \
         patch("core.constants.CDM_SCHEMA_FILE_NAME", mock_constants["CDM_SCHEMA_FILE_NAME"]):
        
        with pytest.raises(Exception, match="Error validating cdm file"):
            validate_cdm_table_name(file_name, cdm_version)

def test_validate_cdm_table_name_missing_file():
    file_name = "/mock/path/person.csv"
    cdm_version = "5.3"

    with patch("builtins.open", side_effect=FileNotFoundError), \
         patch("core.constants.CDM_SCHEMA_PATH", mock_constants["CDM_SCHEMA_PATH"]), \
         patch("core.constants.CDM_SCHEMA_FILE_NAME", mock_constants["CDM_SCHEMA_FILE_NAME"]):
        
        with pytest.raises(Exception, match="Error validating cdm file"):
            validate_cdm_table_name(file_name, cdm_version)
