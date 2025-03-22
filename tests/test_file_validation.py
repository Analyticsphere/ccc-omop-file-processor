import json
import os
from unittest.mock import MagicMock, patch

import pytest

from core.file_validation import (validate_cdm_table_name,
                                  validate_cdm_table_schema, validate_file)


@pytest.fixture
def mock_schema_file(tmp_path):
    schema_content = {
        "person": {
            "columns": {
                "person_id": {"type": "INTEGER"},
                "gender_concept_id": {"type": "INTEGER"}
            }
        }
    }
    schema_file = tmp_path / "schema.json"
    with open(schema_file, "w") as f:
        json.dump(schema_content, f)
    return schema_file

@pytest.fixture
def mock_parquet_file(tmp_path):
    parquet_file = tmp_path / "person.parquet"
    parquet_file.write_text("mock parquet content")  # Simulate the existence of a file
    return parquet_file

def test_validate_cdm_table_name(mock_schema_file, tmp_path):
    file_path = tmp_path / "person.parquet"
    file_path.touch()  # Create an empty file to simulate the parquet file

    # Override schema path to use the temporary schema file path
    os.environ["CDM_SCHEMA_PATH"] = str(mock_schema_file.parent) + "/"
    os.environ["CDM_SCHEMA_FILE_NAME"] = mock_schema_file.name

    valid = validate_cdm_table_name(str(file_path))
    assert valid is True

def test_validate_cdm_table_schema(mock_schema_file, mock_parquet_file, tmp_path):
    # Create a mock DuckDB table schema for the test
    duckdb_schema = [
        ("person_id", "INTEGER"),
        ("gender_concept_id", "INTEGER")
    ]

    with patch("core.utils.create_duckdb_connection") as mock_create_duckdb:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = duckdb_schema
        mock_create_duckdb.return_value = (mock_conn, "/mock/db", "/tmp")

        # Override schema path to use the temporary schema file path
        os.environ["CDM_SCHEMA_PATH"] = str(mock_schema_file.parent) + "/"
        os.environ["CDM_SCHEMA_FILE_NAME"] = mock_schema_file.name

        results = validate_cdm_table_schema(str(mock_parquet_file))

        assert results["status"] == "success"
        assert len(results["missing_columns"]) == 0
        assert len(results["type_mismatches"]) == 0
        assert len(results["unexpected_columns"]) == 0

def test_validate_file(mock_schema_file, mock_parquet_file, tmp_path):
    # Create a mock DuckDB table schema for the test
    duckdb_schema = [
        ("person_id", "INTEGER"),
        ("gender_concept_id", "INTEGER")
    ]

    with patch("core.utils.create_duckdb_connection") as mock_create_duckdb:
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchall.return_value = duckdb_schema
        mock_create_duckdb.return_value = (mock_conn, "/mock/db", "/tmp")

        # Override schema path to use the temporary schema file path
        os.environ["CDM_SCHEMA_PATH"] = str(mock_schema_file.parent) + "/"
        os.environ["CDM_SCHEMA_FILE_NAME"] = mock_schema_file.name

        results = validate_file(str(mock_parquet_file), "5.3")

        assert results[0]["valid_file_name"] is True
        assert results[0]["schema_validation_results"]["status"] == "success"
