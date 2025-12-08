"""
Unit tests for file_validation.py FileValidator class.

Tests validation of OMOP CDM files including table names, column names,
and schema compliance.
"""

from unittest.mock import MagicMock, patch, call

import pytest

from core.file_validation import FileValidator


class TestFileValidatorInit:
    """Tests for FileValidator initialization."""

    def test_init_stores_parameters(self):
        """Test that initialization stores all parameters."""
        validator = FileValidator(
            file_path="synthea53/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="synthea53"
        )

        assert validator.file_path == "synthea53/2025-01-01/person.parquet"
        assert validator.omop_version == "5.4"
        assert validator.delivery_date == "2025-01-01"
        assert validator.storage_path == "synthea53"

    def test_init_computes_derived_attributes(self):
        """Test that initialization computes table_name and bucket_name."""
        validator = FileValidator(
            file_path="test-bucket/2025-01-01/observation.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="test-bucket"
        )

        assert validator.table_name == "observation"
        assert validator.bucket_name == "test-bucket"


class TestFileValidatorTableName:
    """Tests for validate_table_name method."""

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_cdm_schema')
    def test_valid_table_name_creates_valid_artifact(self, mock_get_schema, mock_artifact):
        """Test that valid table name creates a 'valid' report artifact."""
        # Setup mock schema
        mock_get_schema.return_value = {
            'person': {'concept_id': 123456},
            'observation': {'concept_id': 789012}
        }

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        result = validator.validate_table_name()

        assert result is True
        mock_artifact.assert_called_once()
        call_args = mock_artifact.call_args
        assert call_args.kwargs['concept_id'] == 123456
        assert call_args.kwargs['name'] == "Valid table name: person"
        assert call_args.kwargs['value_as_string'] == "valid table name"

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_cdm_schema')
    def test_invalid_table_name_creates_invalid_artifact(self, mock_get_schema, mock_artifact):
        """Test that invalid table name creates an 'invalid' report artifact."""
        # Setup mock schema
        mock_get_schema.return_value = {
            'person': {'concept_id': 123456},
            'observation': {'concept_id': 789012}
        }

        validator = FileValidator(
            file_path="bucket/2025-01-01/unknown_table.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        result = validator.validate_table_name()

        assert result is False
        mock_artifact.assert_called_once()
        call_args = mock_artifact.call_args
        assert call_args.kwargs['concept_id'] is None
        assert call_args.kwargs['name'] == "Invalid table name: unknown_table"
        assert call_args.kwargs['value_as_string'] == "invalid table name"

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_cdm_schema')
    def test_artifact_is_saved(self, mock_get_schema, mock_artifact):
        """Test that report artifact is saved."""
        mock_get_schema.return_value = {'person': {'concept_id': 123456}}
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate_table_name()

        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.file_validation.utils.get_cdm_schema')
    def test_schema_error_raises_exception(self, mock_get_schema):
        """Test that schema loading errors are wrapped in exception."""
        mock_get_schema.side_effect = Exception("Schema load failed")

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        with pytest.raises(Exception) as exc_info:
            validator.validate_table_name()

        assert "Error validating table name" in str(exc_info.value)


class TestFileValidatorColumns:
    """Tests for validate_columns method."""

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_columns_from_file')
    @patch('core.file_validation.utils.get_table_schema')
    def test_all_valid_columns(self, mock_get_table_schema, mock_get_columns, mock_artifact):
        """Test validation when all parquet columns match schema."""
        # Setup mocks
        mock_get_table_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'concept_id': 1001},
                    'gender_concept_id': {'concept_id': 1002},
                    'year_of_birth': {'concept_id': 1003}
                }
            }
        }
        mock_get_columns.return_value = ['person_id', 'gender_concept_id', 'year_of_birth']

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate_columns()

        # Should create 3 valid column artifacts, no invalid or missing
        assert mock_artifact.call_count == 3
        for call_args in mock_artifact.call_args_list:
            assert call_args.kwargs['value_as_string'] == "valid column name"
            assert "Valid column name: person." in call_args.kwargs['name']

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_columns_from_file')
    @patch('core.file_validation.utils.get_table_schema')
    def test_invalid_columns(self, mock_get_table_schema, mock_get_columns, mock_artifact):
        """Test validation when parquet has columns not in schema."""
        mock_get_table_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'concept_id': 1001},
                    'gender_concept_id': {'concept_id': 1002}
                }
            }
        }
        # Include an extra column not in schema
        mock_get_columns.return_value = ['person_id', 'gender_concept_id', 'extra_column']

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate_columns()

        # Should have 2 valid + 1 invalid
        assert mock_artifact.call_count == 3

        # Find the invalid column artifact
        invalid_calls = [c for c in mock_artifact.call_args_list
                        if 'Invalid column name' in c.kwargs['name']]
        assert len(invalid_calls) == 1
        assert invalid_calls[0].kwargs['concept_id'] is None
        assert invalid_calls[0].kwargs['value_as_string'] == "invalid column name"

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_columns_from_file')
    @patch('core.file_validation.utils.get_table_schema')
    def test_missing_columns(self, mock_get_table_schema, mock_get_columns, mock_artifact):
        """Test validation when parquet is missing required schema columns."""
        mock_get_table_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'concept_id': 1001},
                    'gender_concept_id': {'concept_id': 1002},
                    'year_of_birth': {'concept_id': 1003}
                }
            }
        }
        # Missing year_of_birth
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate_columns()

        # Should have 2 valid + 1 missing
        assert mock_artifact.call_count == 3

        # Find the missing column artifact
        missing_calls = [c for c in mock_artifact.call_args_list
                        if 'Missing column' in c.kwargs['name']]
        assert len(missing_calls) == 1
        assert missing_calls[0].kwargs['concept_id'] == 1003
        assert missing_calls[0].kwargs['value_as_string'] == "missing column"

    @patch('core.file_validation.report_artifact.ReportArtifact')
    @patch('core.file_validation.utils.get_columns_from_file')
    @patch('core.file_validation.utils.get_table_schema')
    def test_mixed_valid_invalid_missing(self, mock_get_table_schema, mock_get_columns, mock_artifact):
        """Test validation with mix of valid, invalid, and missing columns."""
        mock_get_table_schema.return_value = {
            'observation': {
                'columns': {
                    'observation_id': {'concept_id': 2001},
                    'person_id': {'concept_id': 2002},
                    'observation_concept_id': {'concept_id': 2003},
                    'observation_date': {'concept_id': 2004}
                }
            }
        }
        # Valid: observation_id, person_id
        # Invalid: extra_column
        # Missing: observation_concept_id, observation_date
        mock_get_columns.return_value = ['observation_id', 'person_id', 'extra_column']

        validator = FileValidator(
            file_path="bucket/2025-01-01/observation.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate_columns()

        # Should have 2 valid + 1 invalid + 2 missing = 5 total
        assert mock_artifact.call_count == 5

        valid_calls = [c for c in mock_artifact.call_args_list
                      if 'Valid column name' in c.kwargs['name']]
        invalid_calls = [c for c in mock_artifact.call_args_list
                        if 'Invalid column name' in c.kwargs['name']]
        missing_calls = [c for c in mock_artifact.call_args_list
                        if 'Missing column' in c.kwargs['name']]

        assert len(valid_calls) == 2
        assert len(invalid_calls) == 1
        assert len(missing_calls) == 2


class TestFileValidatorValidate:
    """Tests for the validate orchestration method."""

    @patch.object(FileValidator, 'validate_columns')
    @patch.object(FileValidator, 'validate_table_name')
    def test_validate_calls_both_validations_when_table_valid(self, mock_validate_table, mock_validate_columns):
        """Test that validate calls both table and column validation when table is valid."""
        mock_validate_table.return_value = True

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate()

        mock_validate_table.assert_called_once()
        mock_validate_columns.assert_called_once()

    @patch.object(FileValidator, 'validate_columns')
    @patch.object(FileValidator, 'validate_table_name')
    def test_validate_skips_columns_when_table_invalid(self, mock_validate_table, mock_validate_columns):
        """Test that validate skips column validation when table name is invalid."""
        mock_validate_table.return_value = False

        validator = FileValidator(
            file_path="bucket/2025-01-01/invalid_table.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        validator.validate()

        mock_validate_table.assert_called_once()
        mock_validate_columns.assert_not_called()

    @patch.object(FileValidator, 'validate_table_name')
    def test_validate_wraps_exceptions(self, mock_validate_table):
        """Test that validate wraps exceptions with file context."""
        mock_validate_table.side_effect = Exception("Validation failed")

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        with pytest.raises(Exception) as exc_info:
            validator.validate()

        assert "Error validating file" in str(exc_info.value)
        assert "bucket/2025-01-01/person.parquet" in str(exc_info.value)


class TestFileValidatorHelpers:
    """Tests for private helper methods."""

    @patch('core.file_validation.utils.get_cdm_schema')
    def test_get_cdm_schema_caches_result(self, mock_get_schema):
        """Test that CDM schema is loaded once and cached."""
        mock_get_schema.return_value = {'person': {'concept_id': 123}}

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        # Call twice
        result1 = validator._get_cdm_schema()
        result2 = validator._get_cdm_schema()

        # Schema should be loaded only once
        mock_get_schema.assert_called_once_with(cdm_version="5.4")
        assert result1 is result2

    @patch('core.file_validation.utils.get_table_schema')
    def test_get_table_schema_caches_result(self, mock_get_table_schema):
        """Test that table schema is loaded once and cached."""
        mock_get_table_schema.return_value = {'person': {'columns': {}}}

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        # Call twice
        result1 = validator._get_table_schema()
        result2 = validator._get_table_schema()

        # Schema should be loaded only once
        mock_get_table_schema.assert_called_once_with(table_name="person", cdm_version="5.4")
        assert result1 is result2

    @patch('core.file_validation.utils.get_columns_from_file')
    def test_get_parquet_columns_returns_set(self, mock_get_columns):
        """Test that parquet columns are returned as a set."""
        mock_get_columns.return_value = ['col1', 'col2', 'col3']

        validator = FileValidator(
            file_path="bucket/2025-01-01/person.parquet",
            omop_version="5.4",
            delivery_date="2025-01-01",
            storage_path="bucket"
        )

        result = validator._get_parquet_columns()

        assert isinstance(result, set)
        assert result == {'col1', 'col2', 'col3'}
