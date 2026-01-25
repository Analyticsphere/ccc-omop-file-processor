"""
Unit tests for normalization.py Normalizer class.

Tests file normalization including data type conversions, default value population,
valid/invalid row separation, and row count artifact creation.
"""

from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
from core.normalization import Normalizer


class TestNormalizerInit:
    """Tests for Normalizer initialization."""

    def test_init_stores_parameters(self):
        """Test that initialization stores all parameters."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        assert normalizer.file_path == "bucket/2025-01-01/person.parquet"
        assert normalizer.cdm_version == "5.4"
        assert normalizer.date_format == "%Y-%m-%d"
        assert normalizer.datetime_format == "%Y-%m-%d %H:%M:%S"

    def test_init_computes_derived_attributes(self):
        """Test that initialization computes table_name, bucket, and delivery_date."""
        normalizer = Normalizer(
            file_path="test-bucket/2025-01-15/observation.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        assert normalizer.table_name == "observation"
        assert normalizer.bucket == "test-bucket"
        assert normalizer.delivery_date == "2025-01-15"

    def test_init_schemas_are_none(self):
        """Test that schema and columns are not loaded during init."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        assert normalizer._schema is None
        assert normalizer._actual_columns is None


class TestNormalizerNormalize:
    """Tests for normalize orchestration method."""

    @patch.object(Normalizer, '_create_row_count_artifacts')
    @patch.object(Normalizer, '_handle_missing_person_ids')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.Normalizer.generate_normalization_sql')
    @patch.object(Normalizer, '_get_actual_columns')
    @patch.object(Normalizer, '_get_schema')
    def test_normalize_executes_sql_and_creates_artifacts(
        self, mock_get_schema, mock_get_cols, mock_gen_sql, mock_execute, mock_handle_missing, mock_create_artifacts
    ):
        """Test that normalize executes SQL and creates artifacts when SQL exists."""
        mock_get_schema.return_value = {'person': {'columns': {}}}
        mock_get_cols.return_value = []
        mock_gen_sql.return_value = "CREATE TABLE test;"

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer.normalize()

        mock_gen_sql.assert_called_once()
        mock_execute.assert_called_once_with(
            "CREATE TABLE test;",
            "Unable to normalize Parquet file bucket/2025-01-01/person.parquet"
        )
        mock_handle_missing.assert_called_once()
        mock_create_artifacts.assert_called_once()

    @patch.object(Normalizer, '_create_row_count_artifacts')
    @patch.object(Normalizer, '_handle_missing_person_ids')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.Normalizer.generate_normalization_sql')
    @patch.object(Normalizer, '_get_actual_columns')
    @patch.object(Normalizer, '_get_schema')
    def test_normalize_skips_when_no_sql(
        self, mock_get_schema, mock_get_cols, mock_gen_sql, mock_execute, mock_handle_missing, mock_create_artifacts
    ):
        """Test that normalize skips execution when no SQL generated."""
        mock_get_schema.return_value = {}
        mock_get_cols.return_value = []
        mock_gen_sql.return_value = ""

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/unknown_table.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer.normalize()

        mock_gen_sql.assert_called_once()
        mock_execute.assert_not_called()
        mock_handle_missing.assert_not_called()
        mock_create_artifacts.assert_not_called()

    @patch.object(Normalizer, '_create_row_count_artifacts')
    @patch.object(Normalizer, '_handle_missing_person_ids')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.Normalizer.generate_normalization_sql')
    @patch.object(Normalizer, '_get_actual_columns')
    @patch.object(Normalizer, '_get_schema')
    def test_normalize_calls_in_correct_order(
        self, mock_get_schema, mock_get_cols, mock_gen_sql, mock_execute, mock_handle_missing, mock_create_artifacts
    ):
        """Test that SQL generation, execution, missing person_id handling, and artifact creation happen in order."""
        mock_get_schema.return_value = {'person': {'columns': {}}}
        mock_get_cols.return_value = []
        call_order = []
        mock_gen_sql.side_effect = lambda *args, **kwargs: (call_order.append('generate'), "CREATE TABLE test;")[1]
        mock_execute.side_effect = lambda *args: call_order.append('execute')
        mock_handle_missing.side_effect = lambda: call_order.append('handle_missing')
        mock_create_artifacts.side_effect = lambda: call_order.append('artifacts')

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer.normalize()

        assert call_order == ['generate', 'execute', 'handle_missing', 'artifacts']


class TestNormalizerGenerateNormalizationSQL:
    """Tests for generate_normalization_sql method."""

    def test_returns_empty_for_unknown_table(self):
        """Test that empty string returned when table not in schema."""
        schema = {}
        actual_columns = []

        sql = Normalizer.generate_normalization_sql(
            file_path="bucket/2025-01-01/unknown.parquet",
            table_name="unknown",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S",
            schema=schema,
            actual_columns=actual_columns
        )

        assert sql == ""

    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_invalid_rows_path_from_path')
    @patch('core.normalization.utils.get_primary_key_column')
    def test_generates_sql_for_valid_table(
        self, mock_get_pk, mock_invalid_path, mock_get_uri
    ):
        """Test that SQL is generated for valid OMOP table."""
        schema = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'},
                    'gender_concept_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        actual_columns = ['person_id', 'gender_concept_id']
        mock_get_pk.return_value = 'person_id'
        mock_invalid_path.return_value = 'bucket/2025-01-01/invalid_person.parquet'
        mock_get_uri.side_effect = lambda x: f"gs://{x}"

        sql = Normalizer.generate_normalization_sql(
            file_path="bucket/2025-01-01/person.parquet",
            table_name="person",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S",
            schema=schema,
            actual_columns=actual_columns
        )

        assert "CREATE OR REPLACE TABLE row_check" in sql
        assert "COPY (" in sql
        assert "row_hash" in sql


class TestNormalizerGenerateColumnExpressions:
    """Tests for generate_column_expressions method."""

    def test_generates_expressions_for_existing_columns(self):
        """Test that expressions generated for columns present in file."""
        columns = {
            'person_id': {'type': 'BIGINT', 'required': 'True'},
            'gender_concept_id': {'type': 'BIGINT', 'required': 'False'}
        }
        ordered_columns = ['person_id', 'gender_concept_id']
        actual_columns = ['person_id', 'gender_concept_id']

        coalesce_exprs, row_validity = Normalizer.generate_column_expressions(
            table_name="person",
            columns=columns,
            ordered_omop_columns=ordered_columns,
            actual_columns=actual_columns,
            connect_id_column_name="",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        assert len(coalesce_exprs) == 2
        # Required column should be in row_validity
        assert len(row_validity) == 1

    def test_generates_placeholders_for_missing_columns(self):
        """Test that placeholder expressions generated for missing columns."""
        columns = {
            'person_id': {'type': 'BIGINT', 'required': 'True'},
            'year_of_birth': {'type': 'INTEGER', 'required': 'False'}
        }
        ordered_columns = ['person_id', 'year_of_birth']
        # Only person_id exists in file
        actual_columns = ['person_id']

        coalesce_exprs, _ = Normalizer.generate_column_expressions(
            table_name="person",
            columns=columns,
            ordered_omop_columns=ordered_columns,
            actual_columns=actual_columns,
            connect_id_column_name="",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        # Should have expression for person_id and placeholder for year_of_birth
        assert len(coalesce_exprs) == 2
        assert "year_of_birth" in coalesce_exprs[1]


class TestNormalizerGenerateColumnCastExpression:
    """Tests for generate_column_cast_expression method."""

    def test_date_type_expression(self):
        """Test cast expression for DATE type columns."""
        from pathlib import Path

        result = Normalizer.generate_column_cast_expression(
            column_name="birth_date",
            column_type="DATE",
            default_value="'1970-01-01'",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_column_cast_expression_date.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()

    def test_datetime_type_expression(self):
        """Test cast expression for DATETIME type columns."""
        from pathlib import Path

        result = Normalizer.generate_column_cast_expression(
            column_name="visit_start_datetime",
            column_type="DATETIME",
            default_value="'1970-01-01 00:00:00'",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_column_cast_expression_datetime.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()

    def test_required_field_expression(self):
        """Test cast expression for required fields with default values."""
        from pathlib import Path

        result = Normalizer.generate_column_cast_expression(
            column_name="person_id",
            column_type="BIGINT",
            default_value="'-1'",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_column_cast_expression_required.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()

    def test_optional_field_expression(self):
        """Test cast expression for optional fields."""
        from pathlib import Path

        result = Normalizer.generate_column_cast_expression(
            column_name="day_of_birth",
            column_type="INTEGER",
            default_value="NULL",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_column_cast_expression_optional.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()


class TestNormalizerGeneratePrimaryKeyClause:
    """Tests for generate_primary_key_clause method."""

    @patch('core.normalization.utils.get_primary_key_column')
    def test_generates_clause_for_surrogate_key_table(self, mock_get_pk):
        """Test that REPLACE clause generated for surrogate key tables."""
        from pathlib import Path

        mock_get_pk.return_value = 'condition_occurrence_id'

        ordered_columns = ['condition_occurrence_id', 'person_id', 'condition_concept_id']
        result = Normalizer.generate_primary_key_clause(
            table_name="condition_occurrence",
            ordered_omop_columns=ordered_columns,
            cdm_version="5.4"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_primary_key_clause_surrogate.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()

    def test_returns_empty_for_non_surrogate_key_table(self):
        """Test that empty string returned for non-surrogate key tables."""
        from pathlib import Path

        ordered_columns = ['person_id', 'gender_concept_id']
        result = Normalizer.generate_primary_key_clause(
            table_name="person",
            ordered_omop_columns=ordered_columns,
            cdm_version="5.4"
        )

        reference_path = Path(__file__).parent / "reference" / "sql" / "normalization" / "generate_primary_key_clause_non_surrogate.sql"
        with open(reference_path, 'r') as f:
            expected = f.read()

        assert result.strip() == expected.strip()


class TestNormalizerCreateRowCountArtifacts:
    """Tests for _create_row_count_artifacts method."""

    @patch('core.normalization.report_artifact.ReportArtifact')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_invalid_rows_path_from_path')
    @patch('core.normalization.utils.get_parquet_artifact_location')
    @patch('core.normalization.utils.get_cdm_schema')
    def test_creates_artifacts_for_valid_and_invalid_rows(
        self, mock_get_schema, mock_get_valid_path, mock_get_invalid_path,
        mock_get_uri, mock_execute, mock_artifact
    ):
        """Test that artifacts created for both valid and invalid row counts."""
        mock_get_schema.return_value = {
            'person': {'concept_id': 123456}
        }
        mock_get_valid_path.return_value = 'bucket/2025-01-01/person.parquet'
        mock_get_invalid_path.return_value = 'bucket/2025-01-01/invalid_person.parquet'
        mock_get_uri.side_effect = lambda x: f"gs://{x}"
        mock_execute.return_value = [[100]]  # Mock row count
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer._create_row_count_artifacts()

        # Should create 2 artifacts (valid + invalid)
        assert mock_artifact.call_count == 2
        assert mock_artifact_instance.save_artifact.call_count == 2

        # Check artifact parameters
        valid_artifact_call = mock_artifact.call_args_list[0]
        assert valid_artifact_call.kwargs['name'] == "Valid row count: person"
        assert valid_artifact_call.kwargs['value_as_number'] == 100


class TestNormalizerHelpers:
    """Tests for helper methods."""

    @patch('core.normalization.utils.get_table_schema')
    def test_get_schema_caches_result(self, mock_get_schema):
        """Test that schema is loaded once and cached."""
        mock_get_schema.return_value = {'person': {'columns': {}}}

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        # Call twice
        result1 = normalizer._get_schema()
        result2 = normalizer._get_schema()

        # Should only load once
        mock_get_schema.assert_called_once_with("person", "5.4")
        assert result1 is result2

    @patch('core.normalization.utils.get_columns_from_file')
    def test_get_actual_columns_caches_result(self, mock_get_columns):
        """Test that actual columns are loaded once and cached."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        # Call twice
        result1 = normalizer._get_actual_columns()
        result2 = normalizer._get_actual_columns()

        # Should only load once
        mock_get_columns.assert_called_once_with("bucket/2025-01-01/person.parquet")
        assert result1 is result2

    def test_find_connect_id_column_finds_connectid(self):
        """Test finding Connect_ID column with various naming patterns."""
        actual_columns = ['person_id', 'ConnectID', 'gender_concept_id']

        result = Normalizer._find_connect_id_column(actual_columns)

        assert result == 'ConnectID'

    def test_find_connect_id_column_finds_connect_id(self):
        """Test finding Connect_ID column with underscore."""
        actual_columns = ['person_id', 'connect_id', 'gender_concept_id']

        result = Normalizer._find_connect_id_column(actual_columns)

        assert result == 'connect_id'

    def test_find_connect_id_column_returns_empty_when_not_found(self):
        """Test that empty string returned when Connect_ID not found."""
        actual_columns = ['person_id', 'gender_concept_id']

        result = Normalizer._find_connect_id_column(actual_columns)

        assert result == ""


class TestNormalizerHandleMissingPersonIds:
    """Tests for _handle_missing_person_ids method."""

    @patch('core.normalization.report_artifact.ReportArtifact')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_cdm_schema')
    @patch('core.normalization.utils.get_table_schema')
    def test_removes_rows_with_missing_person_id_for_person_table(
        self, mock_get_schema, mock_get_cdm_schema, mock_get_uri, mock_execute, mock_artifact
    ):
        """Test that rows with person_id = -1 are removed from person table."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        mock_get_cdm_schema.return_value = {
            'person': {'concept_id': 123456}
        }
        mock_get_uri.side_effect = lambda x: f"gs://{x}"
        # First call returns count of 5, second call executes the filter
        mock_execute.side_effect = [[[5]], None]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer._handle_missing_person_ids()

        # Should execute count query and filter query
        assert mock_execute.call_count == 2

        # Verify count query
        count_call = mock_execute.call_args_list[0]
        assert "WHERE person_id = -1" in count_call[0][0]

        # Verify filter query
        filter_call = mock_execute.call_args_list[1]
        assert "WHERE person_id != -1" in filter_call[0][0]
        assert "COPY" in filter_call[0][0]

        # Verify artifact created with correct name for person table
        mock_artifact.assert_called_once()
        artifact_call = mock_artifact.call_args
        assert artifact_call.kwargs['name'] == "Number of persons with missing person_id"
        assert artifact_call.kwargs['value_as_number'] == 5
        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.normalization.report_artifact.ReportArtifact')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_cdm_schema')
    @patch('core.normalization.utils.get_table_schema')
    def test_removes_rows_with_missing_person_id_for_other_tables(
        self, mock_get_schema, mock_get_cdm_schema, mock_get_uri, mock_execute, mock_artifact
    ):
        """Test that rows with person_id = -1 are removed from non-person tables."""
        mock_get_schema.return_value = {
            'condition_occurrence': {
                'columns': {
                    'condition_occurrence_id': {'type': 'BIGINT', 'required': 'True'},
                    'person_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        mock_get_cdm_schema.return_value = {
            'condition_occurrence': {'concept_id': 789012}
        }
        mock_get_uri.side_effect = lambda x: f"gs://{x}"
        mock_execute.side_effect = [[[3]], None]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/condition_occurrence.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer._handle_missing_person_ids()

        # Verify artifact created with correct name for non-person table
        mock_artifact.assert_called_once()
        artifact_call = mock_artifact.call_args
        assert artifact_call.kwargs['name'] == "Number of rows removed due to missing person_id values: condition_occurrence"
        assert artifact_call.kwargs['value_as_number'] == 3
        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.normalization.report_artifact.ReportArtifact')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_cdm_schema')
    @patch('core.normalization.utils.get_table_schema')
    def test_creates_artifact_even_when_no_missing_person_ids(
        self, mock_get_schema, mock_get_cdm_schema, mock_get_uri, mock_execute, mock_artifact
    ):
        """Test that artifact is created even when no rows have missing person_id."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        mock_get_cdm_schema.return_value = {
            'person': {'concept_id': 123456}
        }
        mock_get_uri.side_effect = lambda x: f"gs://{x}"
        # Count returns 0 - no missing rows
        mock_execute.return_value = [[0]]
        mock_artifact_instance = MagicMock()
        mock_artifact.return_value = mock_artifact_instance

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer._handle_missing_person_ids()

        # Should only execute count query (no filter needed)
        assert mock_execute.call_count == 1

        # Verify artifact still created with 0 count
        mock_artifact.assert_called_once()
        artifact_call = mock_artifact.call_args
        assert artifact_call.kwargs['value_as_number'] == 0
        mock_artifact_instance.save_artifact.assert_called_once()

    @patch('core.normalization.utils.get_table_schema')
    def test_skips_tables_without_person_id_column(self, mock_get_schema):
        """Test that method returns early for tables without person_id column."""
        mock_get_schema.return_value = {
            'location': {
                'columns': {
                    'location_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/location.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        # Should not raise an error and should return early
        normalizer._handle_missing_person_ids()

        # Schema was loaded to check columns
        mock_get_schema.assert_called_once()

    @patch('core.normalization.utils.get_table_schema')
    def test_skips_when_table_not_in_schema(self, mock_get_schema):
        """Test that method returns early when table not found in schema."""
        mock_get_schema.return_value = {}

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/unknown_table.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        # Should not raise an error and should return early
        normalizer._handle_missing_person_ids()

    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_table_schema')
    def test_raises_exception_on_error(self, mock_get_schema, mock_get_uri, mock_execute):
        """Test that exceptions are properly raised when SQL execution fails."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        mock_get_uri.side_effect = lambda x: f"gs://{x}"
        mock_execute.side_effect = Exception("Database error")

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        with pytest.raises(Exception) as exc_info:
            normalizer._handle_missing_person_ids()

        assert "Unable to handle missing person_id values" in str(exc_info.value)


class TestNormalizerStaticMethods:
    """Tests for static methods."""

    def test_generate_birth_datetime_expression_with_column(self):
        """Test birth_datetime SQL generation when column exists."""
        sql = Normalizer.generate_birth_datetime_sql_expression(
            "%Y-%m-%d %H:%M:%S", column_exists_in_file=True
        )

        assert "COALESCE" in sql
        assert "TRY_STRPTIME" in sql
        assert "birth_datetime" in sql
        assert "year_of_birth" in sql

    def test_generate_birth_datetime_expression_without_column(self):
        """Test birth_datetime SQL generation when column doesn't exist."""
        sql = Normalizer.generate_birth_datetime_sql_expression(
            "%Y-%m-%d %H:%M:%S", column_exists_in_file=False
        )

        assert "TRY_STRPTIME" not in sql
        assert "year_of_birth" in sql
        assert "month_of_birth" in sql
        assert "day_of_birth" in sql

    def test_generate_row_count_sql(self):
        """Test row count SQL generation."""
        sql = Normalizer.generate_row_count_sql("gs://bucket/file.parquet")

        assert "SELECT COUNT(*)" in sql
        assert "read_parquet('gs://bucket/file.parquet')" in sql
