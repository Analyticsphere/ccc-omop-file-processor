"""
Unit tests for normalization.py Normalizer class.

Tests file normalization including data type conversions, default value population,
valid/invalid row separation, and row count artifact creation.
"""

from unittest.mock import MagicMock, patch, call

import pytest

from core.normalization import Normalizer
import core.constants as constants


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
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch.object(Normalizer, '_generate_normalization_sql')
    def test_normalize_executes_sql_and_creates_artifacts(
        self, mock_gen_sql, mock_execute, mock_create_artifacts
    ):
        """Test that normalize executes SQL and creates artifacts when SQL exists."""
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
        mock_create_artifacts.assert_called_once()

    @patch.object(Normalizer, '_create_row_count_artifacts')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch.object(Normalizer, '_generate_normalization_sql')
    def test_normalize_skips_when_no_sql(
        self, mock_gen_sql, mock_execute, mock_create_artifacts
    ):
        """Test that normalize skips execution when no SQL generated."""
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
        mock_create_artifacts.assert_not_called()

    @patch.object(Normalizer, '_create_row_count_artifacts')
    @patch('core.normalization.utils.execute_duckdb_sql')
    @patch.object(Normalizer, '_generate_normalization_sql')
    def test_normalize_calls_in_correct_order(
        self, mock_gen_sql, mock_execute, mock_create_artifacts
    ):
        """Test that SQL generation, execution, and artifact creation happen in order."""
        mock_gen_sql.return_value = "CREATE TABLE test;"
        call_order = []
        mock_gen_sql.side_effect = lambda: (call_order.append('generate'), "CREATE TABLE test;")[1]
        mock_execute.side_effect = lambda *args: call_order.append('execute')
        mock_create_artifacts.side_effect = lambda: call_order.append('artifacts')

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        normalizer.normalize()

        assert call_order == ['generate', 'execute', 'artifacts']


class TestNormalizerGenerateNormalizationSQL:
    """Tests for _generate_normalization_sql method."""

    @patch('core.normalization.utils.get_columns_from_file')
    @patch('core.normalization.utils.get_table_schema')
    def test_returns_empty_for_unknown_table(self, mock_get_schema, mock_get_columns):
        """Test that empty string returned when table not in schema."""
        mock_get_schema.return_value = {}

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/unknown.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        sql = normalizer._generate_normalization_sql()

        assert sql == ""

    @patch('core.normalization.storage.get_uri')
    @patch('core.normalization.utils.get_invalid_rows_path_from_path')
    @patch('core.normalization.utils.get_primary_key_column')
    @patch('core.normalization.utils.get_columns_from_file')
    @patch('core.normalization.utils.get_table_schema')
    def test_generates_sql_for_valid_table(
        self, mock_get_schema, mock_get_columns, mock_get_pk,
        mock_invalid_path, mock_get_uri
    ):
        """Test that SQL is generated for valid OMOP table."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'},
                    'gender_concept_id': {'type': 'BIGINT', 'required': 'True'}
                }
            }
        }
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']
        mock_get_pk.return_value = 'person_id'
        mock_invalid_path.return_value = 'bucket/2025-01-01/invalid_person.parquet'
        mock_get_uri.side_effect = lambda x: f"gs://{x}"

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        sql = normalizer._generate_normalization_sql()

        assert "CREATE OR REPLACE TABLE row_check" in sql
        assert "COPY (" in sql
        assert "row_hash" in sql


class TestNormalizerGenerateColumnExpressions:
    """Tests for _generate_column_expressions method."""

    @patch('core.normalization.utils.get_columns_from_file')
    @patch('core.normalization.utils.get_table_schema')
    def test_generates_expressions_for_existing_columns(self, mock_get_schema, mock_get_columns):
        """Test that expressions generated for columns present in file."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'},
                    'gender_concept_id': {'type': 'BIGINT', 'required': 'False'}
                }
            }
        }
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        columns = mock_get_schema.return_value['person']['columns']
        ordered_columns = ['person_id', 'gender_concept_id']
        actual_columns = mock_get_columns.return_value

        coalesce_exprs, row_validity = normalizer._generate_column_expressions(
            columns, ordered_columns, actual_columns, ""
        )

        assert len(coalesce_exprs) == 2
        # Required column should be in row_validity
        assert len(row_validity) == 1

    @patch('core.normalization.utils.get_columns_from_file')
    @patch('core.normalization.utils.get_table_schema')
    def test_generates_placeholders_for_missing_columns(self, mock_get_schema, mock_get_columns):
        """Test that placeholder expressions generated for missing columns."""
        mock_get_schema.return_value = {
            'person': {
                'columns': {
                    'person_id': {'type': 'BIGINT', 'required': 'True'},
                    'year_of_birth': {'type': 'INTEGER', 'required': 'False'}
                }
            }
        }
        # Only person_id exists in file
        mock_get_columns.return_value = ['person_id']

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        columns = mock_get_schema.return_value['person']['columns']
        ordered_columns = ['person_id', 'year_of_birth']
        actual_columns = mock_get_columns.return_value

        coalesce_exprs, _ = normalizer._generate_column_expressions(
            columns, ordered_columns, actual_columns, ""
        )

        # Should have expression for person_id and placeholder for year_of_birth
        assert len(coalesce_exprs) == 2
        assert "year_of_birth" in coalesce_exprs[1]


class TestNormalizerGenerateColumnCastExpression:
    """Tests for _generate_column_cast_expression method."""

    def test_date_type_expression(self):
        """Test cast expression for DATE type columns."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        expr = normalizer._generate_column_cast_expression(
            "birth_date", "DATE", "'1970-01-01'"
        )

        assert "TRY_STRPTIME" in expr
        assert "%Y-%m-%d" in expr
        assert "AS DATE" in expr

    def test_datetime_type_expression(self):
        """Test cast expression for DATETIME type columns."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/visit_occurrence.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        expr = normalizer._generate_column_cast_expression(
            "visit_start_datetime", "DATETIME", "'1901-01-01 00:00:00'"
        )

        assert "TRY_STRPTIME" in expr
        assert "%Y-%m-%d %H:%M:%S" in expr
        assert "AS DATETIME" in expr

    def test_required_field_expression(self):
        """Test cast expression for required fields with default values."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        expr = normalizer._generate_column_cast_expression(
            "person_id", "BIGINT", "'-1'"
        )

        assert "COALESCE" in expr
        assert "'-1'" in expr
        assert "AS BIGINT" in expr

    def test_optional_field_expression(self):
        """Test cast expression for optional fields."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        expr = normalizer._generate_column_cast_expression(
            "day_of_birth", "INTEGER", "NULL"
        )

        assert "TRY_CAST" in expr
        assert "AS INTEGER" in expr
        # Should not have COALESCE for NULL default
        assert "COALESCE" not in expr


class TestNormalizerGeneratePrimaryKeyClause:
    """Tests for _generate_primary_key_clause method."""

    @patch('core.normalization.utils.get_primary_key_column')
    def test_generates_clause_for_surrogate_key_table(self, mock_get_pk):
        """Test that REPLACE clause generated for surrogate key tables."""
        mock_get_pk.return_value = 'condition_occurrence_id'

        normalizer = Normalizer(
            file_path="bucket/2025-01-01/condition_occurrence.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        ordered_columns = ['condition_occurrence_id', 'person_id', 'condition_concept_id']
        clause = normalizer._generate_primary_key_clause(ordered_columns)

        assert "REPLACE" in clause
        assert "hash" in clause
        assert "condition_occurrence_id" in clause
        # Primary key should not be included in hash
        assert "person_id" in clause
        assert "condition_concept_id" in clause

    def test_returns_empty_for_non_surrogate_key_table(self):
        """Test that empty string returned for non-surrogate key tables."""
        normalizer = Normalizer(
            file_path="bucket/2025-01-01/person.parquet",
            cdm_version="5.4",
            date_format="%Y-%m-%d",
            datetime_format="%Y-%m-%d %H:%M:%S"
        )

        ordered_columns = ['person_id', 'gender_concept_id']
        clause = normalizer._generate_primary_key_clause(ordered_columns)

        assert clause == ""


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
