"""
Unit tests for transformer.py Transformer class.

Tests OMOP-to-OMOP ETL transformations including column mapping, type conversion,
composite key generation for surrogate key tables, and placeholder replacement.
"""

from unittest.mock import MagicMock, mock_open, patch

import pytest

import core.constants as constants
from core.transformer import Transformer


class TestTransformerInit:
    """Tests for Transformer initialization."""

    def test_init_stores_all_parameters(self):
        """Test that initialization stores all parameters correctly."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        assert transformer.site == "test_site"
        assert transformer.file_path == "gs://bucket/2025-01-01/"
        assert transformer.cdm_version == "5.4"
        assert transformer.source_table == "condition_occurrence"
        assert transformer.target_table == "observation"
        assert transformer.etl_artifact_path == "gs://bucket/2025-01-01/artifacts/omop_etl/"


class TestTransformerGetTransformedPath:
    """Tests for get_transformed_path method."""

    def test_get_transformed_path_returns_correct_structure(self):
        """Test that transformed path follows expected structure."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.get_transformed_path()

        assert "observation" in result
        assert "parts" in result
        assert "observation_from_condition_occurrence" in result
        assert result.endswith(".parquet")

    def test_get_transformed_path_uses_etl_artifact_path(self):
        """Test that transformed path uses the ETL artifact path."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/",
            cdm_version="5.4",
            source_table="measurement",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.get_transformed_path()

        assert result.startswith("gs://bucket/2025-01-01/artifacts/omop_etl/")


class TestTransformerPlaceholderToFilePath:
    """Tests for placeholder_to_file_path method."""

    def test_placeholder_to_file_path_replaces_placeholders(self):
        """Test that placeholders are replaced with file paths."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        sql = "SELECT * FROM read_parquet('@CONDITION_OCCURRENCE')"
        result = transformer.placeholder_to_file_path(sql)

        assert "@CONDITION_OCCURRENCE" not in result
        assert "bucket/2025-01-01/harmonized/*" in result
        assert ".parquet" in result

    def test_placeholder_to_file_path_handles_multiple_placeholders(self):
        """Test that multiple placeholders are all replaced."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        sql = """
            SELECT * FROM read_parquet('@CONDITION_OCCURRENCE')
            JOIN read_parquet('@DRUG_EXPOSURE') ON x = y
        """
        result = transformer.placeholder_to_file_path(sql)

        assert "@CONDITION_OCCURRENCE" not in result
        assert "@DRUG_EXPOSURE" not in result
        assert result.count("bucket/2025-01-01/harmonized/*") >= 2

    def test_placeholder_to_file_path_no_placeholders(self):
        """Test that SQL without placeholders remains unchanged."""
        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        sql = "SELECT * FROM some_table"
        result = transformer.placeholder_to_file_path(sql)

        assert result == sql


class TestTransformerOmopToOmopEtl:
    """Tests for omop_to_omop_etl method."""

    @patch('core.transformer.utils.execute_duckdb_sql')
    @patch.object(Transformer, 'generate_omop_to_omop_sql')
    def test_omop_to_omop_etl_executes_sql(self, mock_generate_sql, mock_execute):
        """Test that ETL executes the generated SQL."""
        mock_generate_sql.return_value = "SELECT * FROM table"

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        transformer.omop_to_omop_etl()

        mock_generate_sql.assert_called_once()
        mock_execute.assert_called_once_with(
            "SELECT * FROM table",
            "Unable to execute OMOP ETL SQL transformation"
        )

    @patch('core.transformer.utils.execute_duckdb_sql')
    @patch.object(Transformer, 'generate_omop_to_omop_sql')
    def test_omop_to_omop_etl_handles_exceptions(self, mock_generate_sql, mock_execute):
        """Test that ETL propagates exceptions from SQL execution."""
        mock_generate_sql.return_value = "SELECT * FROM table"
        mock_execute.side_effect = Exception("SQL execution failed")

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        with pytest.raises(Exception) as exc_info:
            transformer.omop_to_omop_etl()

        assert "SQL execution failed" in str(exc_info.value)


class TestTransformerGenerateOmopToOmopSql:
    """Tests for generate_omop_to_omop_sql method - basic functionality."""

    @patch('core.transformer.utils.get_primary_key_column')
    @patch('core.transformer.utils.get_table_schema')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_sql_reads_template_file(self, mock_file, mock_get_schema, mock_get_pk):
        """Test that SQL generation reads the template file."""
        mock_file.return_value.read.return_value = """
SELECT
    condition_occurrence_id AS observation_id,
    person_id AS person_id
FROM read_parquet('@CONDITION_OCCURRENCE')
"""
        mock_get_schema.return_value = {
            "observation": {
                "columns": {
                    "observation_id": {"type": "BIGINT", "required": "True"},
                    "person_id": {"type": "BIGINT", "required": "True"}
                }
            }
        }
        mock_get_pk.return_value = "observation_id"

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()

        # Verify template file was opened
        mock_file.assert_called_once()
        assert "condition_occurrence_to_observation.sql" in str(mock_file.call_args)

    @patch('core.transformer.utils.get_primary_key_column')
    @patch('core.transformer.utils.get_table_schema')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_sql_wraps_in_copy_statement(self, mock_file, mock_get_schema, mock_get_pk):
        """Test that generated SQL is wrapped in COPY statement."""
        mock_file.return_value.read.return_value = """
SELECT
    condition_occurrence_id AS observation_id
FROM read_parquet('@CONDITION_OCCURRENCE')
"""
        mock_get_schema.return_value = {
            "observation": {
                "columns": {
                    "observation_id": {"type": "BIGINT", "required": "True"}
                }
            }
        }
        mock_get_pk.return_value = "observation_id"

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()

        assert "COPY (" in result
        assert ") TO" in result
        assert "WHERE target_table = 'observation'" in result

    @patch('core.transformer.utils.get_primary_key_column')
    @patch('core.transformer.utils.get_table_schema')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_sql_adds_cast_for_required_fields(self, mock_file, mock_get_schema, mock_get_pk):
        """Test that required fields get COALESCE and CAST."""
        mock_file.return_value.read.return_value = """
SELECT
    person_id AS person_id
FROM read_parquet('@CONDITION_OCCURRENCE')
"""
        mock_get_schema.return_value = {
            "observation": {
                "columns": {
                    "person_id": {"type": "BIGINT", "required": "True"}
                }
            }
        }
        mock_get_pk.return_value = "observation_id"

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()

        assert "CAST(COALESCE(person_id," in result
        assert "AS BIGINT)" in result

    @patch('core.transformer.utils.get_primary_key_column')
    @patch('core.transformer.utils.get_table_schema')
    @patch('builtins.open', new_callable=mock_open)
    def test_generate_sql_adds_try_cast_for_optional_fields(self, mock_file, mock_get_schema, mock_get_pk):
        """Test that optional fields get TRY_CAST."""
        mock_file.return_value.read.return_value = """
        SELECT
            value_as_number AS value_as_number
        FROM read_parquet('@OBSERVATION')
        """
        mock_get_schema.return_value = {
            "measurement": {
                "columns": {
                    "value_as_number": {"type": "DOUBLE", "required": "False"}
                }
            }
        }
        mock_get_pk.return_value = "measurement_id"

        transformer = Transformer(
            site="test_site",
            file_path="gs://bucket/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="observation",
            target_table="measurement",
            etl_artifact_path="gs://bucket/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()

        assert "TRY_CAST(value_as_number AS DOUBLE)" in result
        # Should NOT have COALESCE for optional fields
        assert "COALESCE(value_as_number" not in result


class TestTransformerGenerateOmopToOmopSqlGoldenFiles:
    """Golden file tests for generate_omop_to_omop_sql method."""

    from pathlib import Path

    # Path to reference SQL files
    REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "transformer"

    @staticmethod
    def normalize_sql(sql: str) -> str:
        """
        Normalize SQL for comparison by removing extra whitespace.
        Makes SQL comparison whitespace-insensitive.
        """
        lines = [line.strip() for line in sql.strip().split('\n')]
        lines = [line for line in lines if line]
        return '\n'.join(lines)

    def load_reference_sql(self, filename: str) -> str:
        """Load reference SQL from file."""
        filepath = self.REFERENCE_DIR / filename
        with open(filepath, 'r') as f:
            return f.read()

    def test_surrogate_key_table_with_composite_key_generation(self):
        """Test transformation to surrogate key table (measurement) with composite key generation."""
        transformer = Transformer(
            site="test_site",
            file_path="synthea53/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="observation",
            target_table="measurement",
            etl_artifact_path="synthea53/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()
        expected = self.load_reference_sql("generate_omop_to_omop_sql_observation_to_measurement.sql")

        assert self.normalize_sql(result) == self.normalize_sql(expected)

    def test_natural_key_table_without_composite_key_generation(self):
        """Test transformation to natural key table (visit_occurrence) without composite key generation."""
        transformer = Transformer(
            site="test_site",
            file_path="synthea53/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="visit_occurrence",
            etl_artifact_path="synthea53/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()
        expected = self.load_reference_sql("generate_omop_to_omop_sql_condition_occurrence_to_visit_occurrence.sql")

        assert self.normalize_sql(result) == self.normalize_sql(expected)

    def test_surrogate_key_to_surrogate_key_transformation(self):
        """Test transformation between two surrogate key tables."""
        transformer = Transformer(
            site="test_site",
            file_path="synthea53/2025-01-01/harmonized/",
            cdm_version="5.4",
            source_table="condition_occurrence",
            target_table="observation",
            etl_artifact_path="synthea53/2025-01-01/artifacts/omop_etl/"
        )

        result = transformer.generate_omop_to_omop_sql()
        expected = self.load_reference_sql("generate_omop_to_omop_sql_condition_occurrence_to_observation.sql")

        assert self.normalize_sql(result) == self.normalize_sql(expected)
