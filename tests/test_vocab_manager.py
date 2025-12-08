"""
Unit tests for vocab_manager.py VocabularyManager class.

Tests vocabulary management including CSV to Parquet conversion,
optimized vocabulary file creation, and BigQuery loading.
"""

from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
from core.vocab_manager import VocabularyManager


class TestVocabularyManagerInit:
    """Tests for VocabularyManager initialization."""

    def test_init_stores_parameters(self):
        """Test that initialization stores all parameters."""
        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        assert manager.vocab_version == "v5.0_23-JAN-23"
        assert manager.vocab_path == "gs://vocab-bucket/vocab"

    def test_init_computes_derived_paths(self):
        """Test that initialization computes derived path attributes."""
        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        assert manager.vocab_root_path == "gs://vocab-bucket/vocab/v5.0_23-JAN-23/"
        assert "optimized" in manager.optimized_vocab_folder_path
        assert manager.optimized_vocab_folder_path.endswith("/")


class TestVocabularyManagerConvertToParquet:
    """Tests for convert_to_parquet method."""

    @patch('core.vocab_manager.utils.execute_duckdb_sql')
    @patch('core.vocab_manager.utils.get_columns_from_file')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.list_files')
    def test_convert_to_parquet_success(self, mock_list_files, mock_file_exists,
                                        mock_valid, mock_get_columns, mock_execute):
        """Test successful vocabulary CSV to Parquet conversion."""
        mock_list_files.return_value = ['CONCEPT.csv', 'CONCEPT_RELATIONSHIP.csv']
        mock_file_exists.return_value = False
        mock_get_columns.return_value = ['concept_id', 'concept_name', 'valid_start_date']

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        manager.convert_to_parquet()

        # Should call execute_duckdb_sql twice (once for each file)
        assert mock_execute.call_count == 2
        mock_list_files.assert_called_once()

    @patch('core.vocab_manager.utils.list_files')
    def test_convert_to_parquet_no_vocab_files(self, mock_list_files):
        """Test that exception is raised when no vocabulary files found."""
        mock_list_files.return_value = []

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        with pytest.raises(Exception) as exc_info:
            manager.convert_to_parquet()

        assert "Vocabulary path" in str(exc_info.value)
        assert "not found" in str(exc_info.value)

    @patch('core.vocab_manager.utils.execute_duckdb_sql')
    @patch('core.vocab_manager.utils.get_columns_from_file')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.list_files')
    def test_convert_to_parquet_skips_existing_valid_files(self, mock_list_files,
                                                           mock_file_exists, mock_valid,
                                                           mock_get_columns, mock_execute):
        """Test that existing valid parquet files are skipped."""
        mock_list_files.return_value = ['CONCEPT.csv']
        mock_file_exists.return_value = True
        mock_valid.return_value = True

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        manager.convert_to_parquet()

        # Should not call execute_duckdb_sql since file already exists and is valid
        mock_execute.assert_not_called()


class TestVocabularyManagerCreateOptimizedVocabFile:
    """Tests for create_optimized_vocab_file method."""

    @patch('core.vocab_manager.utils.execute_duckdb_sql')
    @patch('core.vocab_manager.storage.file_exists')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.get_optimized_vocab_file_path')
    def test_create_optimized_vocab_file_success(self, mock_get_path, mock_file_exists,
                                                 mock_valid, mock_storage_exists, mock_execute):
        """Test successful optimized vocabulary file creation."""
        mock_get_path.return_value = "gs://vocab-bucket/vocab/v5.0/optimized_vocab/optimized_vocab_file.parquet"
        mock_file_exists.return_value = False
        mock_valid.return_value = False
        mock_storage_exists.return_value = True

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        manager.create_optimized_vocab_file()

        mock_execute.assert_called_once()

    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.get_optimized_vocab_file_path')
    def test_create_optimized_vocab_file_skips_existing(self, mock_get_path, mock_file_exists):
        """Test that existing optimized vocab file is skipped."""
        mock_get_path.return_value = "gs://vocab-bucket/vocab/v5.0/optimized_vocab_file.parquet"
        mock_file_exists.return_value = True

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        # Should return early without error
        manager.create_optimized_vocab_file()

    @patch('core.vocab_manager.storage.file_exists')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.get_optimized_vocab_file_path')
    def test_create_optimized_vocab_file_concept_not_found(self, mock_get_path, mock_file_exists,
                                                           mock_valid, mock_storage_exists):
        """Test that exception is raised when concept file not found."""
        mock_get_path.return_value = "gs://vocab-bucket/vocab/v5.0/optimized_vocab_file.parquet"
        mock_file_exists.return_value = False
        mock_valid.return_value = False
        mock_storage_exists.return_value = False

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        with pytest.raises(Exception) as exc_info:
            manager.create_optimized_vocab_file()

        assert "Vocabulary path" in str(exc_info.value)
        assert "not found" in str(exc_info.value)


class TestVocabularyManagerLoadToBigQuery:
    """Tests for load_vocabulary_table_to_bq method."""

    @patch('core.vocab_manager.gcp_services.load_parquet_to_bigquery')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    def test_load_vocabulary_table_to_bq_success(self, mock_file_exists, mock_valid, mock_load):
        """Test successful vocabulary table load to BigQuery."""
        mock_file_exists.return_value = True
        mock_valid.return_value = True

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        manager.load_vocabulary_table_to_bq(
            table_file_name="concept",
            project_id="my-project",
            dataset_id="my-dataset"
        )

        mock_load.assert_called_once()
        call_args = mock_load.call_args
        assert "concept" in call_args[0][0]  # vocab_parquet_path
        assert call_args[0][1] == "my-project"
        assert call_args[0][2] == "my-dataset"
        assert call_args[0][3] == "concept"

    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    def test_load_vocabulary_table_to_bq_file_not_found(self, mock_file_exists, mock_valid):
        """Test that exception is raised when vocabulary table not found."""
        mock_file_exists.return_value = False

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        with pytest.raises(Exception) as exc_info:
            manager.load_vocabulary_table_to_bq(
                table_file_name="concept",
                project_id="my-project",
                dataset_id="my-dataset"
            )

        assert "not found" in str(exc_info.value)

    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    def test_load_vocabulary_table_to_bq_invalid_file(self, mock_file_exists, mock_valid):
        """Test that exception is raised when vocabulary table is invalid."""
        mock_file_exists.return_value = True
        mock_valid.return_value = False

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        with pytest.raises(Exception) as exc_info:
            manager.load_vocabulary_table_to_bq(
                table_file_name="concept",
                project_id="my-project",
                dataset_id="my-dataset"
            )

        assert "not found" in str(exc_info.value)


class TestVocabularyManagerStaticMethods:
    """Tests for static methods."""

    def test_generate_vocab_version_query_sql(self):
        """Test SQL generation for vocabulary version query."""
        sql = VocabularyManager.generate_vocab_version_query_sql(
            "gs://vocab-bucket/vocab/v5.0/optimized_vocab/vocabulary.parquet"
        )

        assert "SELECT vocabulary_version" in sql
        assert "FROM read_parquet" in sql
        assert "WHERE vocabulary_id = 'None'" in sql

    def test_generate_convert_vocab_sql_standard_columns(self):
        """Test CSV to Parquet SQL generation with standard columns."""
        sql = VocabularyManager.generate_convert_vocab_sql(
            csv_file_path="gs://vocab-bucket/vocab/v5.0/CONCEPT.csv",
            parquet_file_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
            csv_columns=["concept_id", "concept_name", "domain_id"]
        )

        assert "COPY (" in sql
        assert "read_csv" in sql
        assert "delim='\t'" in sql
        assert "concept_id" in sql
        assert "concept_name" in sql
        assert "domain_id" in sql

    def test_generate_convert_vocab_sql_with_date_columns(self):
        """Test CSV to Parquet SQL generation with date columns."""
        sql = VocabularyManager.generate_convert_vocab_sql(
            csv_file_path="gs://vocab-bucket/vocab/v5.0/CONCEPT.csv",
            parquet_file_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
            csv_columns=["concept_id", "valid_start_date", "valid_end_date"]
        )

        # Date columns should have special CAST handling
        assert "CAST(STRPTIME" in sql
        assert "valid_start_date" in sql
        assert "valid_end_date" in sql
        assert "%Y%m%d" in sql

    def test_generate_optimized_vocab_sql(self):
        """Test SQL generation for optimized vocabulary file creation."""
        sql = VocabularyManager.generate_optimized_vocab_sql(
            concept_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept.parquet",
            concept_relationship_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/concept_relationship.parquet",
            output_path="gs://vocab-bucket/vocab/v5.0/optimized_vocab/optimized_vocab_file.parquet"
        )

        assert "COPY (" in sql
        assert "SELECT DISTINCT" in sql
        assert "c1.concept_id AS concept_id" in sql
        assert "LEFT JOIN" in sql
        assert "concept_relationship" in sql
        assert "target_concept_id" in sql


class TestVocabularyManagerIntegration:
    """Integration tests for VocabularyManager."""

    @patch('core.vocab_manager.utils.execute_duckdb_sql')
    @patch('core.vocab_manager.utils.get_columns_from_file')
    @patch('core.vocab_manager.utils.valid_parquet_file')
    @patch('core.vocab_manager.utils.parquet_file_exists')
    @patch('core.vocab_manager.utils.list_files')
    def test_full_vocabulary_conversion_flow(self, mock_list_files, mock_file_exists,
                                             mock_valid, mock_get_columns, mock_execute):
        """Test complete vocabulary conversion flow from initialization to completion."""
        mock_list_files.return_value = ['CONCEPT.csv', 'VOCABULARY.csv']
        mock_file_exists.return_value = False
        mock_get_columns.side_effect = [
            ['concept_id', 'concept_name'],
            ['vocabulary_id', 'vocabulary_name']
        ]

        manager = VocabularyManager(
            vocab_version="v5.0_23-JAN-23",
            vocab_path="gs://vocab-bucket/vocab"
        )

        manager.convert_to_parquet()

        # Verify all steps executed
        assert mock_list_files.called
        assert mock_get_columns.call_count == 2
        assert mock_execute.call_count == 2
