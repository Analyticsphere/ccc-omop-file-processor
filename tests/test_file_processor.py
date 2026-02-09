"""
Unit tests for file_processor.py FileProcessor class.

Tests file processing including CSV to Parquet conversion, Parquet file
processing, retry logic, and special handling for reserved keywords.
"""

from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
from core.file_processor import FileProcessor


class TestFileProcessorInit:
    """Tests for FileProcessor initialization."""

    def test_init_stores_parameters(self):
        """Test that initialization stores all parameters."""
        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        assert processor.file_path == "bucket/2025-01-01/person.csv"
        assert processor.file_type == constants.CSV

    def test_init_computes_derived_attributes(self):
        """Test that initialization computes output_path and table_name."""
        processor = FileProcessor(
            file_path="test-bucket/2025-01-15/observation.parquet",
            file_type=constants.PARQUET
        )

        assert processor.table_name == "observation"
        # Output path should be computed
        assert "observation" in processor.output_path


class TestFileProcessorProcess:
    """Tests for process orchestration method."""

    @patch.object(FileProcessor, '_process_csv')
    def test_process_routes_csv_files(self, mock_process_csv):
        """Test that CSV files are routed to _process_csv."""
        mock_process_csv.return_value = "output.parquet"

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor.process()

        mock_process_csv.assert_called_once()
        assert result == "output.parquet"

    @patch.object(FileProcessor, '_process_csv')
    def test_process_routes_csv_gz_files(self, mock_process_csv):
        """Test that CSV.GZ files are routed to _process_csv."""
        mock_process_csv.return_value = "output.parquet"

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv.gz",
            file_type=constants.CSV_GZ
        )

        result = processor.process()

        mock_process_csv.assert_called_once()
        assert result == "output.parquet"

    @patch.object(FileProcessor, '_process_parquet')
    def test_process_routes_parquet_files(self, mock_process_parquet):
        """Test that Parquet files are routed to _process_parquet."""
        mock_process_parquet.return_value = "output.parquet"

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.parquet",
            file_type=constants.PARQUET
        )

        result = processor.process()

        mock_process_parquet.assert_called_once()
        assert result == "output.parquet"

    def test_process_raises_for_invalid_file_type(self):
        """Test that invalid file types raise an exception."""
        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.txt",
            file_type=".txt"
        )

        with pytest.raises(Exception) as exc_info:
            processor.process()

        assert "Invalid source file format" in str(exc_info.value)


class TestFileProcessorProcessParquet:
    """Tests for _process_parquet method."""

    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.valid_parquet_file')
    def test_process_parquet_success(self, mock_valid, mock_get_columns, mock_execute):
        """Test successful Parquet file processing."""
        mock_valid.return_value = True
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.parquet",
            file_type=constants.PARQUET
        )

        result = processor._process_parquet()

        mock_valid.assert_called_once_with("bucket/2025-01-01/person.parquet")
        mock_get_columns.assert_called_once_with("bucket/2025-01-01/person.parquet")
        mock_execute.assert_called_once()
        assert result == processor.output_path

    @patch('core.file_processor.utils.valid_parquet_file')
    def test_process_parquet_invalid_file(self, mock_valid):
        """Test that invalid Parquet file raises exception."""
        mock_valid.return_value = False

        processor = FileProcessor(
            file_path="bucket/2025-01-01/invalid.parquet",
            file_type=constants.PARQUET
        )

        with pytest.raises(Exception) as exc_info:
            processor._process_parquet()

        assert "Invalid Parquet file" in str(exc_info.value)


class TestFileProcessorProcessCSV:
    """Tests for _process_csv method."""

    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_success_first_attempt(self, mock_get_columns, mock_execute, mock_encoding):
        """Test successful CSV conversion on first attempt."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor._process_csv()

        mock_get_columns.assert_called_once_with("bucket/2025-01-01/person.csv", encoding='utf-8')
        mock_execute.assert_called_once()
        assert result == processor.output_path

    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_retries_on_failure(self, mock_get_columns, mock_execute, mock_encoding):
        """Test that CSV conversion retries with permissive settings on failure."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        # First call fails, second succeeds
        mock_execute.side_effect = [Exception("Parse error"), None]

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor._process_csv()

        # Should be called twice (initial + retry)
        assert mock_execute.call_count == 2

        # Second call should have error handling options
        second_call_sql = mock_execute.call_args_list[1][0][0]
        assert "store_rejects=True" in second_call_sql or "ignore_errors=True" in second_call_sql

    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_raises_after_retry_fails(self, mock_get_columns, mock_execute, mock_encoding):
        """Test that exception is raised if retry also fails."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        # Both attempts fail
        mock_execute.side_effect = [Exception("Parse error"), Exception("Still failing")]

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        with pytest.raises(Exception) as exc_info:
            processor._process_csv()

        assert "Still failing" in str(exc_info.value)

    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_with_conversion_options(self, mock_get_columns, mock_execute, mock_encoding):
        """Test CSV processing with explicit conversion options."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor._process_csv(conversion_options=['parallel=False'])

        # Check that SQL includes the option
        sql = mock_execute.call_args[0][0]
        assert "parallel=False" in sql


class TestFileProcessorStaticMethods:
    """Tests for static methods."""

    def test_generate_process_incoming_parquet_sql_standard_columns(self):
        """Test Parquet SQL generation with standard columns."""
        sql = FileProcessor.generate_process_incoming_parquet_sql(
            file_path="bucket/2025-01-01/person.parquet",
            parquet_columns=["person_id", "gender_concept_id", "year_of_birth"]
        )

        assert "COPY (" in sql
        assert "read_parquet" in sql
        assert "CAST(person_id AS VARCHAR)" in sql
        assert "CAST(gender_concept_id AS VARCHAR)" in sql

    def test_generate_process_incoming_parquet_sql_offset_column(self):
        """Test Parquet SQL generation with offset reserved keyword."""
        sql = FileProcessor.generate_process_incoming_parquet_sql(
            file_path="bucket/2025-01-01/note_nlp.parquet",
            parquet_columns=["note_nlp_id", "offset", "snippet"]
        )

        assert '"offset"' in sql
        assert "note_nlp_id" in sql

    def test_generate_csv_to_parquet_sql_no_options(self):
        """Test CSV to Parquet SQL generation without options."""
        sql = FileProcessor.generate_csv_to_parquet_sql(
            file_path="bucket/2025-01-01/person.csv",
            csv_column_names=["person_id", "gender_concept_id"],
            conversion_options=[]
        )

        assert "COPY (" in sql
        assert "read_csv" in sql
        assert "null_padding=True" in sql
        assert "ALL_VARCHAR=True" in sql

    def test_generate_csv_to_parquet_sql_with_options(self):
        """Test CSV to Parquet SQL generation with conversion options."""
        sql = FileProcessor.generate_csv_to_parquet_sql(
            file_path="bucket/2025-01-01/person.csv",
            csv_column_names=["person_id", "gender_concept_id"],
            conversion_options=['store_rejects=True', 'ignore_errors=True']
        )

        assert "store_rejects=True" in sql
        assert "ignore_errors=True" in sql

    def test_format_list_empty(self):
        """Test format_list with empty list."""
        result = FileProcessor.format_list([])

        assert result == ''

    def test_format_list_single_item(self):
        """Test format_list with single item."""
        result = FileProcessor.format_list(['parallel=False'])

        assert result == ',parallel=False'

    def test_format_list_multiple_items(self):
        """Test format_list with multiple items."""
        result = FileProcessor.format_list(['store_rejects=True', 'ignore_errors=True', 'parallel=False'])

        assert result == ',store_rejects=True, ignore_errors=True, parallel=False'


class TestFileProcessorIntegration:
    """Integration tests for FileProcessor."""

    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.valid_parquet_file')
    def test_full_parquet_processing_flow(self, mock_valid, mock_get_columns, mock_execute):
        """Test complete Parquet processing flow from initialization to completion."""
        mock_valid.return_value = True
        mock_get_columns.return_value = ['person_id', 'gender_concept_id', 'year_of_birth']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.parquet",
            file_type=constants.PARQUET
        )

        result = processor.process()

        # Verify all steps executed
        assert mock_valid.called
        assert mock_get_columns.called
        assert mock_execute.called
        assert result == processor.output_path

    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_full_csv_processing_flow_with_retry(self, mock_get_columns, mock_execute, mock_encoding):
        """Test complete CSV processing flow with retry on failure."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        # Simulate failure then success
        mock_execute.side_effect = [Exception("Malformed CSV"), None]

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor.process()

        # Verify retry happened
        assert mock_execute.call_count == 2
        assert result == processor.output_path
