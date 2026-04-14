"""
Unit tests for file_processor.py FileProcessor class.

Tests file processing including CSV to Parquet conversion, Parquet file
processing, retry logic, and special handling for reserved keywords.
"""

from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

import core.constants as constants
from core.file_processor import FileProcessor

# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "file_processor"


def normalize_sql(sql: str) -> str:
    """
    Normalize SQL for comparison by removing extra whitespace.
    Makes SQL comparison whitespace-insensitive.
    """
    lines = [line.strip() for line in sql.strip().split('\n')]
    lines = [line for line in lines if line]
    return '\n'.join(lines)


def load_reference_sql(filename: str) -> str:
    """Load reference SQL from file."""
    filepath = REFERENCE_DIR / filename
    with open(filepath, 'r') as f:
        return f.read()


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

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.valid_parquet_file')
    def test_process_parquet_success(self, mock_valid, mock_get_columns, mock_execute, mock_cleanup):
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
        mock_cleanup.assert_called_once_with(processor.output_path)
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

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_success_first_attempt(self, mock_get_columns, mock_execute, mock_encoding, mock_cleanup):
        """Test successful CSV conversion on first attempt."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor._process_csv()

        mock_get_columns.assert_called_once_with("bucket/2025-01-01/person.csv", encoding='utf-8')
        mock_execute.assert_called_once()
        mock_cleanup.assert_called_once_with(processor.output_path)
        assert result == processor.output_path

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_retries_on_failure(self, mock_get_columns, mock_execute, mock_encoding, mock_cleanup):
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

        # Second call should match the retry golden file
        second_call_sql = mock_execute.call_args_list[1][0][0]
        expected = load_reference_sql("generate_csv_to_parquet_sql_retry.sql")
        assert normalize_sql(second_call_sql) == normalize_sql(expected)
        mock_cleanup.assert_called_once_with(processor.output_path)

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_raises_after_retry_fails(self, mock_get_columns, mock_execute, mock_encoding, mock_cleanup):
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
        mock_cleanup.assert_not_called()

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_process_csv_with_conversion_options(self, mock_get_columns, mock_execute, mock_encoding, mock_cleanup):
        """Test CSV processing with explicit conversion options."""
        mock_get_columns.return_value = ['person_id', 'gender_concept_id']

        processor = FileProcessor(
            file_path="bucket/2025-01-01/person.csv",
            file_type=constants.CSV
        )

        result = processor._process_csv(conversion_options=['parallel=False'])

        # Check that SQL matches golden file
        sql = mock_execute.call_args[0][0]
        expected = load_reference_sql("generate_csv_to_parquet_sql_with_parallel_false.sql")
        assert normalize_sql(sql) == normalize_sql(expected)
        mock_cleanup.assert_called_once_with(processor.output_path)


class TestFileProcessorNullStringCleanup:
    """Tests for rewriting literal string null markers in Parquet files."""

    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    def test_convert_parquet_string_nulls_to_null_success(
        self, mock_execute, mock_get_columns
    ):
        """Test successful Parquet null-string cleanup."""
        mock_get_columns.return_value = ['person_id', 'row_count']

        result = FileProcessor.convert_parquet_string_nulls_to_null(
            "bucket/2025-01-01/artifacts/converted_files/person.parquet"
        )

        mock_get_columns.assert_called_once_with(
            "bucket/2025-01-01/artifacts/converted_files/person.parquet"
        )
        sql = mock_execute.call_args[0][0]
        expected = load_reference_sql("convert_parquet_string_nulls_to_null_standard.sql")
        assert normalize_sql(sql) == normalize_sql(expected)
        assert result == "bucket/2025-01-01/artifacts/converted_files/person.parquet"

    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    def test_convert_parquet_string_nulls_to_null_quotes_offset_column(
        self, mock_execute, mock_get_columns
    ):
        """Test reserved keyword columns like offset are quoted in cleanup SQL."""
        mock_get_columns.return_value = ['note_nlp_id', 'offset', 'lexical_variant']

        FileProcessor.convert_parquet_string_nulls_to_null(
            "bucket/2025-01-01/artifacts/converted_files/note_nlp.parquet"
        )

        sql = mock_execute.call_args[0][0]
        expected = load_reference_sql("convert_parquet_string_nulls_to_null_offset_column.sql")
        assert normalize_sql(sql) == normalize_sql(expected)


class TestFileProcessorStaticMethods:
    """Tests for static methods."""

    def test_generate_process_incoming_parquet_sql_standard_columns(self):
        """Test Parquet SQL generation with standard columns."""
        sql = FileProcessor.generate_process_incoming_parquet_sql(
            file_path="bucket/2025-01-01/person.parquet",
            parquet_columns=["person_id", "gender_concept_id", "year_of_birth"]
        )

        expected = load_reference_sql("generate_process_incoming_parquet_sql_standard_columns_v2.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generate_process_incoming_parquet_sql_offset_column(self):
        """Test Parquet SQL generation with offset reserved keyword."""
        sql = FileProcessor.generate_process_incoming_parquet_sql(
            file_path="bucket/2025-01-01/note_nlp.parquet",
            parquet_columns=["note_nlp_id", "offset", "snippet"]
        )

        expected = load_reference_sql("generate_process_incoming_parquet_sql_offset_column.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generate_csv_to_parquet_sql_no_options(self):
        """Test CSV to Parquet SQL generation without options."""
        sql = FileProcessor.generate_csv_to_parquet_sql(
            file_path="bucket/2025-01-01/person.csv",
            csv_column_names=["person_id", "gender_concept_id"],
            conversion_options=[]
        )

        expected = load_reference_sql("generate_csv_to_parquet_sql_no_options.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_generate_csv_to_parquet_sql_with_options(self):
        """Test CSV to Parquet SQL generation with conversion options."""
        sql = FileProcessor.generate_csv_to_parquet_sql(
            file_path="bucket/2025-01-01/person.csv",
            csv_column_names=["person_id", "gender_concept_id"],
            conversion_options=['store_rejects=True', 'ignore_errors=True']
        )

        expected = load_reference_sql("generate_csv_to_parquet_sql_with_string_options.sql")
        assert normalize_sql(sql) == normalize_sql(expected)

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

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    @patch('core.file_processor.utils.valid_parquet_file')
    def test_full_parquet_processing_flow(self, mock_valid, mock_get_columns, mock_execute, mock_cleanup):
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
        mock_cleanup.assert_called_once_with(processor.output_path)
        assert result == processor.output_path

    @patch.object(FileProcessor, 'convert_parquet_string_nulls_to_null')
    @patch('core.file_processor.utils.get_csv_file_encoding', return_value='utf-8')
    @patch('core.file_processor.utils.execute_duckdb_sql')
    @patch('core.file_processor.utils.get_columns_from_file')
    def test_full_csv_processing_flow_with_retry(self, mock_get_columns, mock_execute, mock_encoding, mock_cleanup):
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
        mock_cleanup.assert_called_once_with(processor.output_path)
        assert result == processor.output_path
