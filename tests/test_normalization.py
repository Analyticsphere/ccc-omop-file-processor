import pytest

from core.normalization import get_birth_datetime_sql_expression


class TestBirthDatetimeSqlExpression:
    """Tests for the get_birth_datetime_sql_expression function."""

    @pytest.fixture
    def datetime_format(self):
        """Standard datetime format for testing."""
        return "%Y-%m-%d %H:%M:%S"

    def test_column_exists_in_file(self, datetime_format):
        """Test SQL generation when birth_datetime column exists in source file."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=True)

        # Should use COALESCE to try existing value first
        assert "COALESCE" in sql
        # Should try to parse existing birth_datetime value
        assert "TRY_STRPTIME(birth_datetime" in sql
        assert f"'{datetime_format}'" in sql
        # Should fall back to calculation from year/month/day
        assert "year_of_birth" in sql
        assert "month_of_birth" in sql
        assert "day_of_birth" in sql
        # Should alias as birth_datetime
        assert "AS birth_datetime" in sql

    def test_column_does_not_exist_in_file(self, datetime_format):
        """Test SQL generation when birth_datetime column does NOT exist in source file."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=False)

        # Should NOT try to parse existing value
        assert "TRY_STRPTIME" not in sql
        # Should directly calculate from year/month/day
        assert "year_of_birth" in sql
        assert "month_of_birth" in sql
        assert "day_of_birth" in sql
        # Should use LPAD for proper date formatting
        assert "LPAD" in sql
        # Should alias as birth_datetime
        assert "AS birth_datetime" in sql

    def test_default_values(self, datetime_format):
        """Test that proper default values are used for missing date components."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=False)

        # Default year to '1900' if missing (string literal because columns are VARCHAR at this stage)
        assert "COALESCE(year_of_birth, '1900')" in sql
        # Default month to '1' if missing (string literal)
        assert "COALESCE(month_of_birth, '1')" in sql
        # Default day to '1' if missing (string literal)
        assert "COALESCE(day_of_birth, '1')" in sql
        # Use midnight time (UTC)
        assert "00:00:00" in sql

    def test_output_format(self, datetime_format):
        """Test that SQL output has correct format and type casting."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=False)

        # Should use TRY_CAST for safe type conversion
        assert "TRY_CAST" in sql
        # Should cast to DATETIME type
        assert "AS DATETIME" in sql
        # Should alias result as birth_datetime
        assert "AS birth_datetime" in sql

    def test_padding_for_date_components(self, datetime_format):
        """Test that date components are properly padded to ensure YYYY-MM-DD format."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=False)

        # Year should be padded to 4 digits (columns are already VARCHAR at this stage)
        assert "LPAD(COALESCE(year_of_birth, '1900'), 4, '0')" in sql
        # Month should be padded to 2 digits
        assert "LPAD(COALESCE(month_of_birth, '1'), 2, '0')" in sql
        # Day should be padded to 2 digits
        assert "LPAD(COALESCE(day_of_birth, '1'), 2, '0')" in sql

    def test_concat_operation(self, datetime_format):
        """Test that date components are concatenated with proper separators."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=False)

        # Should concatenate with hyphens and space before time
        assert "CONCAT(" in sql
        # Verify format is YYYY-MM-DD HH:MM:SS
        assert "'-'" in sql  # Separators between date components
        assert "' 00:00:00'" in sql  # Space and time component

    @pytest.mark.parametrize(
        "column_exists,expected_in_sql,not_expected_in_sql",
        [
            (True, "TRY_STRPTIME", None),
            (False, "TRY_CAST", "TRY_STRPTIME"),
        ]
    )
    def test_conditional_logic(self, datetime_format, column_exists, expected_in_sql, not_expected_in_sql):
        """Test that SQL changes based on whether column exists in file."""
        sql = get_birth_datetime_sql_expression(datetime_format, column_exists_in_file=column_exists)

        if expected_in_sql:
            assert expected_in_sql in sql
        if not_expected_in_sql:
            assert not_expected_in_sql not in sql or column_exists  # Allow if column exists
