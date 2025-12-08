"""
Unit tests for vocab_harmonization.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/vocab_harmonization/
"""

import pytest
from pathlib import Path

from core.vocab_harmonization import VocabHarmonizer
import core.utils as utils


# Path to reference SQL files
REFERENCE_DIR = Path(__file__).parent / "reference" / "sql" / "vocab_harmonization"


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


class TestGenerateSourceTargetRemappingSql:
    """Tests for generate_source_target_remapping_sql()."""

    def test_standard_condition_occurrence(self):
        """Test SQL generation for source-to-target remapping with condition_occurrence table."""
        schema = utils.get_table_schema('condition_occurrence', '5.3')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_source_target_remapping_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='condition_source_concept_id',
            primary_key='condition_occurrence_id'
        )

        expected = load_reference_sql("generate_source_target_remapping_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCheckNewTargetsSql:
    """Tests for generate_check_new_targets_sql()."""

    def test_target_remap_mode(self):
        """Test SQL generation for TARGET_REMAP mode (Maps to relationships)."""
        schema = utils.get_table_schema('condition_occurrence', '5.3')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            primary_key_column='condition_occurrence_id',
            vocab_status_string='existing non-standard target remapped to standard code',
            mapping_relationships="'Maps to', 'Maps to value'",
            existing_files_where_clause=''
        )

        expected = load_reference_sql("generate_check_new_targets_sql_target_remap.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_target_replacement_mode(self):
        """Test SQL generation for TARGET_REPLACEMENT mode (Concept replaced by relationship)."""
        schema = utils.get_table_schema('condition_occurrence', '5.3')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            primary_key_column='condition_occurrence_id',
            vocab_status_string='existing non-standard target replaced with standard code',
            mapping_relationships="'Concept replaced by'",
            existing_files_where_clause=''
        )

        expected = load_reference_sql("generate_check_new_targets_sql_target_replacement.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateDomainTableCheckSql:
    """Tests for generate_domain_table_check_sql()."""

    def test_standard_domain_check(self):
        """Test SQL generation for domain table check."""
        schema = utils.get_table_schema('condition_occurrence', '5.3')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_domain_table_check_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='tbl.condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            existing_files_where_clause=''
        )

        expected = load_reference_sql("generate_domain_table_check_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)
