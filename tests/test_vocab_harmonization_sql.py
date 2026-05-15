"""
Unit tests for vocab_harmonization.py SQL generation functions.

Tests that SQL generation functions produce output matching reference SQL files.
Reference SQL files were captured from known-good function output and are stored
in tests/reference/sql/vocab_harmonization/
"""

from pathlib import Path

import pytest

import core.utils as utils
from core.utils import get_concept_id_source_pairs
from core.vocab_harmonization import VocabHarmonizer

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
        """Test complete SQL generation for source-to-target remapping including COPY statement."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_source_target_remapping_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='condition_source_concept_id',
            primary_key='condition_occurrence_id',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_source_target_remap.parquet'
        )

        expected = load_reference_sql("generate_source_target_remapping_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCheckNewTargetsSql:
    """Tests for generate_check_new_targets_sql()."""

    def test_target_remap_mode(self):
        """Test complete SQL generation for TARGET_REMAP mode with COPY and paths."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            primary_key_column='condition_occurrence_id',
            vocab_status_string='existing non-standard target remapped to standard code',
            mapping_relationships="'Maps to', 'Maps to value'",
            existing_files_where_clause='',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_target_remap.parquet'
        )

        expected = load_reference_sql("generate_check_new_targets_sql_target_remap.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_target_replacement_mode(self):
        """Test complete SQL generation for TARGET_REPLACEMENT mode with COPY and paths."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            primary_key_column='condition_occurrence_id',
            vocab_status_string='existing non-standard target replaced with standard code',
            mapping_relationships="'Concept replaced by'",
            existing_files_where_clause='',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_target_replacement.parquet'
        )

        expected = load_reference_sql("generate_check_new_targets_sql_target_replacement.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_target_remap_with_exclusion(self):
        """Test SQL generation for TARGET_REMAP mode includes NOT IN clause when exclusion provided."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        # Simulate the exclusion clause that would be generated
        exclusion_clause = """
                AND tbl.condition_occurrence_id NOT IN (
                    SELECT condition_occurrence_id FROM read_parquet('gs://synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                )
            """

        result = VocabHarmonizer.generate_check_new_targets_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            primary_key_column='condition_occurrence_id',
            vocab_status_string='existing non-standard target remapped to standard code',
            mapping_relationships="'Maps to', 'Maps to value'",
            existing_files_where_clause=exclusion_clause,
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_target_remap.parquet'
        )

        expected = load_reference_sql("generate_check_new_targets_sql_target_remap_with_exclusion.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateDomainTableCheckSql:
    """Tests for generate_domain_table_check_sql()."""

    def test_standard_domain_check(self):
        """Test complete SQL generation for domain table check including COPY statement."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        result = VocabHarmonizer.generate_domain_table_check_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='tbl.condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            existing_files_where_clause='',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_domain_check.parquet'
        )

        expected = load_reference_sql("generate_domain_table_check_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_domain_check_with_exclusion(self):
        """Test SQL generation for domain table check includes WHERE NOT IN clause when exclusion provided."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())

        # Simulate the exclusion clause that would be generated (note: use_and=False for domain check)
        exclusion_clause = """
                WHERE tbl.condition_occurrence_id NOT IN (
                    SELECT condition_occurrence_id FROM read_parquet('gs://synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                )
            """

        result = VocabHarmonizer.generate_domain_table_check_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            target_concept_id_column='tbl.condition_concept_id',
            source_concept_id_column='tbl.condition_source_concept_id',
            existing_files_where_clause=exclusion_clause,
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_domain_check.parquet'
        )

        expected = load_reference_sql("generate_domain_table_check_sql_with_exclusion.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCheckDuplicatesSql:
    """Tests for generate_check_duplicates_sql()."""

    def test_standard_check_duplicates(self):
        """Test SQL generation for checking duplicate primary keys."""
        result = VocabHarmonizer.generate_check_duplicates_sql(
            file_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet',
            primary_key_column='condition_occurrence_id'
        )

        expected = load_reference_sql("generate_check_duplicates_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCreateDuplicateKeysTableSql:
    """Tests for generate_create_duplicate_keys_table_sql()."""

    def test_standard_create_temp_table(self):
        """Test SQL generation for creating temp table with duplicate keys."""
        result = VocabHarmonizer.generate_create_duplicate_keys_table_sql(
            file_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet',
            primary_key_column='condition_occurrence_id'
        )

        expected = load_reference_sql("generate_create_duplicate_keys_table_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateCountDuplicatesSql:
    """Tests for generate_count_duplicates_sql()."""

    def test_standard_count_duplicates(self):
        """Test SQL generation for counting duplicate keys in temp table."""
        result = VocabHarmonizer.generate_count_duplicates_sql()

        expected = load_reference_sql("generate_count_duplicates_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateWriteNonDuplicatesSql:
    """Tests for generate_write_non_duplicates_sql()."""

    def test_standard_write_non_duplicates(self):
        """Test SQL generation for writing non-duplicate rows to temp file."""
        result = VocabHarmonizer.generate_write_non_duplicates_sql(
            file_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet',
            primary_key_column='condition_occurrence_id',
            tmp_output_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_non_dup_abc123.parquet'
        )

        expected = load_reference_sql("generate_write_non_duplicates_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateFixDuplicatesSql:
    """Tests for generate_fix_duplicates_sql()."""

    def test_standard_fix_duplicates(self):
        """Test SQL generation for fixing duplicate primary keys with hash-based generation."""
        result = VocabHarmonizer.generate_fix_duplicates_sql(
            file_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet',
            primary_key_column='condition_occurrence_id',
            primary_key_type='BIGINT',
            tmp_output_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_dup_fixed_abc123.parquet'
        )

        expected = load_reference_sql("generate_fix_duplicates_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateMergeDeduplicatedSql:
    """Tests for generate_merge_deduplicated_sql()."""

    def test_standard_merge_deduplicated(self):
        """Test SQL generation for merging non-duplicate and fixed duplicate rows."""
        result = VocabHarmonizer.generate_merge_deduplicated_sql(
            tmp_non_dup_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_non_dup_abc123.parquet',
            tmp_dup_fixed_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/tmp/tmp_dup_fixed_abc123.parquet',
            output_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet'
        )

        expected = load_reference_sql("generate_merge_deduplicated_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateConsolidateSingleTableSql:
    """Tests for generate_consolidate_single_table_sql()."""

    def test_standard_consolidate_sql(self):
        """Test SQL generation for consolidating multiple parquet files."""
        result = VocabHarmonizer.generate_consolidate_single_table_sql(
            source_parquet_pattern='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/parts/*.parquet',
            output_path='gs://bucket/2025-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet'
        )

        expected = load_reference_sql("generate_consolidate_single_table_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateGetTargetTablesSql:
    """Tests for generate_get_target_tables_sql()."""

    def test_standard_get_target_tables(self):
        """Test SQL generation for getting distinct target tables from harmonized files."""
        result = VocabHarmonizer.generate_get_target_tables_sql(
            parquet_path='synthea53/2025-01-01/artifacts/harmonized/*.parquet'
        )

        expected = load_reference_sql("generate_get_target_tables_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateTableTransitionCountSql:
    """Tests for generate_table_transition_count_sql()."""

    def test_standard_table_transition_count(self):
        """Test SQL generation for counting rows by target table for reporting."""
        result = VocabHarmonizer.generate_table_transition_count_sql(
            parquet_path='synthea53/2025-01-01/artifacts/harmonized/*.parquet'
        )

        expected = load_reference_sql("generate_table_transition_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateVocabStatusCountSql:
    """Tests for generate_vocab_status_count_sql()."""

    def test_standard_vocab_status_count(self):
        """Test SQL generation for counting rows by vocab harmonization status for reporting."""
        result = VocabHarmonizer.generate_vocab_status_count_sql(
            parquet_path='synthea53/2025-01-01/artifacts/harmonized/*.parquet'
        )

        expected = load_reference_sql("generate_vocab_status_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateSameTableMappingCardinalityCountSql:
    """Tests for generate_same_table_mapping_cardinality_count_sql()."""

    def test_standard_same_table_mapping_cardinality_count(self):
        """Test SQL generation for counting same-table mapping cardinalities (1:1, 1:2, 1:N) for reporting."""
        result = VocabHarmonizer.generate_same_table_mapping_cardinality_count_sql(
            parquet_path='synthea53/2025-01-01/artifacts/harmonized/*.parquet',
            source_table_name='measurement',
            primary_key_column='measurement_id'
        )

        expected = load_reference_sql("generate_mapping_cardinality_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateRowDispositionCountSql:
    """Tests for generate_row_disposition_count_sql()."""

    def test_standard_row_disposition_count(self):
        """Test SQL generation for counting row dispositions (stayed only, stayed and copied, moved) for reporting."""
        result = VocabHarmonizer.generate_row_disposition_count_sql(
            parquet_path='gs://synthea53/2025-01-01/artifacts/harmonized/measurement/*.parquet',
            source_table_name='measurement',
            primary_key_column='measurement_id'
        )

        expected = load_reference_sql("generate_row_disposition_count_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGetConceptIdSourcePairs:
    """Tests for utils.get_concept_id_source_pairs()."""

    def test_single_pair_table(self):
        """Test that condition_occurrence returns its single concept pair."""
        pairs = get_concept_id_source_pairs('condition_occurrence', '5.4')
        assert ('condition_concept_id', 'condition_source_concept_id') in pairs
        assert len(pairs) == 1

    def test_multi_pair_table(self):
        """Test that measurement returns both concept pairs."""
        pairs = get_concept_id_source_pairs('measurement', '5.4')
        assert ('measurement_concept_id', 'measurement_source_concept_id') in pairs
        assert ('unit_concept_id', 'unit_source_concept_id') in pairs
        assert len(pairs) == 2

    def test_table_without_source_concept_id(self):
        """Test that tables with no source_concept_id columns return empty list."""
        pairs = get_concept_id_source_pairs('note', '5.4')
        assert pairs == []

    def test_nonexistent_table(self):
        """Test that a nonexistent table returns empty list."""
        pairs = get_concept_id_source_pairs('nonexistent_table', '5.4')
        assert pairs == []


class TestGetPrimaryConceptPair:
    """Tests for VocabHarmonizer._get_primary_concept_pair()."""

    def _make_harmonizer(self, table_name: str) -> VocabHarmonizer:
        return VocabHarmonizer(
            file_path=f'gs://bucket/2025-01-01/{table_name}.csv',
            cdm_version='5.4',
            site='test_site',
            vocab_version='v5.0',
            vocab_path='vocabularies/',
            project_id='test_project',
            dataset_id='test_dataset'
        )

    def test_measurement_returns_primary_not_unit(self):
        """Primary pair for measurement is measurement_concept_id, not unit_concept_id."""
        harmonizer = self._make_harmonizer('measurement')
        concept_id, source_concept_id = harmonizer._get_primary_concept_pair()
        assert concept_id == 'measurement_concept_id'
        assert source_concept_id == 'measurement_source_concept_id'

    def test_specimen_returns_primary_with_empty_source(self):
        """specimen has no source_concept_id column — should return empty string."""
        harmonizer = self._make_harmonizer('specimen')
        concept_id, source_concept_id = harmonizer._get_primary_concept_pair()
        assert concept_id == 'specimen_concept_id'
        assert source_concept_id == ''

    def test_condition_occurrence(self):
        """Single-pair table should return its only pair as primary."""
        harmonizer = self._make_harmonizer('condition_occurrence')
        concept_id, source_concept_id = harmonizer._get_primary_concept_pair()
        assert concept_id == 'condition_concept_id'
        assert source_concept_id == 'condition_source_concept_id'

    def test_device_exposure_returns_primary_not_unit(self):
        """Primary pair for device_exposure is device_concept_id, not unit_concept_id."""
        harmonizer = self._make_harmonizer('device_exposure')
        concept_id, source_concept_id = harmonizer._get_primary_concept_pair()
        assert concept_id == 'device_concept_id'
        assert source_concept_id == 'device_source_concept_id'


class TestGenerateSourceConceptOverrideSql:
    """Tests for generate_source_concept_override_sql()."""

    def test_standard_single_pair(self):
        """Test SQL generation for source concept override with a single concept pair (condition_occurrence)."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())
        concept_pairs = get_concept_id_source_pairs('condition_occurrence', '5.4')

        result = VocabHarmonizer.generate_source_concept_override_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            concept_pairs=concept_pairs,
            primary_key_column='condition_occurrence_id',
            existing_files_where_clause='',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_source_concept_override.parquet'
        )

        expected = load_reference_sql("generate_source_concept_override_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_multi_pair(self):
        """Test SQL generation for source concept override with multiple concept pairs (measurement)."""
        schema = utils.get_table_schema('measurement', '5.4')
        ordered_omop_columns = list(schema['measurement']['columns'].keys())
        concept_pairs = get_concept_id_source_pairs('measurement', '5.4')

        result = VocabHarmonizer.generate_source_concept_override_sql(
            source_table_name='measurement',
            ordered_omop_columns=ordered_omop_columns,
            concept_pairs=concept_pairs,
            primary_key_column='measurement_id',
            existing_files_where_clause='',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/measurement_source_concept_override.parquet'
        )

        expected = load_reference_sql("generate_source_concept_override_sql_multi_pair.sql")
        assert normalize_sql(result) == normalize_sql(expected)

    def test_with_exclusion(self):
        """Test SQL generation includes NOT IN clause when exclusion is provided."""
        schema = utils.get_table_schema('condition_occurrence', '5.4')
        ordered_omop_columns = list(schema['condition_occurrence']['columns'].keys())
        concept_pairs = get_concept_id_source_pairs('condition_occurrence', '5.4')

        exclusion_clause = """
                AND tbl.condition_occurrence_id NOT IN (
                    SELECT condition_occurrence_id FROM read_parquet('gs://synthea53/2025-01-01/artifacts/harmonized/*.parquet')
                )
            """

        result = VocabHarmonizer.generate_source_concept_override_sql(
            source_table_name='condition_occurrence',
            ordered_omop_columns=ordered_omop_columns,
            concept_pairs=concept_pairs,
            primary_key_column='condition_occurrence_id',
            existing_files_where_clause=exclusion_clause,
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized/condition_occurrence_source_concept_override.parquet'
        )

        expected = load_reference_sql("generate_source_concept_override_sql_with_exclusion.sql")
        assert normalize_sql(result) == normalize_sql(expected)


class TestGenerateSecondaryConceptOverrideSql:
    """Tests for generate_secondary_concept_override_sql()."""

    def test_measurement_unit_pair(self):
        """Test SQL generation for secondary concept override on measurement (unit_concept_id)."""
        import core.constants as constants

        all_pairs = get_concept_id_source_pairs('measurement', '5.4')
        primary = constants.PRIMARY_CONCEPT_ID_COLUMNS['measurement']
        secondary_pairs = [(c, s) for c, s in all_pairs if c != primary]

        result = VocabHarmonizer.generate_secondary_concept_override_sql(
            secondary_pairs=secondary_pairs,
            harmonized_parquet_file='file:///data/synthea53/2025-01-01/artifacts/harmonized_files/measurement/*.parquet',
            site='synthea53',
            bucket='synthea53',
            delivery_date='2025-01-01',
            vocab_version='v5.0_22-JAN-23',
            vocab_path='vocabularies/',
            output_path='synthea53/2025-01-01/artifacts/harmonized_files/measurement/measurement_secondary_concept_override.parquet'
        )

        expected = load_reference_sql("generate_secondary_concept_override_sql_standard.sql")
        assert normalize_sql(result) == normalize_sql(expected)
