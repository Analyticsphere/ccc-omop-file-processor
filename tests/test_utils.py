from datetime import datetime
from unittest.mock import patch

import pytest

import core.utils as utils
from core.storage_backend import storage


@pytest.mark.parametrize(
    "gcs_path,expected",
    [
        ("synthea53/2024-12-31/care_site.parquet", "care_site"),
        ("bucket/folder/person.csv", "person"),
        ("bucket/folder/observation_pipeline_fix_formatting.csv", "observation"),
        ("bucket/folder/visit_occurrence", "visit_occurrence"),
        ("bucket/folder/observation_pipeline_fix_formatting.parquet", "observation"),
        ("bucket/folder/observation.csv", "observation"),
        ("bucket/folder/observation", "observation"),
        ("bucket/folder/observation.csv.gz", "observation"),
    ]
)
def test_get_table_name_from_path(gcs_path, expected):
    assert utils.get_table_name_from_path(gcs_path) == expected


@pytest.mark.parametrize(
    "cdm_version,expected_concept_id",
    [
        (utils.constants.CDM_v53, utils.constants.CDM_v53_CONCEPT_ID),
        (utils.constants.CDM_v54, utils.constants.CDM_v54_CONCEPT_ID),
        ("unknown", 0),
        ("", 0),
        (None, 0),
    ]
)
def test_get_cdm_version_concept_id(cdm_version, expected_concept_id):
    assert utils.get_cdm_version_concept_id(cdm_version) == expected_concept_id


@pytest.mark.parametrize(
    "bucket,delivery_date,expected_path",
    [
        ("synthea53", "2024-12-31", "synthea53/2024-12-31/artifacts/delivery_report/tmp/"),
        ("bucket", "folder", "bucket/folder/artifacts/delivery_report/tmp/"),
        ("test-bucket", "2025-10-04", "test-bucket/2025-10-04/artifacts/delivery_report/tmp/"),
    ]
)
def test_get_report_tmp_artifacts_path(bucket, delivery_date, expected_path):
    assert utils.get_report_tmp_artifacts_path(bucket, delivery_date) == expected_path


@pytest.mark.parametrize(
    "gcs_path,expected_bucket,expected_date",
    [
        ("synthea53/2024-12-31/care_site.parquet", "synthea53", "2024-12-31"),
        ("bucket/folder/person.csv", "bucket", "folder"),
        ("gs://bucket/folder/observation.csv", "bucket", "folder"),
        ("gs://synthea53/2024-12-31/visit_occurrence", "synthea53", "2024-12-31"),
        ("bucket/2025-01-01/observation.csv.gz", "bucket", "2025-01-01"),
    ]
)
def test_get_bucket_and_delivery_date_from_path(gcs_path, expected_bucket, expected_date):
    bucket, delivery_date = utils.get_bucket_and_delivery_date_from_path(gcs_path)
    assert bucket == expected_bucket
    assert delivery_date == expected_date


@pytest.mark.parametrize(
    "gcs_path,expected_artifact_path",
    [
        (
            "synthea53/2024-12-31/care_site.parquet",
            f"synthea53/2024-12-31/artifacts/converted_files/care_site.parquet"
        ),
        (
            "bucket/folder/person.csv",
            f"bucket/folder/artifacts/converted_files/person.parquet"
        ),
        (
            "bucket/folder/observation_pipeline_fix_formatting.csv",
            f"bucket/folder/artifacts/converted_files/observation.parquet"
        ),
        (
            "bucket/2025-01-01/observation.csv.gz",
            f"bucket/2025-01-01/artifacts/converted_files/observation.parquet"
        ),
    ]
)
def test_get_parquet_artifact_location(gcs_path, expected_artifact_path):
    assert utils.get_parquet_artifact_location(gcs_path) == expected_artifact_path


@pytest.mark.parametrize(
    "gcs_path,expected_harmonized_path",
    [
        (
            "synthea53/2024-12-31/care_site.parquet",
            f"synthea53/2024-12-31/artifacts/harmonized_files/care_site/"
        ),
        (
            "bucket/folder/person.csv",
            f"bucket/folder/artifacts/harmonized_files/person/"
        ),
        (
            "bucket/folder/observation_pipeline_fix_formatting.csv",
            f"bucket/folder/artifacts/harmonized_files/observation/"
        ),
        (
            "bucket/2025-01-01/observation.csv.gz",
            f"bucket/2025-01-01/artifacts/harmonized_files/observation/"
        ),
    ]
)
def test_get_parquet_harmonized_path(gcs_path, expected_harmonized_path):
    assert utils.get_parquet_harmonized_path(gcs_path) == expected_harmonized_path


@pytest.mark.parametrize(
    "gcs_path,expected_invalid_rows_path",
    [
        (
            "synthea53/2024-12-31/care_site.parquet",
            f"synthea53/2024-12-31/artifacts/invalid_rows/care_site.parquet"
        ),
        (
            "bucket/folder/person.csv",
            f"bucket/folder/artifacts/invalid_rows/person.parquet"
        ),
        (
            "bucket/folder/observation_pipeline_fix_formatting.csv",
            f"bucket/folder/artifacts/invalid_rows/observation.parquet"
        ),
        (
            "bucket/2025-01-01/observation.csv.gz",
            f"bucket/2025-01-01/artifacts/invalid_rows/observation.parquet"
        ),
    ]
)
def test_get_invalid_rows_path_from_gcs_path(gcs_path, expected_invalid_rows_path):
    assert utils.get_invalid_rows_path_from_gcs_path(gcs_path) == expected_invalid_rows_path


@pytest.mark.parametrize(
    "vocab_version,vocab_gcs_bucket,expected_path",
    [
        (
            "v20240101",
            "my-vocab-bucket",
            "my-vocab-bucket/v20240101/optimized/optimized_vocab_file.parquet"
        ),
        (
            "2025-10-04",
            "bucket",
            "bucket/2025-10-04/optimized/optimized_vocab_file.parquet"
        ),
        (
            "v1.0",
            "another-bucket",
            "another-bucket/v1.0/optimized/optimized_vocab_file.parquet"
        ),
    ]
)
def test_get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket, expected_path):
    assert utils.get_optimized_vocab_file_path(vocab_version, vocab_gcs_bucket) == expected_path


@pytest.mark.parametrize(
    "gcs_path,expected_etl_path",
    [
        (
            "synthea53/2024-12-31/care_site.parquet",
            "synthea53/2024-12-31/artifacts/omop_etl/"
        ),
        (
            "bucket/folder/person.csv",
            "bucket/folder/artifacts/omop_etl/"
        ),
        (
            "gs://bucket/2025-01-01/observation.csv.gz",
            "bucket/2025-01-01/artifacts/omop_etl/"
        ),
        (
            "test-bucket/2025-10-04/condition_occurrence.parquet",
            "test-bucket/2025-10-04/artifacts/omop_etl/"
        ),
    ]
)
def test_get_omop_etl_destination_path(gcs_path, expected_etl_path):
    assert utils.get_omop_etl_destination_path(gcs_path) == expected_etl_path


@pytest.mark.parametrize(
    "column_name,expected",
    [
        ("Person_Id", "person_id"),
        ("OBSERVATION_ID", "observation_id"),
        ("  care_site_id  ", "care_site_id"),
        ("visit-occurrence-id", "visitoccurrenceid"),
        ("drug@exposure#id", "drugexposureid"),
        ("measurement_123", "measurement_123"),
        ("column name with spaces", "columnnamewithspaces"),
        ('"offset"', "offset"),
        ("provider_ID", "provider_id"),
        ("Mix3d_CaSe_123", "mix3d_case_123"),
        ("column.with.dots", "columnwithdots"),
        ("column'with'quotes", "columnwithquotes"),
    ]
)
def test_clean_column_name_for_sql(column_name, expected):
    assert utils.clean_column_name_for_sql(column_name) == expected


@pytest.mark.parametrize(
    "column_name,column_type,expected_value",
    [
        # Concept ID columns always return '0' regardless of type
        ("person_concept_id", "INTEGER", "'0'"),
        ("drug_concept_id", "BIGINT", "'0'"),
        ("measurement_concept_id", "VARCHAR", "'0'"),
        ("visit_concept_id", "TIMESTAMP", "'0'"),

        # Non-concept_id columns use type-specific defaults from constants.DEFAULT_COLUMN_VALUES
        ("provider_name", "VARCHAR", "''"),
        ("visit_date", "DATE", "'1970-01-01'"),
        ("person_id", "BIGINT", "'-1'"),
        ("measurement_value", "DOUBLE", "'-1.0'"),
        ("birth_datetime", "TIMESTAMP", "'1901-01-01 00:00:00'"),

        # Test with different column names using same types
        ("care_site_id", "BIGINT", "'-1'"),
        ("note_date", "DATE", "'1970-01-01'"),
    ]
)
def test_get_placeholder_value(column_name, column_type, expected_value):
    assert utils.get_placeholder_value(column_name, column_type) == expected_value


# =============================================================================
# Tests for placeholder_to_file_path()
# =============================================================================

@pytest.mark.parametrize(
    "site,site_bucket,delivery_date,sql_script,vocab_version,vocab_gcs_bucket,expected",
    [
        # Test single clinical data placeholder - person
        (
            "synthea",
            "synthea-data",
            "2025-03-20",
            "SELECT * FROM @PERSON WHERE person_id = 1",
            "v5.4.0",
            "vocab-bucket",
            f"SELECT * FROM {storage.get_uri('synthea-data/2025-03-20/artifacts/converted_files/person.parquet')} WHERE person_id = 1"
        ),
        # Test single vocabulary placeholder - concept
        (
            "site456",
            "bucket1",
            "2024-05-10",
            "SELECT * FROM @CONCEPT",
            "v2024",
            "my-vocab",
            f"SELECT * FROM {storage.get_uri('my-vocab/v2024/optimized/concept.parquet')}"
        ),
        # Test multiple clinical data placeholders
        (
            "multi",
            "multi-bucket",
            "2024-08-01",
            "SELECT * FROM @PERSON p JOIN @VISIT_OCCURRENCE v ON p.person_id = v.person_id",
            "v1.0",
            "vocab",
            f"SELECT * FROM {storage.get_uri('multi-bucket/2024-08-01/artifacts/converted_files/person.parquet')} p JOIN {storage.get_uri('multi-bucket/2024-08-01/artifacts/converted_files/visit_occurrence.parquet')} v ON p.person_id = v.person_id"
        ),
        # Test mix of clinical and vocabulary placeholders
        (
            "mixed",
            "mixed-bucket",
            "2024-09-15",
            "SELECT co.* FROM @CONDITION_OCCURRENCE co JOIN @CONCEPT c ON co.condition_concept_id = c.concept_id",
            "v2024.1",
            "vocab-mixed",
            f"SELECT co.* FROM {storage.get_uri('mixed-bucket/2024-09-15/artifacts/converted_files/condition_occurrence.parquet')} co JOIN {storage.get_uri('vocab-mixed/v2024.1/optimized/concept.parquet')} c ON co.condition_concept_id = c.concept_id"
        ),
        # Test with no placeholders - should return unchanged
        (
            "unchanged",
            "bucket",
            "2024-10-01",
            "SELECT 1 as test",
            "v1.0",
            "vocab",
            "SELECT 1 as test"
        ),
        # Test all clinical data placeholders
        (
            "all-clinical",
            "all-bucket",
            "2024-11-01",
            "@CONDITION_OCCURRENCE @DRUG_EXPOSURE @VISIT_OCCURRENCE @DEATH @PERSON @MEASUREMENT @OBSERVATION @DEVICE_EXPOSURE @NOTE @PROCEDURE_OCCURRENCE @SPECIMEN",
            "v1",
            "v",
            f"{storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/condition_occurrence.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/drug_exposure.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/visit_occurrence.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/death.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/person.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/measurement.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/observation.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/device_exposure.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/note.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/procedure_occurrence.parquet')} {storage.get_uri('all-bucket/2024-11-01/artifacts/converted_files/specimen.parquet')}"
        ),
        # Test all vocabulary placeholders
        (
            "all-vocab",
            "bucket",
            "2024-12-01",
            "@CONCEPT_ANCESTOR @CONCEPT @OPTIMIZED_VOCABULARY",
            "v5.3",
            "vocab-all",
            f"{storage.get_uri('vocab-all/v5.3/optimized/concept_ancestor.parquet')} {storage.get_uri('vocab-all/v5.3/optimized/concept.parquet')} {storage.get_uri('vocab-all/v5.3/optimized/optimized_vocab_file.parquet')}"
        ),
    ]
)
def test_placeholder_to_file_path_without_site_and_date(site, site_bucket, delivery_date, sql_script, vocab_version, vocab_gcs_bucket, expected):
    """Test placeholder_to_file_path() for clinical and vocabulary table replacements (excluding @SITE and @CURRENT_DATE)."""
    result = utils.placeholder_to_file_path(site, site_bucket, delivery_date, sql_script, vocab_version, vocab_gcs_bucket)
    assert result == expected


def test_placeholder_to_file_path_site_replacement():
    """Test that @SITE placeholder is replaced with the site name."""
    sql_script = "SELECT '@SITE' as site_name"
    expected = "SELECT 'my-site' as site_name"

    result = utils.placeholder_to_file_path(
        site="my-site",
        site_bucket="bucket",
        delivery_date="2024-01-01",
        sql_script=sql_script,
        vocab_version="v1.0",
        vocab_gcs_bucket="vocab"
    )

    assert result == expected


def test_placeholder_to_file_path_multiple_site_replacements():
    """Test that multiple @SITE placeholders are all replaced."""
    sql_script = "@SITE and @SITE and @SITE"
    expected = "test-site and test-site and test-site"

    result = utils.placeholder_to_file_path(
        site="test-site",
        site_bucket="bucket",
        delivery_date="2024-01-01",
        sql_script=sql_script,
        vocab_version="v1.0",
        vocab_gcs_bucket="vocab"
    )

    assert result == expected


@patch('core.utils.datetime')
def test_placeholder_to_file_path_current_date_replacement(mock_datetime):
    """Test that @CURRENT_DATE placeholder is replaced with current date in YYYY-MM-DD format."""
    # Mock datetime.now() to return a fixed date
    mock_datetime.now.return_value = datetime(2024, 3, 15, 10, 30, 45)

    sql_script = "SELECT '@CURRENT_DATE' as current_date"
    expected = "SELECT '2024-03-15' as current_date"

    result = utils.placeholder_to_file_path(
        site="site",
        site_bucket="bucket",
        delivery_date="2024-01-01",
        sql_script=sql_script,
        vocab_version="v1.0",
        vocab_gcs_bucket="vocab"
    )

    assert result == expected


@patch('core.utils.datetime')
def test_placeholder_to_file_path_all_placeholders_together(mock_datetime):
    """Test comprehensive replacement of all placeholder types in a single SQL script."""
    # Mock datetime.now() to return a fixed date
    mock_datetime.now.return_value = datetime(2025, 12, 25, 0, 0, 0)

    sql_script = """
    -- Site: @SITE, Date: @CURRENT_DATE
    SELECT
        p.person_id,
        co.condition_concept_id,
        c.concept_name
    FROM @PERSON p
    JOIN @CONDITION_OCCURRENCE co ON p.person_id = co.person_id
    JOIN @CONCEPT c ON co.condition_concept_id = c.concept_id
    WHERE co.condition_start_date <= '@CURRENT_DATE'
    """

    expected = f"""
    -- Site: comprehensive-site, Date: 2025-12-25
    SELECT
        p.person_id,
        co.condition_concept_id,
        c.concept_name
    FROM {storage.get_uri('comprehensive-bucket/2025-06-15/artifacts/converted_files/person.parquet')} p
    JOIN {storage.get_uri('comprehensive-bucket/2025-06-15/artifacts/converted_files/condition_occurrence.parquet')} co ON p.person_id = co.person_id
    JOIN {storage.get_uri('vocab-comprehensive/v2025/optimized/concept.parquet')} c ON co.condition_concept_id = c.concept_id
    WHERE co.condition_start_date <= '2025-12-25'
    """

    result = utils.placeholder_to_file_path(
        site="comprehensive-site",
        site_bucket="comprehensive-bucket",
        delivery_date="2025-06-15",
        sql_script=sql_script,
        vocab_version="v2025",
        vocab_gcs_bucket="vocab-comprehensive"
    )

    assert result == expected


# =============================================================================
# Tests for placeholder_to_harmonized_file_path()
# =============================================================================

@pytest.mark.parametrize(
    "site,site_bucket,delivery_date,sql_script,vocab_version,vocab_gcs_bucket,expected",
    [
        # Test harmonized table (condition_occurrence) - should use omop_etl path
        (
            "site1",
            "bucket1",
            "2024-01-01",
            "SELECT * FROM @CONDITION_OCCURRENCE",
            "v1.0",
            "vocab",
            f"SELECT * FROM {storage.get_uri('bucket1/2024-01-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')}"
        ),
        # Test harmonized table (visit_occurrence) - should use omop_etl path
        (
            "site2",
            "bucket2",
            "2024-02-15",
            "SELECT * FROM @VISIT_OCCURRENCE",
            "v2.0",
            "vocab2",
            f"SELECT * FROM {storage.get_uri('bucket2/2024-02-15/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet')}"
        ),
        # Test harmonized table (drug_exposure) - should use omop_etl path
        (
            "site3",
            "bucket3",
            "2024-03-20",
            "SELECT * FROM @DRUG_EXPOSURE",
            "v3.0",
            "vocab3",
            f"SELECT * FROM {storage.get_uri('bucket3/2024-03-20/artifacts/omop_etl/drug_exposure/drug_exposure.parquet')}"
        ),
        # Test harmonized table (procedure_occurrence) - should use omop_etl path
        (
            "site4",
            "bucket4",
            "2024-04-10",
            "SELECT * FROM @PROCEDURE_OCCURRENCE",
            "v4.0",
            "vocab4",
            f"SELECT * FROM {storage.get_uri('bucket4/2024-04-10/artifacts/omop_etl/procedure_occurrence/procedure_occurrence.parquet')}"
        ),
        # Test harmonized table (device_exposure) - should use omop_etl path
        (
            "site5",
            "bucket5",
            "2024-05-05",
            "SELECT * FROM @DEVICE_EXPOSURE",
            "v5.0",
            "vocab5",
            f"SELECT * FROM {storage.get_uri('bucket5/2024-05-05/artifacts/omop_etl/device_exposure/device_exposure.parquet')}"
        ),
        # Test harmonized table (measurement) - should use omop_etl path
        (
            "site6",
            "bucket6",
            "2024-06-01",
            "SELECT * FROM @MEASUREMENT",
            "v6.0",
            "vocab6",
            f"SELECT * FROM {storage.get_uri('bucket6/2024-06-01/artifacts/omop_etl/measurement/measurement.parquet')}"
        ),
        # Test harmonized table (observation) - should use omop_etl path
        (
            "site7",
            "bucket7",
            "2024-07-15",
            "SELECT * FROM @OBSERVATION",
            "v7.0",
            "vocab7",
            f"SELECT * FROM {storage.get_uri('bucket7/2024-07-15/artifacts/omop_etl/observation/observation.parquet')}"
        ),
        # Test harmonized table (note) - should use omop_etl path
        (
            "site8",
            "bucket8",
            "2024-08-20",
            "SELECT * FROM @NOTE",
            "v8.0",
            "vocab8",
            f"SELECT * FROM {storage.get_uri('bucket8/2024-08-20/artifacts/omop_etl/note/note.parquet')}"
        ),
        # Test harmonized table (specimen) - should use omop_etl path
        (
            "site9",
            "bucket9",
            "2024-09-25",
            "SELECT * FROM @SPECIMEN",
            "v9.0",
            "vocab9",
            f"SELECT * FROM {storage.get_uri('bucket9/2024-09-25/artifacts/omop_etl/specimen/specimen.parquet')}"
        ),
        # Test NON-harmonized table (person) - should use converted_files path
        (
            "site10",
            "bucket10",
            "2024-10-01",
            "SELECT * FROM @PERSON",
            "v10.0",
            "vocab10",
            f"SELECT * FROM {storage.get_uri('bucket10/2024-10-01/artifacts/converted_files/person.parquet')}"
        ),
        # Test NON-harmonized table (death) - should use converted_files path
        (
            "site11",
            "bucket11",
            "2024-11-05",
            "SELECT * FROM @DEATH",
            "v11.0",
            "vocab11",
            f"SELECT * FROM {storage.get_uri('bucket11/2024-11-05/artifacts/converted_files/death.parquet')}"
        ),
        # Test mix of harmonized and non-harmonized tables
        (
            "mixed-site",
            "mixed-bucket",
            "2024-12-01",
            "SELECT * FROM @PERSON p JOIN @CONDITION_OCCURRENCE co ON p.person_id = co.person_id",
            "v12.0",
            "vocab12",
            f"SELECT * FROM {storage.get_uri('mixed-bucket/2024-12-01/artifacts/converted_files/person.parquet')} p JOIN {storage.get_uri('mixed-bucket/2024-12-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')} co ON p.person_id = co.person_id"
        ),
        # Test vocabulary placeholder (concept) - should use optimized path
        (
            "vocab-site",
            "vocab-bucket",
            "2025-01-01",
            "SELECT * FROM @CONCEPT",
            "v2025",
            "my-vocab",
            f"SELECT * FROM {storage.get_uri('my-vocab/v2025/optimized/concept.parquet')}"
        ),
        # Test vocabulary placeholder (concept_ancestor) - should use optimized path
        (
            "ancestor-site",
            "ancestor-bucket",
            "2025-02-01",
            "SELECT * FROM @CONCEPT_ANCESTOR",
            "v2025.2",
            "ancestor-vocab",
            f"SELECT * FROM {storage.get_uri('ancestor-vocab/v2025.2/optimized/concept_ancestor.parquet')}"
        ),
        # Test complex query with all table types: harmonized, non-harmonized, and vocabulary
        (
            "complex",
            "complex-bucket",
            "2025-03-15",
            """
            SELECT p.person_id, co.condition_concept_id, c.concept_name
            FROM @PERSON p
            JOIN @CONDITION_OCCURRENCE co ON p.person_id = co.person_id
            JOIN @CONCEPT c ON co.condition_concept_id = c.concept_id
            WHERE p.person_id IN (SELECT person_id FROM @DEATH)
            """,
            "v2025.3",
            "complex-vocab",
            f"""
            SELECT p.person_id, co.condition_concept_id, c.concept_name
            FROM {storage.get_uri('complex-bucket/2025-03-15/artifacts/converted_files/person.parquet')} p
            JOIN {storage.get_uri('complex-bucket/2025-03-15/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')} co ON p.person_id = co.person_id
            JOIN {storage.get_uri('complex-vocab/v2025.3/optimized/concept.parquet')} c ON co.condition_concept_id = c.concept_id
            WHERE p.person_id IN (SELECT person_id FROM {storage.get_uri('complex-bucket/2025-03-15/artifacts/converted_files/death.parquet')})
            """
        ),
        # Test with no placeholders - should return unchanged
        (
            "no-placeholder",
            "bucket",
            "2025-04-01",
            "SELECT 1 as constant",
            "v1",
            "vocab",
            "SELECT 1 as constant"
        ),
        # Test all harmonized tables together
        (
            "all-harmonized",
            "all-harm-bucket",
            "2025-05-01",
            "@VISIT_OCCURRENCE @CONDITION_OCCURRENCE @DRUG_EXPOSURE @PROCEDURE_OCCURRENCE @DEVICE_EXPOSURE @MEASUREMENT @OBSERVATION @NOTE @SPECIMEN",
            "v5",
            "v",
            f"{storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/visit_occurrence/visit_occurrence.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/drug_exposure/drug_exposure.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/procedure_occurrence/procedure_occurrence.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/device_exposure/device_exposure.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/measurement/measurement.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/observation/observation.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/note/note.parquet')} {storage.get_uri('all-harm-bucket/2025-05-01/artifacts/omop_etl/specimen/specimen.parquet')}"
        ),
    ]
)
def test_placeholder_to_harmonized_file_path_table_replacements(site, site_bucket, delivery_date, sql_script, vocab_version, vocab_gcs_bucket, expected):
    """
    Test placeholder_to_harmonized_file_path() for proper path resolution.

    Harmonized tables (in VOCAB_HARMONIZED_TABLES) should use: omop_etl/{table}/{table}.parquet
    Non-harmonized tables should use: converted_files/{table}.parquet
    Vocabulary tables should use: optimized/{table}.parquet
    """
    result = utils.placeholder_to_harmonized_file_path(site, site_bucket, delivery_date, sql_script, vocab_version, vocab_gcs_bucket)
    assert result == expected


def test_placeholder_to_harmonized_file_path_site_replacement():
    """Test that @SITE placeholder is replaced with the site name in harmonized paths."""
    sql_script = "SELECT '@SITE' as site_name"
    expected = "SELECT 'harmonized-site' as site_name"

    result = utils.placeholder_to_harmonized_file_path(
        site="harmonized-site",
        site_bucket="bucket",
        delivery_date="2024-01-01",
        sql_script=sql_script,
        vocab_version="v1.0",
        vocab_gcs_bucket="vocab"
    )

    assert result == expected


@patch('core.utils.datetime')
def test_placeholder_to_harmonized_file_path_current_date_replacement(mock_datetime):
    """Test that @CURRENT_DATE placeholder is replaced with current date in harmonized paths."""
    # Mock datetime.now() to return a fixed date
    mock_datetime.now.return_value = datetime(2025, 7, 4, 12, 0, 0)

    sql_script = "SELECT '@CURRENT_DATE' as processing_date"
    expected = "SELECT '2025-07-04' as processing_date"

    result = utils.placeholder_to_harmonized_file_path(
        site="site",
        site_bucket="bucket",
        delivery_date="2024-01-01",
        sql_script=sql_script,
        vocab_version="v1.0",
        vocab_gcs_bucket="vocab"
    )

    assert result == expected


@patch('core.utils.datetime')
def test_placeholder_to_harmonized_file_path_comprehensive(mock_datetime):
    """Test comprehensive replacement of all placeholder types with harmonized paths."""
    # Mock datetime.now() to return a fixed date
    mock_datetime.now.return_value = datetime(2026, 1, 1, 0, 0, 0)

    sql_script = """
    -- Comprehensive test for @SITE on @CURRENT_DATE
    SELECT
        p.person_id,
        co.condition_concept_id,
        c.concept_name,
        ca.ancestor_concept_id,
        '@SITE' as site,
        '@CURRENT_DATE' as date
    FROM @PERSON p
    INNER JOIN @CONDITION_OCCURRENCE co ON p.person_id = co.person_id
    INNER JOIN @CONCEPT c ON co.condition_concept_id = c.concept_id
    LEFT JOIN @CONCEPT_ANCESTOR ca ON c.concept_id = ca.descendant_concept_id
    LEFT JOIN @DEATH d ON p.person_id = d.person_id
    WHERE co.condition_start_date <= '@CURRENT_DATE'
    """

    expected = f"""
    -- Comprehensive test for final-site on 2026-01-01
    SELECT
        p.person_id,
        co.condition_concept_id,
        c.concept_name,
        ca.ancestor_concept_id,
        'final-site' as site,
        '2026-01-01' as date
    FROM {storage.get_uri('final-bucket/2026-12-31/artifacts/converted_files/person.parquet')} p
    INNER JOIN {storage.get_uri('final-bucket/2026-12-31/artifacts/omop_etl/condition_occurrence/condition_occurrence.parquet')} co ON p.person_id = co.person_id
    INNER JOIN {storage.get_uri('final-vocab/v2026/optimized/concept.parquet')} c ON co.condition_concept_id = c.concept_id
    LEFT JOIN {storage.get_uri('final-vocab/v2026/optimized/concept_ancestor.parquet')} ca ON c.concept_id = ca.descendant_concept_id
    LEFT JOIN {storage.get_uri('final-bucket/2026-12-31/artifacts/converted_files/death.parquet')} d ON p.person_id = d.person_id
    WHERE co.condition_start_date <= '2026-01-01'
    """

    result = utils.placeholder_to_harmonized_file_path(
        site="final-site",
        site_bucket="final-bucket",
        delivery_date="2026-12-31",
        sql_script=sql_script,
        vocab_version="v2026",
        vocab_gcs_bucket="final-vocab"
    )

    assert result == expected