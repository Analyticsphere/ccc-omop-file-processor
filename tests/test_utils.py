import pytest

import core.utils as utils


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
def test_get_table_name_from_gcs_path(gcs_path, expected):
    assert utils.get_table_name_from_gcs_path(gcs_path) == expected


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
def test_get_report_tmp_artifacts_gcs_path(bucket, delivery_date, expected_path):
    # NOTE: This function now returns WITHOUT storage scheme prefix (e.g., gs://),
    # consistent with all other path utility functions. Callers should use storage.get_uri()
    # if they need the full URI.
    assert utils.get_report_tmp_artifacts_gcs_path(bucket, delivery_date) == expected_path


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
def test_get_bucket_and_delivery_date_from_gcs_path(gcs_path, expected_bucket, expected_date):
    bucket, delivery_date = utils.get_bucket_and_delivery_date_from_gcs_path(gcs_path)
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