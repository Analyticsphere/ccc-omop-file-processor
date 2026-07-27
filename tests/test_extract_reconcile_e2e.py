"""
End-to-end tests for the merge extract + reconcile flow using real DuckDB and temp parquet.

Two tiny per-"site" tables are extracted (scope=ALL) into provenance-named chunk files,
then reconciled into a single merged table. Asserts union row counts, column union
(union_by_name), and provenance file naming. A second test exercises the participant-id
subset (v2) extract path.

The shared storage backend is switched to 'local' so MergeProcessor (and the fixture
helpers below) read/write real files via the file:// scheme; DuckDB's temp directory is
redirected to a temp path. All DuckDB access goes through utils.execute_duckdb_sql — the
project's connection helper — so no test opens a raw duckdb connection.
"""

import pytest

import core.constants as constants
import core.merge as merge
import core.merge_reporting as merge_reporting
import core.utils as utils
from core.storage_backend import storage as shared_storage


@pytest.fixture
def local_backend(tmp_path, monkeypatch):
    """Point the shared storage backend and DuckDB temp dir at a local temp tree."""
    monkeypatch.setattr(shared_storage, 'backend', constants.LOCAL_BACKEND)
    monkeypatch.setattr(shared_storage, 'scheme', constants.BACKENDS[constants.LOCAL_BACKEND])
    monkeypatch.setattr(constants, 'STORAGE_BACKEND', constants.LOCAL_BACKEND)

    duckdb_tmp = tmp_path / "duckdb_tmp"
    duckdb_tmp.mkdir()
    monkeypatch.setenv('DUCKDB_TEMP_DIR', f"{duckdb_tmp}/")

    return tmp_path


def _write_parquet(path: str, select_sql: str) -> None:
    """
    Write a parquet fixture via utils.execute_duckdb_sql.

    The COPY target is a file:// uri so execute_duckdb_sql's local-backend hook
    creates the parent directories automatically.
    """
    uri = shared_storage.get_uri(path)
    utils.execute_duckdb_sql(
        f"COPY ({select_sql}) TO '{uri}' (FORMAT parquet)",
        f"Unable to write test fixture to {uri}",
    )


def _query(select_sql: str) -> list:
    """Run a verification query through the helper and return all rows."""
    return utils.execute_duckdb_sql(select_sql, "Verification query failed", return_results=True)


def test_extract_all_then_reconcile_unions_rows_and_columns(local_backend):
    root = local_backend

    # siteA: 2 rows, 3 columns
    site_a_src = str(root / "siteA/2025-01-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        site_a_src,
        "SELECT * FROM (VALUES (1,101,'5.0'),(2,102,'6.0')) AS t(measurement_id, person_id, value)",
    )
    # siteB: 3 rows, with an EXTRA column to exercise union_by_name schema alignment
    site_b_src = str(root / "siteB/2025-02-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        site_b_src,
        "SELECT * FROM (VALUES (10,201,'7.0','x'),(11,202,'8.0','y'),(12,203,'9.0','z')) "
        "AS t(measurement_id, person_id, value, extra_col)",
    )

    chunk_dir = root / "ehr_merged/2026-06-24/artifacts/merge_chunks/measurement"
    chunk_a = str(chunk_dir / "measurement__siteA__2025-01-01.parquet")
    chunk_b = str(chunk_dir / "measurement__siteB__2025-02-01.parquet")
    output = str(root / "ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet")

    merge.MergeProcessor.extract_chunk(site_a_src, chunk_a, constants.PARTICIPANT_SCOPE_ALL)
    merge.MergeProcessor.extract_chunk(site_b_src, chunk_b, constants.PARTICIPANT_SCOPE_ALL)

    # Provenance-named chunk files land in the shared per-table staging folder.
    assert shared_storage.file_exists(chunk_a)
    assert shared_storage.file_exists(chunk_b)

    merge.MergeProcessor.reconcile_chunks(str(chunk_dir / "*.parquet"), output)
    assert shared_storage.file_exists(output)

    output_uri = shared_storage.get_uri(output)
    merged_rows = _query(f"SELECT extra_col FROM read_parquet('{output_uri}')")
    assert len(merged_rows) == 5  # 2 (siteA) + 3 (siteB)

    columns = {r[0] for r in _query(f"DESCRIBE SELECT * FROM read_parquet('{output_uri}')")}
    assert {"measurement_id", "person_id", "value", "extra_col"} <= columns

    # siteA rows (no extra_col) are NULL-filled for the unioned column.
    null_extra = sum(1 for (extra_col,) in merged_rows if extra_col is None)
    assert null_extra == 2


def test_extract_id_scope_subsets_participants(local_backend):
    root = local_backend

    src = str(root / "siteA/2025-01-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        src,
        "SELECT * FROM (VALUES (1,101),(2,102),(3,103)) AS t(measurement_id, person_id)",
    )
    # Keep only participants 101 and 103. The id column is numeric, matching person_id.
    ids_uri = str(root / "ehr_merged/2026-06-24/artifacts/merge_chunks/_ids/keep.parquet")
    _write_parquet(ids_uri, "SELECT * FROM (VALUES (101),(103)) AS t(id)")

    chunk = str(root / "ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet")

    merge.MergeProcessor.extract_chunk(src, chunk, ids_uri)

    chunk_uri = shared_storage.get_uri(chunk)
    kept = [
        r[0]
        for r in _query(f"SELECT person_id FROM read_parquet('{chunk_uri}') ORDER BY person_id")
    ]
    assert kept == [101, 103]


def test_extract_stamps_person_care_site_id(local_backend):
    root = local_backend

    # Source person rows carry an unrelated care_site_id that must be overwritten.
    src = str(root / "siteA/2025-01-01/artifacts/converted_files/person.parquet")
    _write_parquet(
        src,
        "SELECT * FROM (VALUES (101, 999),(102, 999)) AS t(person_id, care_site_id)",
    )
    chunk = str(root / "ehr_merged/2026-06-24/artifacts/merge_chunks/person/person__siteA__2025-01-01.parquet")

    merge.MergeProcessor.extract_chunk(
        src, chunk, constants.PARTICIPANT_SCOPE_ALL, site_display_name="Site A"
    )

    expected_id = merge.MergeProcessor.hash_care_site_id("Site A")
    chunk_uri = shared_storage.get_uri(chunk)
    ids = _query(f"SELECT DISTINCT care_site_id FROM read_parquet('{chunk_uri}')")
    assert ids == [(expected_id,)]


def test_build_care_site_writes_typed_row_per_site(local_backend):
    root = local_backend
    output = str(root / "ehr_merged/2026-06-24/artifacts/converted_files/care_site.parquet")

    merge.MergeProcessor.build_care_site(output, ["Site A", "Site B"], "5.4")

    output_uri = shared_storage.get_uri(output)
    rows = _query(
        f"SELECT care_site_id, care_site_name FROM read_parquet('{output_uri}') ORDER BY care_site_name"
    )
    assert rows == [
        (merge.MergeProcessor.hash_care_site_id("Site A"), "Site A"),
        (merge.MergeProcessor.hash_care_site_id("Site B"), "Site B"),
    ]

    # Types come from the OMOP care_site schema, so the parquet loads to BQ cleanly.
    types = {name: col_type for name, col_type, *_ in _query(f"DESCRIBE SELECT * FROM read_parquet('{output_uri}')")}
    assert types["care_site_id"] == "BIGINT"
    assert types["care_site_name"] == "VARCHAR"


def test_build_cdm_source_writes_one_row_with_latest_release_date(local_backend):
    root = local_backend

    # Two sites' cdm_source files with different source_release_date values.
    src_a = str(root / "siteA/2025-01-01/artifacts/converted_files/cdm_source.parquet")
    _write_parquet(src_a, "SELECT CAST('2024-06-01' AS DATE) AS source_release_date")
    src_b = str(root / "siteB/2025-02-01/artifacts/converted_files/cdm_source.parquet")
    _write_parquet(src_b, "SELECT CAST('2025-03-15' AS DATE) AS source_release_date")

    output = str(root / "ehr_merged/2026-06-24/artifacts/converted_files/cdm_source.parquet")
    merge.MergeProcessor.build_cdm_source(
        output_uri=output,
        source_cdm_source_uris=[src_a, src_b],
        site_count=2,
        cdm_version="5.4",
        vocabulary_version="v5.0 27-AUG-25",
        cdm_release_date="2026-06-24",
    )

    output_uri = shared_storage.get_uri(output)
    rows = _query(
        "SELECT cdm_source_name, source_description, source_release_date, cdm_release_date, "
        f"cdm_version, cdm_version_concept_id, vocabulary_version FROM read_parquet('{output_uri}')"
    )
    assert len(rows) == 1
    (name, desc, source_release, cdm_release, cdm_ver, concept_id, vocab) = rows[0]
    assert name == constants.MERGE_CDM_SOURCE_NAME
    assert "from 2 sites" in desc
    assert str(source_release) == "2025-03-15"  # latest across the two sites
    assert str(cdm_release) == "2026-06-24"
    assert cdm_ver == "5.4"
    assert concept_id == 756265
    assert vocab == "v5.0 27-AUG-25"


def test_generate_merge_report_writes_provenance_csv(local_backend):
    root = local_backend
    merge_bucket = str(root / "ehr_merged")
    run_date = "2026-06-24"
    chunks = f"{merge_bucket}/{run_date}/artifacts/merge_chunks"

    # siteA delivery: measurement (2 rows) + person (1 row) = 3 rows into the merge.
    _write_parquet(
        f"{chunks}/measurement/measurement__siteA__2025-01-01.parquet",
        "SELECT * FROM (VALUES (1,101),(2,102)) AS t(measurement_id, person_id)",
    )
    _write_parquet(
        f"{chunks}/person/person__siteA__2025-01-01.parquet",
        "SELECT * FROM (VALUES (101)) AS t(person_id)",
    )
    # siteB delivery: measurement (3 rows) = 3 rows into the merge.
    _write_parquet(
        f"{chunks}/measurement/measurement__siteB__2025-02-01.parquet",
        "SELECT * FROM (VALUES (10,201),(11,202),(12,203)) AS t(measurement_id, person_id)",
    )

    deliveries = [
        {"site": "siteA", "delivery_date": "2025-01-01"},
        {"site": "siteB", "delivery_date": "2025-02-01"},
    ]
    merge_reporting.MergeReporter.generate_merge_report(
        merge_bucket=merge_bucket, run_date=run_date, site="merged_ehr", deliveries=deliveries
    )

    csv_uri = shared_storage.get_uri(
        f"{merge_bucket}/{run_date}/artifacts/delivery_report/delivery_report_merged_ehr_{run_date}.csv"
    )
    report = _query(f"SELECT name, value_as_string, value_as_number FROM read_csv('{csv_uri}', header=true)")

    # Which sites were included.
    included_sites = {value_as_string for name, value_as_string, _ in report if name == "Merge source site"}
    assert included_sites == {"siteA", "siteB"}

    # Which deliveries were included + rows each contributed.
    delivery_row_counts = {
        value_as_string: int(value_as_number)
        for name, value_as_string, value_as_number in report
        if name == "Merge source delivery row count"
    }
    assert delivery_row_counts == {"siteA__2025-01-01": 3, "siteB__2025-02-01": 3}

    # Total rows across the whole merge (sum of the per-delivery counts).
    total = [int(value_as_number) for name, _, value_as_number in report if name == "Merge total row count"]
    assert total == [6]
