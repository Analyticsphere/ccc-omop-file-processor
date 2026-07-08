"""
End-to-end tests for the merge extract + reconcile flow using real DuckDB and temp parquet.

Two tiny per-"site" tables are extracted (scope=ALL) into provenance-named chunk files,
then reconciled into a single merged table. Asserts union row counts, column union
(union_by_name), and provenance file naming. A second test exercises the participant-id
subset (v2) extract path.

The shared storage backend is switched to 'local' so MergeProcessor reads/writes real
files via the file:// scheme; DuckDB's temp directory is redirected to a temp path.
"""

import os

import duckdb
import pytest

import core.constants as constants
import core.merge as merge
from core.storage_backend import storage as shared_storage


def _write_parquet(con, path: str, select_sql: str) -> None:
    """Write a parquet file at a plain local path (creating parent dirs)."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    con.execute(f"COPY ({select_sql}) TO '{path}' (FORMAT parquet)")


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


def test_extract_all_then_reconcile_unions_rows_and_columns(local_backend):
    root = local_backend
    con = duckdb.connect()

    # siteA: 2 rows, 3 columns
    site_a_src = str(root / "siteA/2025-01-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        con,
        site_a_src,
        "SELECT * FROM (VALUES (1,'101','5.0'),(2,'102','6.0')) AS t(measurement_id, person_id, value)",
    )
    # siteB: 3 rows, with an EXTRA column to exercise union_by_name schema alignment
    site_b_src = str(root / "siteB/2025-02-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        con,
        site_b_src,
        "SELECT * FROM (VALUES (10,'201','7.0','x'),(11,'202','8.0','y'),(12,'203','9.0','z')) "
        "AS t(measurement_id, person_id, value, extra_col)",
    )

    chunk_dir = root / "ehr_merged/2026-06-24/artifacts/merge_chunks/measurement"
    chunk_a = str(chunk_dir / "measurement__siteA__2025-01-01.parquet")
    chunk_b = str(chunk_dir / "measurement__siteB__2025-02-01.parquet")
    output = str(root / "ehr_merged/2026-06-24/artifacts/converted_files/measurement.parquet")

    merge.MergeProcessor.extract_chunk(site_a_src, chunk_a, constants.PARTICIPANT_SCOPE_ALL)
    merge.MergeProcessor.extract_chunk(site_b_src, chunk_b, constants.PARTICIPANT_SCOPE_ALL)

    # Provenance-named chunk files land in the shared per-table staging folder.
    assert os.path.exists(chunk_a)
    assert os.path.exists(chunk_b)

    merge.MergeProcessor.reconcile_chunks(str(chunk_dir / "*.parquet"), output)
    assert os.path.exists(output)

    verify = duckdb.connect()
    row_count = verify.execute(f"SELECT count(*) FROM read_parquet('{output}')").fetchone()[0]
    assert row_count == 5  # 2 (siteA) + 3 (siteB)

    columns = {r[0] for r in verify.execute(f"DESCRIBE SELECT * FROM read_parquet('{output}')").fetchall()}
    assert {"measurement_id", "person_id", "value", "extra_col"} <= columns

    # siteA rows (no extra_col) are NULL-filled for the unioned column.
    null_extra = verify.execute(
        f"SELECT count(*) FROM read_parquet('{output}') WHERE extra_col IS NULL"
    ).fetchone()[0]
    assert null_extra == 2


def test_extract_id_scope_subsets_participants(local_backend):
    root = local_backend
    con = duckdb.connect()

    src = str(root / "siteA/2025-01-01/artifacts/converted_files/measurement.parquet")
    _write_parquet(
        con,
        src,
        "SELECT * FROM (VALUES (1,'101'),(2,'102'),(3,'103')) AS t(measurement_id, person_id)",
    )
    # Keep only participants 101 and 103.
    ids_uri = str(root / "ehr_merged/2026-06-24/artifacts/merge_chunks/_ids/keep.parquet")
    _write_parquet(con, ids_uri, "SELECT * FROM (VALUES (101),(103)) AS t(id)")

    chunk = str(root / "ehr_merged/2026-06-24/artifacts/merge_chunks/measurement/chunk.parquet")

    merge.MergeProcessor.extract_chunk(src, chunk, ids_uri)

    verify = duckdb.connect()
    kept = [
        r[0]
        for r in verify.execute(
            f"SELECT person_id FROM read_parquet('{chunk}') ORDER BY person_id"
        ).fetchall()
    ]
    assert kept == ['101', '103']
