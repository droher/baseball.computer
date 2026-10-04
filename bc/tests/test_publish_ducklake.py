"""DuckLake publish script behavior."""

from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "publish_ducklake.py"


def _load_script() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"publish_ducklake_test_{uuid.uuid4().hex}", SCRIPT_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def script() -> Any:
    return _load_script()


def test_source_database_is_attached_read_only(script: Any, tmp_path: Path) -> None:
    source_path = tmp_path / "source.db"
    source = duckdb.connect(str(source_path))
    _ = source.execute("CREATE TABLE source_table (id INTEGER)")
    source.close()

    con = duckdb.connect(":memory:")
    script.attach_source_database(con, source_path)

    with pytest.raises(duckdb.InvalidInputException):
        _ = con.execute("CREATE TABLE bc.unexpected_write (id INTEGER)")


def test_publish_table_matches_read_only_source_rows(
    script: Any, tmp_path: Path
) -> None:
    source_path = tmp_path / "source.db"
    source = duckdb.connect(str(source_path))
    _ = source.execute("CREATE SCHEMA main_models")
    _ = source.execute(
        "CREATE TABLE main_models.players AS "
        "SELECT id, 'player-' || id::VARCHAR AS name FROM range(3) AS t(id)"
    )
    source.close()

    con = duckdb.connect(":memory:")
    script.attach_source_database(con, source_path)
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS bc_publish "
        f"(DATA_PATH '{tmp_path / 'data'}/')"
    )
    _ = con.execute("CREATE SCHEMA bc_publish.main_models")

    assert script.publish_table(con, "main_models", "players") == 3
    assert con.execute(
        "SELECT COUNT(*) FROM bc_publish.main_models.players"
    ).fetchone() == (3,)


def test_reconcile_drops_tables_missing_from_source(
    script: Any, tmp_path: Path
) -> None:
    source_path = tmp_path / "source.db"
    source = duckdb.connect(str(source_path))
    _ = source.execute("CREATE SCHEMA main_models")
    _ = source.execute("CREATE TABLE main_models.players (id INTEGER)")
    source.close()

    con = duckdb.connect(":memory:")
    script.attach_source_database(con, source_path)
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS bc_publish "
        f"(DATA_PATH '{tmp_path / 'data'}/')"
    )
    _ = con.execute("CREATE SCHEMA bc_publish.main_models")
    _ = con.execute("CREATE TABLE bc_publish.main_models.players (id INTEGER)")
    _ = con.execute("CREATE TABLE bc_publish.main_models.removed_table (id INTEGER)")

    script.reconcile_published_tables(con)

    assert script.list_tables(con, "main_models", "bc_publish") == ["players"]


def test_catalog_data_path_must_match_data_version(script: Any, tmp_path: Path) -> None:
    catalog_path = tmp_path / "catalog.ducklake"
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{catalog_path}' AS bc_publish "
        f"(DATA_PATH '{script.public_data_path('1')}')"
    )
    _ = con.execute("DETACH bc_publish")

    script.assert_catalog_data_path(con, catalog_path, "1")
    with pytest.raises(SystemExit, match="rerun with --reset"):
        script.assert_catalog_data_path(con, catalog_path, "2")


def test_missing_catalog_fails_before_smoke_attachment(
    script: Any, tmp_path: Path
) -> None:
    with pytest.raises(SystemExit, match="catalog not found"):
        script.assert_catalog_exists(tmp_path / "missing.ducklake")


def test_empty_catalog_fails_smoke(script: Any, tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{tmp_path / 'catalog.ducklake'}' AS bc_publish "
        f"(DATA_PATH '{tmp_path / 'data'}/')"
    )

    with pytest.raises(RuntimeError, match="no published tables"):
        script._smoke_run(con)


def test_reset_and_smoke_only_are_mutually_exclusive(script: Any) -> None:
    with pytest.raises(SystemExit) as exc_info:
        _ = script.main(["--reset", "--smoke-only"])
    assert exc_info.value.code == 2
