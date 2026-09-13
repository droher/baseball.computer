"""Row-count floors guard the DuckLake publish against empty estimated tables."""

from __future__ import annotations

import ast
import importlib.util
import re
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest

from python_models.statistical.publication_tiers import estimated_model_names

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "publish_ducklake.py"
COVERAGE_MODELS_DIR = REPO_ROOT / "bc" / "models" / "intermediate" / "coverage"
DOCUMENTED_EMPTY_TABLES = frozenset(
    {"pitch_count_coverage", "imputed_advancement_probabilities"}
)


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


def _declared_min_row_counts() -> dict[str, int]:
    declared: dict[str, int] = {}
    for path in sorted(COVERAGE_MODELS_DIR.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if not (
                isinstance(node, ast.Tuple)
                and len(node.elts) == 2
                and isinstance(node.elts[0], ast.Constant)
                and node.elts[0].value == "min_row_count"
                and isinstance(node.elts[1], ast.Dict)
            ):
                continue
            args = {
                key.value: ast.literal_eval(value)
                for key, value in zip(
                    node.elts[1].keys, node.elts[1].values, strict=True
                )
                if isinstance(key, ast.Constant)
            }
            declared[path.stem] = int(args["threshold"])
    for path in sorted(COVERAGE_MODELS_DIR.glob("*.sql")):
        text = path.read_text(encoding="utf-8")
        match = re.search(r"min_row_count\s*\(\s*threshold\s*:=\s*(\d+)\s*\)", text)
        if match is not None:
            declared[path.stem] = int(match.group(1))
    return declared


def test_floors_match_model_audit_declarations(script: Any) -> None:
    assert _declared_min_row_counts() == script.ESTIMATED_ROW_COUNT_FLOORS


def test_every_estimated_table_has_a_floor_or_is_documented_empty(script: Any) -> None:
    floored = set(script.ESTIMATED_ROW_COUNT_FLOORS)
    assert floored.isdisjoint(DOCUMENTED_EMPTY_TABLES)
    assert floored | DOCUMENTED_EMPTY_TABLES == set(estimated_model_names())


def test_floors_are_positive(script: Any) -> None:
    assert all(floor > 0 for floor in script.ESTIMATED_ROW_COUNT_FLOORS.values())


def _connection_with_tables(rows_by_table: dict[str, int]) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _ = con.execute("CREATE SCHEMA main_models")
    for table, rows in rows_by_table.items():
        _ = con.execute(
            f'CREATE TABLE main_models."{table}" AS SELECT * FROM range({rows}) AS t(id)'
        )
    return con


def test_populated_tables_pass(script: Any) -> None:
    floors = {"alpha": 10, "beta": 3}
    con = _connection_with_tables({"alpha": 10, "beta": 50})
    counts = script.assert_estimated_tables_populated(
        con, catalog="memory", floors=floors
    )
    assert counts == {"alpha": 10, "beta": 50}


def test_empty_table_refuses_publish(script: Any) -> None:
    floors = {"alpha": 10, "beta": 3}
    con = _connection_with_tables({"alpha": 0, "beta": 50})
    with pytest.raises(SystemExit, match=r"main_models\.alpha: 0 rows < 10"):
        script.assert_estimated_tables_populated(con, catalog="memory", floors=floors)


def test_table_below_floor_refuses_publish(script: Any) -> None:
    floors = {"alpha": 10}
    con = _connection_with_tables({"alpha": 9})
    with pytest.raises(SystemExit, match="refusing to publish"):
        script.assert_estimated_tables_populated(con, catalog="memory", floors=floors)


def test_source_database_is_attached_read_only(script: Any, tmp_path: Path) -> None:
    source_path = tmp_path / "source.db"
    source = duckdb.connect(str(source_path))
    _ = source.execute("CREATE TABLE source_table (id INTEGER)")
    source.close()

    con = duckdb.connect(":memory:")
    script.attach_source_database(con, source_path)

    with pytest.raises(duckdb.InvalidInputException):
        _ = con.execute("CREATE TABLE bc.unexpected_write (id INTEGER)")


def test_candidate_gate_refuses_missing_root(script: Any, tmp_path: Path) -> None:
    con = duckdb.connect(":memory:")
    with pytest.raises(SystemExit, match="invalid PBP imputation root"):
        script.assert_pbp_candidate_ready(con, tmp_path / "missing")


def test_candidate_gate_refuses_false_report(script: Any, tmp_path: Path) -> None:
    source_path = tmp_path / "source.db"
    source = duckdb.connect(str(source_path))
    source.close()
    root = tmp_path / "candidate"
    root.mkdir()
    con = duckdb.connect(":memory:")
    script.attach_source_database(con, source_path)

    captured: list[object] = []

    def rejected(*args: object) -> dict[str, object]:
        captured.extend(args)
        return {"candidate_ready": False}

    with pytest.raises(SystemExit, match="candidate validation failed"):
        script.assert_pbp_candidate_ready(con, root, checker=rejected)
    assert captured[2] == "main_models"
    assert con.execute("SELECT current_database()").fetchone() == ("memory",)


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
