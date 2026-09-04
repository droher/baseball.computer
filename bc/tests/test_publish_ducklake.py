"""Row-count floors guard the DuckLake publish against empty estimated tables."""

from __future__ import annotations

import ast
import importlib.util
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
