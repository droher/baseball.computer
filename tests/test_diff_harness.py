"""Unit tests for `scripts/diff_dbt_vs_sqlmesh.py`.

Each test builds two trivial DuckDB files in a tmp dir and exercises one
property of the diff harness. Tests are isolated — no module-level state
mutation, no shared fixtures, no global path patches.
"""

from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from diff_dbt_vs_sqlmesh import run_diff  # noqa: E402


def _build_db(path: Path, schema: str, tables: dict[str, list[tuple]]) -> None:
    """Create a DuckDB file with the given schema + tables.

    `tables` maps table_name -> list of (id INT, name VARCHAR) tuples.
    """
    conn = duckdb.connect(str(path))
    try:
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        for name, rows in tables.items():
            conn.execute(
                f"CREATE TABLE {schema}.{name} (id INTEGER, name VARCHAR)"
            )
            for row in rows:
                conn.execute(
                    f"INSERT INTO {schema}.{name} VALUES (?, ?)", list(row)
                )
    finally:
        conn.close()


def test_identical_tables_match(tmp_path: Path) -> None:
    rows = [(1, "a"), (2, "b"), (3, "c")]
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _build_db(a, "main_models", {"t1": rows})
    _build_db(b, "main_models__dev", {"t1": rows})

    verdicts = run_diff(a, b, dbt_schema="main_models", sqlmesh_schema="main_models__dev")
    assert len(verdicts) == 1
    v = verdicts[0]
    assert v.matches, f"expected match, got: {v}"
    assert v.row_count_dbt == v.row_count_sqlmesh == 3


def test_one_row_diff_detected(tmp_path: Path) -> None:
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _build_db(a, "main_models", {"t1": [(1, "a"), (2, "b"), (3, "c")]})
    _build_db(b, "main_models__dev", {"t1": [(1, "a"), (2, "b"), (3, "DIFFERENT")]})

    verdicts = run_diff(a, b, dbt_schema="main_models", sqlmesh_schema="main_models__dev")
    v = verdicts[0]
    assert not v.matches
    assert v.row_count_dbt == v.row_count_sqlmesh == 3  # row count matches
    assert v.sum_hash_dbt != v.sum_hash_sqlmesh  # but hash diverges


def test_extra_column_detected(tmp_path: Path) -> None:
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA main_models")
    conn_a.execute("CREATE TABLE main_models.t1 (id INTEGER, name VARCHAR)")
    conn_a.execute("INSERT INTO main_models.t1 VALUES (1, 'a')")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA main_models__dev")
    conn_b.execute("CREATE TABLE main_models__dev.t1 (id INTEGER, name VARCHAR, extra INTEGER)")
    conn_b.execute("INSERT INTO main_models__dev.t1 VALUES (1, 'a', 99)")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="main_models", sqlmesh_schema="main_models__dev")
    v = verdicts[0]
    assert not v.matches
    assert v.cols_only_in_sqlmesh == ("extra",)
    assert v.cols_only_in_dbt == ()


def test_missing_table_in_sqlmesh(tmp_path: Path) -> None:
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _build_db(a, "main_models", {"t1": [(1, "a")], "t2": [(2, "b")]})
    _build_db(b, "main_models__dev", {"t1": [(1, "a")]})

    verdicts = run_diff(a, b, dbt_schema="main_models", sqlmesh_schema="main_models__dev")
    by_name = {v.table: v for v in verdicts}
    assert by_name["t1"].matches
    assert by_name["t2"].only_in_dbt
    assert not by_name["t2"].matches


def test_row_order_does_not_affect_hash(tmp_path: Path) -> None:
    """Cosmetic ordering differences must not register as a mismatch."""
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _build_db(a, "s", {"t1": [(1, "a"), (2, "b"), (3, "c")]})
    _build_db(b, "s2", {"t1": [(3, "c"), (1, "a"), (2, "b")]})

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert verdicts[0].matches


def test_enum_cast_to_varchar_for_hash(tmp_path: Path) -> None:
    """ENUM ordinal differences must not poison the hash."""
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TYPE side_a AS ENUM ('Home', 'Away')")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, side side_a)")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 'Home'), (2, 'Away')")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    # Ordinals reversed — would yield different HASH(int) but same HASH(VARCHAR).
    conn_b.execute("CREATE TYPE side_b AS ENUM ('Away', 'Home')")
    conn_b.execute("CREATE TABLE s2.t1 (id INTEGER, side side_b)")
    conn_b.execute("INSERT INTO s2.t1 VALUES (1, 'Home'), (2, 'Away')")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert verdicts[0].matches, f"ENUM ordinal divergence leaked into hash: {verdicts[0]}"


def test_decimal_precision_drift_does_not_register(tmp_path: Path) -> None:
    """DECIMAL(p,s) widening across engines must not register as a diff
    when the values themselves are identical.

    `metrics_table_generator` and other dbt models occasionally produce
    a tighter decimal (e.g. DECIMAL(2,2)) than the SQLMesh-built copy
    (DECIMAL(3,2)) for the same numeric values. Hashing the raw row
    breaks because the VARCHAR rendering of the type differs even when
    the value is identical; canonicalizing through DOUBLE fixes it.
    """
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, price DECIMAL(2,2))")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 0.10), (2, 0.25), (3, NULL)")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    conn_b.execute("CREATE TABLE s2.t1 (id INTEGER, price DECIMAL(3,2))")
    conn_b.execute("INSERT INTO s2.t1 VALUES (1, 0.10), (2, 0.25), (3, NULL)")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert verdicts[0].matches, f"precision drift leaked into hash: {verdicts[0]}"


def test_column_ordinal_swap_does_not_register(tmp_path: Path) -> None:
    """Same logical columns + same data, swapped CREATE TABLE column order
    must hash equal — engines occasionally materialize the same model with
    different ordinal_position. The actual values per (row, col) match.
    """
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, name VARCHAR)")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 'a'), (2, 'b')")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    conn_b.execute("CREATE TABLE s2.t1 (name VARCHAR, id INTEGER)")  # swapped
    conn_b.execute("INSERT INTO s2.t1 VALUES ('a', 1), ('b', 2)")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert verdicts[0].matches, f"col-ordinal swap leaked into hash: {verdicts[0]}"


def test_value_swap_between_rows_still_caught(tmp_path: Path) -> None:
    """Sanity: column-name-sorted hashing must still bind cells within
    one row. Swapping two rows' name field but keeping ids constant must
    still register as a mismatch (otherwise per-column sum-hash would
    falsely pass)."""
    a, b = tmp_path / "a.db", tmp_path / "b.db"
    _build_db(a, "s", {"t1": [(1, "a"), (2, "b")]})
    _build_db(b, "s2", {"t1": [(1, "b"), (2, "a")]})  # names swapped

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert not verdicts[0].matches


def test_real_value_diff_in_decimal_still_caught(tmp_path: Path) -> None:
    """Sanity: canonical hashing must still catch genuine value diffs."""
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, price DECIMAL(3,2))")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 0.10), (2, 0.25)")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    conn_b.execute("CREATE TABLE s2.t1 (id INTEGER, price DECIMAL(3,2))")
    conn_b.execute("INSERT INTO s2.t1 VALUES (1, 0.10), (2, 0.99)")  # different
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    assert not verdicts[0].matches


def test_base_type_divergence_detected(tmp_path: Path) -> None:
    """A column declared INTEGER on one side and VARCHAR on the other must
    register a type-diff even when the canonical-VARCHAR hash happens to
    agree (e.g. INTEGER 1 vs VARCHAR '1' both render to "1").
    """
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, code INTEGER)")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 1), (2, 2)")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    conn_b.execute("CREATE TABLE s2.t1 (id INTEGER, code VARCHAR)")
    conn_b.execute("INSERT INTO s2.t1 VALUES (1, '1'), (2, '2')")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    v = verdicts[0]
    assert not v.matches, "type divergence (INTEGER vs VARCHAR) must register"
    assert any(t[0] == "code" for t in v.type_diffs), f"expected code in type_diffs, got: {v.type_diffs}"


def test_decimal_precision_drift_does_not_register_as_type_diff(tmp_path: Path) -> None:
    """DECIMAL(2,2) vs DECIMAL(3,2) is a precision widen, not a base-type
    divergence. The harness must not flag it as a type diff."""
    a, b = tmp_path / "a.db", tmp_path / "b.db"

    conn_a = duckdb.connect(str(a))
    conn_a.execute("CREATE SCHEMA s")
    conn_a.execute("CREATE TABLE s.t1 (id INTEGER, price DECIMAL(2,2))")
    conn_a.execute("INSERT INTO s.t1 VALUES (1, 0.10)")
    conn_a.close()

    conn_b = duckdb.connect(str(b))
    conn_b.execute("CREATE SCHEMA s2")
    conn_b.execute("CREATE TABLE s2.t1 (id INTEGER, price DECIMAL(3,2))")
    conn_b.execute("INSERT INTO s2.t1 VALUES (1, 0.10)")
    conn_b.close()

    verdicts = run_diff(a, b, dbt_schema="s", sqlmesh_schema="s2")
    v = verdicts[0]
    assert v.matches, f"precision-only drift incorrectly flagged: {v}"
    assert v.type_diffs == ()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
