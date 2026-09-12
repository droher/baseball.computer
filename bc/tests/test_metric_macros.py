"""Every published metric macro compiles and matches the packet's METRIC expr.

Runs on a throwaway DuckLake catalog with random counting stats, so it is
hermetic. The macro call for each METRIC record is taken from the same
description text the packet hands to the LLM, which is what a reader will
copy.
"""

from __future__ import annotations

import importlib.util
import random
import re
import sys
from pathlib import Path
from typing import Any

import duckdb
import pytest

from python_models.metrics import _constants as metric_constants
from python_models.metrics import registry as metric_registry
from semantic import _layout as bsl_layout

REPO_ROOT = Path(__file__).resolve().parents[2]
PUBLISHER_PATH = REPO_ROOT / "scripts" / "publish_ducklake.py"
GENERATOR_PATH = REPO_ROOT / "scripts" / "generate_llm_context.py"

_MACRO_CALL = re.compile(r"macro metrics\.(\w+)\(([^)]*)\)")


def _load(name: str, path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def publisher() -> Any:
    return _load("_publish_ducklake_for_macros", PUBLISHER_PATH)


@pytest.fixture(scope="module")
def generator() -> Any:
    return _load("_llm_ctx_gen_for_macros", GENERATOR_PATH)


@pytest.fixture(scope="module")
def packet_metrics(generator: Any) -> list[Any]:
    supplement = generator._load_supplement(generator.DEFAULT_SUPPLEMENT)
    _tables, metrics = generator._build_bsl_tables(
        metric_registry, bsl_layout, supplement, metric_constants
    )
    return metrics


def _macro_call(metric: Any) -> tuple[str, str]:
    call = _MACRO_CALL.search(metric.description)
    assert call is not None, f"{metric.metric_id} description lacks its macro call"
    return call.group(1), call.group(2)


@pytest.fixture
def catalog_root(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def catalog(
    catalog_root: Path, publisher: Any, packet_metrics: list[Any]
) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{catalog_root / 'cat.ducklake'}' AS bc_publish "
        f"(DATA_PATH '{catalog_root / 'data'}/')"
    )
    specs = publisher.macro_specs(publisher.metric_slices())
    columns = {c for s in specs for c in s.params}
    for m in packet_metrics:
        _name, args = _macro_call(m)
        columns.update(a.strip() for a in args.split(","))
    columns = sorted(columns)
    rng = random.Random(3)
    rows = 300
    values = ", ".join(
        "(" + ", ".join([str(i % 5), *(str(rng.randint(1, 40)) for _ in columns)]) + ")"
        for i in range(rows)
    )
    _ = con.execute("CREATE SCHEMA bc_publish.main_models")
    _ = con.execute(
        f"CREATE TABLE bc_publish.main_models.sample (grp INTEGER, {', '.join(f'{c} INTEGER' for c in columns)})"
    )
    _ = con.execute(f"INSERT INTO bc_publish.main_models.sample VALUES {values}")
    _ = publisher.publish_metric_macros(con, specs)
    return con


def _expand(expr: str, siblings: dict[str, str], depth: int = 0) -> str:
    assert depth < 10, "sibling expansion did not converge"
    out = expr
    for name, sibling in siblings.items():
        out = re.sub(rf"\b{name}\b", f"({sibling})", out)
    return out if out == expr else _expand(out, siblings, depth + 1)


def test_every_metric_record_has_a_callable_macro_matching_its_expr(
    catalog: duckdb.DuckDBPyConnection, packet_metrics: list[Any]
) -> None:
    by_table: dict[str, dict[str, str]] = {}
    for m in packet_metrics:
        by_table.setdefault(m.base_table, {})[m.name] = m.expr
    published = {
        r[0]
        for r in catalog.execute(
            "SELECT function_name FROM duckdb_functions()"
            " WHERE database_name = 'bc_publish' AND schema_name = 'metrics'"
        ).fetchall()
    }
    assert published == {m.name for m in packet_metrics}
    for m in packet_metrics:
        name, args = _macro_call(m)
        assert name == m.name
        expanded = _expand(m.expr, by_table[m.base_table])
        rows = catalog.execute(
            f"SELECT grp, bc_publish.metrics.{name}({args}) AS via_macro, {expanded} AS via_expr"
            " FROM bc_publish.main_models.sample GROUP BY grp ORDER BY grp"
        ).fetchall()
        assert len(rows) == 5
        for grp, via_macro, via_expr in rows:
            assert via_macro is not None, f"{m.metric_id} grp={grp} macro returned NULL"
            assert abs(float(via_macro) - float(via_expr)) < 1e-9, (
                f"{m.metric_id} grp={grp}: macro={via_macro} expr={via_expr}"
            )


def test_macros_survive_read_only_reattach(
    catalog: duckdb.DuckDBPyConnection, catalog_root: Path
) -> None:
    _ = catalog.execute("DETACH bc_publish")
    reader = duckdb.connect(":memory:")
    _ = reader.execute("LOAD ducklake")
    _ = reader.execute(
        f"ATTACH 'ducklake:{catalog_root / 'cat.ducklake'}' AS elsewhere "
        f"(DATA_PATH '{catalog_root / 'data'}/', READ_ONLY)"
    )
    row = reader.execute(
        "SELECT elsewhere.metrics.batting_average(hits, at_bats) FROM elsewhere.main_models.sample"
    ).fetchone()
    assert row is not None and row[0] is not None


def test_publishing_macros_drops_ones_no_longer_registered(
    catalog: duckdb.DuckDBPyConnection, publisher: Any
) -> None:
    _ = catalog.execute(
        'CREATE MACRO "bc_publish"."metrics"."retired_rate"(a, b) AS sum(a) / sum(b)'
    )
    assert "retired_rate" in publisher.list_macros(catalog, "metrics")
    names = publisher.publish_metric_macros(
        catalog, publisher.macro_specs(publisher.metric_slices())
    )
    published = publisher.list_macros(catalog, "metrics")
    assert "retired_rate" not in published
    assert sorted(published) == sorted(names)
