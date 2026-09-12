"""The supplement's verified queries parse, cover the layer, and run.

The fast checks read ``docs/llm/supplement.yaml`` directly. The slow
check executes every SQL entry against the local published DuckLake
catalog and requires at least one row with a non-null value, so a wrong
player id or column name fails instead of returning a quiet NULL.
"""

from __future__ import annotations

import importlib.util
import re
import sys
import uuid
from pathlib import Path
from typing import Any

import duckdb
import pytest
import sqlglot

from semantic._views import SEMANTIC_SCHEMA, SEMANTIC_VIEWS

REPO_ROOT = Path(__file__).resolve().parents[2]
GENERATOR_PATH = REPO_ROOT / "scripts" / "generate_llm_context.py"
SUPPLEMENT_PATH = REPO_ROOT / "docs" / "llm" / "supplement.yaml"
CATALOG_PATH = REPO_ROOT / "bc" / "bc_publish.ducklake"
DATA_PATH = REPO_ROOT / "bc" / "bc_publish_data"
MIN_SQL_QUERIES = 30
MIN_CLARIFY_QUERIES = 2


def _load_generator() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"llm_ctx_gen_vq_{uuid.uuid4().hex}", GENERATOR_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def supplement() -> Any:
    return _load_generator()._load_supplement(SUPPLEMENT_PATH)


def _sql_queries(supplement: Any) -> list[Any]:
    return [vq for vq in supplement.verified_queries if vq.sql is not None]


def _tables_referenced(sql: str) -> set[str]:
    tree = sqlglot.parse_one(sql, read="duckdb")
    out: set[str] = set()
    for table in tree.find_all(sqlglot.exp.Table):
        if table.db:
            out.add(f"{table.db}.{table.name}")
    return out


def test_verified_queries_have_unique_ids_and_parse(supplement: Any) -> None:
    ids = [vq.id for vq in supplement.verified_queries]
    assert len(ids) == len(set(ids))
    sql_queries = _sql_queries(supplement)
    assert len(sql_queries) >= MIN_SQL_QUERIES
    for vq in sql_queries:
        tree = sqlglot.parse_one(vq.sql, read="duckdb")
        assert isinstance(tree, (sqlglot.exp.Select, sqlglot.exp.Union)), vq.id


def test_verified_queries_cover_every_semantic_view(supplement: Any) -> None:
    referenced: set[str] = set()
    for vq in _sql_queries(supplement):
        referenced |= _tables_referenced(vq.sql)
    for name, _kind, _grain in SEMANTIC_VIEWS:
        assert f"{SEMANTIC_SCHEMA}.{name}" in referenced, name


def test_verified_queries_cover_required_grains_and_eras(supplement: Any) -> None:
    sql_queries = _sql_queries(supplement)
    joined_sql = "\n".join(vq.sql for vq in sql_queries)
    assert "metrics_player_career_" in joined_sql
    assert re.search(r"group by team_id", joined_sql)
    assert "franchise_id" in joined_sql
    assert re.search(r"season = 1[89]\d\d\b", joined_sql)
    assert "is_postseason" in joined_sql
    assert re.search(r"game_type = '\w+Series'", joined_sql)
    outside = [
        vq
        for vq in sql_queries
        if "join main_models." in vq.sql
        and not _tables_referenced(vq.sql)
        & {f"{SEMANTIC_SCHEMA}.{name}" for name, _k, _g in SEMANTIC_VIEWS}
    ]
    assert len(outside) >= 1
    joins = [vq for vq in sql_queries if "join main_models." in vq.sql]
    assert len(joins) >= 2
    assert re.search(r"metrics\.\w+\(", joined_sql)


def test_clarify_queries_reference_ambiguous_terms(supplement: Any) -> None:
    clarify = [vq for vq in supplement.verified_queries if vq.clarify]
    assert len(clarify) >= MIN_CLARIFY_QUERIES
    terms = {a.term.lower() for a in supplement.ambiguities}
    for vq in clarify:
        assert vq.sql is None
        text = f"{vq.question} {vq.notes or ''}".lower()
        assert any(term in text for term in terms), vq.id


@pytest.mark.slow
def test_verified_queries_execute_against_published_catalog(supplement: Any) -> None:
    if not CATALOG_PATH.exists():
        pytest.skip(f"published catalog missing: {CATALOG_PATH}")
    con = duckdb.connect(":memory:")
    _ = con.execute("LOAD ducklake")
    _ = con.execute(
        f"ATTACH 'ducklake:{CATALOG_PATH}' AS bc_publish "
        f"(DATA_PATH '{DATA_PATH}/', OVERRIDE_DATA_PATH true, READ_ONLY)"
    )
    _ = con.execute("USE bc_publish")
    for vq in _sql_queries(supplement):
        rows = con.execute(vq.sql).fetchall()
        assert rows, f"{vq.id} returned no rows"
        assert all(any(v is not None for v in row) for row in rows), (
            f"{vq.id} returned NULLs"
        )
