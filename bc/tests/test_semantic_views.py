"""The published semantic views equal the ibis backing expressions row for row.

Builds a throwaway DuckLake catalog from a sample of the local bc.db
(every seed, the season tables for a handful of seasons, the event tables
and team_game_start_info for a hashed sample of games), publishes the
views into it, and diffs each view against the ibis expression in
``semantic._tables_common`` in both directions with EXCEPT ALL.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import uuid
from pathlib import Path
from typing import Any

import duckdb
import ibis
import pytest

from python_models.metrics import _constants as metric_constants
from semantic import _tables_common as tables_common
from semantic._views import SEMANTIC_SCHEMA, SEMANTIC_VIEWS, semantic_view_name

REPO_ROOT = Path(__file__).resolve().parents[2]
PUBLISHER_PATH = REPO_ROOT / "scripts" / "publish_ducklake.py"
SAMPLE_SEASONS = (1884, 1927, 1968, 1994, 2019)
SAMPLE_GAME_SEASONS = (1927, 2019)
SAMPLE_GAMES = 40


def _bc_db_path() -> Path:
    return Path(os.environ.get("BC_DB_PATH", str(REPO_ROOT / "bc.db")))


def _load_publisher() -> Any:
    spec = importlib.util.spec_from_file_location(
        f"publish_ducklake_views_{uuid.uuid4().hex}", PUBLISHER_PATH
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _copy_table(
    con: duckdb.DuckDBPyConnection, publisher: Any, schema: str, table: str, where: str
) -> None:
    cols = publisher.column_types(con, schema, table)
    select_list = publisher.select_with_enum_casts(cols)
    _ = con.execute(
        f'CREATE TABLE "bc_publish"."{schema}"."{table}" AS '
        f'SELECT {select_list} FROM "bc"."{schema}"."{table}" WHERE {where}'
    )


@pytest.fixture(scope="module")
def catalog_root(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("semantic_views")


@pytest.fixture(scope="module")
def catalog(catalog_root: Path) -> duckdb.DuckDBPyConnection:
    db = _bc_db_path()
    if not db.exists():
        pytest.skip(f"bc.db not built: {db}")
    publisher = _load_publisher()
    root = catalog_root
    con = duckdb.connect(":memory:")
    _ = con.execute("INSTALL ducklake")
    _ = con.execute("LOAD ducklake")
    publisher.attach_source_database(con, db)
    _ = con.execute(
        f"ATTACH 'ducklake:{root / 'cat.ducklake'}' AS bc_publish (DATA_PATH '{root / 'data'}/')"
    )
    _ = con.execute("CREATE SCHEMA bc_publish.main_models")
    _ = con.execute("CREATE SCHEMA bc_publish.main_seeds")
    for seed in publisher.list_tables(con, "main_seeds"):
        _copy_table(con, publisher, "main_seeds", seed, "true")
    seasons = ", ".join(str(s) for s in SAMPLE_SEASONS)
    for kind in metric_constants.SEASON_MODELS:
        table = tables_common.SEASON_MODELS[kind].split(".", 1)[1]
        _copy_table(con, publisher, "main_models", table, f"season IN ({seasons})")
    game_seasons = ", ".join(str(s) for s in SAMPLE_GAME_SEASONS)
    _ = con.execute(
        "CREATE TEMP TABLE sample_games AS "
        "SELECT DISTINCT game_id FROM bc.main_models.team_game_start_info "
        f"WHERE season IN ({game_seasons}) ORDER BY hash(game_id) LIMIT {SAMPLE_GAMES}"
    )
    in_sample = "game_id IN (SELECT game_id FROM sample_games)"
    _copy_table(con, publisher, "main_models", "team_game_start_info", in_sample)
    for kind in metric_constants.EVENT_MODELS:
        table = tables_common.EVENT_MODELS[kind].split(".", 1)[1]
        _copy_table(con, publisher, "main_models", table, in_sample)
    _ = con.execute(f'CREATE SCHEMA "bc_publish"."{SEMANTIC_SCHEMA}"')
    _ = con.execute(
        f'CREATE VIEW "bc_publish"."{SEMANTIC_SCHEMA}"."retired" AS SELECT 1 AS x'
    )
    _ = publisher.publish_semantic_views(con)
    _ = con.execute("USE bc_publish")
    return con


def _ibis_sql(con: duckdb.DuckDBPyConnection, kind: Any, grain: Any) -> str:
    ibis_con = ibis.duckdb.from_connection(con)
    if grain == "season":
        expr = tables_common.season_with_league(ibis_con, kind, "prod")
    else:
        expr = tables_common.event_with_game_info(ibis_con, kind, "prod")
    return str(ibis.to_sql(expr, dialect="duckdb"))


def test_view_names_follow_kind_and_grain() -> None:
    assert [name for name, _k, _g in SEMANTIC_VIEWS] == [
        semantic_view_name(kind, grain) for _n, kind, grain in SEMANTIC_VIEWS
    ]
    assert len({name for name, _k, _g in SEMANTIC_VIEWS}) == len(SEMANTIC_VIEWS)


@pytest.mark.slow
def test_publishing_views_drops_ones_the_layout_no_longer_defines(
    catalog: duckdb.DuckDBPyConnection,
) -> None:
    views = {
        r[0]
        for r in catalog.execute(
            "SELECT view_name FROM duckdb_views()"
            f" WHERE database_name = 'bc_publish' AND schema_name = '{SEMANTIC_SCHEMA}'"
        ).fetchall()
    }
    assert views == {name for name, _k, _g in SEMANTIC_VIEWS}


@pytest.mark.slow
def test_views_bind_under_a_read_only_attach_with_another_alias(
    catalog: duckdb.DuckDBPyConnection, catalog_root: Path
) -> None:
    _ = catalog.execute("USE memory")
    _ = catalog.execute("DETACH bc_publish")
    try:
        reader = duckdb.connect(":memory:")
        _ = reader.execute("LOAD ducklake")
        _ = reader.execute(
            f"ATTACH 'ducklake:{catalog_root / 'cat.ducklake'}' AS elsewhere "
            f"(DATA_PATH '{catalog_root / 'data'}/', READ_ONLY)"
        )
        for name, _kind, _grain in SEMANTIC_VIEWS:
            (rows,) = reader.execute(
                f'SELECT count(*) FROM "elsewhere"."{SEMANTIC_SCHEMA}"."{name}"'
            ).fetchone() or (0,)
            assert rows > 0, name
        reader.close()
    finally:
        _ = catalog.execute(
            f"ATTACH 'ducklake:{catalog_root / 'cat.ducklake'}' AS bc_publish "
            f"(DATA_PATH '{catalog_root / 'data'}/')"
        )
        _ = catalog.execute("USE bc_publish")


@pytest.mark.slow
@pytest.mark.parametrize("name,kind,grain", SEMANTIC_VIEWS)
def test_view_matches_ibis_definition(
    catalog: duckdb.DuckDBPyConnection, name: str, kind: Any, grain: Any
) -> None:
    view = f'"bc_publish"."{SEMANTIC_SCHEMA}"."{name}"'
    reference = _ibis_sql(catalog, kind, grain)
    (view_rows,) = catalog.execute(f"SELECT count(*) FROM {view}").fetchone() or (0,)
    assert view_rows > 0, f"{name} sample is empty"
    view_cols = [
        d[0] for d in catalog.execute(f"SELECT * FROM {view} LIMIT 0").description
    ]
    ref_cols = [
        d[0]
        for d in catalog.execute(f"SELECT * FROM ({reference}) LIMIT 0").description
    ]
    assert view_cols == ref_cols
    for direction, left, right in (
        ("view - ibis", f"SELECT * FROM {view}", reference),
        ("ibis - view", reference, f"SELECT * FROM {view}"),
    ):
        (missing,) = catalog.execute(
            f"SELECT count(*) FROM (({left}) EXCEPT ALL ({right}))"
        ).fetchone() or (0,)
        assert missing == 0, f"{name}: {missing} rows in {direction}"
