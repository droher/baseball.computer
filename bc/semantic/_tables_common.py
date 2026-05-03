"""Helpers that resolve BSL backing tables against a live DuckDB connection.

Forked from ``bc/python_models/metrics/builders.py`` so the BSL package
stays independent of the SQLMesh serialization path. The shared piece
between the two is ``python_models.metrics._constants`` (pure data, no
sqlmesh imports).
"""

from __future__ import annotations

from typing import Any, Literal

import ibis

from python_models.metrics._constants import (
    EVENT_MODELS,
    GAME_COLS,
    SEASON_MODELS,
)

MetricKind = Literal["offense", "pitching", "fielding"]
Env = Literal["dev", "prod"]
TableExpr = Any


def schema_for(env: Env) -> str:
    return "main_models__dev" if env == "dev" else "main_models"


def _resolve(model: str, env: Env) -> tuple[str, str]:
    db, name = model.split(".", 1)
    if db == "main_models":
        db = schema_for(env)
    return db, name


def _seed_franchises(con: Any) -> TableExpr:
    return con.table("seed_franchises", database="main_seeds")


def _seed_game_types(con: Any) -> TableExpr:
    return con.table("seed_game_types", database="main_seeds")


def _team_game_start_info(con: Any, env: Env) -> TableExpr:
    db, _ = _resolve("main_models.team_game_start_info", env)
    return con.table("team_game_start_info", database=db)


def _season_table(con: Any, kind: MetricKind, env: Env) -> TableExpr:
    db, name = _resolve(SEASON_MODELS[kind], env)
    return con.table(name, database=db)


def _event_table(con: Any, kind: MetricKind, env: Env) -> TableExpr:
    db, name = _resolve(EVENT_MODELS[kind], env)
    return con.table(name, database=db)


def season_with_league(
    con: Any, kind: MetricKind, env: Env, regular_season_only: bool = True
) -> TableExpr:
    """player_*_season_*_stats LEFT JOIN seed_franchises (date range).

    Mirrors ``build_metric_sql``: by default also filters to regular
    season game types so BSL season-grain output matches the
    materialized ``metrics_*`` tables row-for-row.
    """
    s = _season_table(con, kind, env)
    f = _seed_franchises(con)
    joined = s.left_join(
        f,
        [
            s.team_id == f.team_id,
            s.season >= f.date_start.year(),
            s.season <= ibis.coalesce(f.date_end.year(), 9999),
        ],
    )
    out = joined.select(s, league=ibis.coalesce(f.league, "N/A"))
    if regular_season_only:
        gt = _seed_game_types(con)
        out = out.filter(out.game_type.isin(gt.filter(gt.is_regular_season).game_type))
    return out


def event_with_game_info(con: Any, kind: MetricKind, env: Env) -> TableExpr:
    """event_*_stats LEFT JOIN team_game_start_info on (team_id, game_id).

    Unlike ``build_metric_sql`` we do **not** filter to regular-season
    games. BSL queries get full-fidelity dimensions; consumers can
    filter by ``dimensions=['game_type', 'is_regular_season']``.
    """
    e = _event_table(con, kind, env)
    g = _team_game_start_info(con, env)
    joined = e.left_join(g, ["team_id", "game_id"])
    return joined.select(e, *(g[c] for c in GAME_COLS))
