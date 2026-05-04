"""Programmatic @model registration for the metrics_* tables.

One call per (kind, scope) collapses what used to be 9 hand-written
.py models with ~220-line columns/column_descriptions dicts.
"""

from __future__ import annotations

from typing import Any

from sqlglot import exp

from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models.metrics.builders import build_metric_sql
from python_models.metrics.columns import metric_columns, metric_column_descriptions

_GRAINS: dict[str, list[str]] = {
    "player_career": ["player_id"],
    "player_season_league": ["player_id", "season", "league"],
    "team_season": ["team_id", "season"],
}

_DESCRIPTION_NOUN: dict[str, str] = {
    "offense": "offensive statistics and averages",
    "pitching": "pitching statistics and averages",
    "fielding": "fielding statistics and averages",
}

_DESCRIPTION_SCOPE: dict[str, str] = {
    "player_career": "over player careers",
    "player_season_league": (
        "for each player-season, split if the player played in multiple leagues that year"
    ),
    "team_season": "for each team-season",
}


def _description(kind: str, scope: str) -> str:
    return f"Aggregate {_DESCRIPTION_NOUN[kind]} {_DESCRIPTION_SCOPE[scope]}. Regular season only."


def register_metric_model(kind: str, scope: str) -> None:
    """Register one metrics_* model with @model. Closure captures kind, grain.

    SQLMesh's metaprogramming serializes closure freevars into python_env
    (utils/metaprogramming.py:func_globals), so the entrypoint resolves
    `_kind` and `_grain` at SQL-generation time.
    """
    grain = _GRAINS[scope]
    name = f"main_models.metrics_{scope}_{kind}"
    parquet_url = (
        f"https://data.baseball.computer/dbt/main_models_metrics_{scope}_{kind}.parquet"
    )
    _kind = kind
    _grain = grain

    # not_null on the grain only. The volume column (outs_played /
    # plate_appearances / batters_faced) can legitimately be null on
    # the fielding side — Negro Leagues (NN1, NN2) and pre-1900s rows
    # don't carry an outs_played figure, and the downstream rate
    # metrics tolerate that by producing null.
    not_null_cols = exp.Tuple(expressions=[exp.column(c) for c in grain])
    audits: list[tuple[str, dict[str, Any]]] = [
        ("not_null", {"columns": not_null_cols}),
    ]
    if "season" in grain:
        audits.append(("valid_baseball_season", {"column": exp.column("season")}))

    @model(
        name,
        is_sql=True,
        kind="FULL",
        description=_description(kind, scope),
        grain=grain,
        columns=metric_columns(kind, grain),
        column_descriptions=metric_column_descriptions(kind, grain),
        physical_properties={"download_parquet": parquet_url},
        audits=audits,
    )
    def entrypoint(evaluator: MacroEvaluator) -> str:
        del evaluator
        return build_metric_sql(_kind, _grain)
