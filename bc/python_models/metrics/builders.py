"""Build the SQL body for one of the 9 metrics_* tables.

Mirrors the CTE shape of bc/macros/_metric_table_body.py:

    season       — player_*_season_*_stats LEFT JOIN seed_franchises (date range)
    event        — event_*_stats LEFT JOIN team_game_start_info (team_id, game_id)
    basic_stats  — GROUP BY keys, SUM int_cols, basic-rate metrics
    event_agg    — GROUP BY keys, COUNT(DISTINCT game_id), event-based metrics
    final        — basic_stats LEFT JOIN event_agg, cast SUMs back to INT,
                   compute event_coverage_rate

Ibis emits a single optimized query rather than literal CTEs; the diff
harness compares row values, not SQL text.
"""

from __future__ import annotations

from typing import Any, Literal

import ibis

from python_models.metrics._constants import (
    EVENT_INT_COLS,
    EVENT_MODELS,
    GAME_COLS,
    INT_COLS,
    SEASON_MODELS,
)

MetricKind = Literal["offense", "pitching", "fielding"]

TableExpr = Any
IbisExpr = Any


def _season_table(kind: MetricKind, int_cols: list[str]) -> TableExpr:
    """Declare the player_*_season_*_stats table with a permissive schema.

    DuckDB resolves columns at query time, so the schema only has to
    cover what's actually referenced in metric formulas + grouping keys
    + filter predicates.
    """
    schema: dict[str, str] = {
        "player_id": "string",
        "team_id": "string",
        "season": "int16",
        "game_type": "string",
        "games": "int64",
    }
    for c in int_cols:
        schema.setdefault(c, "int64")
    db, name = SEASON_MODELS[kind].split(".", 1)
    return ibis.table(schema, name=name, database=db)


def _event_table(kind: MetricKind, int_cols: list[str]) -> TableExpr:
    schema: dict[str, str] = {
        "game_id": "string",
        "team_id": "string",
        "player_id": "string",
    }
    for c in int_cols:
        schema.setdefault(c, "int64")
    db, name = EVENT_MODELS[kind].split(".", 1)
    return ibis.table(schema, name=name, database=db)


def _seed_franchises() -> TableExpr:
    return ibis.table(
        {
            "team_id": "string",
            "league": "string",
            "date_start": "date",
            "date_end": "date",
        },
        name="seed_franchises",
        database="main_seeds",
    )


def _seed_game_types() -> TableExpr:
    return ibis.table(
        {"game_type": "string", "is_regular_season": "bool"},
        name="seed_game_types",
        database="main_seeds",
    )


def _team_game_start_info() -> TableExpr:
    schema: dict[str, str] = {
        "team_id": "string",
        "game_id": "string",
        "season": "int16",
    }
    for c in GAME_COLS:
        schema.setdefault(c, "string")
    return ibis.table(schema, name="team_game_start_info", database="main_models")


def _game_start_info() -> TableExpr:
    return ibis.table(
        {"game_id": "string", "is_regular_season": "bool"},
        name="game_start_info",
        database="main_models",
    )


def _season_with_league(kind: MetricKind, int_cols: list[str]) -> TableExpr:
    s = _season_table(kind, int_cols)
    f = _seed_franchises()
    joined = s.left_join(
        f,
        [
            s.team_id == f.team_id,
            s.season >= f.date_start.year(),
            s.season <= ibis.coalesce(f.date_end.year(), 9999),
        ],
    )
    return joined.select(s, league=ibis.coalesce(f.league, "N/A"))


def _event_with_game_info(kind: MetricKind, int_cols: list[str]) -> TableExpr:
    e = _event_table(kind, int_cols)
    g = _team_game_start_info()
    joined = e.left_join(g, ["team_id", "game_id"])
    return joined.select(e, *(g[c] for c in GAME_COLS))


def build_metric_sql(kind: MetricKind, grouping_keys: list[str]) -> str:
    """Return the SELECT body for a metrics_* model.

    Algebraically equivalent to ``@metric_table_body(kind, *grouping_keys)``.
    """
    if kind not in INT_COLS:
        raise ValueError(
            f"Invalid kind {kind!r} — must be one of offense/pitching/fielding"
        )
    if not grouping_keys:
        raise ValueError("build_metric_sql requires at least one grouping key")

    int_cols = INT_COLS[kind]
    event_cols = EVENT_INT_COLS[kind]
    season = _season_with_league(kind, int_cols)
    event = _event_with_game_info(kind, event_cols)

    # basic_stats: filter to regular season game types, group by keys,
    # SUM each int counter, evaluate basic-rate metrics.
    seed_gt = _seed_game_types()
    season_filtered = season.filter(
        season.game_type.isin(seed_gt.filter(seed_gt.is_regular_season).game_type)
    )

    basic_aggs: dict[str, IbisExpr] = {"games": season_filtered.games.sum()}
    for c in int_cols:
        basic_aggs[c] = season_filtered[c].sum()
    # Lazy import: keep the METRICS dict (containing lambdas) out of
    # module-level globals so SQLMesh's python-env serializer doesn't
    # try to repr+eval it.
    from python_models.metrics import _metric_registrations  # noqa: F401
    from python_models.metrics.registry import evaluate_all, metrics_for

    season_metrics = metrics_for(kind, "season")
    basic_aggs.update(evaluate_all(season_filtered, season_metrics))

    basic_stats = season_filtered.group_by(grouping_keys).aggregate(**basic_aggs)

    # event_agg: filter to regular-season game_ids, group by keys,
    # COUNT(DISTINCT game_id), evaluate event-based metrics.
    gsi = _game_start_info()
    event_filtered = event.filter(
        event.game_id.isin(gsi.filter(gsi.is_regular_season).game_id)
    )

    event_aggs: dict[str, IbisExpr] = {"games": event_filtered.game_id.nunique()}
    event_metrics = metrics_for(kind, "event")
    event_aggs.update(evaluate_all(event_filtered, event_metrics))

    event_agg = event_filtered.group_by(grouping_keys).aggregate(**event_aggs)

    # final: cast int counters back to INT, copy basic-rate metrics,
    # compute event_coverage_rate, copy event-based metrics.
    join_predicates: list[Any] = [basic_stats[k] == event_agg[k] for k in grouping_keys]
    joined = basic_stats.left_join(event_agg, join_predicates)

    select_args: list[Any] = [basic_stats[k] for k in grouping_keys]
    for c in int_cols:
        select_args.append(basic_stats[c].cast("int32").name(c))
    for m in season_metrics:
        select_args.append(basic_stats[m.name])

    select_args.append(
        ibis.coalesce(event_agg.games / basic_stats.games, 0).name(
            "event_coverage_rate"
        )
    )
    for m in event_metrics:
        select_args.append(event_agg[m.name])

    final = joined.select(*select_args)
    return ibis.to_sql(final, dialect="duckdb")
