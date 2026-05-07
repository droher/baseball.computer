"""Actual vs realized synthetic lineup targets by player, stint, and position."""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models._enum_types import PLAYER_ID, TEAM_ID
from python_models.synthetic_box_scores import (
    build_synthetic_lineup_report_from_assignments,
)

_UPSTREAM_ASSIGNMENTS = "synthetic_box_score.lineup_assignments"
_UPSTREAM_APPEARANCES = "main_models.stg_databank_appearances"
_UPSTREAM_BATTING = "main_models.stg_databank_batting"
_UPSTREAM_PEOPLE = "main_models.stg_people"


_ASSIGNMENTS_SQL = """
SELECT
    game_id::VARCHAR AS game_id,
    season,
    team_id::VARCHAR AS team_id,
    player_id::VARCHAR AS player_id,
    stint,
    side,
    lineup_position,
    fielding_position
FROM {assignments}
"""

_CANDIDATES_SQL = """
WITH valid_team_seasons AS (
    SELECT DISTINCT season, team_id::VARCHAR AS team_id
    FROM {assignments}
),

appearances AS (
    SELECT
        a.season,
        a.team_id::VARCHAR AS team_id,
        a.player_id::VARCHAR AS player_id,
        a.stint,
        a.fielding_position,
        SUM(a.games_at_position)::INTEGER AS games_at_position,
        MAX(a.games_total)::INTEGER AS games_total,
        MAX(a.outs_played)::INTEGER AS outs_played
    FROM {appearances} AS a
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE a.fielding_position BETWEEN 1 AND 9
      AND a.games_at_position > 0
    GROUP BY 1, 2, 3, 4, 5
),

batting AS (
    SELECT
        b.season,
        b.team_id::VARCHAR AS team_id,
        people.retrosheet_player_id::VARCHAR AS player_id,
        b.stint,
        SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
        SUM(COALESCE(b.games, 0))::INTEGER AS games_played
    FROM {batting} AS b
    INNER JOIN {people} AS people USING (databank_player_id)
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE people.retrosheet_player_id IS NOT NULL
    GROUP BY 1, 2, 3, 4
)

SELECT
    a.season,
    a.team_id,
    a.player_id,
    a.stint,
    a.fielding_position,
    a.games_at_position,
    a.games_total,
    a.outs_played,
    COALESCE(b.plate_appearances, 0)::INTEGER AS plate_appearances,
    COALESCE(b.games_played, 0)::INTEGER AS games_played
FROM appearances AS a
LEFT JOIN batting AS b USING (season, team_id, player_id, stint)
"""


_GRAIN = exp.Tuple(
    expressions=[
        exp.column("season"),
        exp.column("team_id"),
        exp.column("player_id"),
        exp.column("stint"),
        exp.column("metric_type"),
        exp.column("fielding_position"),
    ]
)
_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("season"),
                    exp.column("team_id"),
                    exp.column("player_id"),
                    exp.column("stint"),
                    exp.column("metric_type"),
                    exp.column("fielding_position"),
                    exp.column("actual_games"),
                    exp.column("realized_games"),
                    exp.column("signed_error"),
                    exp.column("abs_error"),
                    exp.column("pct_error"),
                    exp.column("signed_pct_error"),
                    exp.column("error_rank"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": _GRAIN}),
    ("valid_baseball_season", {"column": exp.column("season")}),
]


@model(
    "synthetic_box_score.lineup_optimization_report",
    kind="FULL",
    description=(
        "Actual vs realized appearance targets for the synthetic lineup "
        "optimizer. Total rows compare each player-stint's sum of scaled "
        "non-pitcher fielding games (Lahman fielding.g, after _scale_position_targets); "
        "position rows compare fielding-position targets. fielding_position "
        "is 0 on total rows."
    ),
    columns={
        "season": "SMALLINT",
        "team_id": TEAM_ID,
        "player_id": PLAYER_ID,
        "stint": "SMALLINT",
        "metric_type": "VARCHAR",
        "fielding_position": "UTINYINT",
        "actual_games": "DOUBLE",
        "realized_games": "INTEGER",
        "signed_error": "DOUBLE",
        "abs_error": "DOUBLE",
        "pct_error": "DOUBLE",
        "signed_pct_error": "DOUBLE",
        "error_rank": "UINTEGER",
    },
    column_descriptions={
        "season": doc("season"),
        "team_id": doc("team_id"),
        "player_id": doc("player_id"),
        "stint": doc("stint"),
        "fielding_position": doc("fielding_position"),
    },
    grain=[
        "season",
        "team_id",
        "player_id",
        "stint",
        "metric_type",
        "fielding_position",
    ],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": (
            "https://data.baseball.computer/dbt/"
            "synthetic_box_score_lineup_optimization_report.parquet"
        ),
    },
    depends_on={
        _UPSTREAM_ASSIGNMENTS,
        _UPSTREAM_APPEARANCES,
        _UPSTREAM_BATTING,
        _UPSTREAM_PEOPLE,
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pd.DataFrame]:
    del kwargs
    assignments_table = context.resolve_table(_UPSTREAM_ASSIGNMENTS)
    assignments_sql = _ASSIGNMENTS_SQL.format(assignments=assignments_table)
    candidates_sql = _CANDIDATES_SQL.format(
        assignments=assignments_table,
        appearances=context.resolve_table(_UPSTREAM_APPEARANCES),
        batting=context.resolve_table(_UPSTREAM_BATTING),
        people=context.resolve_table(_UPSTREAM_PEOPLE),
    )
    assignments: pl.DataFrame = context.engine_adapter.cursor.sql(assignments_sql).pl()
    candidates: pl.DataFrame = context.engine_adapter.cursor.sql(candidates_sql).pl()

    report = build_synthetic_lineup_report_from_assignments(assignments, candidates)
    if report.is_empty():
        return
    yield report.to_pandas()
