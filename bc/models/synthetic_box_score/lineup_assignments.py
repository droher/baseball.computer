"""Optimized synthetic starter assignments for gamelog-only games."""

from __future__ import annotations

import typing as t
from collections.abc import Iterator

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models._enum_types import GAME_ID, PLAYER_ID, TEAM_ID, udt
from python_models.synthetic_box_scores import build_synthetic_lineup_assignments

_UPSTREAM_GAMES = "synthetic_box_score.box_score_games"
_UPSTREAM_LINEUPS = "synthetic_box_score.team_season_modal_lineups"
_UPSTREAM_GAMELOG = "main_models.stg_gamelog"
_UPSTREAM_APPEARANCES = "main_models.stg_databank_appearances"
_UPSTREAM_BATTING = "main_models.stg_databank_batting"
_UPSTREAM_PEOPLE = "main_models.stg_people"


_GAMES_SQL = """
SELECT
    g.game_id::VARCHAR AS game_id,
    g.date,
    g.season,
    g.use_dh,
    g.home_team_id::VARCHAR AS home_team_id,
    g.away_team_id::VARCHAR AS away_team_id,
    gl.home_starting_pitcher_id::VARCHAR AS home_starting_pitcher_id,
    gl.away_starting_pitcher_id::VARCHAR AS away_starting_pitcher_id
FROM {games} AS g
INNER JOIN {gamelog} AS gl USING (game_id)
WHERE g.use_dh = FALSE
"""

_LINEUPS_SQL = """
SELECT
    season,
    team_id::VARCHAR AS team_id,
    lineup_position,
    fielding_position,
    player_id::VARCHAR AS player_id
FROM {lineups}
"""

_CANDIDATES_SQL = """
WITH valid_team_seasons AS (
    SELECT DISTINCT season, away_team_id::VARCHAR AS team_id
    FROM {games}
    UNION
    SELECT DISTINCT season, home_team_id::VARCHAR AS team_id
    FROM {games}
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


_LINEUP_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("lineup_position"),
    ]
)
_POSITION_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("fielding_position"),
    ]
)
_PLAYER_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("player_id"),
    ]
)
_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("game_id"),
                    exp.column("season"),
                    exp.column("team_id"),
                    exp.column("player_id"),
                    exp.column("stint"),
                    exp.column("side"),
                    exp.column("lineup_position"),
                    exp.column("fielding_position"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": _LINEUP_GRAIN}),
    ("unique_grain", {"columns": _POSITION_GRAIN}),
    ("unique_grain", {"columns": _PLAYER_GRAIN}),
    ("valid_baseball_season", {"column": exp.column("season")}),
]


@model(
    "synthetic_box_score.lineup_assignments",
    kind="FULL",
    description=(
        "One row per synthetic game side starter assignment for non-DH "
        "gamelog-only games. Runs the seeded non-pitcher lineup optimizer "
        "once; downstream batting, fielding, and report models read this "
        "table. DH games are excluded because the pitcher row inserted here "
        "is wrong for DH games and gamelog-only games are all pre-1973."
    ),
    columns={
        "game_id": GAME_ID,
        "season": "SMALLINT",
        "team_id": TEAM_ID,
        "player_id": PLAYER_ID,
        "stint": "SMALLINT",
        "side": udt("SIDE"),
        "lineup_position": "UTINYINT",
        "fielding_position": "UTINYINT",
    },
    column_descriptions={
        "game_id": doc("game_id"),
        "season": doc("season"),
        "team_id": doc("team_id"),
        "player_id": doc("player_id"),
        "stint": doc("stint"),
        "side": doc("side"),
        "lineup_position": doc("lineup_position"),
        "fielding_position": doc("fielding_position"),
    },
    grain=["game_id", "side", "lineup_position"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": (
            "https://data.baseball.computer/dbt/"
            "synthetic_box_score_lineup_assignments.parquet"
        ),
    },
    depends_on={
        _UPSTREAM_GAMES,
        _UPSTREAM_LINEUPS,
        _UPSTREAM_GAMELOG,
        _UPSTREAM_APPEARANCES,
        _UPSTREAM_BATTING,
        _UPSTREAM_PEOPLE,
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> Iterator[pd.DataFrame]:
    del kwargs
    games_table = context.resolve_table(_UPSTREAM_GAMES)
    games_sql = _GAMES_SQL.format(
        games=games_table,
        gamelog=context.resolve_table(_UPSTREAM_GAMELOG),
    )
    lineups_sql = _LINEUPS_SQL.format(
        lineups=context.resolve_table(_UPSTREAM_LINEUPS),
    )
    candidates_sql = _CANDIDATES_SQL.format(
        games=games_table,
        appearances=context.resolve_table(_UPSTREAM_APPEARANCES),
        batting=context.resolve_table(_UPSTREAM_BATTING),
        people=context.resolve_table(_UPSTREAM_PEOPLE),
    )
    games: pl.DataFrame = context.engine_adapter.cursor.sql(games_sql).pl()
    lineups: pl.DataFrame = context.engine_adapter.cursor.sql(lineups_sql).pl()
    candidates: pl.DataFrame = context.engine_adapter.cursor.sql(candidates_sql).pl()

    assignments = build_synthetic_lineup_assignments(games, lineups, candidates)
    if assignments.is_empty():
        return
    yield assignments.to_pandas()
