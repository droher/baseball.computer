"""Synthetic fielding lines for gamelog-only games.

Cross-joins gamelog-only games with the modal seasonal fielding map of
each side's team, then unconditionally replaces fielding_position=1's
fielder with the listed starting pitcher from the gamelog. Stat columns
stay NULL — lineups only, no outcomes.
"""

from __future__ import annotations

import typing as t

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models._enum_types import GAME_ID, PLAYER_ID, udt
from python_models.synthetic_box_scores import build_synthetic_fielding_core

_UPSTREAM_GAMES = "synthetic_box_score.box_score_games"
_UPSTREAM_LINEUPS = "main_models.team_season_modal_lineups"
_UPSTREAM_GAMELOG = "main_models.stg_gamelog"
_UPSTREAM_APPEARANCES = "main_models.stg_databank_appearances"
_UPSTREAM_BATTING = "main_models.stg_databank_batting"
_UPSTREAM_PEOPLE = "main_models.stg_people"


_GAMES_SQL = """
SELECT
    g.game_id::VARCHAR AS game_id,
    g.season,
    g.home_team_id::VARCHAR AS home_team_id,
    g.away_team_id::VARCHAR AS away_team_id,
    gl.home_starting_pitcher_id::VARCHAR AS home_starting_pitcher_id,
    gl.away_starting_pitcher_id::VARCHAR AS away_starting_pitcher_id
FROM {games} AS g
INNER JOIN {gamelog} AS gl USING (game_id)
"""

_LINEUPS_SQL = """
SELECT
    season,
    team_id::VARCHAR AS team_id,
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
        a.fielding_position,
        SUM(a.games_at_position)::INTEGER AS games_at_position
    FROM {appearances} AS a
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE a.fielding_position BETWEEN 1 AND 9
      AND a.games_at_position > 0
    GROUP BY 1, 2, 3, 4
),

batting AS (
    SELECT
        b.season,
        b.team_id::VARCHAR AS team_id,
        people.retrosheet_player_id::VARCHAR AS player_id,
        SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
        SUM(COALESCE(b.games, 0))::INTEGER AS games_played
    FROM {batting} AS b
    INNER JOIN {people} AS people USING (databank_player_id)
    INNER JOIN valid_team_seasons AS valid USING (season, team_id)
    WHERE people.retrosheet_player_id IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    a.season,
    a.team_id,
    a.player_id,
    a.fielding_position,
    a.games_at_position,
    COALESCE(b.plate_appearances, 0)::INTEGER AS plate_appearances,
    COALESCE(b.games_played, 0)::INTEGER AS games_played
FROM appearances AS a
LEFT JOIN batting AS b USING (season, team_id, player_id)
"""


_NULL_FIELDING_STATS: tuple[str, ...] = (
    "outs_played",
    "putouts",
    "assists",
    "errors",
    "double_plays",
    "triple_plays",
    "passed_balls",
)


_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("fielder_id"),
    ]
)
_POSITION_GRAIN = exp.Tuple(
    expressions=[
        exp.column("game_id"),
        exp.column("side"),
        exp.column("fielding_position"),
    ]
)
_AUDITS = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    exp.column("game_id"),
                    exp.column("side"),
                    exp.column("fielder_id"),
                    exp.column("fielding_position"),
                    exp.column("nth_position_played_by_player"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": _GRAIN}),
    ("unique_grain", {"columns": _POSITION_GRAIN}),
    (
        "relationships",
        {
            "column": exp.column("game_id"),
            "to_model": exp.to_table("main_models.game_results"),
            "to_column": exp.column("game_id"),
        },
    ),
]


_COLUMNS: dict[str, t.Any] = {
    "game_id": GAME_ID,
    "fielder_id": PLAYER_ID,
    "side": udt("SIDE"),
    "fielding_position": "UTINYINT",
    "nth_position_played_by_player": "UTINYINT",
}
_COLUMNS.update({col: "UTINYINT" for col in _NULL_FIELDING_STATS})


@model(
    "synthetic_box_score.box_score_fielding_lines",
    kind="FULL",
    description=(
        "One row per (game_id, modal fielder, fielding position) for "
        "every gamelog-only game. Mirrors box_score.box_score_fielding_lines "
        "but with stat columns NULL — lineups only, no outcomes. "
        "Fielding position 1 uses the listed starting pitcher without "
        "duplicating a player."
    ),
    columns=_COLUMNS,
    column_descriptions={
        "game_id": doc("game_id"),
        "fielding_position": doc("fielding_position"),
        "side": doc("side"),
    },
    grain=["game_id", "side", "fielder_id"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": (
            "https://data.baseball.computer/dbt/"
            "synthetic_box_score_box_score_fielding_lines.parquet"
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
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
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

    if games.is_empty():
        return _empty_output().to_pandas()

    core = build_synthetic_fielding_core(games, lineups, candidates)
    if core.is_empty():
        return _empty_output().to_pandas()

    out = core.select(
        pl.col("game_id"),
        pl.col("fielder_id"),
        pl.col("side"),
        pl.col("fielding_position"),
        pl.lit(1, dtype=pl.UInt8).alias("nth_position_played_by_player"),
        *(pl.lit(None, dtype=pl.UInt8).alias(col) for col in _NULL_FIELDING_STATS),
    )

    return out.to_pandas()


def _empty_output() -> pl.DataFrame:
    schema: dict[str, pl.DataType | type[pl.DataType]] = {
        "game_id": pl.String,
        "fielder_id": pl.String,
        "side": pl.String,
        "fielding_position": pl.UInt8,
        "nth_position_played_by_player": pl.UInt8,
    }
    for col in _NULL_FIELDING_STATS:
        schema[col] = pl.UInt8
    return pl.DataFrame(schema=schema)
