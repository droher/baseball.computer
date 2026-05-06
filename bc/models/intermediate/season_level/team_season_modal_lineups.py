"""Modal seasonal lineup per (season, team_id).

Drives the synthetic_box_score.* models for gamelog-only games. The
modal lineup starts with the team's nine most-used players, assigns their
most common positions, fills uncovered positions from the remaining
candidate pool, and bats the final nine by plate appearances per game.
See ``python_models/synthetic_box_scores/modal_lineups.py`` for the
ranking rules and tiebreakers.

Filtered to (season, team_id) pairs that actually played a game per
``team_game_start_info`` so casts to TEAM_ID succeed on every row.
Team-seasons that don't fill all nine fielding positions are dropped
silently — see ``notes/followups.md`` for the Negro / minor coverage
gap follow-up.
"""

from __future__ import annotations

import typing as t

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models._enum_types import PLAYER_ID, TEAM_ID
from python_models.synthetic_box_scores import compute_modal_lineups

_UPSTREAM_APPEARANCES = "main_models.stg_databank_appearances"
_UPSTREAM_BATTING = "main_models.stg_databank_batting"
_UPSTREAM_PEOPLE = "main_models.stg_people"
_UPSTREAM_TEAM_GAMES = "main_models.team_game_start_info"


_APPEARANCES_SQL = """
SELECT
    a.season,
    a.team_id,
    a.player_id,
    a.fielding_position,
    SUM(a.games_at_position)::INTEGER AS games_at_position
FROM {appearances} AS a
INNER JOIN (
    SELECT DISTINCT season, team_id::VARCHAR AS team_id
    FROM {team_games}
) AS valid USING (season, team_id)
GROUP BY 1, 2, 3, 4
"""

_BATTING_SQL = """
SELECT
    b.season,
    b.team_id,
    people.retrosheet_player_id AS player_id,
    SUM(COALESCE(b.plate_appearances, 0))::INTEGER AS plate_appearances,
    SUM(COALESCE(b.games, 0))::INTEGER AS games_played
FROM {batting} AS b
INNER JOIN {people} AS people USING (databank_player_id)
INNER JOIN (
    SELECT DISTINCT season, team_id::VARCHAR AS team_id
    FROM {team_games}
) AS valid USING (season, team_id)
WHERE people.retrosheet_player_id IS NOT NULL
GROUP BY 1, 2, 3
"""


_GRAIN = exp.Tuple(
    expressions=[
        exp.column("season"),
        exp.column("team_id"),
        exp.column("lineup_position"),
    ]
)
_FIELDING_GRAIN = exp.Tuple(
    expressions=[
        exp.column("season"),
        exp.column("team_id"),
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
                    exp.column("lineup_position"),
                    exp.column("fielding_position"),
                    exp.column("player_id"),
                ]
            ),
        },
    ),
    ("unique_grain", {"columns": _GRAIN}),
    ("unique_grain", {"columns": _FIELDING_GRAIN}),
    ("valid_baseball_season", {"column": exp.column("season")}),
    (
        "relationships",
        {
            "column": exp.column("team_id"),
            "to_model": exp.to_table("main_seeds.seed_franchises"),
            "to_column": exp.column("team_id"),
        },
    ),
]


@model(
    "main_models.team_season_modal_lineups",
    kind="FULL",
    description=(
        "Modal seasonal lineup for every (season, team_id) where databank "
        "appearances cover all nine fielding positions. Synthesizes the "
        "lineup-shaped tables under the synthetic_box_score schema for the "
        "~25K games that exist only in misc.gamelog. Stat columns are NULL "
        "downstream."
    ),
    columns={
        "season": "SMALLINT",
        "team_id": TEAM_ID,
        "lineup_position": "UTINYINT",
        "fielding_position": "UTINYINT",
        "player_id": PLAYER_ID,
    },
    column_descriptions={
        "season": doc("season"),
        "team_id": doc("team_id"),
        "fielding_position": doc("fielding_position"),
        "player_id": doc("player_id"),
    },
    grain=["season", "team_id", "lineup_position"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": (
            "https://data.baseball.computer/dbt/"
            "main_models_team_season_modal_lineups.parquet"
        ),
    },
    depends_on={
        _UPSTREAM_APPEARANCES,
        _UPSTREAM_BATTING,
        _UPSTREAM_PEOPLE,
        _UPSTREAM_TEAM_GAMES,
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
    del kwargs
    apps_sql = _APPEARANCES_SQL.format(
        appearances=context.resolve_table(_UPSTREAM_APPEARANCES),
        team_games=context.resolve_table(_UPSTREAM_TEAM_GAMES),
    )
    batting_sql = _BATTING_SQL.format(
        batting=context.resolve_table(_UPSTREAM_BATTING),
        people=context.resolve_table(_UPSTREAM_PEOPLE),
        team_games=context.resolve_table(_UPSTREAM_TEAM_GAMES),
    )
    appearances: pl.DataFrame = context.engine_adapter.cursor.sql(apps_sql).pl()
    batting: pl.DataFrame = context.engine_adapter.cursor.sql(batting_sql).pl()

    appearances = appearances.with_columns(
        pl.col("season").cast(pl.Int16),
        pl.col("games_at_position").cast(pl.UInt32),
        pl.col("fielding_position").cast(pl.UInt8),
    )
    batting = batting.with_columns(
        pl.col("season").cast(pl.Int16),
        pl.col("plate_appearances").cast(pl.UInt32),
        pl.col("games_played").cast(pl.UInt32),
    )

    return compute_modal_lineups(appearances, batting).to_pandas()
