"""Phase 5 second wave — Polars rewrite of team_game_start_info.

Replaces the SQL `base` → `add_series_start_flag` → `assign_series_id`
→ `final` chain. The `series_id` `COALESCE(LAG IGNORE NULLS, current)`
becomes `forward_fill().over(...)` (inclusive form). Per-team season /
series counters and rest-day deltas are simple Polars cum_count / shift
expressions.
"""

from __future__ import annotations

import typing as t

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models.game_level import compute_team_game_start_info


def _udt(name: str) -> exp.DataType:
    return exp.DataType.build(name, udt=True, dialect="duckdb")


_UPSTREAM = "main_models.game_start_info"

_BASE_SQL = """
SELECT * EXCLUDE (
    away_team_id,
    home_team_id,
    away_league,
    home_league,
    away_division,
    home_division,
    away_team_name,
    home_team_name,
    away_starting_pitcher_id,
    home_starting_pitcher_id
)
FROM (
    SELECT
        home_team_id AS team_id,
        away_team_id AS opponent_id,
        home_league AS league,
        away_league AS opponent_league,
        home_division AS division,
        away_division AS opponent_division,
        home_team_name AS team_name,
        away_team_name AS opponent_name,
        home_starting_pitcher_id AS starting_pitcher_id,
        away_starting_pitcher_id AS opponent_starting_pitcher_id,
        'Home'::SIDE AS team_side,
        *
    FROM {start_info}
    UNION ALL BY NAME
    SELECT
        away_team_id AS team_id,
        home_team_id AS opponent_id,
        away_league AS league,
        home_league AS opponent_league,
        away_division AS division,
        home_division AS opponent_division,
        away_team_name AS team_name,
        home_team_name AS opponent_name,
        away_starting_pitcher_id AS starting_pitcher_id,
        home_starting_pitcher_id AS opponent_starting_pitcher_id,
        'Away'::SIDE AS team_side,
        *
    FROM {start_info}
)
"""


_GRAIN = exp.Tuple(expressions=[exp.column("game_id"), exp.column("team_id")])
_AUDITS = [
    ("not_null", {"columns": _GRAIN}),
    ("unique_grain", {"columns": _GRAIN}),
    ("valid_baseball_season", {"column": exp.column("season")}),
    (
        "relationships",
        {
            "column": exp.column("game_id"),
            "to_model": exp.to_table("main_models.game_results"),
            "to_column": exp.column("game_id"),
        },
    ),
    (
        "relationships",
        {
            "column": exp.column("park_id"),
            "to_model": exp.to_table("main_models.stg_parks"),
            "to_column": exp.column("park_id"),
        },
    ),
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
    "main_models.team_game_start_info",
    kind="FULL",
    description=(
        "A version of `game_start_info` that includes one row for each team in each game."
    ),
    columns={
        "team_id": _udt("TEAM_ID"),
        "opponent_id": _udt("TEAM_ID"),
        "league": "VARCHAR",
        "opponent_league": "VARCHAR",
        "division": "VARCHAR",
        "opponent_division": "VARCHAR",
        "team_name": "VARCHAR",
        "opponent_name": "VARCHAR",
        "starting_pitcher_id": "VARCHAR",
        "opponent_starting_pitcher_id": "VARCHAR",
        "team_side": _udt("SIDE"),
        "game_id": "VARCHAR",
        "date": "DATE",
        "start_time": "TIMESTAMP",
        "season": "SMALLINT",
        "doubleheader_status": _udt("DOUBLEHEADER_STATUS"),
        "time_of_day": _udt("TIME_OF_DAY"),
        "game_type": _udt("GAME_TYPE"),
        "bat_first_side": _udt("SIDE"),
        "sky": _udt("SKY"),
        "field_condition": _udt("FIELD_CONDITION"),
        "precipitation": _udt("PRECIPITATION"),
        "wind_direction": _udt("WIND_DIRECTION"),
        "park_id": _udt("PARK_ID"),
        "temperature_fahrenheit": "TINYINT",
        "attendance": "UINTEGER",
        "wind_speed_mph": "UTINYINT",
        "use_dh": "BOOLEAN",
        "scorer": "VARCHAR",
        "scoring_method": "VARCHAR",
        "source_type": "VARCHAR",
        "umpire_home_id": "VARCHAR",
        "umpire_first_id": "VARCHAR",
        "umpire_second_id": "VARCHAR",
        "umpire_third_id": "VARCHAR",
        "umpire_left_id": "VARCHAR",
        "umpire_right_id": "VARCHAR",
        "filename": "VARCHAR",
        "is_regular_season": "BOOLEAN",
        "is_postseason": "BOOLEAN",
        "is_integrated": "BOOLEAN",
        "is_negro_leagues": "BOOLEAN",
        "is_segregated_white": "BOOLEAN",
        "away_franchise_id": _udt("TEAM_ID"),
        "home_franchise_id": _udt("TEAM_ID"),
        "is_interleague": "BOOLEAN",
        "lineup_map_away": "MAP(UTINYINT, VARCHAR)",
        "lineup_map_home": "MAP(UTINYINT, VARCHAR)",
        "fielding_map_away": "MAP(UTINYINT, VARCHAR)",
        "fielding_map_home": "MAP(UTINYINT, VARCHAR)",
        "series_id": "VARCHAR",
        "season_game_number": "BIGINT",
        "series_game_number": "BIGINT",
        "days_since_last_game": "BIGINT",
    },
    column_descriptions={
        "team_id": doc("team_id"),
        "league": doc("league"),
        "division": doc("division"),
        "team_name": doc("team_name"),
        "game_id": doc("game_id"),
        "date": doc("date"),
        "start_time": doc("start_time"),
        "season": doc("season"),
        "time_of_day": doc("time_of_day"),
        "game_type": doc("game_type"),
        "bat_first_side": doc("bat_first_side"),
        "sky": doc("sky"),
        "field_condition": doc("field_condition"),
        "precipitation": doc("precipitation"),
        "wind_direction": doc("wind_direction"),
        "park_id": doc("park_id"),
        "temperature_fahrenheit": doc("temperature_fahrenheit"),
        "attendance": doc("attendance"),
        "wind_speed_mph": doc("wind_speed_mph"),
        "source_type": doc("source_type"),
        "umpire_home_id": doc("umpire_home_id"),
        "umpire_first_id": doc("umpire_first_id"),
        "umpire_second_id": doc("umpire_second_id"),
        "umpire_third_id": doc("umpire_third_id"),
        "umpire_left_id": doc("umpire_left_id"),
        "umpire_right_id": doc("umpire_right_id"),
        "filename": doc("filename"),
        "is_regular_season": doc("is_regular_season"),
        "is_postseason": doc("is_postseason"),
        "away_franchise_id": doc("away_franchise_id"),
        "home_franchise_id": doc("home_franchise_id"),
        "is_interleague": doc("is_interleague"),
        "lineup_map_away": doc("lineup_map_away"),
        "lineup_map_home": doc("lineup_map_home"),
        "fielding_map_away": doc("fielding_map_away"),
        "fielding_map_home": doc("fielding_map_home"),
    },
    grain=["game_id", "team_id"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": "https://data.baseball.computer/dbt/main_models_team_game_start_info.parquet",
    },
    depends_on={_UPSTREAM},
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
    del kwargs
    sql = _BASE_SQL.format(start_info=context.resolve_table(_UPSTREAM))
    rows: pl.DataFrame = context.engine_adapter.cursor.sql(sql).pl()
    return compute_team_game_start_info(rows).to_pandas()
