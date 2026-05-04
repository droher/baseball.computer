"""SQLMesh model that joins team-game inputs and computes streak + split counts via Polars."""

from __future__ import annotations

import typing as t

import pandas as pd
import polars as pl
from sqlglot import exp
from sqlmesh import ExecutionContext, model

from python_models._doc_lookup import doc
from python_models.game_level import compute_team_game_results


def _udt(name: str) -> exp.DataType:
    return exp.DataType.build(name, udt=True, dialect="duckdb")


_UPSTREAM_START_INFO = "main_models.team_game_start_info"
_UPSTREAM_GAME_RESULTS = "main_models.game_results"
_UPSTREAM_OFFENSE = "main_models.team_game_offense_stats"
_UPSTREAM_FIELDING = "main_models.team_game_fielding_stats"
_UPSTREAM_PITCHING = "main_models.team_game_pitching_stats"

_JOIN_SQL = """
SELECT
    s.season,
    s.game_id,
    r.game_finish_date,
    s.team_id,
    s.game_type,
    s.team_side,
    s.league,
    s.division,
    s.opponent_league,
    s.opponent_division,
    s.season_game_number,
    s.is_interleague,
    CASE WHEN s.team_id = r.winning_team_id THEN 1 ELSE 0 END AS wins,
    CASE WHEN s.team_id = r.losing_team_id THEN 1 ELSE 0 END AS losses,
    CASE WHEN s.team_side = 'Home' THEN r.home_runs_scored ELSE r.away_runs_scored END AS runs_scored,
    CASE WHEN s.team_side = 'Home' THEN r.away_runs_scored ELSE r.home_runs_scored END AS runs_allowed,
    o.hits,
    f.errors,
    o.left_on_base,
    o.at_bats,
    o.doubles,
    o.triples,
    o.home_runs,
    o.runs_batted_in,
    o.sacrifice_hits,
    o.sacrifice_flies,
    o.hit_by_pitches,
    o.walks,
    o.intentional_walks,
    o.strikeouts,
    o.stolen_bases,
    o.caught_stealing,
    o.grounded_into_double_plays,
    o.reached_on_interferences,
    p.innings_pitched,
    p.individual_earned_runs AS individual_earned_runs_allowed,
    p.earned_runs AS earned_runs_allowed,
    p.wild_pitches,
    p.balks,
    f.putouts,
    f.assists,
    f.passed_balls,
    f.double_plays AS double_plays_turned,
    f.triple_plays AS triple_plays_turned,
    o_opp.team_id AS opponent_team_id,
    o_opp.runs AS opponent_runs,
    o_opp.hits AS opponent_hits,
    f_opp.errors AS opponent_errors,
    o_opp.left_on_base AS opponent_left_on_base,
    o_opp.at_bats AS opponent_at_bats,
    o_opp.doubles AS opponent_doubles,
    o_opp.triples AS opponent_triples,
    o_opp.home_runs AS opponent_home_runs,
    o_opp.runs_batted_in AS opponent_runs_batted_in,
    o_opp.sacrifice_hits AS opponent_sacrifice_hits,
    o_opp.sacrifice_flies AS opponent_sacrifice_flies,
    o_opp.hit_by_pitches AS opponent_hit_by_pitches,
    o_opp.walks AS opponent_walks,
    o_opp.intentional_walks AS opponent_intentional_walks,
    o_opp.strikeouts AS opponent_strikeouts,
    o_opp.stolen_bases AS opponent_stolen_bases,
    o_opp.caught_stealing AS opponent_caught_stealing,
    o_opp.grounded_into_double_plays AS opponent_grounded_into_double_plays,
    o_opp.reached_on_interferences AS opponent_reached_on_interferences,
    p_opp.innings_pitched AS opponent_innings_pitched,
    p_opp.individual_earned_runs AS opponent_individual_earned_runs_allowed,
    p_opp.earned_runs AS opponent_earned_runs_allowed,
    p_opp.wild_pitches AS opponent_wild_pitches,
    p_opp.balks AS opponent_balks,
    f_opp.putouts AS opponent_putouts,
    f_opp.assists AS opponent_assists,
    f_opp.passed_balls AS opponent_passed_balls,
    f_opp.double_plays AS opponent_double_plays,
    f_opp.triple_plays AS opponent_triple_plays
FROM {start_info} AS s
INNER JOIN {game_results} AS r USING (game_id)
LEFT JOIN {offense} AS o USING (game_id, team_id)
LEFT JOIN {fielding} AS f USING (game_id, team_id)
LEFT JOIN {pitching} AS p USING (game_id, team_id)
LEFT JOIN {offense} AS o_opp
    ON o_opp.game_id = o.game_id AND o_opp.team_id != o.team_id
LEFT JOIN {fielding} AS f_opp
    ON f_opp.game_id = f.game_id AND f_opp.team_id != f.team_id
LEFT JOIN {pitching} AS p_opp
    ON p_opp.game_id = p.game_id AND p_opp.team_id != p.team_id
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
            "column": exp.column("team_id"),
            "to_model": exp.to_table("main_seeds.seed_franchises"),
            "to_column": exp.column("team_id"),
        },
    ),
]


@model(
    "main_models.team_game_results",
    kind="FULL",
    description=(
        "A version of `game_results` that includes one row for each team in each game. "
        "Also includes additional statistics (traditional box score stats) for games that "
        "have that information available."
    ),
    columns={
        "season": "SMALLINT",
        "game_id": "VARCHAR",
        "game_finish_date": "DATE",
        "team_id": _udt("TEAM_ID"),
        "game_type": _udt("GAME_TYPE"),
        "team_side": _udt("SIDE"),
        "league": "VARCHAR",
        "division": "VARCHAR",
        "opponent_league": "VARCHAR",
        "opponent_division": "VARCHAR",
        "season_game_number": "BIGINT",
        "is_interleague": "BOOLEAN",
        "wins": "INTEGER",
        "losses": "INTEGER",
        "runs_scored": "UTINYINT",
        "runs_allowed": "UTINYINT",
        "hits": "USMALLINT",
        "errors": "UTINYINT",
        "left_on_base": "USMALLINT",
        "at_bats": "USMALLINT",
        "doubles": "USMALLINT",
        "triples": "USMALLINT",
        "home_runs": "USMALLINT",
        "runs_batted_in": "USMALLINT",
        "sacrifice_hits": "USMALLINT",
        "sacrifice_flies": "USMALLINT",
        "hit_by_pitches": "USMALLINT",
        "walks": "USMALLINT",
        "intentional_walks": "USMALLINT",
        "strikeouts": "USMALLINT",
        "stolen_bases": "USMALLINT",
        "caught_stealing": "USMALLINT",
        "grounded_into_double_plays": "USMALLINT",
        "reached_on_interferences": "USMALLINT",
        "innings_pitched": "DECIMAL(6,4)",
        "individual_earned_runs_allowed": "USMALLINT",
        "earned_runs_allowed": "UTINYINT",
        "wild_pitches": "USMALLINT",
        "balks": "USMALLINT",
        "putouts": "UTINYINT",
        "assists": "UTINYINT",
        "passed_balls": "UTINYINT",
        "double_plays_turned": "UTINYINT",
        "triple_plays_turned": "UTINYINT",
        "opponent_team_id": _udt("TEAM_ID"),
        "opponent_runs": "USMALLINT",
        "opponent_hits": "USMALLINT",
        "opponent_errors": "UTINYINT",
        "opponent_left_on_base": "USMALLINT",
        "opponent_at_bats": "USMALLINT",
        "opponent_doubles": "USMALLINT",
        "opponent_triples": "USMALLINT",
        "opponent_home_runs": "USMALLINT",
        "opponent_runs_batted_in": "USMALLINT",
        "opponent_sacrifice_hits": "USMALLINT",
        "opponent_sacrifice_flies": "USMALLINT",
        "opponent_hit_by_pitches": "USMALLINT",
        "opponent_walks": "USMALLINT",
        "opponent_intentional_walks": "USMALLINT",
        "opponent_strikeouts": "USMALLINT",
        "opponent_stolen_bases": "USMALLINT",
        "opponent_caught_stealing": "USMALLINT",
        "opponent_grounded_into_double_plays": "USMALLINT",
        "opponent_reached_on_interferences": "USMALLINT",
        "opponent_innings_pitched": "DECIMAL(6,4)",
        "opponent_individual_earned_runs_allowed": "USMALLINT",
        "opponent_earned_runs_allowed": "UTINYINT",
        "opponent_wild_pitches": "USMALLINT",
        "opponent_balks": "USMALLINT",
        "opponent_putouts": "UTINYINT",
        "opponent_assists": "UTINYINT",
        "opponent_passed_balls": "UTINYINT",
        "opponent_double_plays": "UTINYINT",
        "opponent_triple_plays": "UTINYINT",
        "home_wins": "INTEGER",
        "home_losses": "INTEGER",
        "away_wins": "INTEGER",
        "away_losses": "INTEGER",
        "interleague_wins": "INTEGER",
        "interleague_losses": "INTEGER",
        "east_wins": "INTEGER",
        "east_losses": "INTEGER",
        "central_wins": "INTEGER",
        "central_losses": "INTEGER",
        "west_wins": "INTEGER",
        "west_losses": "INTEGER",
        "one_run_wins": "INTEGER",
        "one_run_losses": "INTEGER",
        "win_streak_id": "BIGINT",
        "loss_streak_id": "BIGINT",
        "win_streak_length": "BIGINT",
        "loss_streak_length": "BIGINT",
    },
    column_descriptions={
        "season": doc("season"),
        "game_id": doc("game_id"),
        "team_id": doc("team_id"),
        "game_type": doc("game_type"),
        "league": doc("league"),
        "division": doc("division"),
        "is_interleague": doc("is_interleague"),
        "wins": doc("wins"),
        "losses": doc("losses"),
        "hits": doc("hits"),
        "errors": doc("errors"),
        "left_on_base": doc("left_on_base"),
        "at_bats": doc("at_bats"),
        "doubles": doc("doubles"),
        "triples": doc("triples"),
        "home_runs": doc("home_runs"),
        "runs_batted_in": doc("runs_batted_in"),
        "sacrifice_hits": doc("sacrifice_hits"),
        "sacrifice_flies": doc("sacrifice_flies"),
        "hit_by_pitches": doc("hit_by_pitches"),
        "walks": doc("walks"),
        "intentional_walks": doc("intentional_walks"),
        "strikeouts": doc("strikeouts"),
        "stolen_bases": doc("stolen_bases"),
        "caught_stealing": doc("caught_stealing"),
        "grounded_into_double_plays": doc("grounded_into_double_plays"),
        "reached_on_interferences": doc("reached_on_interferences"),
        "innings_pitched": doc("innings_pitched"),
        "wild_pitches": doc("wild_pitches"),
        "balks": doc("balks"),
        "putouts": doc("putouts"),
        "assists": doc("assists"),
        "passed_balls": doc("passed_balls"),
    },
    grain=["game_id", "team_id"],
    audits=_AUDITS,
    physical_properties={
        "download_parquet": "https://data.baseball.computer/dbt/main_models_team_game_results.parquet",
    },
    depends_on={
        _UPSTREAM_START_INFO,
        _UPSTREAM_GAME_RESULTS,
        _UPSTREAM_OFFENSE,
        _UPSTREAM_FIELDING,
        _UPSTREAM_PITCHING,
    },
)
def execute(context: ExecutionContext, **kwargs: t.Any) -> pd.DataFrame:
    del kwargs
    sql = _JOIN_SQL.format(
        start_info=context.resolve_table(_UPSTREAM_START_INFO),
        game_results=context.resolve_table(_UPSTREAM_GAME_RESULTS),
        offense=context.resolve_table(_UPSTREAM_OFFENSE),
        fielding=context.resolve_table(_UPSTREAM_FIELDING),
        pitching=context.resolve_table(_UPSTREAM_PITCHING),
    )
    games: pl.DataFrame = context.engine_adapter.cursor.sql(sql).pl()
    return compute_team_game_results(games).to_pandas()
