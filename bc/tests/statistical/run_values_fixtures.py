"""Shared synthetic row builder for the Model G prep tests.

Every row carries the full set of population-filter columns so the run
expectancy and state-transition preps (and the coverage realizations) accept
it; keyword overrides move a row outside the population.
"""

from __future__ import annotations

from python_models.statistical.models._run_values_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
)
from python_models.statistical.splits import game_hash_fold


def start_key(*, season: int, league: str, outs: int, base: int) -> str:
    return f"{season}_{league}_{outs}_{base}"


def event_row(
    *,
    game_id: str,
    season: int,
    league: str,
    outs: int,
    base: int,
    end_outs: int,
    end_base: int,
    runs_to_end: int = 0,
    runs_on_play: int = 0,
    result_family: str | None = "out_in_play",
    game_type: str = "RegularSeason",
    inning_start: int = 5,
    denominator_policy: str = "include",
) -> dict[str, object]:
    return {
        "game_id": game_id,
        "season": season,
        "league": league,
        "game_type": game_type,
        "inning_start": inning_start,
        "denominator_policy": denominator_policy,
        "result_family": result_family,
        "runs_on_play": runs_on_play,
        "runs_to_end_of_inning": runs_to_end,
        "run_expectancy_start_key": start_key(
            season=season, league=league, outs=outs, base=base
        ),
        "run_expectancy_end_key": start_key(
            season=season, league=league, outs=end_outs, base=end_base
        ),
    }


def train_games(prefix: str, count: int) -> list[str]:
    games: list[str] = []
    i = 0
    while len(games) < count:
        gid = f"{prefix}_T{i:05d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) != HOLDOUT_FOLD_ID:
            games.append(gid)
        i += 1
    return games


def find_holdout_game(prefix: str) -> str:
    i = 0
    while True:
        gid = f"{prefix}_H{i:05d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            return gid
        i += 1
