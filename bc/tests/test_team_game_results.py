"""Unit tests for the Polars FSM behind team_game_results."""

from __future__ import annotations

from datetime import date as Date

import pandas as pd
import polars as pl
import pytest

from python_models.game_level import (
    TEAM_GAME_RESULTS_INPUT_COLUMNS,
    compute_team_game_results,
)


def _row(
    *,
    season: int,
    game_id: str,
    team_id: str,
    opponent_team_id: str,
    season_game_number: int,
    game_finish_date: Date,
    won: bool,
    runs_scored: int = 5,
    runs_allowed: int = 3,
    team_side: str = "Home",
    is_interleague: bool = False,
    opponent_division: str | None = "E",
    division: str | None = "E",
    league: str | None = "AL",
    opponent_league: str | None = "AL",
    game_type: str = "RegularSeason",
) -> dict[str, object]:
    return {
        "season": season,
        "game_id": game_id,
        "game_finish_date": game_finish_date,
        "team_id": team_id,
        "game_type": game_type,
        "team_side": team_side,
        "league": league,
        "division": division,
        "opponent_league": opponent_league,
        "opponent_division": opponent_division,
        "season_game_number": season_game_number,
        "is_interleague": is_interleague,
        "wins": 1 if won else 0,
        "losses": 0 if won else 1,
        "runs_scored": runs_scored,
        "runs_allowed": runs_allowed,
        "hits": 8,
        "errors": 0,
        "left_on_base": 5,
        "at_bats": 30,
        "doubles": 1,
        "triples": 0,
        "home_runs": 1,
        "runs_batted_in": runs_scored,
        "sacrifice_hits": 0,
        "sacrifice_flies": 0,
        "hit_by_pitches": 0,
        "walks": 2,
        "intentional_walks": 0,
        "strikeouts": 6,
        "stolen_bases": 0,
        "caught_stealing": 0,
        "grounded_into_double_plays": 0,
        "reached_on_interferences": 0,
        "innings_pitched": 9.0,
        "individual_earned_runs_allowed": runs_allowed,
        "earned_runs_allowed": runs_allowed,
        "wild_pitches": 0,
        "balks": 0,
        "putouts": 27,
        "assists": 10,
        "passed_balls": 0,
        "double_plays_turned": 1,
        "triple_plays_turned": 0,
        "opponent_team_id": opponent_team_id,
        "opponent_runs": runs_allowed,
        "opponent_hits": 6,
        "opponent_errors": 1,
        "opponent_left_on_base": 4,
        "opponent_at_bats": 31,
        "opponent_doubles": 0,
        "opponent_triples": 0,
        "opponent_home_runs": 0,
        "opponent_runs_batted_in": runs_allowed,
        "opponent_sacrifice_hits": 0,
        "opponent_sacrifice_flies": 0,
        "opponent_hit_by_pitches": 0,
        "opponent_walks": 1,
        "opponent_intentional_walks": 0,
        "opponent_strikeouts": 8,
        "opponent_stolen_bases": 0,
        "opponent_caught_stealing": 0,
        "opponent_grounded_into_double_plays": 0,
        "opponent_reached_on_interferences": 0,
        "opponent_innings_pitched": 8.0,
        "opponent_individual_earned_runs_allowed": runs_scored,
        "opponent_earned_runs_allowed": runs_scored,
        "opponent_wild_pitches": 0,
        "opponent_balks": 0,
        "opponent_putouts": 24,
        "opponent_assists": 9,
        "opponent_passed_balls": 0,
        "opponent_double_plays": 0,
        "opponent_triple_plays": 0,
    }


_INPUT_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "season": pl.Int16,
    "game_id": pl.String,
    "game_finish_date": pl.Date,
    "team_id": pl.String,
    "game_type": pl.String,
    "team_side": pl.String,
    "league": pl.String,
    "division": pl.String,
    "opponent_league": pl.String,
    "opponent_division": pl.String,
    "season_game_number": pl.Int64,
    "is_interleague": pl.Boolean,
    "wins": pl.Int32,
    "losses": pl.Int32,
    "runs_scored": pl.UInt8,
    "runs_allowed": pl.UInt8,
    "hits": pl.UInt16,
    "errors": pl.UInt8,
    "left_on_base": pl.UInt16,
    "at_bats": pl.UInt16,
    "doubles": pl.UInt16,
    "triples": pl.UInt16,
    "home_runs": pl.UInt16,
    "runs_batted_in": pl.UInt16,
    "sacrifice_hits": pl.UInt16,
    "sacrifice_flies": pl.UInt16,
    "hit_by_pitches": pl.UInt16,
    "walks": pl.UInt16,
    "intentional_walks": pl.UInt16,
    "strikeouts": pl.UInt16,
    "stolen_bases": pl.UInt16,
    "caught_stealing": pl.UInt16,
    "grounded_into_double_plays": pl.UInt16,
    "reached_on_interferences": pl.UInt16,
    "innings_pitched": pl.Float64,
    "individual_earned_runs_allowed": pl.UInt16,
    "earned_runs_allowed": pl.UInt8,
    "wild_pitches": pl.UInt16,
    "balks": pl.UInt16,
    "putouts": pl.UInt8,
    "assists": pl.UInt8,
    "passed_balls": pl.UInt8,
    "double_plays_turned": pl.UInt8,
    "triple_plays_turned": pl.UInt8,
    "opponent_team_id": pl.String,
    "opponent_runs": pl.UInt16,
    "opponent_hits": pl.UInt16,
    "opponent_errors": pl.UInt8,
    "opponent_left_on_base": pl.UInt16,
    "opponent_at_bats": pl.UInt16,
    "opponent_doubles": pl.UInt16,
    "opponent_triples": pl.UInt16,
    "opponent_home_runs": pl.UInt16,
    "opponent_runs_batted_in": pl.UInt16,
    "opponent_sacrifice_hits": pl.UInt16,
    "opponent_sacrifice_flies": pl.UInt16,
    "opponent_hit_by_pitches": pl.UInt16,
    "opponent_walks": pl.UInt16,
    "opponent_intentional_walks": pl.UInt16,
    "opponent_strikeouts": pl.UInt16,
    "opponent_stolen_bases": pl.UInt16,
    "opponent_caught_stealing": pl.UInt16,
    "opponent_grounded_into_double_plays": pl.UInt16,
    "opponent_reached_on_interferences": pl.UInt16,
    "opponent_innings_pitched": pl.Float64,
    "opponent_individual_earned_runs_allowed": pl.UInt16,
    "opponent_earned_runs_allowed": pl.UInt8,
    "opponent_wild_pitches": pl.UInt16,
    "opponent_balks": pl.UInt16,
    "opponent_putouts": pl.UInt8,
    "opponent_assists": pl.UInt8,
    "opponent_passed_balls": pl.UInt8,
    "opponent_double_plays": pl.UInt8,
    "opponent_triple_plays": pl.UInt8,
}


def _to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    df = pl.DataFrame(rows, schema=_INPUT_SCHEMA)
    assert set(TEAM_GAME_RESULTS_INPUT_COLUMNS).issubset(df.columns)
    return df


def _streak_input_clean_win() -> pl.DataFrame:
    rows = [
        _row(
            season=2024,
            game_id=f"NYA2024040{i + 1}0",
            team_id="NYA",
            opponent_team_id="BOS",
            season_game_number=i + 1,
            game_finish_date=Date(2024, 4, i + 1),
            won=True,
        )
        for i in range(4)
    ]
    return _to_df(rows)


def _streak_input_reset_on_loss() -> pl.DataFrame:
    outcomes = [True, True, False, True, True, True]
    rows = [
        _row(
            season=2024,
            game_id=f"NYA2024050{i + 1}0",
            team_id="NYA",
            opponent_team_id="BOS",
            season_game_number=i + 1,
            game_finish_date=Date(2024, 5, i + 1),
            won=won,
        )
        for i, won in enumerate(outcomes)
    ]
    return _to_df(rows)


def _streak_input_two_teams() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for i in range(3):
        rows.append(
            _row(
                season=2024,
                game_id=f"NYA2024060{i + 1}0",
                team_id="NYA",
                opponent_team_id="BOS",
                season_game_number=i + 1,
                game_finish_date=Date(2024, 6, i + 1),
                won=True,
            )
        )
    for i in range(3):
        rows.append(
            _row(
                season=2024,
                game_id=f"BOS2024060{i + 1}0",
                team_id="BOS",
                opponent_team_id="NYA",
                season_game_number=i + 1,
                game_finish_date=Date(2024, 6, i + 1),
                won=False,
                team_side="Away",
            )
        )
    return _to_df(rows)


def test_clean_win_streak_lengths_count_up() -> None:
    out = compute_team_game_results(_streak_input_clean_win()).sort(
        "season_game_number"
    )
    lengths = out["win_streak_length"].to_list()
    assert lengths == [1, 2, 3, 4]
    loss_lengths = out["loss_streak_length"].to_list()
    assert loss_lengths == [0, 0, 0, 0]
    streak_ids = out["win_streak_id"].to_list()
    assert all(sid == 1 for sid in streak_ids), (
        f"all rows should belong to streak id 1, got {streak_ids}"
    )
    assert out["loss_streak_id"].null_count() == 4


def test_streak_resets_on_loss_then_resumes() -> None:
    out = compute_team_game_results(_streak_input_reset_on_loss()).sort(
        "season_game_number"
    )
    win_lengths = out["win_streak_length"].to_list()
    loss_lengths = out["loss_streak_length"].to_list()
    win_ids = out["win_streak_id"].to_list()
    loss_ids = out["loss_streak_id"].to_list()

    assert win_lengths == [1, 2, 0, 1, 2, 3]
    assert loss_lengths == [0, 0, 1, 0, 0, 0]
    assert win_ids[0] == 1 and win_ids[1] == 1
    assert win_ids[2] is None
    assert win_ids[3] == 4 and win_ids[4] == 4 and win_ids[5] == 4
    assert loss_ids[2] == 3
    assert loss_ids[0] is None and loss_ids[1] is None
    assert loss_ids[3] is None


def test_partitions_are_isolated_across_teams() -> None:
    out = compute_team_game_results(_streak_input_two_teams())
    nya = out.filter(pl.col("team_id") == "NYA").sort("season_game_number")
    bos = out.filter(pl.col("team_id") == "BOS").sort("season_game_number")

    assert nya["win_streak_length"].to_list() == [1, 2, 3]
    assert nya["loss_streak_length"].to_list() == [0, 0, 0]
    assert bos["win_streak_length"].to_list() == [0, 0, 0]
    assert bos["loss_streak_length"].to_list() == [1, 2, 3]

    assert all(sid == 1 for sid in nya["win_streak_id"].to_list())
    assert all(sid == 1 for sid in bos["loss_streak_id"].to_list())


def test_split_columns_track_team_side_and_division() -> None:
    df = _streak_input_two_teams()
    out = compute_team_game_results(df).sort(["team_id", "season_game_number"])
    nya = out.filter(pl.col("team_id") == "NYA")
    bos = out.filter(pl.col("team_id") == "BOS")
    assert nya["home_wins"].sum() == 3
    assert nya["away_wins"].sum() == 0
    assert bos["away_losses"].sum() == 3
    assert bos["home_losses"].sum() == 0
    assert nya["east_wins"].sum() == 3
    assert bos["east_losses"].sum() == 3


def test_pandas_output_schema_invariant() -> None:
    out = compute_team_game_results(_streak_input_clean_win())
    pdf = out.to_pandas()
    assert isinstance(pdf, pd.DataFrame)
    expected = {
        "win_streak_id",
        "loss_streak_id",
        "win_streak_length",
        "loss_streak_length",
        "home_wins",
        "home_losses",
        "away_wins",
        "away_losses",
        "interleague_wins",
        "interleague_losses",
        "east_wins",
        "east_losses",
        "central_wins",
        "central_losses",
        "west_wins",
        "west_losses",
        "one_run_wins",
        "one_run_losses",
    }
    assert expected.issubset(set(pdf.columns))


def test_one_run_columns_use_absolute_difference() -> None:
    rows = [
        _row(
            season=2024,
            game_id="NYA2024070100",
            team_id="NYA",
            opponent_team_id="BOS",
            season_game_number=1,
            game_finish_date=Date(2024, 7, 1),
            won=True,
            runs_scored=5,
            runs_allowed=4,
        ),
        _row(
            season=2024,
            game_id="NYA2024070200",
            team_id="NYA",
            opponent_team_id="BOS",
            season_game_number=2,
            game_finish_date=Date(2024, 7, 2),
            won=False,
            runs_scored=3,
            runs_allowed=4,
        ),
        _row(
            season=2024,
            game_id="NYA2024070300",
            team_id="NYA",
            opponent_team_id="BOS",
            season_game_number=3,
            game_finish_date=Date(2024, 7, 3),
            won=True,
            runs_scored=10,
            runs_allowed=2,
        ),
    ]
    out = compute_team_game_results(_to_df(rows)).sort("season_game_number")
    assert out["one_run_wins"].to_list() == [1, 0, 0]
    assert out["one_run_losses"].to_list() == [0, 1, 0]


@pytest.mark.parametrize(
    "col, expected_dtype",
    [
        ("win_streak_id", pl.Int64),
        ("loss_streak_id", pl.Int64),
        ("win_streak_length", pl.Int64),
        ("loss_streak_length", pl.Int64),
        ("home_wins", pl.Int32),
        ("season", pl.Int16),
    ],
)
def test_output_dtypes(col: str, expected_dtype: pl.DataType) -> None:
    out = compute_team_game_results(_streak_input_clean_win())
    assert out.schema[col] == expected_dtype, (
        f"{col}: {out.schema[col]} vs {expected_dtype}"
    )
