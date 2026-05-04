"""Unit tests for the Phase 5 second-wave Polars FSM behind team_game_start_info."""

from __future__ import annotations

from datetime import date as Date

import pandas as pd
import polars as pl

from python_models.game_level import compute_team_game_start_info


def _row(
    *,
    season: int,
    game_id: str,
    team_id: str,
    opponent_id: str,
    date: Date,
    doubleheader_status: str = "Single",
    game_type: str = "RegularSeason",
    team_side: str = "Home",
) -> dict[str, object]:
    return {
        "season": season,
        "game_id": game_id,
        "team_id": team_id,
        "opponent_id": opponent_id,
        "date": date,
        "doubleheader_status": doubleheader_status,
        "game_type": game_type,
        "team_side": team_side,
    }


_INPUT_SCHEMA: dict[str, pl.DataType | type[pl.DataType]] = {
    "season": pl.Int16,
    "game_id": pl.String,
    "team_id": pl.String,
    "opponent_id": pl.String,
    "date": pl.Date,
    "doubleheader_status": pl.String,
    "game_type": pl.String,
    "team_side": pl.String,
}


def _to_df(rows: list[dict[str, object]]) -> pl.DataFrame:
    return pl.DataFrame(rows, schema=_INPUT_SCHEMA)


def _three_game_series_then_new_opponent() -> pl.DataFrame:
    rows = [
        _row(
            season=2024,
            game_id=f"NYA2024040{i + 1}0",
            team_id="NYA",
            opponent_id="BOS",
            date=Date(2024, 4, i + 1),
        )
        for i in range(3)
    ]
    rows.extend(
        _row(
            season=2024,
            game_id=f"NYA2024040{i + 4}0",
            team_id="NYA",
            opponent_id="TOR",
            date=Date(2024, 4, i + 4),
        )
        for i in range(3)
    )
    return _to_df(rows)


def test_series_id_carries_through_series() -> None:
    df = _three_game_series_then_new_opponent()
    out = compute_team_game_start_info(df).sort("date")
    series = out["series_id"].to_list()
    assert series[0] == series[1] == series[2] == "NYA2024040" + "1" + "0"
    assert series[3] == series[4] == series[5] == "NYA2024040" + "4" + "0"
    assert series[0] != series[3], "new opponent must produce new series_id"


def test_series_game_number_resets_per_series() -> None:
    df = _three_game_series_then_new_opponent()
    out = compute_team_game_start_info(df).sort("date")
    assert out["series_game_number"].to_list() == [1, 2, 3, 1, 2, 3]


def test_season_game_number_runs_across_series() -> None:
    df = _three_game_series_then_new_opponent()
    out = compute_team_game_start_info(df).sort("date")
    assert out["season_game_number"].to_list() == [1, 2, 3, 4, 5, 6]


def test_days_since_last_game_includes_first_null() -> None:
    df = _three_game_series_then_new_opponent()
    out = compute_team_game_start_info(df).sort("date")
    diffs = out["days_since_last_game"].to_list()
    assert diffs[0] is None
    assert diffs[1:] == [1, 1, 1, 1, 1]


def test_doubleheader_breaks_tie_for_ordering() -> None:
    rows = [
        _row(
            season=2024,
            game_id="NYA2024050100",
            team_id="NYA",
            opponent_id="BOS",
            date=Date(2024, 5, 1),
            doubleheader_status="DoubleHeaderGame1",
        ),
        _row(
            season=2024,
            game_id="NYA2024050101",
            team_id="NYA",
            opponent_id="BOS",
            date=Date(2024, 5, 1),
            doubleheader_status="DoubleHeaderGame2",
        ),
        _row(
            season=2024,
            game_id="NYA2024050200",
            team_id="NYA",
            opponent_id="BOS",
            date=Date(2024, 5, 2),
        ),
    ]
    out = compute_team_game_start_info(_to_df(rows)).sort(
        ["date", "doubleheader_status"]
    )
    series = out["series_id"].to_list()
    assert series[0] == series[1] == series[2], (
        f"a doubleheader does not start a new series: {series}"
    )
    assert out["series_game_number"].to_list() == [1, 2, 3]


def test_partition_by_team_isolates_series() -> None:
    rows: list[dict[str, object]] = []
    for i in range(2):
        rows.append(
            _row(
                season=2024,
                game_id=f"NYA2024060{i + 1}0",
                team_id="NYA",
                opponent_id="BOS",
                date=Date(2024, 6, i + 1),
            )
        )
    for i in range(2):
        rows.append(
            _row(
                season=2024,
                game_id=f"BOS2024060{i + 1}0",
                team_id="BOS",
                opponent_id="NYA",
                date=Date(2024, 6, i + 1),
                team_side="Away",
            )
        )
    out = compute_team_game_start_info(_to_df(rows))
    nya = out.filter(pl.col("team_id") == "NYA").sort("date")
    bos = out.filter(pl.col("team_id") == "BOS").sort("date")
    assert nya["series_game_number"].to_list() == [1, 2]
    assert bos["series_game_number"].to_list() == [1, 2]
    assert nya["series_id"].unique().to_list() != bos["series_id"].unique().to_list()


def test_pandas_output_schema_invariant() -> None:
    out = compute_team_game_start_info(_three_game_series_then_new_opponent())
    pdf = out.to_pandas()
    assert isinstance(pdf, pd.DataFrame)
    for c in (
        "series_id",
        "season_game_number",
        "series_game_number",
        "days_since_last_game",
    ):
        assert c in pdf.columns


def test_output_dtypes() -> None:
    out = compute_team_game_start_info(_three_game_series_then_new_opponent())
    assert out.schema["series_id"] == pl.String
    assert out.schema["season_game_number"] == pl.Int64
    assert out.schema["series_game_number"] == pl.Int64
    assert out.schema["days_since_last_game"] == pl.Int64
