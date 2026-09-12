from __future__ import annotations

import json
from pathlib import Path

import polars as pl
import pytest

from python_models.statistical.backtests.geometry_statcast_acquire import (
    pending_games,
)
from python_models.statistical.backtests.geometry_statcast_bridge import SelectedGame
from python_models.statistical.backtests.geometry_statcast_bridge_selection import (
    assign_selection,
    game_digest,
)


def source_frame(seasons: tuple[int, ...], games_per_season: int) -> pl.DataFrame:
    rows = [
        {
            "game_id": f"BOS{season}0{index:02d}10",
            "season": season,
            "park_id": "BOS07" if index % 2 else "NYA01",
            "date": f"{season}-05-{index + 1:02d}",
            "home_team_id": "BOS",
            "away_team_id": "NYA",
            "primary_fold": "TRAIN",
        }
        for season in seasons
        for index in range(games_per_season)
    ]
    return pl.DataFrame(rows)


def test_selection_marks_fixed_smoke_count_per_season_and_excludes_games() -> None:
    source = source_frame((2016, 2017, 2018), 9)
    excluded = set(source.filter(pl.col("season") != 2018)["game_id"].gather([0, 9]))
    selected = assign_selection(
        source, excluded_games=excluded, smoke_games_per_season=2
    )
    assert not set(selected["game_id"].to_list()) & excluded
    assert selected.height == source.height - len(excluded)
    assert set(selected["acquisition_fold"].to_list()) == {"fitting"}
    smoke = selected.filter(pl.col("is_smoke")).group_by("season").len().sort("season")
    assert smoke["len"].to_list() == [2, 2, 2]
    assert selected.filter(pl.col("is_smoke"))[
        "acquisition_stage"
    ].unique().to_list() == ["mechanics_fit"]
    again = assign_selection(source, excluded_games=excluded, smoke_games_per_season=2)
    assert selected.equals(again)
    for row in selected.to_dicts():
        SelectedGame.model_validate(row)


def test_selection_rejects_duplicates_and_other_folds() -> None:
    source = source_frame((2016,), 3)
    with pytest.raises(ValueError, match="duplicate"):
        assign_selection(pl.concat([source, source]), excluded_games=set())
    with pytest.raises(ValueError, match="TRAIN"):
        assign_selection(
            source.with_columns(pl.lit("TEST").alias("primary_fold")),
            excluded_games=set(),
        )


def test_game_digest_is_order_and_duplicate_invariant() -> None:
    assert game_digest(["b", "a", "a"]) == game_digest(["a", "b"])
    assert game_digest(["a"]) != game_digest(["ab"])


def test_pending_games_keeps_terminal_reports_and_requeues_the_rest(
    tmp_path: Path,
) -> None:
    selected = [
        SelectedGame.model_validate(row)
        for row in assign_selection(
            source_frame((2016,), 4), excluded_games=set()
        ).to_dicts()
    ]
    paired, failed, truncated, untouched = selected
    for game, report in (
        (paired, json.dumps({"status": "paired"})),
        (failed, json.dumps({"status": "failed"})),
        (truncated, "{"),
    ):
        root = tmp_path / "games" / game.game_id
        root.mkdir(parents=True)
        (root / "report.json").write_text(report)
        (root / "statcast.csv").write_text("partial")
    pending = pending_games(tmp_path, selected)
    assert [game.game_id for game in pending] == [
        truncated.game_id,
        untouched.game_id,
    ]
    assert (tmp_path / "games" / paired.game_id / "statcast.csv").exists()
    assert (tmp_path / "games" / failed.game_id / "statcast.csv").exists()
    assert not (tmp_path / "games" / truncated.game_id).exists()
