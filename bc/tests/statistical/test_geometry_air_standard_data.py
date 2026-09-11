from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path
from typing import Literal, cast

import polars as pl
import pytest

from python_models.statistical.backtests.geometry_air_standard_data import (
    FOLD_CONTRACT,
    add_development_folds,
    game_fold,
    load_development_frame,
    park_fold,
    split_development_fold,
    split_leave_one_season_out,
)
from python_models.statistical.backtests.geometry_statcast_targets import (
    standardize_trajectory,
)


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _event(
    event_key: int,
    game_id: str,
    season: int,
    park_id: str,
    recorded_class: str,
    statcast_class: str,
    angle: float | None,
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "game_id": game_id,
        "season": season,
        "park_id": park_id,
        "scorer": f"score{season}",
        "result_family": "out_in_play",
        "recorded_class": recorded_class,
        "statcast_bb_type": statcast_class,
        "launch_angle": angle,
        "game_pairing_complete": True,
        "primary_fold": "TRAIN",
        "acquisition_fold": "fitting",
    }


def _artifact(tmp_path: Path) -> tuple[Path, str, str, str, int, int]:
    root = tmp_path / "fitting"
    rows = [
        _event(1, "AAA201500001", 2015, "PARK1", "Fly", "Fly", 30),
        _event(2, "AAA201500001", 2015, "PARK1", "Unknown", "Fly", 20),
        _event(3, "BBB201900001", 2019, "PARK2", "GroundBall", "Fly", 20),
        _event(4, "BBB201900001", 2019, "PARK2", "PopUpBunt", "PopUp", 55),
        _event(5, "BBB201900001", 2019, "PARK2", "Fly", "Fly", None),
    ]
    selected = pl.DataFrame(
        {
            "game_id": ["AAA201500001", "BBB201900001"],
            "primary_fold": ["TRAIN", "TRAIN"],
            "acquisition_fold": ["fitting", "fitting"],
        }
    )
    root.mkdir()
    selected_path = root / "selected_fitting_games.parquet"
    _ = selected.write_parquet(selected_path)
    aggregate = pl.DataFrame(rows)
    aggregate_path = root / "paired_events.parquet"
    _ = aggregate.write_parquet(aggregate_path)
    files = {
        selected_path.name: _sha256(selected_path),
        aggregate_path.name: _sha256(aggregate_path),
    }
    for game_id in cast(list[str], selected["game_id"].to_list()):
        game_root = root / "games" / game_id
        game_root.mkdir(parents=True)
        path = game_root / "paired_events.parquet"
        _ = aggregate.filter(pl.col("game_id") == game_id).write_parquet(path)
        files[str(path.relative_to(root))] = _sha256(path)
    manifest_path = root / "manifest.json"
    _ = manifest_path.write_text(json.dumps({"files_sha256": files}, sort_keys=True))
    target_path_text = inspect.getsourcefile(standardize_trajectory)
    assert target_path_text is not None
    return (
        root,
        _sha256(manifest_path),
        _sha256(selected_path),
        _sha256(Path(target_path_text)),
        selected.height,
        aggregate.height,
    )


def _load(tmp_path: Path) -> pl.DataFrame:
    root, manifest_hash, selected_hash, target_hash, games, events = _artifact(tmp_path)
    return load_development_frame(
        root,
        acquisition_manifest_sha256=manifest_hash,
        selected_games_sha256=selected_hash,
        target_module_sha256=target_hash,
        expected_games=games,
        expected_events=events,
    )


def test_loader_requires_external_content_bindings(tmp_path: Path) -> None:
    root, manifest_hash, selected_hash, target_hash, games, events = _artifact(tmp_path)
    with pytest.raises(ValueError, match="manifest.json"):
        _ = load_development_frame(
            root,
            acquisition_manifest_sha256="0" * 64,
            selected_games_sha256=selected_hash,
            target_module_sha256=target_hash,
            expected_games=games,
            expected_events=events,
        )
    with pytest.raises(ValueError, match="selected_fitting_games.parquet"):
        _ = load_development_frame(
            root,
            acquisition_manifest_sha256=manifest_hash,
            selected_games_sha256="0" * 64,
            target_module_sha256=target_hash,
            expected_games=games,
            expected_events=events,
        )
    with pytest.raises(ValueError, match="geometry_statcast_targets.py"):
        _ = load_development_frame(
            root,
            acquisition_manifest_sha256=manifest_hash,
            selected_games_sha256=selected_hash,
            target_module_sha256="0" * 64,
            expected_games=games,
            expected_events=events,
        )
    game_path = root / "games" / "AAA201500001" / "paired_events.parquet"
    _ = game_path.write_bytes(game_path.read_bytes() + b"tampered")
    with pytest.raises(ValueError, match="paired_events.parquet"):
        _ = load_development_frame(
            root,
            acquisition_manifest_sha256=manifest_hash,
            selected_games_sha256=selected_hash,
            target_module_sha256=target_hash,
            expected_games=games,
            expected_events=events,
        )


def test_frame_conserves_unresolved_and_separates_predictors(tmp_path: Path) -> None:
    frame = _load(tmp_path)
    assert frame.height == 5
    assert frame["event_key"].n_unique() == frame.height
    assert "statcast_bb_type" not in frame.columns
    assert "launch_angle" not in frame.columns
    unknown = frame.filter(pl.col("recorded_class") == "Unknown").row(named=True)
    assert unknown["recorded_broad_type"] is None
    assert unknown["target_class"] == "LineDrive"
    assert unknown["known_air_evaluation_eligible"] is False
    conflict = frame.filter(pl.col("target_status") == "broad_source_conflict")
    missing = frame.filter(pl.col("target_status") == "air_angle_missing")
    assert conflict.height == 1 and conflict["target_class"].item() is None
    assert missing.height == 1 and missing["target_class"].item() is None
    assert frame["target_class"].is_null().sum() == 2
    bunt = frame.filter(pl.col("recorded_class") == "PopUpBunt").row(named=True)
    assert bunt["recorded_air_subtype"] == "PopUp"
    assert bunt["recorded_bunt"] is True
    assert bunt["known_air_evaluation_eligible"] is True


def test_duplicate_event_population_is_rejected(tmp_path: Path) -> None:
    root, _, selected_hash, target_hash, games, events = _artifact(tmp_path)
    aggregate_path = root / "paired_events.parquet"
    aggregate = pl.read_parquet(aggregate_path)
    duplicate = aggregate.with_columns(
        pl.when(pl.col("event_key") == 5)
        .then(4)
        .otherwise(pl.col("event_key"))
        .alias("event_key")
    )
    _ = duplicate.write_parquet(aggregate_path)
    game_path = root / "games" / "BBB201900001" / "paired_events.parquet"
    _ = duplicate.filter(pl.col("game_id") == "BBB201900001").write_parquet(game_path)
    manifest_path = root / "manifest.json"
    manifest = cast(dict[str, dict[str, str]], json.loads(manifest_path.read_text()))
    manifest["files_sha256"]["paired_events.parquet"] = _sha256(aggregate_path)
    manifest["files_sha256"][str(game_path.relative_to(root))] = _sha256(game_path)
    _ = manifest_path.write_text(json.dumps(manifest, sort_keys=True))
    with pytest.raises(ValueError, match="event_key is not unique"):
        _ = load_development_frame(
            root,
            acquisition_manifest_sha256=_sha256(manifest_path),
            selected_games_sha256=selected_hash,
            target_module_sha256=target_hash,
            expected_games=games,
            expected_events=events,
        )


def test_metadata_folds_exclude_whole_groups_and_seasons(tmp_path: Path) -> None:
    frame = _load(tmp_path)
    folded = add_development_folds(frame)
    game_id = cast(str, folded["game_id"].item(0))
    park_id = cast(str, folded["park_id"].item(0))
    expected_game_fold = (
        int(hashlib.sha256(f"{FOLD_CONTRACT}:game:{game_id}".encode()).hexdigest(), 16)
        % 5
    )
    expected_park_fold = (
        int(hashlib.sha256(f"{FOLD_CONTRACT}:park:{park_id}".encode()).hexdigest(), 16)
        % 5
    )
    assert game_fold(game_id) == expected_game_fold
    assert park_fold(park_id) == expected_park_fold
    altered_targets = frame.with_columns(
        pl.lit("changed").alias("recorded_class"),
        pl.lit(None, dtype=pl.String).alias("target_class"),
    )
    refolded = add_development_folds(altered_targets)
    assert refolded["game_fold"].equals(folded["game_fold"])
    assert refolded["park_fold"].equals(folded["park_fold"])
    for unit in cast(tuple[Literal["game", "park"], ...], ("game", "park")):
        column = f"{unit}_fold"
        for heldout in cast(list[int], folded[column].unique().to_list()):
            training, evaluation = split_development_fold(
                frame, unit=unit, heldout_fold=heldout
            )
            assert not set(training["game_id"]) & set(evaluation["game_id"])
            if unit == "park":
                assert not set(training["park_id"]) & set(evaluation["park_id"])
    season_frame = pl.DataFrame(
        {
            "event_key": [1, 2, 3, 4],
            "game_id": ["g1", "g2", "g3", "g4"],
            "season": [2015, 2019, 2023, 2025],
        }
    )
    training, evaluation = split_leave_one_season_out(season_frame, heldout_season=2023)
    assert set(evaluation["season"]) == {2023}
    assert set(training["season"]) == {2015, 2019, 2025}
    assert not set(training["game_id"]) & set(evaluation["game_id"])
