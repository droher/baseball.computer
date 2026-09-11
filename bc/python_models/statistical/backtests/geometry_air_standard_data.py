from __future__ import annotations

import hashlib
import inspect
from pathlib import Path
from typing import Literal, cast

import polars as pl
from pydantic import BaseModel

from python_models.statistical.backtests.geometry_statcast_targets import (
    broad_type,
    standardize_trajectory,
)

DevelopmentFoldUnit = Literal["game", "park"]
DEVELOPMENT_SEASONS = frozenset({2015, 2019, 2023, 2025})
FOLD_COUNT = 5
FOLD_CONTRACT = "geometry-air-development-v1"


class AcquisitionManifest(BaseModel):
    files_sha256: dict[str, str]


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
    return digest.hexdigest()


def _require_hash(path: Path, expected: str) -> None:
    if len(expected) != 64 or _sha256(path) != expected:
        raise ValueError(f"SHA-256 mismatch for {path.name}")


def _manifest_hash(
    root: Path, manifest: AcquisitionManifest, relative_path: str
) -> str:
    expected = manifest.files_sha256.get(relative_path)
    if expected is None:
        raise ValueError(f"accepted manifest does not bind {relative_path}")
    _require_hash(root / relative_path, expected)
    return expected


def _fold(namespace: str, value: str) -> int:
    payload = f"{FOLD_CONTRACT}:{namespace}:{value}".encode()
    return int(hashlib.sha256(payload).hexdigest(), 16) % FOLD_COUNT


def game_fold(game_id: str) -> int:
    return _fold("game", game_id)


def park_fold(park_id: str) -> int:
    return _fold("park", park_id)


def add_development_folds(frame: pl.DataFrame) -> pl.DataFrame:
    game_ids = cast(list[str], frame["game_id"].to_list())
    park_ids = cast(list[str], frame["park_id"].to_list())
    if any(not value for value in game_ids) or any(not value for value in park_ids):
        raise ValueError("game_id and park_id must be non-empty")
    return frame.with_columns(
        pl.Series(
            "game_fold", [game_fold(value) for value in game_ids], dtype=pl.UInt8
        ),
        pl.Series(
            "park_fold", [park_fold(value) for value in park_ids], dtype=pl.UInt8
        ),
    )


def split_development_fold(
    frame: pl.DataFrame,
    *,
    unit: DevelopmentFoldUnit,
    heldout_fold: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    if heldout_fold not in range(FOLD_COUNT):
        raise ValueError("heldout_fold must be between 0 and 4")
    folded = add_development_folds(frame)
    fold_column = f"{unit}_fold"
    evaluation = folded.filter(pl.col(fold_column) == heldout_fold)
    training = folded.filter(pl.col(fold_column) != heldout_fold)
    if set(training["game_id"].to_list()) & set(evaluation["game_id"].to_list()):
        raise ValueError("game crosses development folds")
    if unit == "park" and set(training["park_id"].to_list()) & set(
        evaluation["park_id"].to_list()
    ):
        raise ValueError("park crosses development folds")
    return training, evaluation


def split_leave_one_season_out(
    frame: pl.DataFrame,
    *,
    heldout_season: int,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    seasons = set(cast(list[int], frame["season"].unique().to_list()))
    if seasons != set(DEVELOPMENT_SEASONS):
        raise ValueError("frame does not contain the declared development seasons")
    if heldout_season not in DEVELOPMENT_SEASONS:
        raise ValueError("heldout_season is outside the development contract")
    evaluation = frame.filter(pl.col("season") == heldout_season)
    training = frame.filter(pl.col("season") != heldout_season)
    if not evaluation.height or heldout_season in set(training["season"].to_list()):
        raise ValueError("invalid leave-one-season-out partition")
    if set(training["game_id"].to_list()) & set(evaluation["game_id"].to_list()):
        raise ValueError("game crosses season partitions")
    return training, evaluation


def load_development_frame(
    root: Path,
    *,
    acquisition_manifest_sha256: str,
    selected_games_sha256: str,
    target_module_sha256: str,
    expected_games: int = 373,
    expected_events: int = 19_436,
) -> pl.DataFrame:
    root = root.resolve()
    manifest_path = root / "manifest.json"
    _require_hash(manifest_path, acquisition_manifest_sha256)
    manifest = AcquisitionManifest.model_validate_json(manifest_path.read_text())
    selected_path = root / "selected_fitting_games.parquet"
    _require_hash(selected_path, selected_games_sha256)
    if _manifest_hash(root, manifest, selected_path.name) != selected_games_sha256:
        raise ValueError(
            "external selected-game binding differs from accepted manifest"
        )
    target_path_text = inspect.getsourcefile(standardize_trajectory)
    if target_path_text is None:
        raise ValueError("canonical target source is unavailable")
    _require_hash(Path(target_path_text), target_module_sha256)
    selected = pl.read_parquet(selected_path)
    if (
        selected.height != expected_games
        or selected["game_id"].n_unique() != expected_games
    ):
        raise ValueError("selected-game population differs from the declared contract")
    if set(selected["primary_fold"].to_list()) != {"TRAIN"}:
        raise ValueError("selected games must all be PRIMARY TRAIN")
    if set(selected["acquisition_fold"].to_list()) != {"fitting"}:
        raise ValueError("selected games must all be fitting games")
    game_ids = sorted(cast(list[str], selected["game_id"].to_list()))
    parts: list[pl.DataFrame] = []
    for game_id in game_ids:
        relative_path = f"games/{game_id}/paired_events.parquet"
        _ = _manifest_hash(root, manifest, relative_path)
        parts.append(pl.read_parquet(root / relative_path))
    paired = pl.concat(parts, how="vertical")
    aggregate_path = "paired_events.parquet"
    _ = _manifest_hash(root, manifest, aggregate_path)
    aggregate = pl.read_parquet(root / aggregate_path)
    if paired.height != expected_events or aggregate.height != expected_events:
        raise ValueError("event population differs from the declared contract")
    if paired["event_key"].n_unique() != expected_events:
        raise ValueError("event_key is not unique")
    if set(cast(list[int], paired["event_key"].to_list())) != set(
        cast(list[int], aggregate["event_key"].to_list())
    ):
        raise ValueError("per-game and aggregate event populations differ")
    if set(cast(list[str], paired["game_id"].unique().to_list())) != set(game_ids):
        raise ValueError("paired-event games differ from selected games")
    if set(paired["primary_fold"].to_list()) != {"TRAIN"}:
        raise ValueError("paired events must all be PRIMARY TRAIN")
    if set(paired["acquisition_fold"].to_list()) != {"fitting"}:
        raise ValueError("paired events must all be fitting events")
    recorded_classes = cast(list[str | None], paired["recorded_class"].to_list())
    statcast_classes = cast(list[str | None], paired["statcast_bb_type"].to_list())
    angles = cast(list[float | None], paired["launch_angle"].to_list())
    pairing_complete = cast(list[bool], paired["game_pairing_complete"].to_list())
    targets = [
        standardize_trajectory(
            recorded,
            reference,
            angle,
            pairing_complete=complete,
        )
        for recorded, reference, angle, complete in zip(
            recorded_classes,
            statcast_classes,
            angles,
            pairing_complete,
            strict=True,
        )
    ]
    recorded_broad = [broad_type(value) for value in recorded_classes]
    recorded_air_subtype = [
        "LineDrive"
        if value in {"LineDrive", "LineDriveBunt"}
        else "Fly"
        if value in {"Fly", "FlyBall"}
        else "PopUp"
        if value in {"PopUp", "PopUpBunt"}
        else None
        for value in recorded_classes
    ]
    frame = paired.select(
        "event_key",
        "game_id",
        "season",
        "park_id",
        pl.col("scorer").alias("game_header_scorer_proxy"),
        "result_family",
        "recorded_class",
        "primary_fold",
        "acquisition_fold",
    ).with_columns(
        pl.Series("recorded_air_subtype", recorded_air_subtype, dtype=pl.String),
        pl.Series("recorded_broad_type", recorded_broad, dtype=pl.String),
        pl.Series(
            "target_class", [target.trajectory for target in targets], dtype=pl.String
        ),
        pl.Series(
            "target_status", [target.status for target in targets], dtype=pl.String
        ),
        pl.Series(
            "recorded_bunt",
            [target.recorded_bunt for target in targets],
            dtype=pl.Boolean,
        ),
    )
    frame = frame.with_columns(
        (
            (pl.col("recorded_broad_type") == "Air")
            & (pl.col("target_status") == "air_angle_standardized")
            & pl.col("target_class").is_not_null()
        )
        .fill_null(False)
        .alias("known_air_evaluation_eligible")
    )
    if frame.height != expected_events:
        raise ValueError("development frame does not conserve events")
    if (
        frame.filter(pl.col("target_class").is_null()).height
        + frame.filter(pl.col("target_class").is_not_null()).height
        != expected_events
    ):
        raise ValueError("resolved and unresolved targets do not conserve events")
    return add_development_folds(frame.sort("game_id", "event_key"))
