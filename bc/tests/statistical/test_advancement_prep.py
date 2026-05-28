"""Synthetic-fixture tests for the runner-advancement prep (Model H)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.statistical.models._advancement_data import (
    ADVANCEMENT_CLASS_LABELS,
    DIMENSION,
    FIXED_EFFECT_COLUMNS,
    build_advancement_production_frame,
    prepare_advancement_inputs,
)
from python_models.statistical.models._geometry_data import GeometryInputs
from python_models.statistical.splits import game_hash_fold

SEASON = 2023
LEAGUE = "AL"
HOLDOUT_FOLD_COUNT = 10
HOLDOUT_FOLD_ID = 0
MIN_EVENTS = 2

_BASERUNNER_BY_BASE = {1: "First", 2: "Second", 3: "Third"}


def _find_game(prefix: str, *, in_holdout: bool) -> str:
    i = 0
    while True:
        gid = f"{prefix}_{i:05d}"
        is_holdout = (
            game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
        )
        if is_holdout == in_holdout:
            return gid
        i += 1


def _row(
    *,
    event_key: int,
    base_start: int,
    advancement_class: str | None,
    game_id: str,
) -> dict[str, object]:
    fe_extra = {
        "outs_start": 1,
        "base_state_start": "100",
        "leverage_bucket": "medium",
        "result_family": "single",
        "alignment_regime": "standard",
        "trajectory_class": "GroundBall",
        "location_depth_class": "Shallow",
        "ball_handler_position_class": "SS",
    }
    return {
        "event_key": event_key,
        "baserunner": _BASERUNNER_BY_BASE[base_start],
        "base_start": base_start,
        "advancement_class": advancement_class,
        "training_weight": 1.0,
        "season": SEASON,
        "league": LEAGUE,
        "game_id": game_id,
        "scorer": "scorer_a",
        "park_id": "PARK01",
        "source_family": "retrosheet",
        **fe_extra,
    }


def _write_dataset(tmp_path: Path) -> tuple[Path, str, str]:
    train_game = _find_game("TRAIN", in_holdout=False)
    train_game_2 = _find_game("TRN2", in_holdout=False)
    holdout_game = _find_game("HOLD", in_holdout=True)

    labels = list(ADVANCEMENT_CLASS_LABELS)
    rows: list[dict[str, object]] = []
    ek = 1

    for n in range(MIN_EVENTS + 6):
        game = train_game if n % 2 == 0 else train_game_2
        base = (n % 3) + 1
        rows.append(
            _row(
                event_key=ek,
                base_start=base,
                advancement_class=labels[n % len(labels)],
                game_id=game,
            )
        )
        ek += 1

    for _ in range(3):
        rows.append(
            _row(
                event_key=ek,
                base_start=1,
                advancement_class=None,
                game_id=train_game,
            )
        )
        ek += 1

    for n in range(MIN_EVENTS + 1):
        rows.append(
            _row(
                event_key=ek,
                base_start=(n % 3) + 1,
                advancement_class=labels[n % len(labels)],
                game_id=holdout_game,
            )
        )
        ek += 1

    dataset_path = tmp_path / "advancement.parquet"
    pl.DataFrame(
        rows,
        schema={
            "event_key": pl.Int64,
            "baserunner": pl.Utf8,
            "base_start": pl.Int64,
            "advancement_class": pl.Utf8,
            "training_weight": pl.Float64,
            "season": pl.Int64,
            "league": pl.Utf8,
            "game_id": pl.Utf8,
            "scorer": pl.Utf8,
            "park_id": pl.Utf8,
            "source_family": pl.Utf8,
            "outs_start": pl.Int64,
            "base_state_start": pl.Utf8,
            "leverage_bucket": pl.Utf8,
            "result_family": pl.Utf8,
            "alignment_regime": pl.Utf8,
            "trajectory_class": pl.Utf8,
            "location_depth_class": pl.Utf8,
            "ball_handler_position_class": pl.Utf8,
        },
    ).write_parquet(dataset_path)
    return dataset_path, train_game, holdout_game


def test_returns_geometry_inputs_with_locked_vocabulary(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    assert isinstance(inputs, GeometryInputs)
    assert inputs.n_classes == len(ADVANCEMENT_CLASS_LABELS)
    assert inputs.class_labels == list(ADVANCEMENT_CLASS_LABELS)
    assert inputs.dimension == DIMENSION
    assert inputs.dl_active is False


def test_counts_are_one_hot_over_seven_classes(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    assert inputs.counts.shape[1] == len(ADVANCEMENT_CLASS_LABELS)
    row_sums = inputs.counts.sum(axis=1)
    assert (row_sums == 1).all()
    assert int(inputs.counts.sum()) == inputs.n_events


def test_null_labels_dropped_and_train_bounded_by_recorded(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    n_recorded = int(raw.get_column("advancement_class").is_not_null().sum())
    n_null = raw.height - n_recorded
    assert n_null > 0
    assert inputs.n_events <= n_recorded


def test_fixed_effects_present_in_fixture_appear_in_inputs(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    fixture_fe = {c for c in FIXED_EFFECT_COLUMNS if c in raw.columns}
    assert fixture_fe
    assert set(inputs.fixed_effects.keys()) == fixture_fe
    for column, design in inputs.fixed_effects.items():
        assert design.codes.shape[0] == inputs.n_events
        assert f"{column}_levels" in inputs.coords


def test_held_out_and_train_event_keys_disjoint(tmp_path: Path) -> None:
    dataset_path, _train, holdout_game = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    holdout_games = {
        g
        for g in raw.get_column("game_id").unique().to_list()
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    }
    assert holdout_game in holdout_games

    train_keys = set(inputs.event_keys.tolist())
    held_keys = set(inputs.held_out.event_keys.tolist())
    assert held_keys
    assert train_keys
    assert train_keys.isdisjoint(held_keys)


def test_production_frame_parallel_to_baserunner_labels(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_advancement_inputs(dataset_path, min_events_per_season=MIN_EVENTS)

    frame, baserunner_labels = build_advancement_production_frame(
        dataset_path,
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
    )

    n_row = len(frame.event_keys)
    assert n_row > 0
    assert len(baserunner_labels) == n_row
    assert frame.dl_logit_per_class.shape == (n_row, len(ADVANCEMENT_CLASS_LABELS))
    for design in frame.fixed_effects.values():
        assert design.codes.shape[0] == n_row
