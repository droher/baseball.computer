"""Synthetic-fixture tests for the fielding-responsibility prep (Model I)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl

from python_models.statistical.models._geometry_data import (
    GeometryInputs,
    GeometryProductionFrame,
)
from python_models.statistical.models._responsibility_data import (
    BUNT_TRAJECTORY_CLASSES,
    DIMENSION,
    FIXED_EFFECT_COLUMNS,
    LABEL_COLUMN,
    MAX_POSITION,
    MIN_POSITION,
    RESPONSIBILITY_POSITION_LABELS,
    build_responsibility_production_frame,
    prepare_responsibility_inputs,
)
from python_models.statistical.splits import game_hash_fold

SEASON = 2023
LEAGUE = "AL"
HOLDOUT_FOLD_COUNT = 10
HOLDOUT_FOLD_ID = 0
MIN_EVENTS = 2


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
    ball_handler_position: int | None,
    training_weight: float,
    trajectory_class: str,
    game_id: str,
) -> dict[str, object]:
    fe_extra = {
        "trajectory_class": trajectory_class,
        "location_side_class": "Center",
        "location_depth_class": "Shallow",
        "location_edge_class": "Infield",
        "base_state_start": "100",
        "outs_start": 1,
        "result_family": "single",
        "alignment_regime": "standard",
        "batter_hand": "R",
    }
    return {
        "event_key": event_key,
        LABEL_COLUMN: ball_handler_position,
        "training_weight": training_weight,
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

    valid_positions = list(range(MIN_POSITION, MAX_POSITION + 1))
    rows: list[dict[str, object]] = []
    ek = 1

    for n in range(MIN_EVENTS + 6):
        game = train_game if n % 2 == 0 else train_game_2
        rows.append(
            _row(
                event_key=ek,
                ball_handler_position=valid_positions[n % len(valid_positions)],
                training_weight=1.0,
                trajectory_class="GroundBall",
                game_id=game,
            )
        )
        ek += 1

    for position in (1, 2):
        rows.append(
            _row(
                event_key=ek,
                ball_handler_position=position,
                training_weight=1.0,
                trajectory_class="GroundBall",
                game_id=train_game,
            )
        )
        ek += 1

    rows.append(
        _row(
            event_key=ek,
            ball_handler_position=None,
            training_weight=1.0,
            trajectory_class="GroundBall",
            game_id=train_game,
        )
    )
    ek += 1

    rows.append(
        _row(
            event_key=ek,
            ball_handler_position=6,
            training_weight=0.0,
            trajectory_class="GroundBall",
            game_id=train_game,
        )
    )
    ek += 1

    rows.append(
        _row(
            event_key=ek,
            ball_handler_position=5,
            training_weight=1.0,
            trajectory_class=BUNT_TRAJECTORY_CLASSES[0],
            game_id=train_game,
        )
    )
    ek += 1

    for n in range(MIN_EVENTS + 1):
        rows.append(
            _row(
                event_key=ek,
                ball_handler_position=valid_positions[n % len(valid_positions)],
                training_weight=1.0,
                trajectory_class="FlyBall",
                game_id=holdout_game,
            )
        )
        ek += 1

    dataset_path = tmp_path / "responsibility.parquet"
    pl.DataFrame(
        rows,
        schema={
            "event_key": pl.Int64,
            LABEL_COLUMN: pl.Int64,
            "training_weight": pl.Float64,
            "season": pl.Int64,
            "league": pl.Utf8,
            "game_id": pl.Utf8,
            "scorer": pl.Utf8,
            "park_id": pl.Utf8,
            "source_family": pl.Utf8,
            "trajectory_class": pl.Utf8,
            "location_side_class": pl.Utf8,
            "location_depth_class": pl.Utf8,
            "location_edge_class": pl.Utf8,
            "base_state_start": pl.Utf8,
            "outs_start": pl.Int64,
            "result_family": pl.Utf8,
            "alignment_regime": pl.Utf8,
            "batter_hand": pl.Utf8,
        },
    ).write_parquet(dataset_path)
    return dataset_path, train_game, holdout_game


def test_returns_geometry_inputs_with_locked_vocabulary(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

    assert isinstance(inputs, GeometryInputs)
    assert inputs.n_classes == len(RESPONSIBILITY_POSITION_LABELS)
    assert inputs.class_labels == list(RESPONSIBILITY_POSITION_LABELS)
    assert inputs.class_labels == [
        str(p) for p in range(MIN_POSITION, MAX_POSITION + 1)
    ]
    assert inputs.dimension == DIMENSION
    assert inputs.dl_active is False


def test_counts_are_one_hot_over_position_classes(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

    assert inputs.counts.shape[1] == len(RESPONSIBILITY_POSITION_LABELS)
    row_sums = inputs.counts.sum(axis=1)
    assert (row_sums == 1).all()
    assert int(inputs.counts.sum()) == inputs.n_events


def test_invalid_rows_dropped(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

    valid_range = pl.col(LABEL_COLUMN).is_between(MIN_POSITION, MAX_POSITION)
    clean = raw.filter(
        (pl.col("training_weight") > 0.0)
        & pl.col(LABEL_COLUMN).is_not_null()
        & valid_range
        & ~pl.col("trajectory_class").is_in(list(BUNT_TRAJECTORY_CLASSES))
    )
    assert clean.height < raw.height

    kept_keys = set(inputs.event_keys.tolist()) | set(
        inputs.held_out.event_keys.tolist()
    )
    assert kept_keys == set(clean.get_column("event_key").to_list())

    dropped = raw.filter(~pl.col("event_key").is_in(list(kept_keys)))
    assert dropped.height > 0
    has_out_of_range = (
        dropped.filter(~pl.col(LABEL_COLUMN).is_between(MIN_POSITION, MAX_POSITION)).height
        > 0
    )
    has_null = dropped.filter(pl.col(LABEL_COLUMN).is_null()).height > 0
    has_zero_weight = dropped.filter(pl.col("training_weight") == 0.0).height > 0
    has_bunt = (
        dropped.filter(
            pl.col("trajectory_class").is_in(list(BUNT_TRAJECTORY_CLASSES))
        ).height
        > 0
    )
    assert has_out_of_range
    assert has_null
    assert has_zero_weight
    assert has_bunt


def test_fixed_effects_present_in_fixture_appear_in_inputs(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

    fixture_fe = {c for c in FIXED_EFFECT_COLUMNS if c in raw.columns}
    assert fixture_fe
    assert set(inputs.fixed_effects.keys()) == fixture_fe
    for column, design in inputs.fixed_effects.items():
        assert design.codes.shape[0] == inputs.n_events
        assert f"{column}_levels" in inputs.coords


def test_held_out_and_train_event_keys_disjoint(tmp_path: Path) -> None:
    dataset_path, _train, holdout_game = _write_dataset(tmp_path)
    raw = pl.read_parquet(dataset_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

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


def test_production_frame_grain_and_shape(tmp_path: Path) -> None:
    dataset_path, _train, _holdout = _write_dataset(tmp_path)
    inputs = prepare_responsibility_inputs(
        dataset_path, min_events_per_season=MIN_EVENTS
    )

    frame = build_responsibility_production_frame(
        dataset_path,
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
    )

    assert isinstance(frame, GeometryProductionFrame)
    n_event = len(frame.event_keys)
    assert n_event > 0
    assert frame.dl_logit_per_class.shape == (
        n_event,
        len(RESPONSIBILITY_POSITION_LABELS),
    )
    assert len(set(frame.event_keys.tolist())) == n_event
    for design in frame.fixed_effects.values():
        assert design.codes.shape[0] == n_event
