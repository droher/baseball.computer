"""Synthetic-fixture tests for the propensity MNAR covariate prep."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.models._ball_handler_data import (
    build_ball_handler_production_frame,
    prepare_ball_handler_inputs,
)
from python_models.statistical.models._geometry_data import (
    PROPENSITY_CLIP,
    PROPENSITY_COLUMN,
    build_geometry_production_frame,
    prepare_geometry_inputs,
)
from python_models.statistical.splits import game_hash_fold

GEOMETRY_DIMENSION = "general_location"
GEOMETRY_LABELS = ("Hole", "Gap", "Alley")
BALL_HANDLER_DIMENSION = "ball_handler_position"


@pytest.fixture(autouse=True)
def published_root(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    root = tmp_path_factory.mktemp("published")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(root))
    return root


def _logit(p: np.ndarray) -> np.ndarray:
    clipped = np.clip(p, PROPENSITY_CLIP, 1.0 - PROPENSITY_CLIP)
    return np.log(clipped / (1.0 - clipped))


def _expected_training_stats(p_train: np.ndarray) -> tuple[float, float]:
    finite = np.isfinite(p_train)
    logit = _logit(p_train[finite])
    mean = float(np.mean(logit))
    std = float(np.std(logit, ddof=0))
    if std < 1e-12:
        std = 1.0
    return mean, std


def _expected_z(p: np.ndarray, *, mean: float, std: float) -> np.ndarray:
    finite = np.isfinite(p)
    safe = np.where(finite, p, 0.5)
    return np.where(finite, (_logit(safe) - mean) / std, 0.0)


def _geometry_rows(
    *,
    with_propensity: bool,
    propensity_all_null: bool = False,
    n_games: int = 24,
    events_per_game: int = 10,
    n_production_events: int = 0,
    seed: int = 20260610,
) -> list[dict[str, object]]:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 100_000 + g * events_per_game + e
            row: dict[str, object] = {
                "event_key": eid,
                "geometry_dimension": GEOMETRY_DIMENSION,
                "observed_status": "observed",
                "raw_value": GEOMETRY_LABELS[eid % len(GEOMETRY_LABELS)],
                "training_weight": 1.0,
                "dl_p_class": None,
                "dl_artifact_id": None,
                "game_id": f"G{g:03d}",
                "season": 1925,
                "league": "AL",
                "source_family": "play_by_play",
                "park_id": "ARL01",
                "scorer": "scorerA",
                "batter_hand": "R",
                "base_state_start": 0,
                "outs_start": 1,
                "result_family": "out_in_play",
                "alignment_regime": "shift_growth_era",
            }
            if with_propensity:
                if propensity_all_null or eid % 7 == 0:
                    row[PROPENSITY_COLUMN] = None
                else:
                    row[PROPENSITY_COLUMN] = float(rng.uniform(0.55, 0.95))
            rows.append(row)
    for i in range(n_production_events):
        eid = 900_000 + i
        row = {
            "event_key": eid,
            "geometry_dimension": GEOMETRY_DIMENSION,
            "observed_status": "unobserved",
            "raw_value": None,
            "training_weight": 0.0,
            "dl_p_class": None,
            "dl_artifact_id": None,
            "game_id": f"GP{i:04d}",
            "season": 1925,
            "league": "AL",
            "source_family": "play_by_play",
            "park_id": "ARL01",
            "scorer": "scorerA",
            "batter_hand": "R",
            "base_state_start": 0,
            "outs_start": 1,
            "result_family": "out_in_play",
            "alignment_regime": "shift_growth_era",
        }
        if with_propensity:
            if propensity_all_null or i % 5 == 0:
                row[PROPENSITY_COLUMN] = None
            else:
                row[PROPENSITY_COLUMN] = float(rng.uniform(0.05, 0.45))
        rows.append(row)
    return rows


def _geometry_dataset(tmp_path: Path, **kwargs: object) -> Path:
    rows = _geometry_rows(**kwargs)  # pyright: ignore[reportArgumentType]
    dataset_path = tmp_path / "geometry.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def _p_lookup(dataset_path: Path) -> dict[int, float | None]:
    df = pl.read_parquet(dataset_path)
    return dict(
        zip(
            df.get_column("event_key").to_list(),
            df.get_column(PROPENSITY_COLUMN).to_list(),
        )
    )


def _p_array(lookup: dict[int, float | None], event_keys: list[int]) -> np.ndarray:
    return np.asarray(
        [np.nan if lookup[ek] is None else float(lookup[ek]) for ek in event_keys],
        dtype=np.float64,
    )


def test_geometry_propensity_z_matches_hand_derivation(tmp_path: Path) -> None:
    dataset_path = _geometry_dataset(tmp_path, with_propensity=True)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.propensity_active is True
    assert inputs.propensity_z is not None
    assert inputs.propensity_z.shape == (inputs.n_events,)

    lookup = _p_lookup(dataset_path)
    p_train = _p_array(lookup, inputs.event_keys.tolist())
    mean, std = _expected_training_stats(p_train)
    assert inputs.propensity_logit_mean == pytest.approx(mean)
    assert inputs.propensity_logit_std == pytest.approx(std)
    np.testing.assert_allclose(
        inputs.propensity_z, _expected_z(p_train, mean=mean, std=std), atol=1e-12
    )

    finite = np.isfinite(p_train)
    assert finite.any() and (~finite).any()
    np.testing.assert_allclose(inputs.propensity_z[~finite], 0.0)
    assert inputs.propensity_z[finite].mean() == pytest.approx(0.0, abs=1e-9)
    assert inputs.propensity_z[finite].std() == pytest.approx(1.0, abs=1e-9)


def test_geometry_all_null_training_slice_raises(tmp_path: Path) -> None:
    dataset_path = _geometry_dataset(
        tmp_path, with_propensity=True, propensity_all_null=True
    )
    with pytest.raises(ValueError, match=r"100% NULL.*training slice"):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension=GEOMETRY_DIMENSION,
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_geometry_column_absent_is_inactive_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _geometry_dataset(tmp_path, with_propensity=False)
    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.models._geometry_data"
    ):
        inputs = prepare_geometry_inputs(
            dataset_path,
            dimension=GEOMETRY_DIMENSION,
            min_events_per_season=1,
        )
    assert inputs.propensity_active is False
    assert inputs.propensity_z is not None
    assert inputs.held_out_propensity_z is not None
    np.testing.assert_allclose(inputs.propensity_z, 0.0)
    np.testing.assert_allclose(inputs.held_out_propensity_z, 0.0)
    assert inputs.held_out_propensity_z.shape == (inputs.held_out.n_events,)
    assert inputs.propensity_logit_mean == 0.0
    assert inputs.propensity_logit_std == 1.0
    assert any(
        PROPENSITY_COLUMN in record.getMessage() and "inactive" in record.getMessage()
        for record in caplog.records
    )


def test_geometry_null_rate_logged_at_info(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _geometry_dataset(tmp_path, with_propensity=True)
    with caplog.at_level(
        logging.INFO, logger="python_models.statistical.models._geometry_data"
    ):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension=GEOMETRY_DIMENSION,
            min_events_per_season=1,
            held_out_fold_count=999,
        )
    assert any(
        "propensity covariate null rate" in record.getMessage()
        for record in caplog.records
    )


def test_geometry_held_out_z_uses_frozen_training_stats(tmp_path: Path) -> None:
    dataset_path = _geometry_dataset(tmp_path, with_propensity=True, n_games=40)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        min_events_per_season=1,
    )
    held = inputs.held_out
    assert held.n_events > 0
    assert inputs.held_out_propensity_z is not None
    assert inputs.held_out_propensity_z.shape == (held.n_events,)

    lookup = _p_lookup(dataset_path)
    p_held = _p_array(lookup, held.event_keys.tolist())
    expected_frozen = _expected_z(
        p_held, mean=inputs.propensity_logit_mean, std=inputs.propensity_logit_std
    )
    np.testing.assert_allclose(
        inputs.held_out_propensity_z, expected_frozen, atol=1e-12
    )

    held_mean, held_std = _expected_training_stats(p_held)
    self_standardized = _expected_z(p_held, mean=held_mean, std=held_std)
    assert not np.allclose(inputs.held_out_propensity_z, self_standardized)


def test_geometry_production_frame_z_uses_frozen_training_stats(
    tmp_path: Path,
) -> None:
    dataset_path = _geometry_dataset(
        tmp_path, with_propensity=True, n_production_events=200
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        propensity_logit_mean=inputs.propensity_logit_mean,
        propensity_logit_std=inputs.propensity_logit_std,
        propensity_active=inputs.propensity_active,
    )
    assert frame.n_events > 0
    assert frame.propensity_z is not None
    assert frame.propensity_z.shape == (frame.n_events,)

    lookup = _p_lookup(dataset_path)
    p_production = _p_array(lookup, frame.event_keys.tolist())
    expected_frozen = _expected_z(
        p_production,
        mean=inputs.propensity_logit_mean,
        std=inputs.propensity_logit_std,
    )
    np.testing.assert_allclose(frame.propensity_z, expected_frozen, atol=1e-12)
    np.testing.assert_allclose(frame.propensity_z[~np.isfinite(p_production)], 0.0)

    production_mean, production_std = _expected_training_stats(p_production)
    self_standardized = _expected_z(
        p_production, mean=production_mean, std=production_std
    )
    assert not np.allclose(frame.propensity_z, self_standardized)


def test_geometry_production_frame_z_zero_when_inactive(tmp_path: Path) -> None:
    dataset_path = _geometry_dataset(
        tmp_path, with_propensity=False, n_production_events=50
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension=GEOMETRY_DIMENSION,
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        propensity_logit_mean=inputs.propensity_logit_mean,
        propensity_logit_std=inputs.propensity_logit_std,
        propensity_active=inputs.propensity_active,
    )
    assert frame.n_events > 0
    assert frame.propensity_z is not None
    np.testing.assert_allclose(frame.propensity_z, 0.0)


def _ball_handler_rows(
    *,
    with_propensity: bool,
    propensity_all_null: bool = False,
    n_games: int = 24,
    events_per_game: int = 10,
    n_production_events: int = 0,
    seed: int = 20260610,
) -> list[dict[str, object]]:
    rng = np.random.default_rng(seed)
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 200_000 + g * events_per_game + e
            row: dict[str, object] = {
                "event_key": eid,
                "dimension": BALL_HANDLER_DIMENSION,
                "observed_status": "observed",
                "raw_value": str(int(rng.integers(1, 10))),
                "training_weight": 1.0,
                "game_id": f"G{g:03d}",
                "season": 1925,
                "league": "AL",
                "source_family": "play_by_play",
                "park_id": "ARL01",
                "scorer": "scorerA",
                "base_state_start": 0,
                "outs_start": 1,
                "result_family": "out_in_play",
                "alignment_regime": "shift_growth_era",
            }
            if with_propensity:
                if propensity_all_null or eid % 7 == 0:
                    row[PROPENSITY_COLUMN] = None
                else:
                    row[PROPENSITY_COLUMN] = float(rng.uniform(0.55, 0.95))
            rows.append(row)
    for i in range(n_production_events):
        eid = 950_000 + i
        row = {
            "event_key": eid,
            "dimension": BALL_HANDLER_DIMENSION,
            "observed_status": "unobserved",
            "raw_value": None,
            "training_weight": 0.0,
            "game_id": f"GP{i:04d}",
            "season": 1925,
            "league": "AL",
            "source_family": "play_by_play",
            "park_id": "ARL01",
            "scorer": "scorerA",
            "base_state_start": 0,
            "outs_start": 1,
            "result_family": "out_in_play",
            "alignment_regime": "shift_growth_era",
        }
        if with_propensity:
            row[PROPENSITY_COLUMN] = (
                None if propensity_all_null else float(rng.uniform(0.05, 0.45))
            )
        rows.append(row)
    return rows


def _ball_handler_dataset(tmp_path: Path, **kwargs: object) -> Path:
    rows = _ball_handler_rows(**kwargs)  # pyright: ignore[reportArgumentType]
    dataset_path = tmp_path / "ball_handler.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_ball_handler_propensity_z_matches_hand_derivation(tmp_path: Path) -> None:
    dataset_path = _ball_handler_dataset(tmp_path, with_propensity=True)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.propensity_active is True
    assert inputs.propensity_z is not None
    assert inputs.propensity_z.shape == (inputs.n_events,)

    lookup = _p_lookup(dataset_path)
    p_train = _p_array(lookup, inputs.event_keys.tolist())
    mean, std = _expected_training_stats(p_train)
    assert inputs.propensity_logit_mean == pytest.approx(mean)
    assert inputs.propensity_logit_std == pytest.approx(std)
    np.testing.assert_allclose(
        inputs.propensity_z, _expected_z(p_train, mean=mean, std=std), atol=1e-12
    )
    finite = np.isfinite(p_train)
    assert finite.any() and (~finite).any()
    np.testing.assert_allclose(inputs.propensity_z[~finite], 0.0)


def test_ball_handler_all_null_training_slice_raises(tmp_path: Path) -> None:
    dataset_path = _ball_handler_dataset(
        tmp_path, with_propensity=True, propensity_all_null=True
    )
    with pytest.raises(ValueError, match=r"100% NULL.*training slice"):
        _ = prepare_ball_handler_inputs(
            dataset_path,
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_ball_handler_column_absent_is_inactive(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _ball_handler_dataset(tmp_path, with_propensity=False)
    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.models._ball_handler_data"
    ):
        inputs = prepare_ball_handler_inputs(
            dataset_path,
            min_events_per_season=1,
        )
    assert inputs.propensity_active is False
    assert inputs.propensity_z is not None
    assert inputs.held_out_propensity_z is not None
    np.testing.assert_allclose(inputs.propensity_z, 0.0)
    np.testing.assert_allclose(inputs.held_out_propensity_z, 0.0)
    assert inputs.held_out_propensity_z.shape == (inputs.held_out.n_events,)
    assert any(
        PROPENSITY_COLUMN in record.getMessage() and "inactive" in record.getMessage()
        for record in caplog.records
    )


def test_ball_handler_held_out_z_uses_frozen_training_stats(tmp_path: Path) -> None:
    dataset_path = _ball_handler_dataset(tmp_path, with_propensity=True, n_games=40)
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
    )
    held = inputs.held_out
    assert held.n_events > 0
    assert inputs.held_out_propensity_z is not None
    assert inputs.held_out_propensity_z.shape == (held.n_events,)

    lookup = _p_lookup(dataset_path)
    p_held = _p_array(lookup, held.event_keys.tolist())
    expected_frozen = _expected_z(
        p_held, mean=inputs.propensity_logit_mean, std=inputs.propensity_logit_std
    )
    np.testing.assert_allclose(
        inputs.held_out_propensity_z, expected_frozen, atol=1e-12
    )

    train_keys = set(inputs.event_keys.tolist())
    held_keys = set(held.event_keys.tolist())
    assert train_keys.isdisjoint(held_keys)
    for game_id in (
        pl.read_parquet(dataset_path)
        .filter(pl.col("event_key").is_in(list(held_keys)))
        .get_column("game_id")
        .unique()
        .to_list()
    ):
        assert game_hash_fold(game_id, fold_count=10) == 0


def test_ball_handler_production_frame_z_uses_frozen_training_stats(
    tmp_path: Path,
) -> None:
    dataset_path = _ball_handler_dataset(
        tmp_path, with_propensity=True, n_production_events=120
    )
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_ball_handler_production_frame(
        dataset_path,
        dimension=BALL_HANDLER_DIMENSION,
        fixed_effects=inputs.fixed_effects,
        propensity_logit_mean=inputs.propensity_logit_mean,
        propensity_logit_std=inputs.propensity_logit_std,
        propensity_active=inputs.propensity_active,
    )
    assert frame.n_events > 0
    assert frame.propensity_z is not None

    lookup = _p_lookup(dataset_path)
    p_production = _p_array(lookup, frame.event_keys.tolist())
    expected_frozen = _expected_z(
        p_production,
        mean=inputs.propensity_logit_mean,
        std=inputs.propensity_logit_std,
    )
    np.testing.assert_allclose(frame.propensity_z, expected_frozen, atol=1e-12)

    production_mean, production_std = _expected_training_stats(p_production)
    self_standardized = _expected_z(
        p_production, mean=production_mean, std=production_std
    )
    assert not np.allclose(frame.propensity_z, self_standardized)


def test_ball_handler_production_frame_z_zero_when_inactive(tmp_path: Path) -> None:
    dataset_path = _ball_handler_dataset(
        tmp_path, with_propensity=False, n_production_events=50
    )
    inputs = prepare_ball_handler_inputs(
        dataset_path,
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_ball_handler_production_frame(
        dataset_path,
        dimension=BALL_HANDLER_DIMENSION,
        fixed_effects=inputs.fixed_effects,
        propensity_logit_mean=inputs.propensity_logit_mean,
        propensity_logit_std=inputs.propensity_logit_std,
        propensity_active=inputs.propensity_active,
    )
    assert frame.n_events > 0
    assert frame.propensity_z is not None
    np.testing.assert_allclose(frame.propensity_z, 0.0)
