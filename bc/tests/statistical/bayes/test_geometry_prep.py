"""Synthetic-fixture tests for ``prepare_geometry_inputs`` (Model E)."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.bayes.dl_covariate import (
    center_dl_log_probs,
    compute_dl_log_probs_per_class_with_mask,
)
from python_models.statistical.deep.targets.geometry import (
    GEOMETRY_SPECS as DEEP_GEOMETRY_SPECS,
)
from python_models.statistical.manifests import write_published_pointer
from python_models.statistical.models._geometry_data import (
    FIXED_EFFECT_COLUMNS,
    GEOMETRY_DIMENSIONS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    PRODUCTION_DL_NULL_RATE_CEILING,
    GeometryInputs,
    _assert_dl_covariate_informative_on_production,
    _handler_z_with_frozen_stats,
    _standardized_handler_logit,
    build_geometry_production_frame,
    prepare_geometry_inputs,
)
from python_models.statistical.schemas import PublishedPointer
from python_models.statistical.splits import game_hash_fold

_TRAJECTORY_SPEC_LABELS = GEOMETRY_DIMENSIONS["trajectory"].class_labels
assert _TRAJECTORY_SPEC_LABELS is not None
TRAJECTORY_LABELS: tuple[str, ...] = _TRAJECTORY_SPEC_LABELS


@pytest.fixture(autouse=True)
def published_root(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    root = tmp_path_factory.mktemp("published")
    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(root))
    return root


@pytest.fixture(autouse=True)
def deep_root(
    tmp_path_factory: pytest.TempPathFactory, monkeypatch: pytest.MonkeyPatch
) -> Path:
    root = tmp_path_factory.mktemp("deep_artifacts")
    monkeypatch.setattr(cfg, "DEEP_ROOT", root)
    return root


def _deep_target_name(dimension: str) -> str:
    for spec in DEEP_GEOMETRY_SPECS:
        if spec.published_manifest_name() == f"dl_proposal_{dimension}":
            return spec.name
    raise AssertionError(f"no deep geometry spec for dimension={dimension!r}")


def _write_dl_artifact(
    deep_root: Path,
    *,
    dimension: str,
    artifact_id: str,
    labels: list[str],
) -> Path:
    exports = deep_root / _deep_target_name(dimension) / artifact_id / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    path = exports / "class_labels.json"
    path.write_text(json.dumps({"labels": labels}), encoding="utf-8")
    return path


def _publish_dl_proposal(
    published_root: Path,
    artifacts_root: Path,
    *,
    dimension: str,
    labels: list[str],
) -> None:
    artifact_dir = artifacts_root / f"geometry_{dimension}" / "dl-aid-1"
    exports = artifact_dir / "exports"
    exports.mkdir(parents=True, exist_ok=True)
    (exports / "class_labels.json").write_text(
        json.dumps({"labels": labels}), encoding="utf-8"
    )
    _ = write_published_pointer(
        PublishedPointer(
            model_name=f"dl_proposal_{dimension}",
            artifact_id="dl-aid-1",
            published_at=datetime.now(tz=timezone.utc),
            manifest_path=artifact_dir / "manifest.json",
        ),
        root=published_root,
    )


def _observed_row(
    *,
    event_key: int,
    dimension: str,
    raw_value: str,
    game_id: str,
    season: int,
    league: str,
    dl_p_class: list[float] | None = None,
    source_family: str = "play_by_play",
    result_family: str | None = "out_in_play",
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "geometry_dimension": dimension,
        "observed_status": "observed",
        "raw_value": raw_value,
        "training_weight": 1.0,
        "dl_p_class": dl_p_class,
        "dl_artifact_id": None if dl_p_class is None else "artifact-x",
        "game_id": game_id,
        "season": season,
        "league": league,
        "source_family": source_family,
        "park_id": "ARL01",
        "scorer": "scorerA",
        "batter_hand": "R",
        "base_state_start": 0,
        "outs_start": 1,
        "result_family": result_family,
        "alignment_regime": "shift_growth_era",
    }


def _unobserved_row(
    *,
    event_key: int,
    dimension: str,
    game_id: str,
    season: int,
    league: str,
    result_family: str | None,
    dl_p_class: list[float] | None = None,
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "geometry_dimension": dimension,
        "observed_status": "unobserved",
        "raw_value": None,
        "training_weight": 0.0,
        "dl_p_class": dl_p_class,
        "dl_artifact_id": None,
        "game_id": game_id,
        "season": season,
        "league": league,
        "source_family": "play_by_play",
        "park_id": "ARL01",
        "scorer": "scorerA",
        "batter_hand": "R",
        "base_state_start": 0,
        "outs_start": 1,
        "result_family": result_family,
        "alignment_regime": "shift_growth_era",
    }


def _trajectory_dataset(
    tmp_path: Path,
    *,
    n_games: int = 24,
    events_per_game: int = 10,
    season: int = 1925,
    league: str = "AL",
    n_production_events: int = 0,
    production_result_family_rate: float = 1.0,
    with_dl: bool = False,
    dl_rate: float = 1.0,
    production_dl_rate: float = 1.0,
    seed: int = 20260525,
) -> Path:
    rng = np.random.default_rng(seed)
    k = len(TRAJECTORY_LABELS)
    weights = np.linspace(1.0, 3.0, k)
    weights = weights / weights.sum()
    rows: list[dict[str, object]] = []
    for g in range(n_games):
        for e in range(events_per_game):
            eid = 100_000 + g * events_per_game + e
            label_idx = int(rng.choice(k, p=weights))
            dl_p = None
            if with_dl and rng.random() < dl_rate:
                logits = rng.normal(size=k)
                exp = np.exp(logits - logits.max())
                dl_p = (exp / exp.sum()).tolist()
            rows.append(
                _observed_row(
                    event_key=eid,
                    dimension="trajectory",
                    raw_value=TRAJECTORY_LABELS[label_idx],
                    game_id=f"G{g:03d}",
                    season=season,
                    league=league,
                    dl_p_class=dl_p,
                )
            )
    for i in range(n_production_events):
        eid = 900_000 + i
        keep = rng.random() < production_result_family_rate
        dl_p = None
        if with_dl and rng.random() < production_dl_rate:
            logits = rng.normal(size=k)
            exp = np.exp(logits - logits.max())
            dl_p = (exp / exp.sum()).tolist()
        rows.append(
            _unobserved_row(
                event_key=eid,
                dimension="trajectory",
                game_id=f"GP{i:04d}",
                season=season,
                league=league,
                result_family="out_in_play" if keep else None,
                dl_p_class=dl_p,
            )
        )
    dataset_path = tmp_path / "geometry.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)
    return dataset_path


def test_counts_one_hot_and_k_equals_vocab(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.n_classes == len(TRAJECTORY_LABELS)
    assert inputs.class_labels == list(TRAJECTORY_LABELS)
    assert "position" not in inputs.coords
    assert inputs.counts.shape == (inputs.n_events, inputs.n_classes)
    row_sums = inputs.counts.sum(axis=1)
    assert (row_sums == 1).all()
    assert inputs.counts.min() == 0 and inputs.counts.max() == 1


def test_bunt_variants_collapse_to_bunt_class(tmp_path: Path) -> None:
    bunt_idx = list(TRAJECTORY_LABELS).index("Bunt")
    variants = ["FoulBunt", "GroundBallBunt", "LineDriveBunt", "PopUpBunt"]
    rows: list[dict[str, object]] = []
    for g in range(12):
        for e in range(10):
            eid = 200_000 + g * 10 + e
            raw = (
                variants[eid % len(variants)]
                if eid % 3 == 0
                else TRAJECTORY_LABELS[eid % len(TRAJECTORY_LABELS)]
            )
            rows.append(
                _observed_row(
                    event_key=eid,
                    dimension="trajectory",
                    raw_value=raw,
                    game_id=f"B{g:03d}",
                    season=1930,
                    league="NL",
                )
            )
    dataset_path = tmp_path / "bunts.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)

    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    df = pl.read_parquet(dataset_path)
    variant_event_keys = (
        df.filter(pl.col("raw_value").is_in(variants)).get_column("event_key").to_list()
    )
    key_to_row = {int(ek): i for i, ek in enumerate(inputs.event_keys.tolist())}
    seen = 0
    for ek in variant_event_keys:
        if int(ek) not in key_to_row:
            continue
        seen += 1
        row = inputs.counts[key_to_row[int(ek)]]
        assert int(np.argmax(row)) == bunt_idx
    assert seen > 0, "fixture should leave some bunt-variant rows in the training set"


def test_train_and_holdout_games_disjoint(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path, n_games=24, events_per_game=10)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
    )
    assert inputs.held_out.n_events > 0
    train_keys = set(inputs.event_keys.tolist())
    holdout_keys = set(inputs.held_out.event_keys.tolist())
    assert train_keys.isdisjoint(holdout_keys)

    df = pl.read_parquet(dataset_path)
    held_game_event_keys = (
        df.filter(
            pl.col("game_id").map_elements(
                lambda g: game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT)
                == HOLDOUT_FOLD_ID,
                return_dtype=pl.Boolean,
            )
        )
        .get_column("event_key")
        .to_list()
    )
    assert train_keys.isdisjoint(set(held_game_event_keys))


def test_held_out_truth_is_zero_based_class(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
    )
    held = inputs.held_out
    assert held.n_events > 0
    assert held.true_position.min() >= 0
    assert held.true_position.max() < inputs.n_classes
    assert (held.U == 1).all()
    df = pl.read_parquet(dataset_path).filter(
        pl.col("event_key").is_in(held.event_keys.tolist())
    )
    label_to_idx = {label: i for i, label in enumerate(inputs.class_labels)}
    truth_lookup = dict(
        zip(
            df.get_column("event_key").to_list(),
            [label_to_idx[v] for v in df.get_column("raw_value").to_list()],
        )
    )
    for ek, tp in zip(held.event_keys.tolist(), held.true_position.tolist()):
        assert truth_lookup[ek] == tp


def test_season_league_floor_drops_thin_cells(tmp_path: Path) -> None:
    rows: list[dict[str, object]] = []
    for g in range(20):
        for e in range(10):
            eid = 100_000 + g * 10 + e
            rows.append(
                _observed_row(
                    event_key=eid,
                    dimension="trajectory",
                    raw_value=TRAJECTORY_LABELS[eid % len(TRAJECTORY_LABELS)],
                    game_id=f"BIG{g:03d}",
                    season=1925,
                    league="AL",
                )
            )
    thin_keys: list[int] = []
    for e in range(8):
        eid = 700_000 + e
        thin_keys.append(eid)
        rows.append(
            _observed_row(
                event_key=eid,
                dimension="trajectory",
                raw_value=TRAJECTORY_LABELS[e % len(TRAJECTORY_LABELS)],
                game_id=f"THIN{e:03d}",
                season=1926,
                league="NL",
            )
        )
    dataset_path = tmp_path / "floor.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)

    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=50,
        held_out_fold_count=999,
    )
    kept = set(inputs.event_keys.tolist()) | set(inputs.held_out.event_keys.tolist())
    assert kept.isdisjoint(set(thin_keys))
    assert "1926|NL" not in inputs.coords["season_league"]
    assert "1925|AL" in inputs.coords["season_league"]


def test_coverage_guard_raises_when_fe_sparse_on_production(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=300,
        production_result_family_rate=0.0,
    )
    with pytest.raises(ValueError, match=r"production slice.*result_family"):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_coverage_guard_passes_when_fe_dense_on_production(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=300,
        production_result_family_rate=0.5,
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.n_events > 0


def test_general_location_vocab_is_freq_desc_alpha_tiebreak(tmp_path: Path) -> None:
    label_counts = {"Hole": 30, "Gap": 30, "DownLine": 10, "Alley": 30}
    rows: list[dict[str, object]] = []
    eid = 300_000
    g = 0
    for label, n in label_counts.items():
        for _ in range(n):
            rows.append(
                _observed_row(
                    event_key=eid,
                    dimension="general_location",
                    raw_value=label,
                    game_id=f"GL{g:03d}",
                    season=1940,
                    league="AL",
                )
            )
            eid += 1
            if eid % 5 == 0:
                g += 1
    dataset_path = tmp_path / "general_location.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)

    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="general_location",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.dl_active is False
    assert inputs.class_labels == ["Alley", "Gap", "Hole", "DownLine"]


def test_dl_logit_is_zero_when_dl_p_class_null(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=False)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.dl_logit_per_class.shape == (inputs.n_events, inputs.n_classes)
    assert np.allclose(inputs.dl_logit_per_class, 0.0)
    assert inputs.dl_logit_class_means.shape == (inputs.n_classes,)
    assert np.allclose(inputs.dl_logit_class_means, 0.0)


def test_held_out_dl_logit_shape_and_row_alignment(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path, n_games=24, events_per_game=10, with_dl=True
    )
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
    )
    held = inputs.held_out
    assert held.n_events > 0
    assert inputs.held_out_dl_logit_per_class.shape == (
        held.n_events,
        inputs.n_classes,
    )

    df = pl.read_parquet(dataset_path).filter(
        pl.col("geometry_dimension") == "trajectory"
    )
    p_lookup = dict(
        zip(
            df.get_column("event_key").to_list(),
            df.get_column("dl_p_class").to_list(),
        )
    )
    clip = 1e-7
    means = inputs.dl_logit_class_means.tolist()
    for ek, row in zip(
        held.event_keys.tolist(), inputs.held_out_dl_logit_per_class.tolist()
    ):
        p = p_lookup[ek]
        assert p is not None
        for class_idx in range(inputs.n_classes):
            raw = float(np.log(min(max(p[class_idx], clip), 1.0 - clip)))
            assert abs(row[class_idx] - (raw - means[class_idx])) < 1e-9


def test_held_out_dl_logit_zero_when_dl_p_class_null(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path, n_games=24, events_per_game=10, with_dl=False
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
    )
    held = inputs.held_out
    assert held.n_events > 0
    assert inputs.held_out_dl_logit_per_class.shape == (
        held.n_events,
        inputs.n_classes,
    )
    assert np.allclose(inputs.held_out_dl_logit_per_class, 0.0)
    assert np.allclose(inputs.dl_logit_class_means, 0.0)


def test_dl_logit_is_centered_to_training_class_means(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=True)
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.dl_logit_per_class.shape == (inputs.n_events, inputs.n_classes)
    assert inputs.dl_logit_class_means.shape == (inputs.n_classes,)
    assert not np.allclose(inputs.dl_logit_per_class, 0.0)
    assert np.allclose(inputs.dl_logit_per_class.mean(axis=0), 0.0, atol=1e-9)

    df = pl.read_parquet(dataset_path).filter(
        pl.col("geometry_dimension") == "trajectory"
    )
    p_lookup = dict(
        zip(
            df.get_column("event_key").to_list(),
            df.get_column("dl_p_class").to_list(),
        )
    )
    clip = 1e-7
    means = inputs.dl_logit_class_means.tolist()
    for ek, row in zip(inputs.event_keys.tolist(), inputs.dl_logit_per_class.tolist()):
        p = p_lookup[ek]
        assert p is not None
        for class_idx in range(inputs.n_classes):
            raw = float(np.log(min(max(p[class_idx], clip), 1.0 - clip)))
            assert abs(row[class_idx] - (raw - means[class_idx])) < 1e-9


def test_build_production_frame_encodes_against_training_vocab(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=200,
        production_result_family_rate=0.5,
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
    )
    production_keys = set(
        pl.scan_parquet(dataset_path)
        .filter(
            (pl.col("geometry_dimension") == "trajectory")
            & (pl.col("observed_status") != "observed")
        )
        .select("event_key")
        .unique()
        .collect()
        .get_column("event_key")
        .to_list()
    )
    assert production_keys
    frame_keys = frame.event_keys.tolist()
    assert set(frame_keys) == production_keys
    assert len(frame_keys) == len(set(frame_keys))
    assert frame_keys == sorted(frame_keys)
    assert set(frame.fixed_effects.keys()) == set(inputs.fixed_effects.keys())
    assert frame.dl_logit_per_class.shape == (frame.n_events, inputs.n_classes)
    assert np.allclose(frame.dl_logit_per_class, 0.0)
    for column, design in frame.fixed_effects.items():
        train_design = inputs.fixed_effects[column]
        assert design.levels == train_design.levels
        assert design.codes.shape[0] == frame.n_events
        n_levels = len(design.levels)
        for code in design.codes.tolist():
            assert code == -1 or 0 <= code < n_levels


def test_production_frame_dl_logit_centered_by_training_means(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=200,
        production_result_family_rate=1.0,
        with_dl=True,
    )
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert not np.allclose(inputs.dl_logit_class_means, 0.0)
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
    )
    assert frame.n_events > 0
    raw, present = _production_dl_log_probs(dataset_path, n_classes=inputs.n_classes)
    assert present.all()
    expected = raw - inputs.dl_logit_class_means[None, :]
    assert np.allclose(frame.dl_logit_per_class, expected)


def _production_dl_log_probs(
    dataset_path: Path, *, n_classes: int
) -> tuple[np.ndarray, np.ndarray]:
    per_event = (
        pl.scan_parquet(dataset_path)
        .filter(
            (pl.col("geometry_dimension") == "trajectory")
            & (pl.col("observed_status") != "observed")
        )
        .select(["event_key", "dl_p_class", *FIXED_EFFECT_COLUMNS])
        .unique(subset=["event_key"])
        .sort("event_key")
        .collect()
    )
    return compute_dl_log_probs_per_class_with_mask(per_event, n_classes=n_classes)


def _inputs_with_nonzero_dl_means(
    tmp_path: Path, deep_root: Path, *, production_dl_rate: float
) -> tuple[Path, GeometryInputs]:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=200,
        production_result_family_rate=1.0,
        with_dl=True,
        production_dl_rate=production_dl_rate,
    )
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert not np.allclose(inputs.dl_logit_class_means, 0.0)
    return dataset_path, inputs


def test_production_frame_null_dl_rows_are_exactly_zero_under_zero_flavor(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path, inputs = _inputs_with_nonzero_dl_means(
        tmp_path, deep_root, production_dl_rate=0.0
    )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        gamma_dl_active=False,
    )
    assert frame.n_events > 0
    assert np.all(frame.dl_logit_per_class == 0.0)


def test_production_frame_mixes_zero_null_rows_with_centered_present_rows(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path, inputs = _inputs_with_nonzero_dl_means(
        tmp_path, deep_root, production_dl_rate=0.99
    )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        gamma_dl_active=True,
    )
    raw, present = _production_dl_log_probs(dataset_path, n_classes=inputs.n_classes)
    assert present.any() and not present.all()
    assert np.all(frame.dl_logit_per_class[~present] == 0.0)
    expected_present = raw[present] - inputs.dl_logit_class_means[None, :]
    assert np.allclose(frame.dl_logit_per_class[present], expected_present)
    assert np.allclose(
        frame.dl_logit_per_class,
        center_dl_log_probs(raw, present, inputs.dl_logit_class_means),
    )


def test_all_null_production_dl_raises_under_active_gamma_only(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path, inputs = _inputs_with_nonzero_dl_means(
        tmp_path, deep_root, production_dl_rate=0.0
    )
    with pytest.raises(ValueError, match="gamma_dl_zero"):
        _ = build_geometry_production_frame(
            dataset_path,
            dimension="trajectory",
            fixed_effects=inputs.fixed_effects,
            n_classes=inputs.n_classes,
            dl_logit_class_means=inputs.dl_logit_class_means,
            gamma_dl_active=True,
        )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        gamma_dl_active=False,
    )
    assert frame.n_events > 0


def test_null_rate_above_ceiling_raises_under_active_gamma(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path, inputs = _inputs_with_nonzero_dl_means(
        tmp_path, deep_root, production_dl_rate=0.5
    )
    _, present = _production_dl_log_probs(dataset_path, n_classes=inputs.n_classes)
    null_rate = float((~present).sum()) / float(present.shape[0])
    assert null_rate > PRODUCTION_DL_NULL_RATE_CEILING
    with pytest.raises(ValueError, match="gamma_dl_zero"):
        _ = build_geometry_production_frame(
            dataset_path,
            dimension="trajectory",
            fixed_effects=inputs.fixed_effects,
            n_classes=inputs.n_classes,
            dl_logit_class_means=inputs.dl_logit_class_means,
            gamma_dl_active=True,
        )
    frame = build_geometry_production_frame(
        dataset_path,
        dimension="trajectory",
        fixed_effects=inputs.fixed_effects,
        n_classes=inputs.n_classes,
        dl_logit_class_means=inputs.dl_logit_class_means,
        gamma_dl_active=False,
    )
    assert frame.n_events > 0


@pytest.mark.parametrize("gamma_dl_active", [True, False])
def test_null_rate_guard_is_inert_when_gamma_is_off_or_target_is_empty(
    gamma_dl_active: bool,
) -> None:
    _assert_dl_covariate_informative_on_production(
        null_rate=1.0,
        n_production=0 if gamma_dl_active else 10,
        dimension="trajectory",
        gamma_dl_active=gamma_dl_active,
    )


@pytest.mark.parametrize("ceiling", [0.0, 0.02, PRODUCTION_DL_NULL_RATE_CEILING, 0.5])
def test_null_rate_guard_raises_just_above_and_passes_at_and_below_the_ceiling(
    ceiling: float, caplog: pytest.LogCaptureFixture
) -> None:
    epsilon = 1e-9
    logger_name = "python_models.statistical.models._geometry_data"
    with pytest.raises(ValueError, match=f"{ceiling}"):
        _assert_dl_covariate_informative_on_production(
            null_rate=ceiling + epsilon,
            n_production=10,
            dimension="trajectory",
            gamma_dl_active=True,
            max_null_rate=ceiling,
        )
    with pytest.raises(ValueError):
        _assert_dl_covariate_informative_on_production(
            null_rate=1.0,
            n_production=10,
            dimension="trajectory",
            gamma_dl_active=True,
            max_null_rate=ceiling,
        )
    for rate in (ceiling, max(ceiling - epsilon, 0.0), 0.0):
        with caplog.at_level(logging.WARNING, logger=logger_name):
            _assert_dl_covariate_informative_on_production(
                null_rate=rate,
                n_production=10,
                dimension="trajectory",
                gamma_dl_active=True,
                max_null_rate=ceiling,
            )
        warned = any("DL covariate is NULL" in r.getMessage() for r in caplog.records)
        assert warned == (rate > 0.0)
        caplog.clear()


def test_default_ceiling_is_a_fraction_strictly_between_zero_and_one() -> None:
    assert 0.0 < PRODUCTION_DL_NULL_RATE_CEILING < 1.0


def test_partial_null_production_dl_below_ceiling_warns_under_active_gamma(
    tmp_path: Path, deep_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path, inputs = _inputs_with_nonzero_dl_means(
        tmp_path, deep_root, production_dl_rate=0.99
    )
    _, present = _production_dl_log_probs(dataset_path, n_classes=inputs.n_classes)
    expected_rate = float((~present).sum()) / float(present.shape[0])
    assert 0.0 < expected_rate <= PRODUCTION_DL_NULL_RATE_CEILING
    logger_name = "python_models.statistical.models._geometry_data"
    with caplog.at_level(logging.WARNING, logger=logger_name):
        frame = build_geometry_production_frame(
            dataset_path,
            dimension="trajectory",
            fixed_effects=inputs.fixed_effects,
            n_classes=inputs.n_classes,
            dl_logit_class_means=inputs.dl_logit_class_means,
            gamma_dl_active=True,
        )
    assert frame.n_events > 0
    warnings = [
        r
        for r in caplog.records
        if r.levelno == logging.WARNING and "DL covariate is NULL" in r.getMessage()
    ]
    assert len(warnings) == 1
    assert f"{expected_rate:.4f}" in warnings[0].getMessage()


def test_dl_null_rate_logged_on_training_held_out_and_production(
    tmp_path: Path, deep_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _trajectory_dataset(
        tmp_path,
        n_production_events=50,
        with_dl=True,
        dl_rate=0.5,
        production_dl_rate=0.5,
    )
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    logger_name = "python_models.statistical.models._geometry_data"
    with caplog.at_level(logging.INFO, logger=logger_name):
        inputs = prepare_geometry_inputs(
            dataset_path, dimension="trajectory", min_events_per_season=1
        )
        assert inputs.held_out.n_events > 0
        _ = build_geometry_production_frame(
            dataset_path,
            dimension="trajectory",
            fixed_effects=inputs.fixed_effects,
            n_classes=inputs.n_classes,
            dl_logit_class_means=inputs.dl_logit_class_means,
        )
    contexts = [
        r.getMessage().split(" slice ")[0].removeprefix("DL covariate null rate on ")
        for r in caplog.records
        if r.getMessage().startswith("DL covariate null rate on ")
    ]
    assert sorted(contexts) == ["held-out", "production", "training"]


def test_training_dl_means_use_present_rows_only(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=True, dl_rate=0.5)
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    df = (
        pl.read_parquet(dataset_path)
        .filter(pl.col("event_key").is_in(inputs.event_keys.tolist()))
        .sort("event_key")
    )
    raw, present = compute_dl_log_probs_per_class_with_mask(
        df, n_classes=inputs.n_classes
    )
    assert present.any() and not present.all()
    assert np.allclose(inputs.dl_logit_class_means, raw[present].mean(axis=0))
    assert np.all(inputs.dl_logit_per_class[~present] == 0.0)
    assert np.allclose(inputs.dl_logit_per_class[present].mean(axis=0), 0.0, atol=1e-9)


def test_fixed_effect_columns_present_including_batter_hand(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert "batter_hand" in FIXED_EFFECT_COLUMNS
    for column in FIXED_EFFECT_COLUMNS:
        assert column in inputs.fixed_effects
        assert inputs.fixed_effects[column].codes.shape[0] == inputs.n_events


def test_class_labels_override_pins_vocab(tmp_path: Path) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    override = ["GroundBall", "Fly", "LineDrive", "PopUp", "Bunt"]
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
        class_labels=override,
    )
    assert inputs.class_labels == override


def test_dl_vocab_alignment_matching_passes(
    tmp_path: Path, published_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    _publish_dl_proposal(
        published_root,
        tmp_path / "deep",
        dimension="trajectory",
        labels=list(TRAJECTORY_LABELS),
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.class_labels == list(TRAJECTORY_LABELS)


def test_dl_vocab_alignment_mismatched_order_raises(
    tmp_path: Path, published_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    _publish_dl_proposal(
        published_root,
        tmp_path / "deep",
        dimension="trajectory",
        labels=list(TRAJECTORY_LABELS)[::-1],
    )
    with pytest.raises(ValueError, match="does not match the.*Bayes class vocabulary"):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_dl_vocab_alignment_missing_pointer_skips_with_warning(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _trajectory_dataset(tmp_path)
    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.models._geometry_data"
    ):
        inputs = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )
    assert inputs.n_events > 0
    assert any(
        "skipping DL class-vocab alignment" in record.getMessage()
        for record in caplog.records
    )


def test_dl_vocab_alignment_resolves_dataset_artifact_not_pointer(
    tmp_path: Path, published_root: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=True)
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS),
    )
    _publish_dl_proposal(
        published_root,
        tmp_path / "deep",
        dimension="trajectory",
        labels=list(TRAJECTORY_LABELS)[::-1],
    )
    inputs = prepare_geometry_inputs(
        dataset_path,
        dimension="trajectory",
        min_events_per_season=1,
        held_out_fold_count=999,
    )
    assert inputs.class_labels == list(TRAJECTORY_LABELS)


def test_dl_vocab_alignment_mismatch_via_dataset_artifact_raises(
    tmp_path: Path, deep_root: Path
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=True)
    _ = _write_dl_artifact(
        deep_root,
        dimension="trajectory",
        artifact_id="artifact-x",
        labels=list(TRAJECTORY_LABELS)[::-1],
    )
    with pytest.raises(
        ValueError, match=r"artifact-x.*does not match the.*Bayes class vocabulary"
    ):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_dl_vocab_alignment_missing_class_labels_raises_contextual(
    tmp_path: Path,
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=True)
    with pytest.raises(
        FileNotFoundError, match=r"artifact-x.*class_labels\.json"
    ) as excinfo:
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )
    message = str(excinfo.value)
    assert "trajectory" in message
    assert "re-run" in message or "re-freeze" in message


def test_dl_vocab_alignment_no_artifact_id_falls_back_to_pointer_with_warning(
    tmp_path: Path, published_root: Path, caplog: pytest.LogCaptureFixture
) -> None:
    dataset_path = _trajectory_dataset(tmp_path, with_dl=False)
    _publish_dl_proposal(
        published_root,
        tmp_path / "deep",
        dimension="trajectory",
        labels=list(TRAJECTORY_LABELS),
    )
    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.models._geometry_data"
    ):
        inputs = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )
    assert inputs.n_events > 0
    assert any(
        "falling back to the published" in record.getMessage()
        for record in caplog.records
    )


def test_multiple_distinct_dl_artifact_ids_raise(tmp_path: Path) -> None:
    rng = np.random.default_rng(20260609)
    k = len(TRAJECTORY_LABELS)
    rows: list[dict[str, object]] = []
    for g in range(4):
        for e in range(10):
            eid = 400_000 + g * 10 + e
            logits = rng.normal(size=k)
            exp = np.exp(logits - logits.max())
            row = _observed_row(
                event_key=eid,
                dimension="trajectory",
                raw_value=TRAJECTORY_LABELS[eid % k],
                game_id=f"M{g:03d}",
                season=1950,
                league="AL",
                dl_p_class=(exp / exp.sum()).tolist(),
            )
            row["dl_artifact_id"] = "artifact-x" if g % 2 == 0 else "artifact-y"
            rows.append(row)
    dataset_path = tmp_path / "mixed_artifacts.parquet"
    pl.DataFrame(rows).write_parquet(dataset_path)

    with pytest.raises(ValueError, match=r"distinct dl_artifact_id"):
        _ = prepare_geometry_inputs(
            dataset_path,
            dimension="trajectory",
            min_events_per_season=1,
            held_out_fold_count=999,
        )


def test_handler_logit_standardization_freezes_and_maps_null_to_zero() -> None:
    event_keys = np.array([10, 20, 30, 40], dtype=np.int64)
    logit_by_event = {10: 1.0, 20: 3.0}
    z, mean, std = _standardized_handler_logit(
        event_keys, logit_by_event, dimension="trajectory"
    )
    np.testing.assert_allclose(mean, 2.0)
    np.testing.assert_allclose(std, 1.0)
    np.testing.assert_allclose(z, np.array([-1.0, 1.0, 0.0, 0.0]))


def test_handler_logit_frozen_stats_applied_to_other_slice() -> None:
    train_keys = np.array([1, 2], dtype=np.int64)
    logit_by_event = {1: 0.0, 2: 4.0, 5: 8.0, 6: 12.0}
    _, mean, std = _standardized_handler_logit(
        train_keys, logit_by_event, dimension="trajectory"
    )
    other_keys = np.array([5, 6, 7], dtype=np.int64)
    z = _handler_z_with_frozen_stats(
        other_keys,
        logit_by_event,
        mean=mean,
        std=std,
        dimension="trajectory",
        context="held-out",
    )
    np.testing.assert_allclose(z, (np.array([8.0, 12.0, mean]) - mean) / std)
    assert z[2] == 0.0
