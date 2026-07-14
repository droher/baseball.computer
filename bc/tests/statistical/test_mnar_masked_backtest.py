"""Tests for the MNAR masked-backtest harness.

Fast tier: mask determinism / calibration / holdout protection, recovered
share parsing, and the metrics pass logic on hand-built inputs. Slow
tier: the library end to end at smoke scale on a synthetic geometry
universe — asserts output structure and that the mask moved the class
shares in the intended direction, never that the corrected fit wins
(sampling at smoke scale is too noisy for that).
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import hashlib
import json
import math
from collections.abc import Mapping
from pathlib import Path
from typing import TypedDict, cast

import numpy as np
import polars as pl
import pytest
from pydantic import ValidationError

from python_models.statistical import config as cfg
from python_models.statistical.backtests.mnar_masked import (
    OVERALL_MASKED_RANGE,
    SELECTION_OFFSET_CLIP,
    TRUE_LABEL_COLUMN,
    VARIANTS,
    MaskConfig,
    assert_export_covers_masked_slice,
    calibrate_class_mask_probabilities,
    evaluate_criteria,
    evaluate_offset_arm,
    generate_mask,
    oracle_selection_offset,
    realized_class_selection_offset,
    recovered_class_shares,
    reweight_class_shares_per_event,
    run_backtest,
)
from python_models.statistical.models._geometry_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
)
from python_models.statistical.splits import game_hash_fold

GEOMETRY_VARIANT = VARIANTS["geometry"]
TRAJECTORY_LABELS = GEOMETRY_VARIANT.class_labels
LABEL_WEIGHTS = (0.18, 0.47, 0.20, 0.10, 0.05)


def _game_id_pool(*, prefix: str, n_holdout: int, n_train: int) -> list[str]:
    holdout: list[str] = []
    train: list[str] = []
    i = 0
    while len(holdout) < n_holdout or len(train) < n_train:
        gid = f"{prefix}{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            if len(holdout) < n_holdout:
                holdout.append(gid)
        elif len(train) < n_train:
            train.append(gid)
        i += 1
    return holdout + train


def _mask_universe(*, n_rows: int = 6000, seed: int = 7) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    games = _game_id_pool(prefix="GAME", n_holdout=5, n_train=45)
    labels = rng.choice(list(TRAJECTORY_LABELS), size=n_rows, p=LABEL_WEIGHTS)
    return pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.int64),
            TRUE_LABEL_COLUMN: [str(v) for v in labels],
            "game_id": [games[i % len(games)] for i in range(n_rows)],
        }
    )


def test_generate_mask_deterministic_per_seed() -> None:
    universe = _mask_universe()
    first = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=11)
    second = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=11)
    np.testing.assert_array_equal(first.masked, second.masked)
    np.testing.assert_array_equal(first.holdout, second.holdout)
    assert first.summary == second.summary

    other = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=12)
    assert (first.masked != other.masked).any()


def test_generate_mask_hits_target_ranges() -> None:
    universe = _mask_universe()
    result = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=3)
    summary = result.summary

    assert (
        OVERALL_MASKED_RANGE[0]
        <= summary["overall_masked_share"]
        <= OVERALL_MASKED_RANGE[1]
    )
    assert summary["overall_masked_in_range"] is True
    focal_range = GEOMETRY_VARIANT.observed_focal_range
    assert focal_range is not None
    assert focal_range[0] <= summary["observed_focal_share"] <= focal_range[1]
    assert summary["observed_focal_in_range"] is True
    assert (
        summary["observed_focal_share"]
        < summary["truth_shares_maskable"][GEOMETRY_VARIANT.focal_class]
    )

    probabilities = summary["class_mask_probabilities"]
    focal_p = probabilities[GEOMETRY_VARIANT.focal_class]
    for label in TRAJECTORY_LABELS:
        if label != GEOMETRY_VARIANT.focal_class:
            assert probabilities[label] < focal_p
    masked_by_class = summary["masked_share_by_class"]
    for label in TRAJECTORY_LABELS:
        if label != GEOMETRY_VARIANT.focal_class:
            assert (
                masked_by_class[label] < masked_by_class[GEOMETRY_VARIANT.focal_class]
            )
    assert set(summary["masked_share_by_bucket"]) == {
        f"tier_{t:.2f}" for t in MaskConfig().intensity_tiers
    }


def test_generate_mask_never_masks_holdout_fold() -> None:
    universe = _mask_universe()
    result = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=5)
    expected_holdout = np.array(
        [
            game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
            for g in universe.get_column("game_id").to_list()
        ],
        dtype=np.bool_,
    )
    np.testing.assert_array_equal(result.holdout, expected_holdout)
    assert expected_holdout.any()
    assert not result.masked[expected_holdout].any()


def test_calibrate_infeasible_focal_share_raises() -> None:
    config = MaskConfig()
    all_other = np.asarray(["Fly"] * 100, dtype=np.str_)
    with pytest.raises(ValueError, match="zero share"):
        _ = calibrate_class_mask_probabilities(
            all_other,
            focal_class="GroundBall",
            class_labels=TRAJECTORY_LABELS,
            config=config,
        )
    all_focal = np.asarray(["GroundBall"] * 100, dtype=np.str_)
    with pytest.raises(ValueError, match="entire maskable universe"):
        _ = calibrate_class_mask_probabilities(
            all_focal,
            focal_class="GroundBall",
            class_labels=TRAJECTORY_LABELS,
            config=config,
        )


def _shares(values: dict[str, float]) -> dict[str, float]:
    total = sum(values.values())
    assert abs(total - 1.0) < 1e-9
    return values


TRUTH_SHARES = _shares(
    {"Fly": 0.18, "GroundBall": 0.47, "LineDrive": 0.20, "PopUp": 0.10, "Bunt": 0.05}
)
CLOSE_SHARES = _shares(
    {"Fly": 0.19, "GroundBall": 0.45, "LineDrive": 0.21, "PopUp": 0.10, "Bunt": 0.05}
)
BIASED_SHARES = _shares(
    {"Fly": 0.24, "GroundBall": 0.35, "LineDrive": 0.25, "PopUp": 0.11, "Bunt": 0.05}
)
CLEAN_DIAGNOSTICS = {"divergences": 0, "rhat_max": 1.01, "ess_bulk_min": 480.0}
HELD_OUT_GOOD = {"top1_accuracy": 0.610, "log_loss": 1.020}
HELD_OUT_BASE = {"top1_accuracy": 0.612, "log_loss": 1.015}


class _FocalShareError(TypedDict):
    corrected: float
    uncorrected: float
    relative_reduction: float | None


class _MaskedSlice(TypedDict):
    focal_class: str
    class_labels: list[str]
    truth_shares: dict[str, float]
    corrected_shares: dict[str, float]
    uncorrected_shares: dict[str, float]
    focal_share_error: _FocalShareError
    total_variation: dict[str, float]


class _CriteriaMetrics(TypedDict):
    masked_slice: _MaskedSlice
    held_out: dict[str, dict[str, float | None]]
    diagnostics: dict[str, dict[str, object]]
    thresholds: dict[str, float]
    criteria: dict[str, bool]
    overall_pass: bool


def _evaluate(
    *,
    corrected_shares: dict[str, float] = CLOSE_SHARES,
    uncorrected_shares: dict[str, float] = BIASED_SHARES,
    corrected_held_out: Mapping[str, object] | None = None,
    uncorrected_held_out: Mapping[str, object] | None = None,
    corrected_diagnostics: Mapping[str, object] | None = None,
    uncorrected_diagnostics: Mapping[str, object] | None = None,
) -> _CriteriaMetrics:
    metrics = evaluate_criteria(
        truth_shares=TRUTH_SHARES,
        corrected_shares=corrected_shares,
        uncorrected_shares=uncorrected_shares,
        class_labels=TRAJECTORY_LABELS,
        focal_class="GroundBall",
        corrected_held_out=dict(
            corrected_held_out if corrected_held_out is not None else HELD_OUT_GOOD
        ),
        uncorrected_held_out=dict(
            uncorrected_held_out if uncorrected_held_out is not None else HELD_OUT_BASE
        ),
        corrected_diagnostics=dict(
            corrected_diagnostics
            if corrected_diagnostics is not None
            else CLEAN_DIAGNOSTICS
        ),
        uncorrected_diagnostics=dict(
            uncorrected_diagnostics
            if uncorrected_diagnostics is not None
            else CLEAN_DIAGNOSTICS
        ),
    )
    return cast("_CriteriaMetrics", cast(object, metrics))


def test_evaluate_criteria_pass_when_corrected_beats_uncorrected() -> None:
    metrics = _evaluate()
    criteria = metrics["criteria"]
    assert criteria == {
        "focal_share_error_reduced": True,
        "total_variation_reduced": True,
        "held_out_non_regression": True,
        "sampler_diagnostics_clean": True,
    }
    assert metrics["overall_pass"] is True
    focal = metrics["masked_slice"]["focal_share_error"]
    assert focal["corrected"] == pytest.approx(0.02)
    assert focal["uncorrected"] == pytest.approx(0.12)
    assert focal["relative_reduction"] == pytest.approx(1.0 - 0.02 / 0.12)


def test_evaluate_criteria_fails_when_reversed() -> None:
    metrics = _evaluate(corrected_shares=BIASED_SHARES, uncorrected_shares=CLOSE_SHARES)
    criteria = metrics["criteria"]
    assert criteria["focal_share_error_reduced"] is False
    assert criteria["total_variation_reduced"] is False
    assert metrics["overall_pass"] is False


def test_evaluate_criteria_requires_relative_reduction_floor() -> None:
    barely_better = _shares(
        {
            "Fly": 0.235,
            "GroundBall": 0.36,
            "LineDrive": 0.245,
            "PopUp": 0.11,
            "Bunt": 0.05,
        }
    )
    metrics = _evaluate(corrected_shares=barely_better)
    relative_reduction = metrics["masked_slice"]["focal_share_error"][
        "relative_reduction"
    ]
    assert relative_reduction is not None
    assert relative_reduction < 0.25
    assert metrics["criteria"]["focal_share_error_reduced"] is False
    assert metrics["overall_pass"] is False


def test_evaluate_criteria_fails_on_held_out_regression() -> None:
    metrics = _evaluate(
        corrected_held_out={"top1_accuracy": 0.58, "log_loss": 1.20},
        uncorrected_held_out={"top1_accuracy": 0.612, "log_loss": 1.015},
    )
    assert metrics["criteria"]["held_out_non_regression"] is False
    assert metrics["overall_pass"] is False

    missing = _evaluate(corrected_held_out={"n_events": 0})
    assert missing["criteria"]["held_out_non_regression"] is False


def test_evaluate_criteria_fails_on_dirty_diagnostics() -> None:
    for dirty in (
        {"divergences": 3, "rhat_max": 1.01, "ess_bulk_min": 480.0},
        {"divergences": 0, "rhat_max": 1.20, "ess_bulk_min": 480.0},
        {"divergences": 0, "rhat_max": 1.01, "ess_bulk_min": 12.0},
        {"divergences": 0, "rhat_max": float("nan"), "ess_bulk_min": 480.0},
        {},
    ):
        metrics = _evaluate(corrected_diagnostics=dirty)
        assert metrics["criteria"]["sampler_diagnostics_clean"] is False
        assert metrics["overall_pass"] is False


def test_evaluate_criteria_schema() -> None:
    metrics = _evaluate()
    assert set(metrics) == {
        "masked_slice",
        "held_out",
        "diagnostics",
        "thresholds",
        "criteria",
        "overall_pass",
    }
    masked_slice = metrics["masked_slice"]
    assert set(masked_slice["truth_shares"]) == set(TRAJECTORY_LABELS)
    assert set(masked_slice["corrected_shares"]) == set(TRAJECTORY_LABELS)
    assert set(masked_slice["uncorrected_shares"]) == set(TRAJECTORY_LABELS)
    payload = json.dumps(metrics)
    assert json.loads(payload)["overall_pass"] is True


def test_recovered_class_shares_geometry_and_ball_handler() -> None:
    geometry_export = pl.DataFrame(
        {
            "event_key": [1] * 5 + [2] * 5,
            "class_label": list(TRAJECTORY_LABELS) * 2,
            "expected_share": [0.1, 0.5, 0.2, 0.1, 0.1, 0.3, 0.3, 0.2, 0.1, 0.1],
        }
    )
    shares = recovered_class_shares(geometry_export, variant=GEOMETRY_VARIANT)
    assert shares["GroundBall"] == pytest.approx(0.4)
    assert sum(shares.values()) == pytest.approx(1.0)

    bh_variant = VARIANTS["ball_handler"]
    positions = list(range(1, 10))
    bh_export = pl.DataFrame(
        {
            "event_key": [10] * 9,
            "fielding_position": positions,
            "expected_share": [1.0 / 9] * 9,
        }
    )
    bh_shares = recovered_class_shares(bh_export, variant=bh_variant)
    assert bh_shares["6"] == pytest.approx(1.0 / 9)
    assert sum(bh_shares.values()) == pytest.approx(1.0)


def test_oracle_selection_offset_is_logit_of_class_mask_prob() -> None:
    config = MaskConfig()
    w_class = {label: 0.0 for label in TRAJECTORY_LABELS}
    w_class["GroundBall"] = 0.9
    w_class["Fly"] = 0.5
    mean_intensity = float(np.mean(config.intensity_tiers))
    offset = oracle_selection_offset(
        class_mask_probabilities=w_class,
        mean_intensity=mean_intensity,
        class_labels=TRAJECTORY_LABELS,
        max_mask_probability=config.max_mask_probability,
    )
    for label, w in w_class.items():
        p = float(np.clip(w * mean_intensity, 1e-6, config.max_mask_probability))
        assert offset[label] == pytest.approx(float(np.log(p / (1.0 - p))))
    assert offset["GroundBall"] > offset["Fly"]


def test_reweight_zero_offset_matches_recovered_shares() -> None:
    geometry_export = pl.DataFrame(
        {
            "event_key": [1] * 5 + [2] * 5,
            "class_label": list(TRAJECTORY_LABELS) * 2,
            "expected_share": [0.1, 0.5, 0.2, 0.1, 0.1, 0.3, 0.3, 0.2, 0.1, 0.1],
        }
    )
    zero_offset = {label: 0.0 for label in TRAJECTORY_LABELS}
    reweighted = reweight_class_shares_per_event(
        geometry_export, variant=GEOMETRY_VARIANT, offset=zero_offset
    )
    baseline = recovered_class_shares(geometry_export, variant=GEOMETRY_VARIANT)
    for label in TRAJECTORY_LABELS:
        assert reweighted[label] == pytest.approx(baseline[label])


def test_reweight_matches_hand_rolled_per_event_softmax_reweight() -> None:
    shares_e1 = [0.1, 0.5, 0.2, 0.1, 0.1]
    shares_e2 = [0.3, 0.3, 0.2, 0.1, 0.1]
    geometry_export = pl.DataFrame(
        {
            "event_key": [1] * 5 + [2] * 5,
            "class_label": list(TRAJECTORY_LABELS) * 2,
            "expected_share": shares_e1 + shares_e2,
        }
    )
    offset = {label: float(0.5 * (i - 2)) for i, label in enumerate(TRAJECTORY_LABELS)}
    weight = np.exp(np.array([offset[label] for label in TRAJECTORY_LABELS]))

    def corrected(shares: list[float]) -> np.ndarray:
        num = np.array(shares) * weight
        return num / num.sum()

    expected_per_event = np.vstack([corrected(shares_e1), corrected(shares_e2)]).mean(
        axis=0
    )

    got = reweight_class_shares_per_event(
        geometry_export, variant=GEOMETRY_VARIANT, offset=offset
    )
    for i, label in enumerate(TRAJECTORY_LABELS):
        assert got[label] == pytest.approx(float(expected_per_event[i]))
    assert sum(got.values()) == pytest.approx(1.0)


def test_evaluate_offset_arm_gates_on_focal_reduction() -> None:
    truth = {label: 0.0 for label in TRAJECTORY_LABELS}
    truth["GroundBall"] = 0.68
    truth["Fly"] = 0.32
    uncorrected = {label: 0.0 for label in TRAJECTORY_LABELS}
    uncorrected["GroundBall"] = 0.34
    uncorrected["Fly"] = 0.66
    offset_shares = {label: 0.0 for label in TRAJECTORY_LABELS}
    offset_shares["GroundBall"] = 0.66
    offset_shares["Fly"] = 0.34
    clean = {"divergences": 0, "rhat_max": 1.0, "ess_bulk_min": 500.0}
    result = evaluate_offset_arm(
        truth_shares=truth,
        offset_shares=offset_shares,
        uncorrected_shares=uncorrected,
        class_labels=TRAJECTORY_LABELS,
        focal_class="GroundBall",
        uncorrected_held_out={"top1_accuracy": 0.5, "log_loss": 1.0},
        uncorrected_diagnostics=clean,
        selection_offset={label: 0.0 for label in TRAJECTORY_LABELS},
    )
    assert result["pass"] is True
    assert result["criteria"]["focal_share_error_reduced"] is True
    assert result["focal_share_error"]["relative_reduction"] == pytest.approx(
        (0.34 - 0.02) / 0.34
    )


def test_assert_export_covers_masked_slice() -> None:
    export = pl.DataFrame({"event_key": [1, 2, 3], "expected_share": [1.0] * 3})
    assert_export_covers_masked_slice(export, masked_event_keys={1, 2, 3})
    with pytest.raises(AssertionError, match="does not match"):
        assert_export_covers_masked_slice(export, masked_event_keys={1, 2})


def test_generate_mask_default_design_regression_snapshot() -> None:
    universe = _mask_universe()
    result = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=3)

    assert int(result.masked.sum()) == 3679
    assert (
        hashlib.sha256(result.masked.tobytes()).hexdigest()
        == "517e3a7e177fc776e334f490d5a1a5d4bade39eed0781a4b6556d7c435369c29"
    )
    assert (
        hashlib.sha256(result.holdout.tobytes()).hexdigest()
        == "b655db1b915ca7fea7c103dd2fa3a9c1f8acceb76972ffb4f59eb1cbeeea4601"
    )
    assert result.summary["overall_masked_share"] == pytest.approx(0.6812962962962963)
    assert result.summary["observed_focal_share"] == pytest.approx(0.3957001743172574)
    assert result.summary["masked_share_by_class"] == pytest.approx(
        {
            "Fly": 0.6228338430173292,
            "GroundBall": 0.7321007081038552,
            "LineDrive": 0.643796992481203,
            "PopUp": 0.6340579710144928,
            "Bunt": 0.6590038314176245,
        }
    )
    assert result.summary["class_mask_probabilities"] == pytest.approx(
        {
            "Fly": 0.6519706088173546,
            "GroundBall": 0.754,
            "LineDrive": 0.6519706088173546,
            "PopUp": 0.6519706088173546,
            "Bunt": 0.6519706088173546,
        }
    )
    assert result.summary["design"] == "w_class_intensity"


def _mask_universe_with_covariate(
    *, n_rows: int = 6000, seed: int = 7, hand_seed: int = 99
) -> pl.DataFrame:
    universe = _mask_universe(n_rows=n_rows, seed=seed)
    hand_rng = np.random.default_rng(hand_seed)
    hands = hand_rng.choice(["L", "R"], size=n_rows)
    return universe.with_columns(pl.Series("batter_hand", hands))


def test_covariate_joint_design_interacts_with_class() -> None:
    universe = _mask_universe_with_covariate(n_rows=6000, seed=7, hand_seed=99)
    config = MaskConfig(design="covariate_joint")
    result = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=3, config=config)

    assert result.summary["design"] == "covariate_joint"
    assert result.summary["covariate_column"] == "batter_hand"
    assert "masked_share_by_covariate_level" in result.summary

    labels = np.asarray(universe.get_column(TRUE_LABEL_COLUMN).to_list(), dtype=np.str_)
    hands = np.asarray(universe.get_column("batter_hand").to_list(), dtype=np.str_)
    maskable = ~result.holdout
    distinct = sorted(set(hands.tolist()))
    assert distinct == ["L", "R"]
    bucket_by_value = {value: rank % 2 for rank, value in enumerate(distinct)}
    assert bucket_by_value["L"] != bucket_by_value["R"]
    bucket = np.asarray([bucket_by_value[h] for h in hands], dtype=np.int64)

    focal = GEOMETRY_VARIANT.focal_class
    class_mask = (labels == focal) & maskable
    assert int((class_mask & (bucket == 0)).sum()) > 0
    assert int((class_mask & (bucket == 1)).sum()) > 0
    rate_bucket0 = float(np.mean(result.masked[class_mask & (bucket == 0)]))
    rate_bucket1 = float(np.mean(result.masked[class_mask & (bucket == 1)]))
    assert abs(rate_bucket0 - rate_bucket1) > 0.05


def test_covariate_joint_column_fallback() -> None:
    universe = _mask_universe(n_rows=2000, seed=11).with_columns(
        pl.Series("park_id", ["PARK_A", "PARK_B"] * 1000)
    )
    result = generate_mask(
        universe,
        variant=GEOMETRY_VARIANT,
        seed=3,
        config=MaskConfig(design="covariate_joint"),
    )
    assert result.summary["covariate_column"] == "park_id"

    universe_missing = _mask_universe(n_rows=500, seed=13)
    with pytest.raises(ValueError, match="covariate_joint design requires"):
        _ = generate_mask(
            universe_missing,
            variant=GEOMETRY_VARIANT,
            seed=3,
            config=MaskConfig(design="covariate_joint"),
        )


def test_scorer_blocked_masks_whole_games_class_independent() -> None:
    games = _game_id_pool(prefix="BLK", n_holdout=5, n_train=200)
    rng = np.random.default_rng(42)
    n_rows = 6000
    labels = rng.choice(list(TRAJECTORY_LABELS), size=n_rows, p=LABEL_WEIGHTS)
    universe = pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.int64),
            TRUE_LABEL_COLUMN: [str(v) for v in labels],
            "game_id": [games[i % len(games)] for i in range(n_rows)],
        }
    )
    config = MaskConfig(design="scorer_blocked")
    result = generate_mask(universe, variant=GEOMETRY_VARIANT, seed=3, config=config)

    assert result.summary["class_mask_probabilities"] == {
        label: config.block_probability for label in TRAJECTORY_LABELS
    }

    block_share = float(
        np.clip(config.overall_masked_target / config.block_probability, 0.0, 1.0)
    )
    threshold = int(round(block_share * config.block_bucket_count))
    game_ids = universe.get_column("game_id").to_list()
    blocked_games = {
        g
        for g in set(game_ids)
        if game_hash_fold(
            f"{config.block_salt}|{g}", fold_count=config.block_bucket_count
        )
        < threshold
    }

    maskable = ~result.holdout
    for i, g in enumerate(game_ids):
        if maskable[i] and g not in blocked_games:
            assert not result.masked[i]

    blocked_mask_idx = np.asarray(
        [maskable[i] and game_ids[i] in blocked_games for i in range(n_rows)],
        dtype=np.bool_,
    )
    assert int(blocked_mask_idx.sum()) > 0
    mean_masked_rate_blocked = float(np.mean(result.masked[blocked_mask_idx]))
    assert abs(mean_masked_rate_blocked - config.block_probability) < 0.1


ERA_BANDS: tuple[tuple[int, int], ...] = ((1900, 1920), (1955, 1975), (2000, 2020))


def _era_graded_universe(*, rows_per_band: int = 1600, seed: int = 5) -> pl.DataFrame:
    rng = np.random.default_rng(seed)
    games = _game_id_pool(prefix="ERA", n_holdout=5, n_train=60)
    seasons: list[int] = []
    labels: list[str] = []
    for lo, hi in ERA_BANDS:
        seasons.extend(int(s) for s in rng.integers(lo, hi + 1, size=rows_per_band))
        labels.extend(
            str(v)
            for v in rng.choice(
                list(TRAJECTORY_LABELS), size=rows_per_band, p=LABEL_WEIGHTS
            )
        )
    n_rows = len(seasons)
    return pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.int64),
            TRUE_LABEL_COLUMN: labels,
            "game_id": [games[i % len(games)] for i in range(n_rows)],
            "season": seasons,
        }
    )


def test_era_graded_design_is_monotone_by_band() -> None:
    universe = _era_graded_universe()
    result = generate_mask(
        universe,
        variant=GEOMETRY_VARIANT,
        seed=3,
        config=MaskConfig(design="era_graded"),
    )
    assert result.summary["design"] == "era_graded"

    seasons = np.asarray(universe.get_column("season").to_list(), dtype=np.int64)
    maskable = ~result.holdout
    rates = [
        float(np.mean(result.masked[maskable & (seasons >= lo) & (seasons <= hi)]))
        for lo, hi in ERA_BANDS
    ]
    assert rates[0] - rates[1] > 0.02
    assert rates[1] - rates[2] > 0.02


def test_realized_class_selection_offset_matches_logit_formula() -> None:
    masked_share_by_class = {
        "Fly": 0.6,
        "GroundBall": 0.95,
        "LineDrive": 0.3,
        "PopUp": 0.05,
        "Bunt": 0.999,
    }
    for max_mask_probability in (0.98, 0.5):
        offset = realized_class_selection_offset(
            masked_share_by_class,
            class_labels=TRAJECTORY_LABELS,
            max_mask_probability=max_mask_probability,
        )
        for label in TRAJECTORY_LABELS:
            rate = masked_share_by_class[label]
            p = float(np.clip(rate, SELECTION_OFFSET_CLIP, max_mask_probability))
            assert offset[label] == pytest.approx(math.log(p / (1.0 - p)))


def test_mask_config_design_default_and_validation() -> None:
    assert MaskConfig().design == "w_class_intensity"
    with pytest.raises(ValidationError):
        _ = MaskConfig.model_validate({"design": "not-a-real-design"})


def _synthetic_geometry_universe(
    parquet_path: Path,
    *,
    seasons: tuple[int, ...] = (1990, 1991),
    rows_per_season: int = 600,
) -> None:
    rng = np.random.default_rng(20260610)
    rows: list[dict[str, object]] = []
    event_key = 0
    for season in seasons:
        games = _game_id_pool(prefix=f"S{season}G", n_holdout=4, n_train=40)
        for i in range(rows_per_season):
            label = str(rng.choice(list(TRAJECTORY_LABELS), p=LABEL_WEIGHTS))
            rows.append(
                {
                    "event_key": event_key,
                    "geometry_dimension": "trajectory",
                    "observed_status": "observed",
                    "raw_value": label,
                    "training_weight": 1.0,
                    "dl_p_class": None,
                    "game_id": games[i % len(games)],
                    "season": season,
                    "league": "NL",
                    "source_family": str(rng.choice(["play_by_play", "box_score"])),
                    "park_id": str(rng.choice(["PRK1", "PRK2"])),
                    "scorer": str(rng.choice(["A", "B", "C"])),
                    "game_type": "RegularSeason",
                    "frame_start": str(rng.choice(["Top", "Bottom"])),
                    "exposure_status": "complete",
                    "result_family": str(rng.choice(["hit", "out_in_play"])),
                    "leverage_bucket": str(rng.choice(["low", "medium"])),
                    "batter_hand": str(rng.choice(["L", "R"])),
                    "pitcher_hand": str(rng.choice(["L", "R"])),
                    "personnel_confidence": "high",
                    "context_confidence": "high",
                    "alignment_regime": "pre_shift_era",
                    "base_state_start": int(rng.integers(0, 8)),
                    "outs_start": int(rng.integers(0, 3)),
                    "inning_start": int(rng.integers(1, 10)),
                    "score_margin": int(rng.integers(-3, 4)),
                    "leverage_index": float(rng.uniform(0.1, 2.5)),
                    "runs_on_play": int(rng.integers(0, 3)),
                }
            )
            event_key += 1
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(parquet_path)


@pytest.mark.slow
def test_run_backtest_smoke_end_to_end(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pytest.importorskip("pymc")
    pytest.importorskip("arviz")

    monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(tmp_path / "published"))
    for var in (
        "BC_STATS_SMOKE_LIMIT",
        "BC_STATS_BAYES_BACKEND",
        "BC_STATS_BAYES_DRAWS",
        "BC_STATS_BAYES_TUNE",
    ):
        monkeypatch.delenv(var, raising=False)

    universe_path = tmp_path / "universe" / "dataset.parquet"
    _synthetic_geometry_universe(universe_path)

    result = run_backtest(
        model="geometry",
        seed=20260611,
        budget=300,
        smoke=True,
        dataset_parquet=universe_path,
        output_root=tmp_path / "out",
    )

    assert result.run_dir == tmp_path / "out" / "geometry-seed20260611-budget300-smoke"
    assert result.metrics_path.exists()
    assert result.mask_summary_path.exists()

    mask_summary = json.loads(result.mask_summary_path.read_text(encoding="utf-8"))
    assert (
        OVERALL_MASKED_RANGE[0]
        <= mask_summary["overall_masked_share"]
        <= OVERALL_MASKED_RANGE[1]
    )
    assert (
        mask_summary["observed_focal_share"]
        < mask_summary["truth_shares_maskable"]["GroundBall"]
    )
    assert mask_summary["n_holdout_rows"] > 0
    assert set(mask_summary["masked_share_by_class"]) == set(TRAJECTORY_LABELS)

    metrics = json.loads(result.metrics_path.read_text(encoding="utf-8"))
    assert set(metrics["criteria"]) == {
        "focal_share_error_reduced",
        "total_variation_reduced",
        "held_out_non_regression",
        "sampler_diagnostics_clean",
    }
    assert isinstance(metrics["overall_pass"], bool)
    masked_slice = metrics["masked_slice"]
    assert set(masked_slice["truth_shares"]) == set(TRAJECTORY_LABELS)
    assert set(masked_slice["corrected_shares"]) == set(TRAJECTORY_LABELS)
    assert set(masked_slice["uncorrected_shares"]) == set(TRAJECTORY_LABELS)
    assert sum(masked_slice["corrected_shares"].values()) == pytest.approx(1.0)
    assert metrics["n_masked_events"] == mask_summary["n_masked_rows"]
    for arm in ("corrected", "uncorrected"):
        assert metrics["held_out"][arm]["top1_accuracy"] is not None
        assert metrics["held_out"][arm]["log_loss"] is not None
        assert "rhat_max" in metrics["diagnostics"][arm]
    assert metrics["propensity_diagnostics"]["is_smoke"] is True

    bayes_root = result.run_dir / "bayes"
    prop_dir = (
        bayes_root
        / "trajectory_observedness"
        / "geometry-seed20260611-budget300-smoke-prop"
    )
    assert (prop_dir / "exports" / "event_propensity.parquet").exists()
    for arm in ("corrected", "uncorrected"):
        fit_dir = (
            bayes_root
            / "geometry_trajectory"
            / f"geometry-seed20260611-budget300-smoke-{arm}"
        )
        assert (fit_dir / "exports" / "geometry_probabilities.parquet").exists()
        assert (fit_dir / "validation" / "held_out_metrics.json").exists()

    impute_dir = (
        result.run_dir
        / "datasets"
        / "model_input_geometry"
        / "geometry-seed20260611-budget300-smoke-impute"
    )
    impute_df = pl.read_parquet(impute_dir / "dataset.parquet")
    truth_df = pl.read_parquet(impute_dir / "truth.parquet")
    masked_truth = truth_df.filter(pl.col("is_masked"))
    masked_rows = impute_df.filter(pl.col("observed_status") != "observed")
    assert set(masked_rows.get_column("event_key").to_list()) == set(
        masked_truth.get_column("event_key").to_list()
    )
    assert masked_rows.get_column("raw_value").null_count() == masked_rows.height
    assert impute_df.get_column("propensity_p_observed").null_count() < impute_df.height
    assert truth_df.filter(pl.col("is_holdout") & pl.col("is_masked")).height == 0
