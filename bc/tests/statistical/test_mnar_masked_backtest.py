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

import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

from python_models.statistical import config as cfg
from python_models.statistical.backtests.mnar_masked import (
    OVERALL_MASKED_RANGE,
    TRUE_LABEL_COLUMN,
    VARIANTS,
    MaskConfig,
    assert_export_covers_masked_slice,
    calibrate_class_mask_probabilities,
    evaluate_criteria,
    generate_mask,
    recovered_class_shares,
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


def _evaluate(
    *,
    corrected_shares: dict[str, float] = CLOSE_SHARES,
    uncorrected_shares: dict[str, float] = BIASED_SHARES,
    corrected_held_out: dict[str, object] | None = None,
    uncorrected_held_out: dict[str, object] | None = None,
    corrected_diagnostics: dict[str, object] | None = None,
    uncorrected_diagnostics: dict[str, object] | None = None,
) -> dict[str, object]:
    return evaluate_criteria(
        truth_shares=TRUTH_SHARES,
        corrected_shares=corrected_shares,
        uncorrected_shares=uncorrected_shares,
        class_labels=TRAJECTORY_LABELS,
        focal_class="GroundBall",
        corrected_held_out=(
            corrected_held_out
            if corrected_held_out is not None
            else dict(HELD_OUT_GOOD)
        ),
        uncorrected_held_out=(
            uncorrected_held_out
            if uncorrected_held_out is not None
            else dict(HELD_OUT_BASE)
        ),
        corrected_diagnostics=(
            corrected_diagnostics
            if corrected_diagnostics is not None
            else dict(CLEAN_DIAGNOSTICS)
        ),
        uncorrected_diagnostics=(
            uncorrected_diagnostics
            if uncorrected_diagnostics is not None
            else dict(CLEAN_DIAGNOSTICS)
        ),
    )


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
    assert metrics["masked_slice"]["focal_share_error"]["relative_reduction"] < 0.25
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


def test_assert_export_covers_masked_slice() -> None:
    export = pl.DataFrame({"event_key": [1, 2, 3], "expected_share": [1.0] * 3})
    assert_export_covers_masked_slice(export, masked_event_keys={1, 2, 3})
    with pytest.raises(AssertionError, match="does not match"):
        assert_export_covers_masked_slice(export, masked_event_keys={1, 2})


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
