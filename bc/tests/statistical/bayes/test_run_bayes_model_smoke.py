"""End-to-end ``run_bayes_model`` smoke covering prior-only + full numpyro fit.

Marked slow because the numpyro path JIT-compiles JAX kernels (~10-20s
warm-up). The prior-only path stays out of NUTS and is fast enough to
keep in the default tier.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("pymc")
pytest.importorskip("arviz")

from python_models.statistical.models._event_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    ContinuousFeature,
    FixedEffectDesign,
    ObservationHeldOutSet,
)
from python_models.statistical.splits import game_hash_fold


def _game_id_pool(*, n_holdout: int, n_train: int) -> list[str]:
    holdout: list[str] = []
    train: list[str] = []
    i = 0
    while len(holdout) < n_holdout or len(train) < n_train:
        gid = f"GAME{i:04d}"
        if game_hash_fold(gid, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID:
            if len(holdout) < n_holdout:
                holdout.append(gid)
        elif len(train) < n_train:
            train.append(gid)
        i += 1
    return holdout + train


def _expected_split_counts(parquet_path: Path) -> tuple[int, int]:
    game_ids = pl.read_parquet(parquet_path).get_column("game_id").to_list()
    n_held = sum(
        1
        for g in game_ids
        if game_hash_fold(g, fold_count=HOLDOUT_FOLD_COUNT) == HOLDOUT_FOLD_ID
    )
    return len(game_ids) - n_held, n_held


def _write_synthetic_dataset(
    parquet_path: Path,
    n_rows: int = 400,
    *,
    sources: tuple[str, ...] = ("play_by_play", "box_score"),
) -> None:
    rng = np.random.default_rng(20260513)
    game_pool = _game_id_pool(n_holdout=3, n_train=12)
    game_ids = [game_pool[i % len(game_pool)] for i in range(n_rows)]
    score_margin = rng.integers(-3, 3, size=n_rows).astype(np.int64)
    is_observed = rng.random(n_rows) < np.where(score_margin >= 0, 0.9, 0.1)
    df = pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.int64),
            "dimension": ["trajectory"] * n_rows,
            "game_id": pl.Series("game_id", game_ids, dtype=pl.Utf8),
            "is_observed": is_observed,
            "training_weight": np.ones(n_rows, dtype=np.float64),
            "season": pl.Series(
                "season",
                [
                    str(s)
                    for s in rng.choice(["2010", "2015", "2020"], size=n_rows).tolist()
                ],
                dtype=pl.Utf8,
            ),
            "scorer": pl.Series(
                "scorer",
                [str(s) for s in rng.choice(["A", "B", "C"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "park_id": pl.Series(
                "park_id",
                [str(s) for s in rng.choice(["PRK1", "PRK2"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "source_family": pl.Series(
                "source_family",
                [str(s) for s in rng.choice(list(sources), size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "game_type": pl.Series(
                "game_type",
                [
                    str(s)
                    for s in rng.choice(
                        ["RegularSeason", "Postseason"], size=n_rows
                    ).tolist()
                ],
                dtype=pl.Utf8,
            ),
            "frame_start": pl.Series(
                "frame_start",
                [str(s) for s in rng.choice(["Top", "Bottom"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "exposure_status": pl.Series(
                "exposure_status", ["complete"] * n_rows, dtype=pl.Utf8
            ),
            "alignment_regime": pl.Series(
                "alignment_regime", ["pre_shift_era"] * n_rows, dtype=pl.Utf8
            ),
            "league": pl.Series(
                "league",
                [str(s) for s in rng.choice(["AL", "NL"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "result_family": pl.Series(
                "result_family",
                [
                    str(s)
                    for s in rng.choice(["hit", "out_in_play"], size=n_rows).tolist()
                ],
                dtype=pl.Utf8,
            ),
            "hit_or_out": rng.integers(0, 2, size=n_rows).astype(bool),
            "leverage_bucket": pl.Series(
                "leverage_bucket",
                [str(s) for s in rng.choice(["low", "medium"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "batter_hand": pl.Series(
                "batter_hand",
                [str(s) for s in rng.choice(["L", "R"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "pitcher_hand": pl.Series(
                "pitcher_hand",
                [str(s) for s in rng.choice(["L", "R"], size=n_rows).tolist()],
                dtype=pl.Utf8,
            ),
            "personnel_confidence": pl.Series(
                "personnel_confidence", ["high"] * n_rows, dtype=pl.Utf8
            ),
            "context_confidence": pl.Series(
                "context_confidence", ["high"] * n_rows, dtype=pl.Utf8
            ),
            "outs_start": rng.integers(0, 3, size=n_rows).astype(np.int64),
            "inning_start": rng.integers(1, 10, size=n_rows).astype(np.int64),
            "score_margin": score_margin,
            "leverage_index": rng.uniform(0.1, 2.5, size=n_rows).astype(np.float64),
            "runs_on_play": rng.integers(0, 3, size=n_rows).astype(np.int64),
        }
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_path)


def _write_dataset_manifest(
    manifest_path: Path, *, artifact_id: str = "ds-smoke-1"
) -> None:
    from python_models.statistical.manifests import write_manifest
    from python_models.statistical.schemas import ArtifactManifest

    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="dataset",
        name="model_input_observation_batted_ball",
        version="0.2.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"dataset": manifest_path.parent / "dataset.parquet"},
        package_versions={},
    )
    write_manifest(manifest, manifest_path)


def test_prior_only_writes_prior_predictive(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-prior-only"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=300)
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id="ds-prior-only")

    n_train, n_held = _expected_split_counts(dataset_dir / "dataset.parquet")
    assert n_train and n_held

    manifest = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-prior-only",
        artifact_id="bayes-prior-only",
        source_snapshot_id="dev-test",
        smoke=True,
        prior_only=True,
        smoke_limit=300,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "trajectory_observedness" / "bayes-prior-only"
    assert (artifact_dir / "inference" / "prior_predictive.nc").exists()
    assert not (artifact_dir / "inference" / "posterior.nc").exists()
    assert not (artifact_dir / "exports" / "event_propensity.parquet").exists()
    assert not (artifact_dir / "validation" / "held_out_metrics.json").exists()
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.diagnostics_summary.total_draws == 0
    assert manifest.bayes_extras.event_row_count == n_train


@pytest.mark.slow
def test_full_smoke_writes_event_propensity_export(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-full-smoke"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=300)
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id="ds-full-smoke")
    n_train, n_held = _expected_split_counts(dataset_dir / "dataset.parquet")
    assert n_train and n_held

    _ = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-full-smoke",
        artifact_id="bayes-full-smoke",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=300,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "trajectory_observedness" / "bayes-full-smoke"
    for rel in (
        "manifest.json",
        "inference/prior_predictive.nc",
        "inference/posterior.nc",
        "exports/posterior_summary.parquet",
        "exports/calibration_curve.parquet",
        "exports/event_propensity.parquet",
        "validation/diagnostics.json",
        "validation/held_out_metrics.json",
    ):
        assert (artifact_dir / rel).exists(), f"missing {rel}"
    assert not (artifact_dir / "inference" / "posterior_predictive.nc").exists()

    event_propensity = pl.read_parquet(
        artifact_dir / "exports" / "event_propensity.parquet"
    )
    assert event_propensity.height == n_train
    assert set(event_propensity.columns) == {
        "event_key",
        "dimension",
        "p_observed_mean",
    }
    means = event_propensity.get_column("p_observed_mean").to_numpy()
    assert np.all((means >= 0.0) & (means <= 1.0))

    reloaded = read_manifest(artifact_dir / "manifest.json")
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.sampler_config.is_smoke is True
    assert reloaded.bayes_extras.event_row_count == n_train
    payload = json.loads(
        (artifact_dir / "validation" / "diagnostics.json").read_text(encoding="utf-8")
    )
    assert payload["is_smoke"] is True
    assert "source_effect_active" in payload
    assert payload["calibration_ece"] is not None

    held_out_payload = json.loads(
        (artifact_dir / "validation" / "held_out_metrics.json").read_text(
            encoding="utf-8"
        )
    )
    assert set(held_out_payload) == {
        "n_events",
        "n_events_scored",
        "roc_auc",
        "pr_auc",
        "baseline_pr_auc",
        "ece_held_out",
    }
    from python_models.statistical.bayes.training import HELD_OUT_BERNOULLI_LIMIT

    assert held_out_payload["n_events"] == n_held
    assert held_out_payload["n_events_scored"] == min(n_held, HELD_OUT_BERNOULLI_LIMIT)
    assert np.isfinite(held_out_payload["roc_auc"])
    assert np.isfinite(held_out_payload["pr_auc"])
    assert 0.0 < held_out_payload["baseline_pr_auc"] < 1.0
    assert held_out_payload["pr_auc"] > held_out_payload["baseline_pr_auc"]
    assert np.isfinite(held_out_payload["ece_held_out"])


@pytest.mark.slow
def test_full_smoke_single_source_drops_source_block(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = (
        dataset_root / "model_input_observation_batted_ball" / "ds-single-source"
    )
    _write_synthetic_dataset(
        dataset_dir / "dataset.parquet",
        n_rows=300,
        sources=("play_by_play",),
    )
    _write_dataset_manifest(
        dataset_dir / "manifest.json", artifact_id="ds-single-source"
    )
    n_train, _n_held = _expected_split_counts(dataset_dir / "dataset.parquet")

    _ = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-single-source",
        artifact_id="bayes-single-source",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=300,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "trajectory_observedness" / "bayes-single-source"
    reloaded = read_manifest(artifact_dir / "manifest.json")
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.source_effect_active is False
    payload = json.loads(
        (artifact_dir / "validation" / "diagnostics.json").read_text(encoding="utf-8")
    )
    assert payload["source_effect_active"] is False
    event_propensity = pl.read_parquet(
        artifact_dir / "exports" / "event_propensity.parquet"
    )
    assert event_propensity.height == n_train


def test_held_out_metrics_bernoulli_separable_labels() -> None:
    from python_models.statistical.bayes.training import _held_out_metrics_bernoulli

    y = np.array([0, 0, 1, 1], dtype=np.int8)
    p_mean = np.array([0.1, 0.2, 0.8, 0.9], dtype=np.float64)
    metrics = _held_out_metrics_bernoulli(y, p_mean)
    assert metrics["n_events"] == 4
    assert metrics["roc_auc"] == pytest.approx(1.0)
    assert metrics["pr_auc"] == pytest.approx(1.0)
    assert metrics["baseline_pr_auc"] == pytest.approx(0.5)
    assert np.isfinite(metrics["ece_held_out"])


def test_held_out_metrics_bernoulli_single_class_writes_nulls_and_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from python_models.statistical.bayes.training import _held_out_metrics_bernoulli

    y = np.ones(5, dtype=np.int8)
    p_mean = np.full(5, 0.7, dtype=np.float64)
    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.bayes.training"
    ):
        metrics = _held_out_metrics_bernoulli(y, p_mean)
    assert metrics["n_events"] == 5
    assert metrics["roc_auc"] is None
    assert metrics["pr_auc"] is None
    assert metrics["baseline_pr_auc"] == pytest.approx(1.0)
    assert np.isfinite(metrics["ece_held_out"])
    assert any("single-class" in record.message for record in caplog.records)
    payload = json.loads(json.dumps(metrics))
    assert payload["roc_auc"] is None
    assert payload["pr_auc"] is None


def test_held_out_metrics_bernoulli_empty_warns(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from python_models.statistical.bayes.training import _held_out_metrics_bernoulli

    with caplog.at_level(
        logging.WARNING, logger="python_models.statistical.bayes.training"
    ):
        metrics = _held_out_metrics_bernoulli(
            np.zeros(0, dtype=np.int8), np.zeros(0, dtype=np.float64)
        )
    assert metrics["n_events"] == 0
    assert metrics["roc_auc"] is None
    assert metrics["pr_auc"] is None
    assert metrics["baseline_pr_auc"] is None
    assert metrics["ece_held_out"] is None
    assert any("empty" in record.message for record in caplog.records)


def _make_observation_held_out(
    n: int, *, seed: int = 20260610
) -> ObservationHeldOutSet:
    rng = np.random.default_rng(seed)
    return ObservationHeldOutSet(
        y=rng.integers(0, 2, size=n).astype(np.int8),
        event_keys=np.arange(n, dtype=np.int64),
        season_idx=rng.integers(0, 3, size=n).astype(np.int64),
        scorer_idx=rng.integers(0, 4, size=n).astype(np.int64),
        park_idx=rng.integers(0, 2, size=n).astype(np.int64),
        source_idx=np.zeros(n, dtype=np.int64),
        fixed_effects={
            "game_type": FixedEffectDesign(
                levels=("regular", "postseason"),
                codes=rng.integers(0, 2, size=n).astype(np.int64),
            )
        },
        continuous={
            "outs_start": ContinuousFeature(
                values=rng.normal(size=n).astype(np.float64),
                is_missing=np.zeros(n, dtype=np.int8),
                raw_mean=0.0,
                raw_std=1.0,
            )
        },
    )


def test_subsample_observation_held_out_over_budget_deterministic(
    caplog: pytest.LogCaptureFixture,
) -> None:
    from python_models.statistical.bayes.training import (
        HELD_OUT_BERNOULLI_LIMIT,
        _subsample_observation_held_out,
    )

    n = HELD_OUT_BERNOULLI_LIMIT + 7
    held = _make_observation_held_out(n)

    with caplog.at_level(
        logging.INFO, logger="python_models.statistical.bayes.training"
    ):
        first = _subsample_observation_held_out(
            held, limit=HELD_OUT_BERNOULLI_LIMIT, seed=11
        )
    second = _subsample_observation_held_out(
        held, limit=HELD_OUT_BERNOULLI_LIMIT, seed=11
    )

    assert first.n_events == HELD_OUT_BERNOULLI_LIMIT
    np.testing.assert_array_equal(first.event_keys, second.event_keys)
    assert any("subsampling" in record.message for record in caplog.records)

    idx = first.event_keys
    assert np.unique(idx).size == HELD_OUT_BERNOULLI_LIMIT
    np.testing.assert_array_equal(first.y, held.y[idx])
    np.testing.assert_array_equal(first.season_idx, held.season_idx[idx])
    np.testing.assert_array_equal(first.scorer_idx, held.scorer_idx[idx])
    np.testing.assert_array_equal(first.park_idx, held.park_idx[idx])
    np.testing.assert_array_equal(first.source_idx, held.source_idx[idx])
    assert (
        first.fixed_effects["game_type"].levels
        == held.fixed_effects["game_type"].levels
    )
    np.testing.assert_array_equal(
        first.fixed_effects["game_type"].codes,
        held.fixed_effects["game_type"].codes[idx],
    )
    np.testing.assert_array_equal(
        first.continuous["outs_start"].values,
        held.continuous["outs_start"].values[idx],
    )
    np.testing.assert_array_equal(
        first.continuous["outs_start"].is_missing,
        held.continuous["outs_start"].is_missing[idx],
    )


def test_subsample_observation_held_out_under_budget_passthrough() -> None:
    from python_models.statistical.bayes.training import (
        HELD_OUT_BERNOULLI_LIMIT,
        _subsample_observation_held_out,
    )

    held = _make_observation_held_out(64)
    result = _subsample_observation_held_out(
        held, limit=HELD_OUT_BERNOULLI_LIMIT, seed=11
    )
    assert result is held

    exact = _make_observation_held_out(128)
    assert _subsample_observation_held_out(exact, limit=128, seed=11) is exact


def test_posterior_held_out_means_bernoulli_masks_unseen_levels() -> None:
    import arviz as az

    from python_models.statistical.bayes.training import (
        _posterior_held_out_means_bernoulli,
    )
    from python_models.statistical.models._event_data import ObservationHeldOutSet

    rng = np.random.default_rng(20260609)
    n_chain, n_draw = 2, 5
    alpha = rng.normal(size=(n_chain, n_draw))
    beta_season = rng.normal(size=(n_chain, n_draw, 3))
    beta_scorer = rng.normal(size=(n_chain, n_draw, 2))
    beta_park = rng.normal(size=(n_chain, n_draw, 2))
    idata = az.from_dict(
        posterior={
            "alpha": alpha,
            "beta_season": beta_season,
            "beta_scorer": beta_scorer,
            "beta_park": beta_park,
        }
    )
    held = ObservationHeldOutSet(
        y=np.array([1, 0], dtype=np.int8),
        event_keys=np.array([10, 11], dtype=np.int64),
        season_idx=np.array([1, 1], dtype=np.int64),
        scorer_idx=np.array([0, 0], dtype=np.int64),
        park_idx=np.array([1, -1], dtype=np.int64),
        source_idx=np.array([-1, -1], dtype=np.int64),
        fixed_effects={},
        continuous={},
    )

    means = _posterior_held_out_means_bernoulli(idata, held)

    def _sigmoid(x: np.ndarray) -> np.ndarray:
        return 1.0 / (1.0 + np.exp(-x))

    eta_common = alpha + beta_season[..., 1] + beta_scorer[..., 0]
    expected_seen = float(_sigmoid(eta_common + beta_park[..., 1]).mean())
    expected_masked = float(_sigmoid(eta_common).mean())
    assert means[0] == pytest.approx(expected_seen)
    assert means[1] == pytest.approx(expected_masked)
    assert means[1] != pytest.approx(expected_seen)


def test_resolve_sampler_config_default_is_unmodified(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from python_models.statistical.bayes.training import _resolve_sampler_config
    from python_models.statistical.pymc_utils import DEFAULT_CONFIG, SMOKE_CONFIG

    for var in (
        "BC_STATS_BAYES_BACKEND",
        "BC_STATS_BAYES_DRAWS",
        "BC_STATS_BAYES_TUNE",
    ):
        monkeypatch.delenv(var, raising=False)

    assert _resolve_sampler_config(smoke=False, override_seed=None) is DEFAULT_CONFIG
    assert _resolve_sampler_config(smoke=True, override_seed=None) is SMOKE_CONFIG


def test_resolve_sampler_config_accepts_valid_backend_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import get_args

    from python_models.statistical.bayes.training import _resolve_sampler_config
    from python_models.statistical.pymc_utils import NutsBackend

    monkeypatch.delenv("BC_STATS_BAYES_DRAWS", raising=False)
    monkeypatch.delenv("BC_STATS_BAYES_TUNE", raising=False)
    for backend in get_args(NutsBackend):
        monkeypatch.setenv("BC_STATS_BAYES_BACKEND", backend)
        resolved = _resolve_sampler_config(smoke=False, override_seed=None)
        assert resolved.backend == backend


def test_resolve_sampler_config_rejects_invalid_backend_override(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from typing import get_args

    from python_models.statistical.bayes.training import _resolve_sampler_config
    from python_models.statistical.pymc_utils import NutsBackend

    monkeypatch.setenv("BC_STATS_BAYES_BACKEND", "not-a-backend")
    with pytest.raises(ValueError) as excinfo:
        _ = _resolve_sampler_config(smoke=False, override_seed=None)
    message = str(excinfo.value)
    assert "BC_STATS_BAYES_BACKEND" in message
    for backend in get_args(NutsBackend):
        assert backend in message


def test_resolve_sampler_config_draws_tune_overrides(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from python_models.statistical.bayes.training import _resolve_sampler_config
    from python_models.statistical.pymc_utils import DEFAULT_CONFIG

    monkeypatch.delenv("BC_STATS_BAYES_BACKEND", raising=False)
    monkeypatch.setenv("BC_STATS_BAYES_DRAWS", "2000")
    monkeypatch.setenv("BC_STATS_BAYES_TUNE", "3000")

    resolved = _resolve_sampler_config(smoke=False, override_seed=None)
    assert resolved.draws == 2000
    assert resolved.tune == 3000
    assert resolved.draws != DEFAULT_CONFIG.draws
    assert resolved.tune != DEFAULT_CONFIG.tune
    assert resolved.chains == DEFAULT_CONFIG.chains
    assert resolved.target_accept == DEFAULT_CONFIG.target_accept
    assert resolved.backend == DEFAULT_CONFIG.backend
