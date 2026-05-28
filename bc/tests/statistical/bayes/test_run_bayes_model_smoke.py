"""End-to-end ``run_bayes_model`` smoke covering prior-only + full numpyro fit.

Marked slow because the numpyro path JIT-compiles JAX kernels (~10-20s
warm-up). The prior-only path stays out of NUTS and is fast enough to
keep in the default tier.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("pymc")
pytest.importorskip("arviz")


def _write_synthetic_dataset(
    parquet_path: Path,
    n_rows: int = 400,
    *,
    sources: tuple[str, ...] = ("play_by_play", "box_score"),
) -> None:
    rng = np.random.default_rng(20260513)
    df = pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.int64),
            "dimension": ["trajectory"] * n_rows,
            "is_observed": rng.integers(0, 2, size=n_rows).astype(bool),
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
            "score_margin": rng.integers(-3, 3, size=n_rows).astype(np.int64),
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
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.diagnostics_summary.total_draws == 0
    assert manifest.bayes_extras.event_row_count == 300


@pytest.mark.slow
def test_full_smoke_writes_event_propensity_export(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-full-smoke"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=300)
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id="ds-full-smoke")

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
    ):
        assert (artifact_dir / rel).exists(), f"missing {rel}"
    assert not (artifact_dir / "inference" / "posterior_predictive.nc").exists()

    event_propensity = pl.read_parquet(
        artifact_dir / "exports" / "event_propensity.parquet"
    )
    assert event_propensity.height == 300
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
    assert reloaded.bayes_extras.event_row_count == 300
    payload = json.loads(
        (artifact_dir / "validation" / "diagnostics.json").read_text(encoding="utf-8")
    )
    assert payload["is_smoke"] is True
    assert "source_effect_active" in payload


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
    assert event_propensity.height == 300


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
