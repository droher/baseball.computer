"""End-to-end Bayes smoke tests (PyMC required, marked slow)."""

from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("pymc")
pytest.importorskip("arviz")

pytestmark = pytest.mark.slow


def _write_synthetic_dataset(parquet_path: Path, n_rows: int = 1000) -> None:
    rng = np.random.default_rng(20260513)
    seasons = rng.choice([2020, 2021, 2022], size=n_rows)
    scorers = rng.choice(["A", "B", "C", "D"], size=n_rows)
    sources = rng.choice(["play_by_play", "box_score"], size=n_rows)
    p = np.full(n_rows, 0.8)
    is_observed = rng.binomial(1, p, size=n_rows).astype(bool)

    df = pl.DataFrame(
        {
            "event_key": np.arange(n_rows, dtype=np.uint32),
            "dimension": ["trajectory"] * n_rows,
            "is_observed": is_observed,
            "training_weight": np.ones(n_rows, dtype=np.float64),
            "season": seasons.astype(np.int16),
            "scorer": list(scorers),
            "source_family": list(sources),
            "dl_artifact_id": [None] * n_rows,
            "dl_p_class": [None] * n_rows,
        }
    )
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet_path)


def _write_dataset_manifest(manifest_path: Path) -> None:
    from python_models.statistical.manifests import write_manifest
    from python_models.statistical.schemas import ArtifactManifest

    manifest = ArtifactManifest(
        artifact_id="ds-smoke-1",
        kind="dataset",
        name="model_input_observation_batted_ball",
        version="0.2.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"dataset": manifest_path.parent / "dataset.parquet"},
        package_versions={},
    )
    write_manifest(manifest, manifest_path)


def test_builder_returns_pymc_model() -> None:
    import pymc as pm

    from python_models.statistical.models._data import ObservationModelInputs
    from python_models.statistical.models.observation import (
        build_trajectory_observedness_model,
    )

    inputs = ObservationModelInputs(
        y=np.array([1, 0, 1, 1, 0, 1], dtype=np.int8),
        season_idx=np.array([0, 0, 1, 1, 0, 1], dtype=np.int64),
        scorer_idx=np.array([0, 1, 0, 1, 0, 1], dtype=np.int64),
        source_idx=np.array([0, 0, 0, 1, 1, 1], dtype=np.int64),
        dl_logit=np.zeros(6, dtype=np.float64),
        coords={
            "season": ["2020", "2021"],
            "scorer": ["A", "B"],
            "source": ["box_score", "play_by_play"],
        },
    )
    model = build_trajectory_observedness_model(inputs)
    assert isinstance(model, pm.Model)
    assert set(model.coords) >= {"event", "season", "scorer", "source"}
    var_names = {rv.name for rv in model.unobserved_RVs}
    assert {"alpha", "sigma_season", "sigma_scorer", "sigma_source"} <= var_names


def test_run_bayes_model_smoke_writes_all_files(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-smoke-1"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=1000)
    _write_dataset_manifest(dataset_dir / "manifest.json")

    manifest = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-smoke-1",
        artifact_id="bayes-smoke-1",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=500,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    assert manifest.kind == "bayes"
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.sampler_config.is_smoke is True
    assert manifest.bayes_extras.dimension == "trajectory"

    artifact_dir = bayes_root / "trajectory_observedness" / "bayes-smoke-1"
    for rel in (
        "manifest.json",
        "inference/prior_predictive.nc",
        "inference/posterior.nc",
        "inference/posterior_predictive.nc",
        "exports/posterior_summary.parquet",
        "exports/calibration_curve.parquet",
        "validation/diagnostics.json",
    ):
        assert (artifact_dir / rel).exists(), f"missing {rel}"

    reloaded = read_manifest(artifact_dir / "manifest.json")
    assert reloaded.bayes_extras is not None
    div_limit = 0.05 * reloaded.bayes_extras.diagnostics_summary.total_draws
    assert reloaded.bayes_extras.diagnostics_summary.divergences <= max(div_limit, 5)


def test_prior_only_short_circuits(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-prior-only"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=400)
    _write_dataset_manifest(dataset_dir / "manifest.json")

    manifest = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-prior-only",
        artifact_id="bayes-prior-only",
        source_snapshot_id="dev-test",
        smoke=True,
        prior_only=True,
        smoke_limit=400,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "trajectory_observedness" / "bayes-prior-only"
    assert (artifact_dir / "inference" / "prior_predictive.nc").exists()
    assert not (artifact_dir / "inference" / "posterior.nc").exists()
    assert not (artifact_dir / "exports" / "posterior_summary.parquet").exists()
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.diagnostics_summary.total_draws == 0


def test_validate_dispatches_bayes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from python_models.statistical import config as cfg
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.validate import validate_artifact

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / "model_input_observation_batted_ball" / "ds-validate"
    _write_synthetic_dataset(dataset_dir / "dataset.parquet", n_rows=800)
    _write_dataset_manifest(dataset_dir / "manifest.json")

    _ = run_bayes_model(
        model_name="trajectory_observedness",
        dataset_artifact_id="ds-validate",
        artifact_id="bayes-validate-1",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=600,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    monkeypatch.setattr(cfg, "BAYES_ROOT", bayes_root)
    report = validate_artifact(
        "bayes-validate-1",
        candidate_roots=(bayes_root,),
    )
    assert report.kind == "bayes"
    diagnostics_path = (
        bayes_root
        / "trajectory_observedness"
        / "bayes-validate-1"
        / "validation"
        / "diagnostics.json"
    )
    payload = json.loads(diagnostics_path.read_text(encoding="utf-8"))
    assert payload["is_smoke"] is True
    assert report.status in {"passed", "failed"}
