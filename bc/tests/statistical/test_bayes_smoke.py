"""End-to-end Bayes smoke tests (PyMC required, marked slow).

Parametrizes over 4 dims × 2 flavors. To keep the suite tractable we
default ``BC_STATS_TEST_BAYES_LIMIT=300``; the production smoke recipe
(``just fit-bayes``) keeps the 100k row limit.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownArgumentType=false, reportUnknownVariableType=false

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

_TEST_SMOKE_LIMIT: int = 300

_UNDERLYING_DIMENSIONS: tuple[str, ...] = (
    "trajectory",
    "location_side",
    "location_depth",
)


def _write_synthetic_dataset(
    parquet_path: Path,
    n_rows: int = 1000,
    dimensions: tuple[str, ...] = ("trajectory",),
) -> None:
    rng = np.random.default_rng(20260513)
    frames: list[pl.DataFrame] = []
    for offset, dim in enumerate(dimensions):
        seasons = rng.choice([2020, 2021, 2022], size=n_rows)
        scorers = rng.choice(["A", "B", "C", "D"], size=n_rows)
        sources = rng.choice(["play_by_play", "box_score"], size=n_rows)
        p = np.full(n_rows, 0.8)
        is_observed = rng.binomial(1, p, size=n_rows).astype(bool)
        event_keys = np.arange(
            offset * n_rows, (offset + 1) * n_rows, dtype=np.uint32
        )
        frames.append(
            pl.DataFrame(
                {
                    "event_key": event_keys,
                    "dimension": [dim] * n_rows,
                    "is_observed": is_observed,
                    "training_weight": np.ones(n_rows, dtype=np.float64),
                    "season": seasons.astype(np.int16),
                    "scorer": list(scorers),
                    "source_family": list(sources),
                    "dl_artifact_id": [None] * n_rows,
                    "dl_p_class": [None] * n_rows,
                }
            )
        )
    df = pl.concat(frames)
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


def _write_synthetic_dl_artifact(
    *,
    deep_root: Path,
    proposal_dimension: str,
    class_labels: tuple[str, ...],
    event_keys: np.ndarray,
) -> Path:
    """Create a minimal DL artifact + manifest for a single proposal dim."""
    import datetime as _dt

    from python_models.statistical.manifests import write_manifest
    from python_models.statistical.schemas import ArtifactManifest

    artifact_id = f"dl-stub-{proposal_dimension}"
    artifact_dir = deep_root / f"geometry_{proposal_dimension}" / artifact_id
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    rng = np.random.default_rng(2026_05_18)
    n_classes = len(class_labels)
    raw = rng.dirichlet(np.ones(n_classes), size=event_keys.shape[0])
    df = pl.DataFrame(
        {
            "event_key": event_keys.astype(np.uint32),
            "partition": ["OOF"] * event_keys.shape[0],
            "dl_p_class": [row.tolist() for row in raw],
        }
    )
    df.write_parquet(exports_dir / "probabilities.parquet")
    (exports_dir / "class_labels.json").write_text(
        json.dumps({"labels": list(class_labels)}, indent=2)
    )
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="deep",
        name=f"geometry_{proposal_dimension}",
        version="0.1.0",
        created_at=_dt.datetime.now(tz=_dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={
            "probabilities": exports_dir / "probabilities.parquet",
            "class_labels": exports_dir / "class_labels.json",
        },
        package_versions={},
    )
    write_manifest(manifest, artifact_dir / "manifest.json")
    return artifact_dir / "manifest.json"


def _write_dl_pointer(*, published_root: Path, manifest_name: str, artifact_id: str, manifest_path: Path) -> None:
    import datetime as _dt

    from python_models.statistical.manifests import write_published_pointer
    from python_models.statistical.schemas import PublishedPointer

    published_root.mkdir(parents=True, exist_ok=True)
    _ = write_published_pointer(
        PublishedPointer(
            model_name=manifest_name,
            artifact_id=artifact_id,
            published_at=_dt.datetime.now(tz=_dt.timezone.utc),
            manifest_path=manifest_path,
        ),
        root=published_root,
    )


# trajectory pulled from the production spec so broad_contact's
# class-collapse stays aligned if the deep-target labels evolve.
# location_side / location_depth labels are not class-locked downstream
# (no collapse map references them), so they're stub-only.
from python_models.statistical.deep.targets.geometry import (  # noqa: E402
    TRAJECTORY_CLASS_LABELS as _TRAJECTORY_CLASS_LABELS,
)

_DL_CLASS_LABELS: dict[str, tuple[str, ...]] = {
    "trajectory": _TRAJECTORY_CLASS_LABELS,
    "location_side": ("Default", "Foul", "FoulLine", "Left", "Middle", "Right"),
    "location_depth": ("Deep", "Default", "ExtraDeep", "Shallow"),
}


_PARAMS: tuple[tuple[str, str, str], ...] = tuple(
    (target, flavor, target.removesuffix("_observedness"))
    for target in (
        "trajectory_observedness",
        "location_side_observedness",
        "location_depth_observedness",
        "broad_contact_observedness",
    )
    for flavor in ("zero", "shrunk")
)


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


def test_shrunk_flavor_samples_gamma_dl() -> None:
    import pymc as pm

    from python_models.statistical.models._data import ObservationModelInputs
    from python_models.statistical.models.observation import build_observation_model

    inputs = ObservationModelInputs(
        y=np.array([1, 0, 1, 1, 0, 1], dtype=np.int8),
        season_idx=np.array([0, 0, 1, 1, 0, 1], dtype=np.int64),
        scorer_idx=np.array([0, 1, 0, 1, 0, 1], dtype=np.int64),
        source_idx=np.array([0, 0, 0, 1, 1, 1], dtype=np.int64),
        dl_logit=np.full(6, 0.5, dtype=np.float64),
        coords={"season": ["2020", "2021"], "scorer": ["A", "B"], "source": ["box_score", "play_by_play"]},
    )
    model = build_observation_model(
        inputs, gamma_dl_flavor="gamma_dl_shrunk", dimension="trajectory"
    )
    assert isinstance(model, pm.Model)
    var_names = {rv.name for rv in model.unobserved_RVs}
    assert "gamma_dl" in var_names


@pytest.mark.parametrize("target_name,flavor,dimension", _PARAMS)
def test_run_bayes_model_smoke_writes_all_files(
    target_name: str,
    flavor: str,
    dimension: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from python_models.statistical import config as cfg
    from python_models.statistical.bayes import targets as _targets  # noqa: F401
    from python_models.statistical.bayes.registry import get_target
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    deep_root = tmp_path / "deep"
    published_root = tmp_path / "published"

    artifact_id = f"ds-smoke-{target_name}-{flavor}"
    dataset_dir = (
        dataset_root / "model_input_observation_batted_ball" / artifact_id
    )
    _write_synthetic_dataset(
        dataset_dir / "dataset.parquet",
        n_rows=600,
        dimensions=_UNDERLYING_DIMENSIONS,
    )
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id=artifact_id)

    spec = get_target(target_name)
    underlying_keys = pl.read_parquet(dataset_dir / "dataset.parquet").filter(
        pl.col("dimension") == spec.dataset_dimension_filter
    ).get_column("event_key").to_numpy()

    if flavor == "shrunk":
        assert spec.dl_proposal_dimension is not None
        manifest_path = _write_synthetic_dl_artifact(
            deep_root=deep_root,
            proposal_dimension=spec.dl_proposal_dimension,
            class_labels=_DL_CLASS_LABELS[spec.dl_proposal_dimension],
            event_keys=underlying_keys,
        )
        _write_dl_pointer(
            published_root=published_root,
            manifest_name=f"dl_proposal_{spec.dl_proposal_dimension}",
            artifact_id=f"dl-stub-{spec.dl_proposal_dimension}",
            manifest_path=manifest_path,
        )
        monkeypatch.setenv(cfg.ENV_PUBLISHED_ROOT, str(published_root))
        monkeypatch.setattr(cfg, "DEEP_ROOT", deep_root)

    manifest = run_bayes_model(
        model_name=target_name,
        dataset_artifact_id=artifact_id,
        artifact_id=f"bayes-{target_name}-{flavor}",
        source_snapshot_id="dev-test",
        gamma_dl=flavor,
        smoke=True,
        smoke_limit=_TEST_SMOKE_LIMIT,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    expected_status = (
        "gamma_dl_shrunk" if flavor == "shrunk" else "gamma_dl_zero"
    )
    assert manifest.kind == "bayes"
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.sampler_config.is_smoke is True
    assert manifest.bayes_extras.dimension == dimension
    assert manifest.bayes_extras.gamma_dl_flavor == expected_status
    assert manifest.bayes_extras.ablation_status == expected_status
    if flavor == "shrunk":
        assert len(manifest.bayes_extras.dl_proposal_inputs) == 1
    else:
        assert manifest.bayes_extras.dl_proposal_inputs == ()

    artifact_dir = bayes_root / target_name / f"bayes-{target_name}-{flavor}"
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
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id="ds-prior-only")

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
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id="ds-validate")

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
