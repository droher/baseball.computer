"""End-to-end ``run_bayes_model`` smokes for the propensity MNAR covariate.

Mirrors the observation-model smoke conventions on the Model D
``ball_handler_imputation`` target: one smoke with the
``propensity_p_observed`` column present under the class flavor, one
relying on the column-absent path. Both are slow (numpyro NUTS warm-up);
the prior-only extras check stays in the default tier.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false, reportAttributeAccessIssue=false

from __future__ import annotations

import datetime as dt
from pathlib import Path

import numpy as np
import polars as pl
import pytest

pytest.importorskip("pymc")
pytest.importorskip("arviz")

from python_models.statistical.models._ball_handler_data import (
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    N_POSITIONS,
)
from python_models.statistical.splits import game_hash_fold

DATASET_NAME = "model_input_observation_batted_ball"
DIMENSION = "ball_handler_position"


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


def _write_synthetic_dataset(
    parquet_path: Path,
    *,
    with_propensity: bool,
    n_observed: int = 240,
    n_production: int = 30,
) -> None:
    rng = np.random.default_rng(20260610)
    game_pool = _game_id_pool(n_holdout=3, n_train=12)
    result_families = ("out_in_play", "hit_in_play")
    alignment_regimes = ("shift_growth_era", "pre_shift_era")
    rows: list[dict[str, object]] = []
    for i in range(n_observed):
        row: dict[str, object] = {
            "event_key": i,
            "dimension": DIMENSION,
            "observed_status": "observed",
            "raw_value": str(int(rng.integers(1, N_POSITIONS + 1))),
            "training_weight": 1.0,
            "game_id": game_pool[i % len(game_pool)],
            "season": 1925,
            "league": "AL",
            "source_family": "play_by_play",
            "park_id": "ARL01",
            "scorer": "scorerA",
            "base_state_start": int(rng.integers(0, 4)),
            "outs_start": int(rng.integers(0, 3)),
            "result_family": result_families[i % len(result_families)],
            "alignment_regime": alignment_regimes[i % len(alignment_regimes)],
        }
        if with_propensity:
            row["propensity_p_observed"] = (
                None if i % 9 == 0 else float(rng.uniform(0.4, 0.95))
            )
        rows.append(row)
    for j in range(n_production):
        row = {
            "event_key": 900_000 + j,
            "dimension": DIMENSION,
            "observed_status": "unobserved",
            "raw_value": None,
            "training_weight": 0.0,
            "game_id": f"GP{j:04d}",
            "season": 1925,
            "league": "AL",
            "source_family": "play_by_play",
            "park_id": "ARL01",
            "scorer": "scorerA",
            "base_state_start": int(rng.integers(0, 4)),
            "outs_start": int(rng.integers(0, 3)),
            "result_family": result_families[j % len(result_families)],
            "alignment_regime": alignment_regimes[j % len(alignment_regimes)],
        }
        if with_propensity:
            row["propensity_p_observed"] = float(rng.uniform(0.05, 0.45))
        rows.append(row)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)
    pl.DataFrame(rows).write_parquet(parquet_path)


def _write_dataset_manifest(manifest_path: Path, *, artifact_id: str) -> None:
    from python_models.statistical.manifests import write_manifest
    from python_models.statistical.schemas import ArtifactManifest

    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="dataset",
        name=DATASET_NAME,
        version="0.2.0",
        created_at=dt.datetime.now(tz=dt.timezone.utc),
        source_snapshot_id="dev-test",
        output_paths={"dataset": manifest_path.parent / "dataset.parquet"},
        package_versions={},
    )
    write_manifest(manifest, manifest_path)


def _prepare_dataset_dir(
    tmp_path: Path, *, artifact_id: str, with_propensity: bool
) -> tuple[Path, Path]:
    dataset_root = tmp_path / "datasets"
    bayes_root = tmp_path / "bayes"
    dataset_dir = dataset_root / DATASET_NAME / artifact_id
    _write_synthetic_dataset(
        dataset_dir / "dataset.parquet", with_propensity=with_propensity
    )
    _write_dataset_manifest(dataset_dir / "manifest.json", artifact_id=artifact_id)
    return dataset_root, bayes_root


def test_prior_only_records_propensity_extras(tmp_path: Path) -> None:
    from python_models.statistical.bayes.training import run_bayes_model

    dataset_root, bayes_root = _prepare_dataset_dir(
        tmp_path, artifact_id="ds-prop-prior", with_propensity=True
    )
    manifest = run_bayes_model(
        model_name="ball_handler_imputation",
        dataset_artifact_id="ds-prop-prior",
        artifact_id="bayes-prop-prior",
        source_snapshot_id="dev-test",
        smoke=True,
        prior_only=True,
        smoke_limit=400,
        gamma_propensity_flavor="gamma_propensity_class",
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    assert manifest.bayes_extras is not None
    assert manifest.bayes_extras.gamma_propensity_flavor == "gamma_propensity_class"
    assert manifest.bayes_extras.propensity_active is True
    assert manifest.bayes_extras.prior_config.gamma_propensity_scale > 0.0


@pytest.mark.slow
def test_full_smoke_class_flavor_with_propensity_column(tmp_path: Path) -> None:
    import arviz as az

    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root, bayes_root = _prepare_dataset_dir(
        tmp_path, artifact_id="ds-prop-class", with_propensity=True
    )
    _ = run_bayes_model(
        model_name="ball_handler_imputation",
        dataset_artifact_id="ds-prop-class",
        artifact_id="bayes-prop-class",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=400,
        gamma_propensity_flavor="gamma_propensity_class",
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "ball_handler_imputation" / "bayes-prop-class"
    for rel in (
        "manifest.json",
        "inference/posterior.nc",
        "exports/ball_handler_probabilities.parquet",
        "validation/held_out_metrics.json",
    ):
        assert (artifact_dir / rel).exists(), f"missing {rel}"

    reloaded = read_manifest(artifact_dir / "manifest.json")
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.gamma_propensity_flavor == "gamma_propensity_class"
    assert reloaded.bayes_extras.propensity_active is True

    posterior = az.from_netcdf(artifact_dir / "inference" / "posterior.nc").posterior
    assert "gamma_propensity" in posterior.data_vars
    gamma = np.asarray(posterior["gamma_propensity"].values)
    assert gamma.shape[-1] == N_POSITIONS
    np.testing.assert_allclose(gamma.sum(axis=-1), 0.0, atol=1e-6)

    export = pl.read_parquet(
        artifact_dir / "exports" / "ball_handler_probabilities.parquet"
    )
    production_keys = (
        pl.read_parquet(
            dataset_root / DATASET_NAME / "ds-prop-class" / "dataset.parquet"
        )
        .filter(pl.col("observed_status") != "observed")
        .get_column("event_key")
        .to_list()
    )
    assert set(export.get_column("event_key").to_list()) == set(production_keys)
    per_event = export.group_by("event_key").agg(pl.col("expected_share").sum())
    np.testing.assert_allclose(
        per_event.get_column("expected_share").to_numpy(), 1.0, atol=1e-9
    )


@pytest.mark.slow
def test_full_smoke_column_absent_resolves_zero_flavor(tmp_path: Path) -> None:
    import arviz as az

    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.manifests import read_manifest

    dataset_root, bayes_root = _prepare_dataset_dir(
        tmp_path, artifact_id="ds-prop-absent", with_propensity=False
    )
    _ = run_bayes_model(
        model_name="ball_handler_imputation",
        dataset_artifact_id="ds-prop-absent",
        artifact_id="bayes-prop-absent",
        source_snapshot_id="dev-test",
        smoke=True,
        smoke_limit=400,
        artifact_root=bayes_root,
        dataset_root=dataset_root,
    )
    artifact_dir = bayes_root / "ball_handler_imputation" / "bayes-prop-absent"
    assert (artifact_dir / "exports" / "ball_handler_probabilities.parquet").exists()

    reloaded = read_manifest(artifact_dir / "manifest.json")
    assert reloaded.bayes_extras is not None
    assert reloaded.bayes_extras.gamma_propensity_flavor == "gamma_propensity_zero"
    assert reloaded.bayes_extras.propensity_active is False

    posterior = az.from_netcdf(artifact_dir / "inference" / "posterior.nc").posterior
    assert "gamma_propensity" not in posterior.data_vars
