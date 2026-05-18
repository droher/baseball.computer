"""End-to-end Bayes fit dispatcher.

PR1 wires a single dispatcher (``run_bayes_model``) that:

1. Loads the dataset Parquet for the named model.
2. Builds inputs + prior predictive.
3. Samples (NUTS) unless ``prior_only``.
4. Generates posterior predictive draws.
5. Computes posterior summary, calibration curve, diagnostics summary.
6. Writes ``inference/*.nc``, ``exports/*.parquet``, ``validation/diagnostics.json``,
   ``manifest.json`` atomically.

Only ``trajectory_observedness`` resolves in PR1; other model names raise
``NotImplementedError`` until PR2.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportUnusedCallResult=false, reportAttributeAccessIssue=false, reportIndexIssue=false, reportOperatorIssue=false, reportMissingTypeArgument=false, reportArgumentType=false, reportCallIssue=false

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.artifacts import (
    bayes_artifact_dir,
    bayes_exports_dir,
    bayes_inference_dir,
    bayes_validation_dir,
)
from python_models.statistical.calibration import (
    expected_calibration_error,
    reliability_curve,
)
from python_models.statistical.config import BAYES_ROOT, DATASETS_ROOT
from python_models.statistical.manifests import (
    package_versions,
    utc_now,
    write_manifest,
)
from python_models.statistical.models._data import (
    DEFAULT_SEED,
    DEFAULT_SMOKE_LIMIT,
    prepare_observation_inputs,
)
from python_models.statistical.models.observation import (
    build_trajectory_observedness_model,
)
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.pymc_utils import (
    DEFAULT_CONFIG,
    SMOKE_CONFIG,
    SamplingConfig,
    posterior_predictive,
    prior_predictive,
    sample_model,
)
from python_models.statistical.schemas import (
    ArtifactManifest,
    BayesArtifactExtras,
    BayesDiagnosticsSummary,
    BayesPosteriorRow,
    BayesPosteriorSummary,
    BayesPriorConfig,
    BayesSamplerConfig,
)

_log = logging.getLogger(__name__)

MODEL_VERSION: str = "0.1.0"

_DATASET_NAME: str = "model_input_observation_batted_ball"
_DIMENSION_BY_MODEL: dict[str, str] = {
    "trajectory_observedness": "trajectory",
}


def _atomic_write_text(target: Path, payload: str) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            _ = fh.write(payload)
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _atomic_write_netcdf(idata: az.InferenceData, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.stem}.", suffix=".nc"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        idata.to_netcdf(str(tmp_path))
        os.replace(tmp_path, target)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _resolve_sampler_config(
    *, smoke: bool, override_seed: int | None
) -> SamplingConfig:
    base = SMOKE_CONFIG if smoke else DEFAULT_CONFIG
    if override_seed is None:
        return base
    return SamplingConfig(
        draws=base.draws,
        tune=base.tune,
        chains=base.chains,
        target_accept=base.target_accept,
        random_seed=override_seed,
        cores=base.cores,
    )


def _build_posterior_summary(idata: az.InferenceData) -> BayesPosteriorSummary:
    summary_df = az.summary(
        idata,
        var_names=["alpha", "sigma_season", "sigma_scorer", "sigma_source"],
        hdi_prob=0.94,
    )
    rows: list[BayesPosteriorRow] = []
    for variable, row in summary_df.iterrows():
        rows.append(
            BayesPosteriorRow(
                variable=str(variable),
                coord_label=None,
                mean=float(row["mean"]),
                sd=float(row["sd"]),
                hdi_lower=float(row["hdi_3%"]),
                hdi_upper=float(row["hdi_97%"]),
                ess_bulk=float(row["ess_bulk"]),
                ess_tail=float(row["ess_tail"]),
                rhat=float(row["r_hat"]),
            )
        )
    return BayesPosteriorSummary(rows=tuple(rows))


def _diagnostics_from_idata(
    idata: az.InferenceData,
    *,
    calibration_ece: float | None,
    posterior_predictive_max_bucket_dev: float | None,
) -> BayesDiagnosticsSummary:
    rhat_ds = az.rhat(idata)
    ess_bulk_ds = az.ess(idata, method="bulk")
    ess_tail_ds = az.ess(idata, method="tail")

    def _finite_floats(ds: object) -> list[float]:
        out: list[float] = []
        for name in ds.data_vars:
            arr = np.asarray(ds[name].values, dtype=np.float64).ravel()
            out.extend(float(x) for x in arr[np.isfinite(arr)].tolist())
        return out

    rhat_values = _finite_floats(rhat_ds)
    ess_bulk_values = _finite_floats(ess_bulk_ds)
    ess_tail_values = _finite_floats(ess_tail_ds)

    rhat_max = max(rhat_values) if rhat_values else float("nan")
    ess_bulk_min = min(ess_bulk_values) if ess_bulk_values else float("nan")
    ess_tail_min = min(ess_tail_values) if ess_tail_values else float("nan")

    sample_stats = getattr(idata, "sample_stats", None)
    divergences = 0
    if sample_stats is not None and "diverging" in sample_stats:
        divergences = int(np.asarray(sample_stats["diverging"].values).sum())
    posterior = idata.posterior
    total_draws = int(posterior.sizes["chain"] * posterior.sizes["draw"])

    return BayesDiagnosticsSummary(
        rhat_max=rhat_max,
        ess_bulk_min=ess_bulk_min,
        ess_tail_min=ess_tail_min,
        divergences=divergences,
        total_draws=total_draws,
        calibration_ece=calibration_ece,
        posterior_predictive_max_bucket_dev=posterior_predictive_max_bucket_dev,
    )


def _posterior_mean_p_observed(idata: az.InferenceData) -> np.ndarray:
    p = idata.posterior["p_observed"]
    return np.asarray(p.mean(dim=("chain", "draw")).values, dtype=np.float64)


def _posterior_predictive_bucket_dev(
    idata: az.InferenceData, y: np.ndarray, *, n_bins: int = 10
) -> tuple[float | None, pl.DataFrame]:
    if (
        idata.posterior_predictive is None
        or "observed" not in idata.posterior_predictive
    ):
        return None, pl.DataFrame()
    p_mean = _posterior_mean_p_observed(idata)
    rep_mean = np.asarray(
        idata.posterior_predictive["observed"].mean(dim=("chain", "draw")).values,
        dtype=np.float64,
    )
    bin_edges = np.linspace(0.0, 1.0, n_bins + 1)
    bin_ids = np.clip(np.digitize(p_mean, bin_edges[1:-1], right=False), 0, n_bins - 1)
    out_records: list[dict[str, float | int]] = []
    max_dev = 0.0
    for b in range(n_bins):
        mask = bin_ids == b
        n = int(mask.sum())
        if n == 0:
            continue
        emp = float(np.mean(y[mask]))
        rep = float(np.mean(rep_mean[mask]))
        pred = float(np.mean(p_mean[mask]))
        dev = abs(emp - rep)
        if dev > max_dev:
            max_dev = dev
        out_records.append(
            {
                "bin": b,
                "count": n,
                "predicted_mean": pred,
                "posterior_predictive_mean": rep,
                "empirical_rate": emp,
                "abs_deviation": dev,
            }
        )
    return max_dev, pl.DataFrame(out_records)


def _reliability_dataframe(
    p_mean: np.ndarray, y: np.ndarray, *, n_bins: int = 15
) -> pl.DataFrame:
    rows = reliability_curve(p_mean, y.astype(np.int64), n_bins=n_bins)
    if not rows:
        return pl.DataFrame()
    return pl.DataFrame(
        {
            "predicted_mean": [r[0] for r in rows],
            "empirical_rate": [r[1] for r in rows],
            "count": [r[2] for r in rows],
        }
    )


def _posterior_summary_dataframe(summary: BayesPosteriorSummary) -> pl.DataFrame:
    if not summary.rows:
        return pl.DataFrame()
    return pl.DataFrame(
        {
            "variable": [r.variable for r in summary.rows],
            "coord_label": [r.coord_label or "" for r in summary.rows],
            "mean": [r.mean for r in summary.rows],
            "sd": [r.sd for r in summary.rows],
            "hdi_lower": [r.hdi_lower for r in summary.rows],
            "hdi_upper": [r.hdi_upper for r in summary.rows],
            "ess_bulk": [r.ess_bulk for r in summary.rows],
            "ess_tail": [r.ess_tail for r in summary.rows],
            "rhat": [r.rhat for r in summary.rows],
        }
    )


def run_bayes_model(
    *,
    model_name: str,
    dataset_artifact_id: str,
    artifact_id: str,
    source_snapshot_id: str,
    gamma_dl: str = "zero",
    smoke: bool = False,
    prior_only: bool = False,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    artifact_root: Path = BAYES_ROOT,
    dataset_root: Path = DATASETS_ROOT,
) -> ArtifactManifest:
    if gamma_dl != "zero":
        raise NotImplementedError(
            f"gamma_dl flavor {gamma_dl!r} lands in PR2; PR1 fixes gamma_dl=0"
        )
    if model_name not in _DIMENSION_BY_MODEL:
        raise NotImplementedError(
            f"model_name {model_name!r} not in PR1 scope; available: {sorted(_DIMENSION_BY_MODEL)}"
        )
    dimension = _DIMENSION_BY_MODEL[model_name]

    dataset_parquet = (
        dataset_root / _DATASET_NAME / dataset_artifact_id / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {dataset_parquet}; run prepare-dataset first."
        )

    env_limit = os.environ.get("BC_STATS_SMOKE_LIMIT")
    if smoke_limit is None:
        if env_limit is not None:
            smoke_limit = int(env_limit)
        elif smoke:
            smoke_limit = DEFAULT_SMOKE_LIMIT

    inputs = prepare_observation_inputs(
        dataset_parquet,
        dimension=dimension,
        smoke_limit=smoke_limit,
        seed=seed,
    )
    priors = BayesPriorConfig()
    model = build_trajectory_observedness_model(inputs, priors=priors)

    sampler = _resolve_sampler_config(smoke=smoke, override_seed=seed)
    sampler_record = BayesSamplerConfig(
        draws=sampler.draws,
        tune=sampler.tune,
        chains=sampler.chains,
        target_accept=sampler.target_accept,
        random_seed=sampler.random_seed,
        is_smoke=smoke,
    )

    artifact_dir = bayes_artifact_dir(model_name, artifact_id, root=artifact_root)
    inference_dir = bayes_inference_dir(model_name, artifact_id, root=artifact_root)
    exports_dir = bayes_exports_dir(model_name, artifact_id, root=artifact_root)
    validation_dir = bayes_validation_dir(model_name, artifact_id, root=artifact_root)
    artifact_dir.mkdir(parents=True, exist_ok=True)

    _log.info("bayes prior_predictive model=%s artifact=%s", model_name, artifact_id)
    prior_idata = prior_predictive(model, sampler)
    prior_path = inference_dir / "prior_predictive.nc"
    _atomic_write_netcdf(prior_idata, prior_path)

    inference_files: dict[str, Path] = {"prior_predictive": prior_path}
    posterior_summary = BayesPosteriorSummary()
    calibration_ece: float | None = None
    max_bucket_dev: float | None = None
    posterior_idata: az.InferenceData | None = None

    if not prior_only:
        _log.info("bayes sample model=%s artifact=%s", model_name, artifact_id)
        posterior_idata = sample_model(model, sampler)
        posterior_path = inference_dir / "posterior.nc"
        _atomic_write_netcdf(posterior_idata, posterior_path)
        inference_files["posterior"] = posterior_path

        _log.info(
            "bayes posterior_predictive model=%s artifact=%s",
            model_name,
            artifact_id,
        )
        pp_idata = posterior_predictive(model, posterior_idata, sampler)
        posterior_idata.extend(pp_idata)
        pp_path = inference_dir / "posterior_predictive.nc"
        _atomic_write_netcdf(posterior_idata, pp_path)
        inference_files["posterior_predictive"] = pp_path

        posterior_summary = _build_posterior_summary(posterior_idata)
        p_mean = _posterior_mean_p_observed(posterior_idata)
        y_int = inputs.y.astype(np.int64)
        calibration_ece = expected_calibration_error(p_mean, y_int, n_bins=15)
        max_bucket_dev, bucket_df = _posterior_predictive_bucket_dev(
            posterior_idata, y_int, n_bins=10
        )

        write_parquet_atomic(
            _reliability_dataframe(p_mean, y_int),
            exports_dir / "calibration_curve.parquet",
        )
        if not bucket_df.is_empty():
            write_parquet_atomic(
                bucket_df, exports_dir / "posterior_predictive_buckets.parquet"
            )
        write_parquet_atomic(
            _posterior_summary_dataframe(posterior_summary),
            exports_dir / "posterior_summary.parquet",
        )

    diagnostics_summary = (
        _diagnostics_from_idata(
            posterior_idata,
            calibration_ece=calibration_ece,
            posterior_predictive_max_bucket_dev=max_bucket_dev,
        )
        if posterior_idata is not None
        else BayesDiagnosticsSummary(
            rhat_max=float("nan"),
            ess_bulk_min=float("nan"),
            ess_tail_min=float("nan"),
            divergences=0,
            total_draws=0,
            calibration_ece=None,
            posterior_predictive_max_bucket_dev=None,
        )
    )

    diagnostics_payload: dict[str, object] = {
        "rhat_max": diagnostics_summary.rhat_max,
        "ess_bulk_min": diagnostics_summary.ess_bulk_min,
        "ess_tail_min": diagnostics_summary.ess_tail_min,
        "divergences": diagnostics_summary.divergences,
        "total_draws": diagnostics_summary.total_draws,
        "calibration_ece": diagnostics_summary.calibration_ece,
        "posterior_predictive_max_bucket_dev": (
            diagnostics_summary.posterior_predictive_max_bucket_dev
        ),
        "is_smoke": smoke,
    }
    _atomic_write_text(
        validation_dir / "diagnostics.json",
        json.dumps(diagnostics_payload, indent=2, default=_json_default),
    )

    extras = BayesArtifactExtras(
        model_name=model_name,
        model_version=MODEL_VERSION,
        dimension=dimension,
        gamma_dl_flavor="gamma_dl_zero",
        prior_config=priors,
        sampler_config=sampler_record,
        posterior_summary=posterior_summary,
        diagnostics_summary=diagnostics_summary,
        ablation_status="gamma_dl_zero",
        dl_proposal_inputs=(),
        inference_files=inference_files,
    )

    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="bayes",
        name=model_name,
        version=MODEL_VERSION,
        created_at=utc_now(),
        source_snapshot_id=source_snapshot_id,
        dataset_artifact_id=dataset_artifact_id,
        input_artifact_ids=(dataset_artifact_id,),
        output_paths={k: v for k, v in inference_files.items()},
        package_versions=package_versions(),
        random_seed=sampler.random_seed,
        ablation_status="gamma_dl_zero",
        metadata={
            "is_smoke": smoke,
            "prior_only": prior_only,
            "smoke_limit": smoke_limit if smoke_limit is not None else 0,
            "row_count": int(inputs.y.shape[0]),
        },
        bayes_extras=extras,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    _log.info(
        "bayes fit complete model=%s artifact=%s rows=%d smoke=%s",
        model_name,
        artifact_id,
        int(inputs.y.shape[0]),
        smoke,
    )
    return manifest


def _json_default(value: object) -> object:
    if isinstance(value, (np.floating, np.integer)):
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported JSON type {type(value)!r}")
