"""End-to-end Bayes fit dispatcher (event-grain v1).

Looks up a registered ``BayesTargetSpec`` by name, prepares event-grain
inputs, samples prior predictive, NUTS posterior (unless ``prior_only``),
and posterior predictive. Writes ``inference/*.nc``,
``exports/{posterior_summary,calibration_curve,event_propensity}.parquet``,
``validation/diagnostics.json``, and the manifest atomically.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportUnusedCallResult=false, reportAttributeAccessIssue=false, reportIndexIssue=false, reportOperatorIssue=false, reportMissingTypeArgument=false, reportArgumentType=false, reportCallIssue=false

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import arviz as az
import numpy as np
import polars as pl

from python_models.statistical.bayes.artifacts import (
    bayes_artifact_dir,
    bayes_exports_dir,
    bayes_inference_dir,
    bayes_validation_dir,
)
from python_models.statistical.bayes.registry import get_target
from python_models.statistical.calibration import (
    expected_calibration_error,
    reliability_curve,
)
from python_models.statistical.config import BAYES_ROOT, DATASETS_ROOT
from python_models.statistical.manifests import (
    find_published_manifest,
    package_versions,
    utc_now,
    write_manifest,
)
from python_models.statistical.models._credit_data import (
    EventCreditInputs,
    FixedEffectDesign,
    HeldOutSet,
    ProductionScoringFrame,
    build_production_scoring_frame,
)
from python_models.statistical.models._event_data import (
    DEFAULT_SEED,
    DEFAULT_SMOKE_LIMIT,
    EventObservationInputs,
)
from python_models.statistical.outputs import write_parquet_atomic
from python_models.statistical.pymc_utils import (
    DEFAULT_CONFIG,
    SMOKE_CONFIG,
    NutsBackend,
    SamplingConfig,
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

MODEL_VERSION: str = "0.3.0"

POSTERIOR_CHUNK_DEFAULT: int = 250_000
POSTERIOR_CREDIT_CHUNK_DEFAULT: int = 5_000
POSTERIOR_EXPORT_FILENAME: str = "event_propensity.parquet"
CREDIT_EXPORT_FILENAME: str = "event_credit.parquet"

N_POSITIONS_EXPORT: int = 9

HELD_OUT_MARGINALIZED_LIMIT: int = 100_000


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


_NETCDF_ALLOWED_ATTR_TYPES: tuple[type, ...] = (
    str,
    int,
    float,
    bool,
    bytes,
    list,
    tuple,
    np.ndarray,
    np.number,
    type(None),
)


def _sanitize_attrs_dict(attrs: dict[str, object]) -> None:
    for key, value in list(attrs.items()):
        if not isinstance(value, _NETCDF_ALLOWED_ATTR_TYPES):
            attrs[key] = json.dumps(value, default=str)


def _sanitize_idata_attrs(idata: az.InferenceData) -> None:
    """Stringify non-primitive attrs so xarray's netcdf writer accepts them.

    nutpie attaches a nested ``dict`` to one of the group attrs at full
    sample sizes (it survives the small smoke probe but trips the
    netcdf writer at DEFAULT_CONFIG). The netcdf engine only allows
    ``str / Number / ndarray / bytes / list / tuple`` attr values, so
    JSON-encode anything else in place across every group, data
    variable, and coord.
    """
    found_non_primitive: list[tuple[str, str, str]] = []
    root_attrs = getattr(idata, "_attrs", None) or getattr(idata, "attrs", None)
    if root_attrs is not None:
        for k, v in root_attrs.items():
            if not isinstance(v, _NETCDF_ALLOWED_ATTR_TYPES):
                found_non_primitive.append(("root", k, type(v).__name__))
        _sanitize_attrs_dict(root_attrs)
    for group_name in idata.groups():
        group = getattr(idata, group_name)
        for k, v in group.attrs.items():
            if not isinstance(v, _NETCDF_ALLOWED_ATTR_TYPES):
                found_non_primitive.append((f"group:{group_name}", k, type(v).__name__))
        _sanitize_attrs_dict(group.attrs)
        for var_name, da in group.data_vars.items():
            for k, v in da.attrs.items():
                if not isinstance(v, _NETCDF_ALLOWED_ATTR_TYPES):
                    found_non_primitive.append(
                        (f"var:{group_name}.{var_name}", k, type(v).__name__)
                    )
            _sanitize_attrs_dict(da.attrs)
        for coord_name, da in group.coords.items():
            for k, v in da.attrs.items():
                if not isinstance(v, _NETCDF_ALLOWED_ATTR_TYPES):
                    found_non_primitive.append(
                        (f"coord:{group_name}.{coord_name}", k, type(v).__name__)
                    )
            _sanitize_attrs_dict(da.attrs)
    if found_non_primitive:
        _log.info("sanitized non-primitive idata attrs: %s", found_non_primitive)


def _atomic_write_netcdf(idata: az.InferenceData, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target.parent), prefix=f".{target.stem}.", suffix=".nc"
    )
    os.close(fd)
    tmp_path = Path(tmp_name)
    try:
        _sanitize_idata_attrs(idata)
        try:
            idata.to_netcdf(str(tmp_path))
        except TypeError:
            _log.error(
                "to_netcdf TypeError; per-group attrs dump:\n%s",
                _format_idata_attrs_dump(idata),
            )
            raise
        os.replace(tmp_path, target)
    except BaseException:
        try:
            tmp_path.unlink()
        except FileNotFoundError:
            pass
        raise


def _format_idata_attrs_dump(idata: az.InferenceData) -> str:
    lines: list[str] = []
    root_attrs = getattr(idata, "_attrs", None) or getattr(idata, "attrs", None)
    if root_attrs is not None:
        for k, v in root_attrs.items():
            lines.append(
                f"  root.attrs[{k!r}] type={type(v).__name__} value={repr(v)[:200]}"
            )
    for group_name in idata.groups():
        group = getattr(idata, group_name)
        for k, v in group.attrs.items():
            lines.append(
                f"  group {group_name}.attrs[{k!r}] type={type(v).__name__} value={repr(v)[:200]}"
            )
        for var_name, da in group.data_vars.items():
            for k, v in da.attrs.items():
                lines.append(
                    f"  var {group_name}.{var_name}.attrs[{k!r}] type={type(v).__name__} value={repr(v)[:200]}"
                )
        for coord_name, da in group.coords.items():
            for k, v in da.attrs.items():
                lines.append(
                    f"  coord {group_name}.{coord_name}.attrs[{k!r}] type={type(v).__name__} value={repr(v)[:200]}"
                )
    return "\n".join(lines)


def _resolve_sampler_config(
    *, smoke: bool, override_seed: int | None
) -> SamplingConfig:
    base = SMOKE_CONFIG if smoke else DEFAULT_CONFIG
    backend_override = os.environ.get("BC_STATS_BAYES_BACKEND")
    if override_seed is None and backend_override is None:
        return base
    backend_str = backend_override if backend_override is not None else base.backend
    backend = cast("NutsBackend", backend_str)
    return SamplingConfig(
        draws=base.draws,
        tune=base.tune,
        chains=base.chains,
        target_accept=base.target_accept,
        random_seed=override_seed if override_seed is not None else base.random_seed,
        cores=base.cores,
        max_treedepth=base.max_treedepth,
        backend=backend,
    )


def _build_posterior_summary(
    idata: az.InferenceData, *, outcome_kind: str = "bernoulli"
) -> BayesPosteriorSummary:
    posterior = idata.posterior
    if outcome_kind == "multinomial":
        candidate_names: list[str] = [
            "alpha_position",
            "sigma_season",
            "sigma_scorer",
            "sigma_park",
            "sigma_source",
            "beta_season",
            "beta_scorer",
            "beta_park",
            "beta_source",
        ]
        candidate_names.extend(
            name for name in posterior.data_vars if str(name).startswith("delta_")
        )
    else:
        candidate_names = [
            "alpha",
            "sigma_season",
            "sigma_scorer",
            "sigma_park",
            "sigma_source",
        ]
        candidate_names.extend(
            name for name in posterior.data_vars if str(name).startswith("gamma_")
        )
        candidate_names.extend(
            name
            for name in posterior.data_vars
            if str(name).startswith("delta_missing_")
        )
    var_names = [n for n in candidate_names if n in posterior.data_vars]
    summary_df = az.summary(idata, var_names=var_names, hdi_prob=0.94)
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
    outcome_kind: str = "bernoulli",
) -> BayesDiagnosticsSummary:
    posterior = idata.posterior
    if outcome_kind == "multinomial":
        multinomial_focus = {
            "alpha_position",
            "beta_season",
            "beta_scorer",
            "beta_park",
            "beta_source",
            "z_scorer",
            "z_park",
            "z_source",
        }
        relevant_names = [
            name
            for name in posterior.data_vars
            if str(name) in multinomial_focus or str(name).startswith("delta_")
        ]
    else:
        relevant_names = list(posterior.data_vars)
    rhat_ds = az.rhat(idata, var_names=relevant_names)
    ess_bulk_ds = az.ess(idata, var_names=relevant_names, method="bulk")
    ess_tail_ds = az.ess(idata, var_names=relevant_names, method="tail")

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


def _posterior_event_means_bernoulli(
    idata: az.InferenceData,
    inputs: EventObservationInputs,
    *,
    chunk_size: int = POSTERIOR_CHUNK_DEFAULT,
) -> np.ndarray:
    """Compute ``E[sigmoid(η_e)] | data`` per event from posterior parameter draws.

    Reconstructs the linear predictor per event by indexing the posterior
    parameter arrays (alpha, betas, deltas, gammas) — no per-event
    posterior tensor is ever materialized in the model graph (the
    Bernoulli uses ``logit_p=eta`` directly). One ``(chain, draw,
    chunk_size)`` slab is built at a time, sigmoided, and reduced over
    the sample dims; only the chunk-wide mean vector stays in RAM after
    each iteration.
    """
    posterior = idata.posterior
    n_event = inputs.n_events
    alpha = np.asarray(posterior["alpha"].values, dtype=np.float64)
    beta_season = np.asarray(posterior["beta_season"].values, dtype=np.float64)
    beta_scorer = np.asarray(posterior["beta_scorer"].values, dtype=np.float64)
    beta_park = np.asarray(posterior["beta_park"].values, dtype=np.float64)
    n_chain = beta_season.shape[0]
    n_draw = beta_season.shape[1]

    beta_source: np.ndarray | None = None
    if "beta_source" in posterior:
        beta_source = np.asarray(posterior["beta_source"].values, dtype=np.float64)

    deltas_full: dict[str, np.ndarray] = {}
    for column, design in inputs.fixed_effects.items():
        if len(design.levels) <= 1:
            continue
        deltas_full[column] = np.asarray(
            posterior[f"delta_{column}"].values, dtype=np.float64
        )

    gammas: dict[str, np.ndarray] = {}
    delta_missing: dict[str, np.ndarray] = {}
    for column in inputs.continuous:
        gammas[column] = np.asarray(
            posterior[f"gamma_{column}"].values, dtype=np.float64
        )
        if f"delta_missing_{column}" in posterior:
            delta_missing[column] = np.asarray(
                posterior[f"delta_missing_{column}"].values, dtype=np.float64
            )

    means = np.empty(n_event, dtype=np.float64)
    for start in range(0, n_event, chunk_size):
        stop = min(start + chunk_size, n_event)
        sl = slice(start, stop)
        eta = np.broadcast_to(alpha[:, :, None], (n_chain, n_draw, stop - start)).copy()
        eta += beta_season[:, :, inputs.season_idx[sl]]
        eta += beta_scorer[:, :, inputs.scorer_idx[sl]]
        eta += beta_park[:, :, inputs.park_idx[sl]]
        if beta_source is not None:
            eta += beta_source[:, :, inputs.source_idx[sl]]
        for column, df in deltas_full.items():
            eta += df[:, :, inputs.fixed_effects[column].codes[sl]]
        for column, gamma in gammas.items():
            values = inputs.continuous[column].values[sl].astype(np.float64)
            eta += gamma[:, :, None] * values[None, None, :]
            if column in delta_missing:
                is_missing = inputs.continuous[column].is_missing[sl].astype(np.float64)
                eta += delta_missing[column][:, :, None] * is_missing[None, None, :]
        p = 1.0 / (1.0 + np.exp(-eta))
        means[sl] = p.mean(axis=(0, 1))
    return means


def _bucket_dev_from_p_mean(
    p_mean: np.ndarray, y: np.ndarray, *, n_bins: int = 10
) -> tuple[float | None, pl.DataFrame]:
    """Bin events by ``p_mean`` and report max ``|empirical - p_mean|`` per bucket.

    Since ``E[y_rep] = E[sigmoid(η)] = p_mean`` analytically for the
    Bernoulli likelihood, the historical bucket dev (``|empirical -
    rep_mean|``) is — modulo Bernoulli sampling noise that vanishes at
    1000+ draws — the same statistic, and we no longer need to draw
    posterior-predictive samples to compute it.
    """
    if p_mean.size == 0:
        return None, pl.DataFrame()
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
        pred = float(np.mean(p_mean[mask]))
        dev = abs(emp - pred)
        if dev > max_dev:
            max_dev = dev
        out_records.append(
            {
                "bin": b,
                "count": n,
                "predicted_mean": pred,
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


def _export_event_propensities(
    means: np.ndarray,
    inputs: EventObservationInputs,
    *,
    target_path: Path,
) -> pl.DataFrame:
    """Write per-event posterior mean ``p_observed`` to parquet."""
    df = pl.DataFrame(
        {
            "event_key": inputs.event_keys.astype(np.int64),
            "dimension": [inputs.dimension] * inputs.n_events,
            "p_observed_mean": means,
        }
    )
    write_parquet_atomic(df, target_path)
    _log.info(
        "wrote event_propensity export rows=%d path=%s",
        inputs.n_events,
        target_path,
    )
    return df


def _posterior_held_out_softmax(
    idata: az.InferenceData,
    held_out: HeldOutSet,
    *,
    n_positions: int,
    chunk_size: int = POSTERIOR_CREDIT_CHUNK_DEFAULT,
) -> np.ndarray:
    """Compute ``E[softmax(eta_e)] | data`` per held-out event from posterior draws."""
    posterior = idata.posterior
    K = n_positions
    n_event = held_out.n_events
    if n_event == 0:
        return np.zeros((0, K), dtype=np.float64)
    alpha_position = np.asarray(posterior["alpha_position"].values, dtype=np.float64)
    n_chain, n_draw = alpha_position.shape[0], alpha_position.shape[1]
    deltas_fe: dict[str, np.ndarray] = {}
    for column, design in held_out.fixed_effects.items():
        if len(design.levels) <= 1:
            continue
        if f"delta_{column}" not in posterior:
            continue
        deltas_fe[column] = np.asarray(
            posterior[f"delta_{column}"].values, dtype=np.float64
        )

    means = np.empty((n_event, K), dtype=np.float64)
    for start in range(0, n_event, chunk_size):
        stop = min(start + chunk_size, n_event)
        sl = slice(start, stop)
        n_chunk = stop - start
        eta = np.broadcast_to(
            alpha_position[:, :, None, :], (n_chain, n_draw, n_chunk, K)
        ).copy()
        for column, df in deltas_fe.items():
            codes = held_out.fixed_effects[column].codes[sl]
            valid = codes >= 0
            if valid.all():
                eta += df[:, :, codes, :]
            else:
                safe_codes = np.where(valid, codes, 0)
                contrib = df[:, :, safe_codes, :]
                eta += contrib * valid[None, None, :, None]
        eta -= eta.max(axis=-1, keepdims=True)
        exp_eta = np.exp(eta)
        pi = exp_eta / exp_eta.sum(axis=-1, keepdims=True)
        means[sl, :] = pi.mean(axis=(0, 1))
    return means


def _evaluate_held_out(
    inputs: EventCreditInputs,
    idata: az.InferenceData,
    *,
    putout_idata: az.InferenceData | None = None,
) -> dict[str, object]:
    """Compute OOS top-k accuracy, log-loss, and per-position PR-AUC.

    Held-out events have observed Y (true position) and are excluded
    from training via the deterministic game-hash holdout. For the assist
    K=10 model, when ``putout_idata`` is supplied the per-event softmax is
    marginalized over the putout posterior — the production-faithful
    metric, since putout_position is unobserved on the inference target.
    Without it (or for putout), the observed-putout softmax is scored and
    ``putout_marginalized`` is recorded ``False``.
    """
    from sklearn.metrics import average_precision_score, log_loss

    held = inputs.held_out
    if held.n_events == 0:
        return {"n_events": 0}

    marginalize = (
        inputs.credit_type == "assist"
        and inputs.n_positions == 10
        and putout_idata is not None
    )
    observed_shares: np.ndarray | None = None
    if marginalize:
        assert putout_idata is not None
        held = _subsample_held_out(
            held, limit=HELD_OUT_MARGINALIZED_LIMIT, seed=DEFAULT_SEED
        )
        putout_weights = _score_putout_posterior(putout_idata, held)
        shares = _posterior_event_softmax_putout_marginalized(
            idata, held, putout_posterior=putout_weights, n_positions=inputs.n_positions
        )
        observed_shares = _posterior_held_out_softmax(
            idata, held, n_positions=inputs.n_positions
        )
    else:
        shares = _posterior_held_out_softmax(
            idata, held, n_positions=inputs.n_positions
        )
    y_true = held.true_position.astype(np.int64)
    valid = y_true >= 0
    if not valid.any():
        return {"n_events": int(held.n_events), "valid": 0}
    shares = shares[valid]
    y = y_true[valid]
    eps = 1e-12
    safe_shares = np.clip(shares, eps, 1.0)
    top1 = float((np.argmax(safe_shares, axis=1) == y).mean())
    top3_idx = np.argsort(-safe_shares, axis=1)[:, :3]
    top3 = float(np.any(top3_idx == y[:, None], axis=1).mean())
    ll = float(
        log_loss(
            y,
            safe_shares,
            labels=list(range(inputs.n_positions)),
        )
    )
    pr_auc_per_pos: dict[str, float] = {}
    for k in range(inputs.n_positions):
        y_bin = (y == k).astype(np.int64)
        if int(y_bin.sum()) == 0 or int(y_bin.sum()) == y_bin.shape[0]:
            pr_auc_per_pos[str(k + 1)] = float("nan")
            continue
        try:
            pr_auc_per_pos[str(k + 1)] = float(
                average_precision_score(y_bin, safe_shares[:, k])
            )
        except ValueError:
            pr_auc_per_pos[str(k + 1)] = float("nan")
    finite_aucs = [v for v in pr_auc_per_pos.values() if np.isfinite(v)]
    pr_auc_macro = float(np.mean(finite_aucs)) if finite_aucs else float("nan")
    baseline_top1 = float(
        max(
            (int((y == k).sum()) for k in range(inputs.n_positions)),
            default=0,
        )
        / max(y.shape[0], 1)
    )
    distribution = _distribution_calibration(safe_shares, y, n_positions=inputs.n_positions)
    slice_columns: dict[str, np.ndarray] = {
        "season": held.season_idx[valid],
        "source_family": held.source_idx[valid],
        "scorer": held.scorer_idx[valid],
        "park": held.park_idx[valid],
    }
    for col, design in held.fixed_effects.items():
        if marginalize and col == "putout_position":
            continue
        slice_columns[col] = design.codes[valid]
    slice_calibration: dict[str, dict[str, float | int]] = {
        name: _slice_calibration_summary(
            safe_shares, y, codes=codes, n_positions=inputs.n_positions
        )
        for name, codes in slice_columns.items()
    }

    result: dict[str, object] = {
        "n_events": int(held.n_events),
        "n_evaluated": int(valid.sum()),
        "putout_marginalized": marginalize,
        "top1_accuracy": top1,
        "top3_accuracy": top3,
        "log_loss": ll,
        "pr_auc_per_position": pr_auc_per_pos,
        "pr_auc_macro": pr_auc_macro,
        "baseline_top1_accuracy": baseline_top1,
        "distribution_calibration": distribution,
        "slice_calibration": slice_calibration,
    }
    if inputs.credit_type == "assist" and inputs.n_positions == 10:
        none_idx = inputs.n_positions - 1
        y_any = (y != none_idx).astype(np.int64)
        any_score = 1.0 - safe_shares[:, none_idx]
        if 0 < int(y_any.sum()) < y_any.shape[0]:
            try:
                pr_auc_any = float(average_precision_score(y_any, any_score))
            except ValueError:
                pr_auc_any = float("nan")
        else:
            pr_auc_any = float("nan")
        baseline_pr_auc_any = float(y_any.mean())
        result["any_assist"] = {
            "n_events": int(y_any.shape[0]),
            "empirical_rate": float(y_any.mean()),
            "pr_auc": pr_auc_any,
            "baseline_pr_auc": baseline_pr_auc_any,
        }
        if observed_shares is not None:
            obs = np.clip(observed_shares[valid], eps, 1.0)
            obs_any = 1.0 - obs[:, none_idx]
            try:
                obs_pr_auc_any = float(average_precision_score(y_any, obs_any))
            except ValueError:
                obs_pr_auc_any = float("nan")
            result["observed_putout_upper_bound"] = {
                "top1_accuracy": float((np.argmax(obs, axis=1) == y).mean()),
                "any_assist_pr_auc": obs_pr_auc_any,
            }
    return result


def _distribution_calibration(
    shares: np.ndarray, y: np.ndarray, *, n_positions: int
) -> dict[str, object]:
    """Global per-position predicted vs empirical share + total variation distance."""
    n = int(y.shape[0])
    if n == 0:
        return {"n_evaluated": 0}
    predicted_share = shares.mean(axis=0)
    empirical_share = np.bincount(y, minlength=n_positions) / n
    abs_dev = np.abs(predicted_share - empirical_share)
    tv = 0.5 * float(abs_dev.sum())
    per_position: dict[str, dict[str, float]] = {}
    for k in range(n_positions):
        per_position[str(k + 1)] = {
            "predicted_share": float(predicted_share[k]),
            "empirical_share": float(empirical_share[k]),
            "abs_dev": float(abs_dev[k]),
        }
    return {
        "n_evaluated": n,
        "per_position": per_position,
        "max_abs_dev": float(abs_dev.max()) if abs_dev.size else 0.0,
        "total_variation_distance": tv,
    }


def _slice_calibration_summary(
    shares: np.ndarray,
    y: np.ndarray,
    *,
    codes: np.ndarray,
    n_positions: int,
    min_slice_n: int = 50,
) -> dict[str, float | int]:
    """Per-slice total-variation distance: weighted mean + max across qualifying slices."""
    codes = np.asarray(codes, dtype=np.int64)
    unique = np.unique(codes)
    n_qualifying = 0
    total_n = 0
    weighted_sum = 0.0
    max_tv = 0.0
    for s in unique:
        mask = codes == s
        n_s = int(mask.sum())
        if n_s < min_slice_n:
            continue
        pred = shares[mask].mean(axis=0)
        emp = np.bincount(y[mask], minlength=n_positions) / n_s
        tv = 0.5 * float(np.abs(pred - emp).sum())
        n_qualifying += 1
        total_n += n_s
        weighted_sum += tv * n_s
        if tv > max_tv:
            max_tv = tv
    return {
        "n_slices_scored": n_qualifying,
        "n_events_scored": total_n,
        "min_slice_n_threshold": int(min_slice_n),
        "weighted_total_variation": (weighted_sum / total_n) if total_n > 0 else 0.0,
        "max_total_variation": max_tv,
    }


_FixedEffectCarrier = EventCreditInputs | HeldOutSet | ProductionScoringFrame


def _posterior_event_softmax_putout_marginalized(
    idata: az.InferenceData,
    carrier: _FixedEffectCarrier,
    *,
    putout_posterior: np.ndarray,
    n_positions: int,
    chunk_size: int = POSTERIOR_CREDIT_CHUNK_DEFAULT,
) -> np.ndarray:
    """Compute ``E[π_e | data]`` marginalized over a per-event putout posterior.

    For each event ``e`` and putout candidate ``j ∈ {1..9}``,
    ``softmax_k(η_e + δ_PO[j])`` is weighted-averaged by
    ``P(PO_e = j) = putout_posterior[e, j-1]``. ``putout_posterior`` is
    shape ``(n_event, 9)`` with row-sums close to 1. Works on any carrier
    exposing ``fixed_effects`` + ``n_events`` (training inputs, held-out
    set, or production scoring frame).
    """
    posterior = idata.posterior
    k_out = n_positions
    n_event = carrier.n_events
    if "delta_putout_position" not in posterior:
        raise ValueError(
            "putout-marginalized softmax requires a putout_position FE in the model"
        )
    if putout_posterior.shape != (n_event, N_POSITIONS_EXPORT):
        raise AssertionError(
            f"putout_posterior shape mismatch: got {putout_posterior.shape}, "
            f"want ({n_event}, {N_POSITIONS_EXPORT})"
        )

    alpha_position = np.asarray(posterior["alpha_position"].values, dtype=np.float64)
    delta_po = np.asarray(
        posterior["delta_putout_position"].values, dtype=np.float64
    )
    n_chain, n_draw = alpha_position.shape[0], alpha_position.shape[1]

    po_design = carrier.fixed_effects.get("putout_position")
    if po_design is None:
        raise ValueError(
            "putout-marginalized softmax requires fixed_effects['putout_position']"
        )
    po_level_lookup: list[int] = []
    for position in range(1, N_POSITIONS_EXPORT + 1):
        label = str(position)
        if label not in po_design.levels:
            raise ValueError(
                f"cannot marginalize over putout_position={position}: level missing "
                "from training. Ensure _assert_putout_position_levels_in_training held."
            )
        po_level_lookup.append(po_design.levels.index(label))

    deltas_other: dict[str, np.ndarray] = {}
    for column, design in carrier.fixed_effects.items():
        if column == "putout_position" or len(design.levels) <= 1:
            continue
        deltas_other[column] = np.asarray(
            posterior[f"delta_{column}"].values, dtype=np.float64
        )

    means = np.empty((n_event, k_out), dtype=np.float64)
    for start in range(0, n_event, chunk_size):
        stop = min(start + chunk_size, n_event)
        sl = slice(start, stop)
        n_chunk = stop - start
        eta_base = np.broadcast_to(
            alpha_position[:, :, None, :], (n_chain, n_draw, n_chunk, k_out)
        ).copy()
        for column, df in deltas_other.items():
            codes = carrier.fixed_effects[column].codes[sl]
            valid = codes >= 0
            safe = np.where(valid, codes, 0)
            eta_base += df[:, :, safe, :] * valid[None, None, :, None]

        weights = np.asarray(putout_posterior[sl], dtype=np.float64)
        pi_marg = np.zeros((n_chain, n_draw, n_chunk, k_out), dtype=np.float64)
        for position_idx, level_idx in enumerate(po_level_lookup):
            eta_j = eta_base + delta_po[:, :, level_idx, :][:, :, None, :]
            eta_j -= eta_j.max(axis=-1, keepdims=True)
            exp_eta = np.exp(eta_j)
            pi_j = exp_eta / exp_eta.sum(axis=-1, keepdims=True)
            pi_marg += pi_j * weights[None, None, :, position_idx, None]
        means[sl, :] = pi_marg.mean(axis=(0, 1))
    return means


PUTOUT_MARGINALIZATION_MODEL: str = "putout_credit_allocation"


def _resolve_published_putout_idata(
    model_name: str = PUTOUT_MARGINALIZATION_MODEL,
) -> az.InferenceData | None:
    """Load the published putout posterior the assist model marginalizes over."""
    pointer = find_published_manifest(model_name)
    if pointer is None:
        return None
    data = json.loads(pointer.read_text())
    manifest_path = Path(data["manifest_path"])
    posterior_path = manifest_path.parent / "inference" / "posterior.nc"
    if not posterior_path.exists():
        _log.warning(
            "published putout pointer at %s but posterior missing at %s",
            pointer,
            posterior_path,
        )
        return None
    _log.info("resolved putout posterior for marginalization: %s", posterior_path)
    return az.from_netcdf(posterior_path)


def _score_putout_posterior(
    putout_idata: az.InferenceData,
    carrier: _FixedEffectCarrier,
    *,
    chunk_size: int = POSTERIOR_CREDIT_CHUNK_DEFAULT,
) -> np.ndarray:
    """Posterior-mean ``P(putout = j | data)`` per event, shape ``(n_event, 9)``.

    The putout model's FE level vocabularies are aligned to ``carrier``'s
    by label, so a putout fit with different level ordering or an extra
    level (e.g. ``walk``) still scores correctly.
    """
    posterior = putout_idata.posterior
    alpha = np.asarray(posterior["alpha_position"].values, dtype=np.float64)
    n_chain, n_draw = alpha.shape[0], alpha.shape[1]
    k_po = alpha.shape[-1]
    deltas: dict[str, np.ndarray] = {}
    remaps: dict[str, np.ndarray] = {}
    for column, design in carrier.fixed_effects.items():
        var = f"delta_{column}"
        if var not in posterior:
            continue
        da = posterior[var]
        level_dim = next(
            d for d in da.dims if d not in ("chain", "draw", "position")
        )
        putout_levels = [str(x) for x in da.coords[level_dim].values]
        index = {label: i for i, label in enumerate(putout_levels)}
        remaps[column] = np.array(
            [index.get(str(label), -1) for label in design.levels], dtype=np.int64
        )
        deltas[column] = np.asarray(da.values, dtype=np.float64)

    n_event = carrier.n_events
    out = np.empty((n_event, k_po), dtype=np.float64)
    for start in range(0, n_event, chunk_size):
        stop = min(start + chunk_size, n_event)
        sl = slice(start, stop)
        n_chunk = stop - start
        eta = np.broadcast_to(
            alpha[:, :, None, :], (n_chain, n_draw, n_chunk, k_po)
        ).copy()
        for column, df in deltas.items():
            codes = carrier.fixed_effects[column].codes[sl]
            remapped = remaps[column][np.where(codes >= 0, codes, 0)]
            present = (codes >= 0) & (remapped >= 0)
            safe = np.where(remapped >= 0, remapped, 0)
            eta += df[:, :, safe, :] * present[None, None, :, None]
        eta -= eta.max(axis=-1, keepdims=True)
        exp_eta = np.exp(eta)
        pi = exp_eta / exp_eta.sum(axis=-1, keepdims=True)
        out[sl, :] = pi.mean(axis=(0, 1))
    return out


def _subsample_held_out(
    held: HeldOutSet, *, limit: int, seed: int
) -> HeldOutSet:
    """Deterministically subsample a held-out set for tractable scoring."""
    n = held.n_events
    if n <= limit:
        return held
    rng = np.random.default_rng(seed)
    idx = np.sort(rng.choice(n, size=limit, replace=False))
    fixed_effects = {
        column: FixedEffectDesign(levels=design.levels, codes=design.codes[idx])
        for column, design in held.fixed_effects.items()
    }
    return HeldOutSet(
        event_keys=held.event_keys[idx],
        true_position=held.true_position[idx],
        U=held.U[idx],
        season_idx=held.season_idx[idx],
        scorer_idx=held.scorer_idx[idx],
        park_idx=held.park_idx[idx],
        source_idx=held.source_idx[idx],
        fixed_effects=fixed_effects,
    )


def _posterior_event_softmax(
    idata: az.InferenceData,
    inputs: EventCreditInputs,
    *,
    chunk_size: int = POSTERIOR_CREDIT_CHUNK_DEFAULT,
) -> np.ndarray:
    """Compute ``E[softmax(eta_e)] | data`` per (event, position) from posterior draws.

    Per-event additive RE / global FE terms cancel exactly inside the
    per-event softmax, so the export only reconstructs ``alpha_position``
    and the per-position FE deltas. One chunk of events at a time keeps
    RAM bounded.
    """
    posterior = idata.posterior
    K = inputs.n_positions
    n_event = inputs.n_events

    alpha_position = np.asarray(posterior["alpha_position"].values, dtype=np.float64)
    n_chain, n_draw = alpha_position.shape[0], alpha_position.shape[1]
    deltas_fe: dict[str, np.ndarray] = {}
    for column, design in inputs.fixed_effects.items():
        if len(design.levels) <= 1:
            continue
        deltas_fe[column] = np.asarray(
            posterior[f"delta_{column}"].values, dtype=np.float64
        )

    means = np.empty((n_event, K), dtype=np.float64)
    for start in range(0, n_event, chunk_size):
        stop = min(start + chunk_size, n_event)
        sl = slice(start, stop)
        n_chunk = stop - start
        eta = np.broadcast_to(
            alpha_position[:, :, None, :], (n_chain, n_draw, n_chunk, K)
        ).copy()
        for column, df in deltas_fe.items():
            codes = inputs.fixed_effects[column].codes[sl]
            eta += df[:, :, codes, :]
        eta -= eta.max(axis=-1, keepdims=True)
        exp_eta = np.exp(eta)
        pi = exp_eta / exp_eta.sum(axis=-1, keepdims=True)
        means[sl, :] = pi.mean(axis=(0, 1))
    return means


def _export_event_credit_shares(
    means: np.ndarray,
    *,
    event_keys: np.ndarray,
    credit_type: str,
    n_positions: int,
    target_path: Path,
) -> pl.DataFrame:
    """Write per-event-per-position expected share to parquet.

    Schema: ``(event_key int64, fielding_position int8,
    credit_type utf8, expected_share float64, none_share float64?)``.

    For K=9 (putout), one row per (event, position 1..9); ``none_share``
    is NULL. For K=10 (assist with NONE sentinel at column 9), emit
    positions 1..9 only — the NONE share rides along on every row as
    ``none_share = π[:, 9]`` so downstream can compute P(any assist).
    """
    n_event, k = means.shape
    if k != n_positions:
        raise AssertionError(
            f"means shape mismatch: K={k}, n_positions={n_positions}"
        )
    if n_event != event_keys.shape[0]:
        raise AssertionError(
            f"event count mismatch: means={n_event}, event_keys={event_keys.shape[0]}"
        )
    n_emit_positions = N_POSITIONS_EXPORT
    if k == n_emit_positions:
        none_per_event: np.ndarray | None = None
        shares_export = means
    elif k == n_emit_positions + 1:
        none_per_event = means[:, n_emit_positions].astype(np.float64)
        shares_export = means[:, :n_emit_positions]
    else:
        raise AssertionError(
            f"_export_event_credit_shares: unexpected K={k}; expected 9 (putout) or 10 (assist with NONE)"
        )

    repeated_keys = np.repeat(event_keys, n_emit_positions).astype(np.int64)
    positions = np.tile(np.arange(1, n_emit_positions + 1, dtype=np.int8), n_event)
    shares = shares_export.reshape(-1).astype(np.float64)
    data: dict[str, object] = {
        "event_key": repeated_keys,
        "fielding_position": positions,
        "credit_type": [credit_type] * (n_event * n_emit_positions),
        "expected_share": shares,
    }
    if none_per_event is not None:
        data["none_share"] = np.repeat(none_per_event, n_emit_positions).astype(np.float64)
    else:
        data["none_share"] = pl.Series(
            "none_share", [None] * (n_event * n_emit_positions), dtype=pl.Float64
        )
    df = pl.DataFrame(data)
    write_parquet_atomic(df, target_path)
    _log.info(
        "wrote event_credit export rows=%d K=%d path=%s",
        n_event * n_emit_positions,
        k,
        target_path,
    )
    return df


def run_bayes_model(
    *,
    model_name: str,
    dataset_artifact_id: str,
    artifact_id: str,
    source_snapshot_id: str,
    smoke: bool = False,
    prior_only: bool = False,
    smoke_limit: int | None = None,
    seed: int = DEFAULT_SEED,
    artifact_root: Path = BAYES_ROOT,
    dataset_root: Path = DATASETS_ROOT,
) -> ArtifactManifest:
    from python_models.statistical.bayes import targets as _targets  # noqa: F401  # pyright: ignore[reportUnusedImport]

    spec = get_target(model_name)
    dataset_parquet = (
        dataset_root / spec.dataset_name / dataset_artifact_id / "dataset.parquet"
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
        elif spec.sample_size is not None:
            smoke_limit = spec.sample_size

    prep_kwargs: dict[str, object] = {
        "dimension": spec.dataset_dimension_filter,
        "smoke_limit": smoke_limit,
        "seed": seed,
    }

    inputs = spec.prep_fn(dataset_parquet, **prep_kwargs)
    priors = BayesPriorConfig()
    model = spec.builder(inputs, priors=priors)

    sampler = _resolve_sampler_config(smoke=smoke, override_seed=seed)
    sampler_record = BayesSamplerConfig(
        draws=sampler.draws,
        tune=sampler.tune,
        chains=sampler.chains,
        target_accept=sampler.target_accept,
        random_seed=sampler.random_seed,
        max_treedepth=sampler.max_treedepth,
        is_smoke=smoke,
        backend=sampler.backend,
    )

    source_effect_active = len(inputs.coords["source"]) > 1
    _log.info(
        "bayes model=%s artifact=%s source_effect_active=%s backend=%s",
        model_name,
        artifact_id,
        source_effect_active,
        sampler.backend,
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
        progress_log = validation_dir / "sampling_progress.log"
        _log.info(
            "bayes sample model=%s artifact=%s progress_log=%s",
            model_name,
            artifact_id,
            progress_log,
        )
        posterior_idata = sample_model(model, sampler, progress_log_path=progress_log)
        posterior_path = inference_dir / "posterior.nc"
        _atomic_write_netcdf(posterior_idata, posterior_path)
        inference_files["posterior"] = posterior_path

        posterior_summary = _build_posterior_summary(
            posterior_idata, outcome_kind=spec.outcome_kind
        )
        if spec.outcome_kind == "bernoulli":
            p_mean = _posterior_event_means_bernoulli(posterior_idata, inputs)
            y_int = inputs.y.astype(np.int64)
            calibration_ece = expected_calibration_error(p_mean, y_int, n_bins=15)
            max_bucket_dev, bucket_df = _bucket_dev_from_p_mean(
                p_mean, y_int, n_bins=10
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
            _ = _export_event_propensities(
                p_mean, inputs, target_path=exports_dir / POSTERIOR_EXPORT_FILENAME
            )
        else:
            write_parquet_atomic(
                _posterior_summary_dataframe(posterior_summary),
                exports_dir / "posterior_summary.parquet",
            )
            putout_idata = (
                _resolve_published_putout_idata()
                if inputs.credit_type == "assist"
                else None
            )
            credit_export_path = exports_dir / CREDIT_EXPORT_FILENAME
            if inputs.credit_type == "assist" and putout_idata is not None:
                production = build_production_scoring_frame(
                    dataset_parquet,
                    dimension=spec.dataset_dimension_filter,
                    fixed_effects=inputs.fixed_effects,
                )
                if production.n_events > 0:
                    putout_weights = _score_putout_posterior(putout_idata, production)
                    production_shares = _posterior_event_softmax_putout_marginalized(
                        posterior_idata,
                        production,
                        putout_posterior=putout_weights,
                        n_positions=inputs.n_positions,
                    )
                    _ = _export_event_credit_shares(
                        production_shares,
                        event_keys=production.event_keys,
                        credit_type=inputs.credit_type,
                        n_positions=inputs.n_positions,
                        target_path=credit_export_path,
                    )
                    _log.info(
                        "assist export scored production slice events=%d (putout-marginalized)",
                        production.n_events,
                    )
                else:
                    _log.warning(
                        "assist production slice empty; exporting training grain instead"
                    )
                    shares = _posterior_event_softmax(posterior_idata, inputs)
                    _ = _export_event_credit_shares(
                        shares,
                        event_keys=inputs.event_keys,
                        credit_type=inputs.credit_type,
                        n_positions=inputs.n_positions,
                        target_path=credit_export_path,
                    )
            else:
                if inputs.credit_type == "assist":
                    _log.warning(
                        "no published putout posterior resolved; assist export uses "
                        "training grain and held-out metrics are observed-putout only"
                    )
                shares = _posterior_event_softmax(posterior_idata, inputs)
                _ = _export_event_credit_shares(
                    shares,
                    event_keys=inputs.event_keys,
                    credit_type=inputs.credit_type,
                    n_positions=inputs.n_positions,
                    target_path=credit_export_path,
                )
            held_out_metrics = _evaluate_held_out(
                inputs, posterior_idata, putout_idata=putout_idata
            )
            _atomic_write_text(
                validation_dir / "held_out_metrics.json",
                json.dumps(held_out_metrics, indent=2, default=_json_default),
            )
            _log.info(
                "bayes held-out metrics model=%s artifact=%s n_eval=%s top1=%.4f baseline_top1=%.4f",
                model_name,
                artifact_id,
                held_out_metrics.get("n_evaluated", 0),
                float(held_out_metrics.get("top1_accuracy", float("nan"))),
                float(held_out_metrics.get("baseline_top1_accuracy", float("nan"))),
            )

    diagnostics_summary = (
        _diagnostics_from_idata(
            posterior_idata,
            calibration_ece=calibration_ece,
            posterior_predictive_max_bucket_dev=max_bucket_dev,
            outcome_kind=spec.outcome_kind,
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
        "source_effect_active": source_effect_active,
        "backend": sampler.backend,
    }
    _atomic_write_text(
        validation_dir / "diagnostics.json",
        json.dumps(diagnostics_payload, indent=2, default=_json_default),
    )

    extras = BayesArtifactExtras(
        model_name=model_name,
        model_version=MODEL_VERSION,
        dimension=spec.dimension,
        prior_config=priors,
        sampler_config=sampler_record,
        posterior_summary=posterior_summary,
        diagnostics_summary=diagnostics_summary,
        inference_files=inference_files,
        source_effect_active=source_effect_active,
        event_row_count=int(inputs.n_events),
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
        metadata={
            "is_smoke": smoke,
            "prior_only": prior_only,
            "smoke_limit": smoke_limit if smoke_limit is not None else 0,
            "row_count": int(inputs.n_events),
            "backend": sampler.backend,
            "source_effect_active": source_effect_active,
        },
        bayes_extras=extras,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    _log.info(
        "bayes fit complete model=%s artifact=%s rows=%d smoke=%s backend=%s",
        model_name,
        artifact_id,
        int(inputs.n_events),
        smoke,
        sampler.backend,
    )
    return manifest


def _json_default(value: object) -> object:
    if isinstance(value, (np.floating, np.integer)):
        return float(value)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).isoformat()
    raise TypeError(f"unsupported JSON type {type(value)!r}")
