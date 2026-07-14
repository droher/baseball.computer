"""Cross-phase artifact validation library.

``validate_artifact(artifact_id)`` discovers the named artifact under any
of the well-known roots (datasets / deep / bayes / eda) and dispatches
on ``manifest.kind``. Each kind has its own check suite. The result is
a ``ValidationReport`` — Pydantic-serializable JSON — written to
``<artifact_dir>/validation/validation_report.json`` by callers (the CLI
handler does this) so downstream gates can pick it up.

Phase 3 PR2 ships the structural deep checks: probability normalization
on ``dl_p_class`` LIST sums, grain-key uniqueness per partition (proxy
for OOF group-leakage at write time), and no-argmax-published schema
guard. Slice calibration + baseline comparison hooks are present but
no-op unless the manifest's ``output_paths`` advertises a baseline
parquet — PR3 wires the real Geometry baseline.
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import polars as pl

from python_models.statistical import config as _config
from python_models.statistical.manifests import read_manifest
from python_models.statistical.schemas import (
    ArtifactManifest,
    FindingSeverity,
    ValidationFinding,
    ValidationReport,
)

_log = logging.getLogger(__name__)

_PROHIBITED_DEEP_COLUMNS: frozenset[str] = frozenset(
    {"predicted_class", "argmax_class", "argmax", "dl_argmax_class"}
)


def validate_artifact(
    artifact_id: str,
    *,
    model_name: str | None = None,
    candidate_roots: tuple[Path, ...] | None = None,
) -> ValidationReport:
    roots = candidate_roots or (
        _config.DEEP_ROOT,
        _config.BAYES_ROOT,
        _config.DATASETS_ROOT,
        _config.EDA_ROOT,
    )
    manifest_path = find_manifest(artifact_id, roots, model_name=model_name)
    manifest = read_manifest(manifest_path)
    artifact_dir = manifest_path.parent

    match manifest.kind:
        case "deep":
            return _validate_deep(manifest, artifact_dir)
        case "dataset":
            return _validate_dataset(manifest, artifact_dir)
        case "bayes":
            return _validate_bayes(manifest, artifact_dir)
        case other:
            return ValidationReport(
                artifact_id=manifest.artifact_id,
                kind=manifest.kind,
                name=manifest.name,
                status="exploratory",
                findings=(
                    ValidationFinding(
                        severity="info",
                        code="validate_no_dispatch",
                        message=f"no validator registered for kind {other!r}",
                    ),
                ),
                generated_at=datetime.now(tz=timezone.utc),
            )


def find_manifest(
    artifact_id: str, roots: tuple[Path, ...], *, model_name: str | None = None
) -> Path:
    for root in roots:
        if not root.exists():
            continue
        if model_name is not None:
            candidate = root / model_name / artifact_id / "manifest.json"
            if candidate.exists():
                return candidate
            continue
        matches = list(root.rglob(f"{artifact_id}/manifest.json"))
        if len(matches) > 1:
            models = sorted({p.parent.parent.name for p in matches})
            raise FileNotFoundError(
                f"artifact_id={artifact_id!r} is ambiguous under {root} "
                f"(matches under models {models}); pass --model to disambiguate"
            )
        if matches:
            return matches[0]
    raise FileNotFoundError(
        f"no manifest.json for artifact_id={artifact_id!r}"
        + (f" under model {model_name!r}" if model_name else "")
        + f" in {[str(r) for r in roots]}"
    )


def _validate_deep(manifest: ArtifactManifest, artifact_dir: Path) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}

    probabilities_path = artifact_dir / "exports" / "probabilities.parquet"
    if not probabilities_path.exists():
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_missing_probabilities",
                message=f"probabilities.parquet not found at {probabilities_path}",
            )
        )
        return _finalize(manifest, findings, metrics)

    probs = pl.read_parquet(probabilities_path)
    metrics["row_count"] = int(probs.height)

    schema_findings, schema_metrics = _check_deep_schema(probs)
    findings.extend(schema_findings)
    metrics.update(schema_metrics)

    norm_findings, norm_metrics = _check_probability_normalization(probs)
    findings.extend(norm_findings)
    metrics.update(norm_metrics)

    uniq_findings, uniq_metrics = _check_grain_uniqueness_per_partition(probs)
    findings.extend(uniq_findings)
    metrics.update(uniq_metrics)

    fold_findings, fold_metrics = _check_oof_fold_integrity(probs)
    findings.extend(fold_findings)
    metrics.update(fold_metrics)

    baseline_path = artifact_dir / "exports" / "baseline_predictions.parquet"
    if baseline_path.exists():
        compare_findings, compare_metrics = _compare_against_baseline(
            probs, baseline_path
        )
        findings.extend(compare_findings)
        metrics.update(compare_metrics)
    else:
        findings.append(
            ValidationFinding(
                severity="info",
                code="deep_baseline_absent",
                message=(
                    "no exports/baseline_predictions.parquet alongside the artifact; "
                    "baseline comparison skipped"
                ),
            )
        )

    return _finalize(manifest, findings, metrics)


def _check_deep_schema(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    findings: list[ValidationFinding] = []
    columns = set(probs.columns)
    metrics: dict[str, float | int] = {"column_count": len(columns)}

    if "dl_p_class" not in columns:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_missing_dl_p_class",
                message="probabilities.parquet is missing the dl_p_class column",
            )
        )
    else:
        dtype = probs.schema["dl_p_class"]
        if not isinstance(dtype, pl.List):
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="deep_dl_p_class_not_list",
                    message=f"dl_p_class must be a LIST type; got {dtype}",
                )
            )

    if "partition" not in columns:
        findings.append(
            ValidationFinding(
                severity="warn",
                code="deep_missing_partition",
                message="probabilities.parquet has no `partition` column",
            )
        )

    prohibited = columns & _PROHIBITED_DEEP_COLUMNS
    if prohibited:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_argmax_column_exposed",
                message=(
                    "probabilities.parquet exposes prohibited argmax-style columns: "
                    f"{sorted(prohibited)}"
                ),
            )
        )

    return findings, metrics


def _check_probability_normalization(
    probs: pl.DataFrame, *, tol: float = 1e-3
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "dl_p_class" not in probs.columns:
        return [], {}
    sums = probs.with_columns(pl.col("dl_p_class").list.sum().alias("_p_sum"))
    bad = sums.filter((pl.col("_p_sum") < 1.0 - tol) | (pl.col("_p_sum") > 1.0 + tol))
    n_bad = int(bad.height)
    metrics: dict[str, float | int] = {
        "probability_normalization_violations": n_bad,
    }
    if n_bad == 0:
        return [], metrics
    return (
        [
            ValidationFinding(
                severity="block",
                code="deep_probability_not_normalized",
                message=(f"{n_bad} dl_p_class rows do not sum to 1 within {tol}"),
            )
        ],
        metrics,
    )


def _check_grain_uniqueness_per_partition(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "partition" not in probs.columns:
        return [], {}
    grain_candidates = [
        c for c in probs.columns if c not in {"partition", "dl_p_class"}
    ]
    if not grain_candidates:
        return [], {}
    grain_cols = tuple(grain_candidates)
    dup = (
        probs.group_by([*grain_cols, "partition"])
        .agg(pl.len().alias("_n"))
        .filter(pl.col("_n") > 1)
    )
    n_dup = int(dup.height)
    metrics: dict[str, float | int] = {"grain_uniqueness_violations": n_dup}
    if n_dup == 0:
        return [], metrics
    return (
        [
            ValidationFinding(
                severity="block",
                code="deep_grain_not_unique_per_partition",
                message=(
                    f"{n_dup} grain rows appear more than once within a single partition; "
                    f"grain columns: {grain_cols}"
                ),
            )
        ],
        metrics,
    )


def _check_oof_fold_integrity(
    probs: pl.DataFrame,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    if "partition" not in probs.columns:
        return [], {}
    if "fold_id" not in probs.columns:
        return (
            [
                ValidationFinding(
                    severity="warn",
                    code="deep_oof_fold_provenance_unverifiable",
                    message=(
                        "probabilities.parquet has no fold_id column; OOF fold "
                        "provenance is unverifiable (artifact predates fold stamping)"
                    ),
                )
            ],
            {},
        )

    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}

    oof = probs.filter(pl.col("partition") == "OOF")
    n_oof = int(oof.height)
    metrics["oof_row_count"] = n_oof

    n_null_oof = int(oof.filter(pl.col("fold_id").is_null()).height)
    metrics["oof_null_fold_id_rows"] = n_null_oof
    if n_null_oof > 0:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_oof_fold_id_null",
                message=f"{n_null_oof} OOF rows have a null fold_id",
            )
        )

    n_distinct_folds = int(oof.get_column("fold_id").drop_nulls().n_unique())
    metrics["oof_distinct_fold_count"] = n_distinct_folds
    if n_distinct_folds < 2:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_oof_single_fold",
                message=(
                    f"OOF partition carries {n_distinct_folds} distinct fold_id "
                    "value(s); out-of-fold predictions require at least 2"
                ),
            )
        )

    n_non_oof_fold = int(
        probs.filter(
            (pl.col("partition") != "OOF") & pl.col("fold_id").is_not_null()
        ).height
    )
    metrics["non_oof_fold_id_rows"] = n_non_oof_fold
    if n_non_oof_fold > 0:
        findings.append(
            ValidationFinding(
                severity="block",
                code="deep_fold_id_outside_oof",
                message=(
                    f"{n_non_oof_fold} non-OOF rows carry a non-null fold_id; "
                    "fold_id must be null outside the OOF partition"
                ),
            )
        )

    grain_cols = [
        c for c in probs.columns if c not in {"partition", "fold_id", "dl_p_class"}
    ]
    if grain_cols:
        n_cross = int(
            probs.group_by(grain_cols)
            .agg(pl.col("partition").n_unique().alias("_n_partitions"))
            .filter(pl.col("_n_partitions") > 1)
            .height
        )
        metrics["cross_partition_grain_violations"] = n_cross
        if n_cross > 0:
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="deep_grain_spans_partitions",
                    message=(
                        f"{n_cross} grain values appear in more than one partition; "
                        f"grain columns: {tuple(grain_cols)}"
                    ),
                )
            )

    return findings, metrics


def _compare_against_baseline(
    probs: pl.DataFrame, baseline_path: Path
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    baseline = pl.read_parquet(baseline_path)
    metrics: dict[str, float | int] = {
        "baseline_row_count": int(baseline.height),
    }
    findings: list[ValidationFinding] = []
    if "dl_p_class" not in baseline.columns:
        findings.append(
            ValidationFinding(
                severity="warn",
                code="deep_baseline_missing_p_class",
                message=f"baseline {baseline_path} has no dl_p_class column",
            )
        )
    return findings, metrics


def _validate_dataset(
    manifest: ArtifactManifest, artifact_dir: Path
) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    dataset_parquet = artifact_dir / "dataset.parquet"
    if not dataset_parquet.exists():
        findings.append(
            ValidationFinding(
                severity="block",
                code="dataset_missing_parquet",
                message=f"dataset.parquet not found at {dataset_parquet}",
            )
        )
    else:
        n = int(pl.read_parquet(dataset_parquet).height)
        metrics["row_count"] = n
        if n == 0:
            findings.append(
                ValidationFinding(
                    severity="block",
                    code="dataset_empty",
                    message="dataset.parquet has zero rows",
                )
            )
    return _finalize(manifest, findings, metrics)


_BAYES_THRESHOLDS_SMOKE: dict[str, float] = {
    "rhat_max": 1.5,
    "ess_bulk_min": 3.0,
    "divergence_fraction": 0.05,
    "calibration_ece_warn": 0.10,
    "post_pred_bucket_dev_warn": 0.10,
    "held_out_ece_warn": 0.10,
}

_BAYES_THRESHOLDS_DEFAULT: dict[str, float] = {
    "rhat_max": 1.05,
    "ess_bulk_min": 100.0,
    "divergence_fraction": 0.0,
    "calibration_ece_warn": 0.10,
    "post_pred_bucket_dev_warn": 0.05,
    "held_out_ece_warn": 0.05,
}

_WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER: float = 4.0
_WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION: float = 0.5


def weak_identification_thresholds(is_smoke: bool = False) -> dict[str, float]:
    convergence = _BAYES_THRESHOLDS_SMOKE if is_smoke else _BAYES_THRESHOLDS_DEFAULT
    return {
        "ess_bulk_min": (
            convergence["ess_bulk_min"] * _WEAK_IDENTIFICATION_ESS_BULK_MULTIPLIER
        ),
        "rhat_max": 1.0
        + (convergence["rhat_max"] - 1.0) * _WEAK_IDENTIFICATION_RHAT_MARGIN_FRACTION,
    }


def diagnostics_indicate_weak_identification(
    *,
    rhat_max: float,
    ess_bulk_min: float,
    divergences: int,
    is_smoke: bool = False,
) -> bool:
    thresholds = weak_identification_thresholds(is_smoke)
    if not math.isfinite(rhat_max) or not math.isfinite(ess_bulk_min):
        return True
    if divergences > 0:
        return True
    return (
        ess_bulk_min < thresholds["ess_bulk_min"] or rhat_max > thresholds["rhat_max"]
    )


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def _grade_held_out_metrics(
    artifact_dir: Path,
    *,
    thresholds: dict[str, float],
    is_smoke: bool,
) -> tuple[list[ValidationFinding], dict[str, float | int]]:
    """Grade ``validation/held_out_metrics.json`` into findings + metrics.

    Held-out ECE above the (smoke/default) warn threshold is a ``warn``.
    A model whose held-out predictive metric fails to beat its own
    baseline (ROC over chance, PR / top-1 over the marginal, or a positive
    log-lik lift) is a ``block`` on a full-scale fit and a ``warn`` on a
    smoke fit — the baseline-beating check has no numeric threshold to
    relax, so severity is the smoke/default knob. Absence of the file on a
    full-scale fit is itself a ``warn``.
    """
    import json as _json

    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    path = artifact_dir / "validation" / "held_out_metrics.json"
    if not path.exists():
        if not is_smoke:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_held_out_metrics_absent",
                    message=(
                        f"no held_out_metrics.json at {path}; full-scale fit "
                        "ships no out-of-sample calibration evidence"
                    ),
                )
            )
        return findings, metrics

    try:
        loaded = _json.loads(path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        findings.append(
            ValidationFinding(
                severity="warn",
                code="bayes_held_out_metrics_malformed",
                message=f"held_out_metrics.json at {path} is not valid JSON: {exc}",
            )
        )
        return findings, metrics
    if not isinstance(loaded, dict):
        findings.append(
            ValidationFinding(
                severity="warn",
                code="bayes_held_out_metrics_malformed",
                message=(
                    f"held_out_metrics.json at {path} is not a JSON object "
                    f"(got {type(loaded).__name__})"
                ),
            )
        )
        return findings, metrics

    from typing import cast

    payload: dict[str, object] = cast(dict[str, object], loaded)
    baseline_severity: FindingSeverity = "warn" if is_smoke else "block"

    ece = _finite_float(payload.get("ece_held_out"))
    if ece is not None:
        metrics["held_out_ece"] = ece
        if ece > thresholds["held_out_ece_warn"]:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_held_out_ece",
                    message=(
                        f"ece_held_out={ece:.4f} exceeds warn threshold "
                        f"{thresholds['held_out_ece_warn']} (smoke={is_smoke})"
                    ),
                )
            )

    roc_auc = _finite_float(payload.get("roc_auc"))
    if roc_auc is not None:
        metrics["held_out_roc_auc"] = roc_auc
        if roc_auc <= 0.5:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_auc_not_beating_baseline",
                    message=(
                        f"held-out roc_auc={roc_auc:.4f} does not beat the 0.5 "
                        f"chance baseline (smoke={is_smoke})"
                    ),
                )
            )

    pr_auc = _finite_float(payload.get("pr_auc"))
    baseline_pr_auc = _finite_float(payload.get("baseline_pr_auc"))
    if pr_auc is not None and baseline_pr_auc is not None:
        metrics["held_out_pr_auc"] = pr_auc
        metrics["held_out_baseline_pr_auc"] = baseline_pr_auc
        if pr_auc <= baseline_pr_auc:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_pr_auc_not_beating_baseline",
                    message=(
                        f"held-out pr_auc={pr_auc:.4f} does not beat baseline_pr_auc="
                        f"{baseline_pr_auc:.4f} (smoke={is_smoke})"
                    ),
                )
            )

    top1 = _finite_float(payload.get("top1_accuracy"))
    baseline_top1 = _finite_float(payload.get("baseline_top1_accuracy"))
    if top1 is not None and baseline_top1 is not None:
        metrics["held_out_top1_accuracy"] = top1
        metrics["held_out_baseline_top1_accuracy"] = baseline_top1
        if top1 <= baseline_top1:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_top1_not_beating_baseline",
                    message=(
                        f"held-out top1_accuracy={top1:.4f} does not beat "
                        f"baseline_top1_accuracy={baseline_top1:.4f} (smoke={is_smoke})"
                    ),
                )
            )

    loglik_lift = _finite_float(payload.get("loglik_lift"))
    if loglik_lift is not None:
        metrics["held_out_loglik_lift"] = loglik_lift
        if loglik_lift <= 0.0:
            findings.append(
                ValidationFinding(
                    severity=baseline_severity,
                    code="bayes_held_out_no_loglik_lift",
                    message=(
                        f"held-out loglik_lift={loglik_lift:.4f} is not positive; "
                        f"the fit does not beat its baseline (smoke={is_smoke})"
                    ),
                )
            )

    return findings, metrics


def _validate_bayes(manifest: ArtifactManifest, artifact_dir: Path) -> ValidationReport:
    findings: list[ValidationFinding] = []
    metrics: dict[str, float | int] = {}
    diagnostics_path = artifact_dir / "validation" / "diagnostics.json"
    if not diagnostics_path.exists():
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_missing_diagnostics",
                message=f"diagnostics.json not found at {diagnostics_path}",
            )
        )
        return _finalize(manifest, findings, metrics)

    import json as _json
    from typing import cast

    try:
        loaded = _json.loads(diagnostics_path.read_text(encoding="utf-8"))
    except (ValueError, UnicodeDecodeError) as exc:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_malformed_diagnostics",
                message=(
                    f"diagnostics.json at {diagnostics_path} is not valid JSON: {exc}"
                ),
            )
        )
        return _finalize(manifest, findings, metrics)
    if not isinstance(loaded, dict):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_malformed_diagnostics",
                message=(
                    f"diagnostics.json at {diagnostics_path} is not a JSON object "
                    f"(got {type(loaded).__name__}); cannot verify convergence"
                ),
            )
        )
        return _finalize(manifest, findings, metrics)

    payload: dict[str, object] = cast(dict[str, object], loaded)
    is_smoke = bool(payload.get("is_smoke", False))
    thresholds = _BAYES_THRESHOLDS_SMOKE if is_smoke else _BAYES_THRESHOLDS_DEFAULT
    metrics["is_smoke"] = 1 if is_smoke else 0

    rhat_max_raw = payload.get("rhat_max")
    ess_bulk_min_raw = payload.get("ess_bulk_min")
    divergences_raw = payload.get("divergences", 0)
    total_draws_raw = payload.get("total_draws", 0)
    calibration_ece_raw = payload.get("calibration_ece")
    bucket_dev_raw = payload.get("posterior_predictive_max_bucket_dev")

    rhat_max = (
        float(rhat_max_raw) if isinstance(rhat_max_raw, (int, float)) else float("nan")
    )
    ess_bulk_min = (
        float(ess_bulk_min_raw)
        if isinstance(ess_bulk_min_raw, (int, float))
        else float("nan")
    )
    divergences = (
        int(divergences_raw) if isinstance(divergences_raw, (int, float)) else 0
    )
    total_draws = (
        int(total_draws_raw) if isinstance(total_draws_raw, (int, float)) else 0
    )
    metrics["rhat_max"] = rhat_max
    metrics["ess_bulk_min"] = ess_bulk_min
    metrics["divergences"] = divergences
    metrics["total_draws"] = total_draws

    rhat_threshold = thresholds["rhat_max"]
    if not math.isfinite(rhat_max):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_missing_rhat",
                message=(
                    "rhat_max missing or non-finite in diagnostics.json; "
                    "cannot verify convergence"
                ),
            )
        )
    elif rhat_max > rhat_threshold:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_high_rhat",
                message=(
                    f"rhat_max={rhat_max:.4f} exceeds threshold {rhat_threshold} "
                    f"(smoke={is_smoke})"
                ),
            )
        )

    ess_threshold = thresholds["ess_bulk_min"]
    if not math.isfinite(ess_bulk_min):
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_missing_ess",
                message=(
                    "ess_bulk_min missing or non-finite in diagnostics.json; "
                    "cannot verify convergence"
                ),
            )
        )
    elif ess_bulk_min < ess_threshold:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_low_ess",
                message=(
                    f"ess_bulk_min={ess_bulk_min:.1f} below threshold {ess_threshold} "
                    f"(smoke={is_smoke})"
                ),
            )
        )

    divergence_fraction = thresholds["divergence_fraction"]
    div_limit = divergence_fraction * total_draws if total_draws > 0 else 0
    if divergences > div_limit:
        findings.append(
            ValidationFinding(
                severity="block",
                code="bayes_divergences",
                message=(
                    f"divergences={divergences} exceeds limit "
                    f"{div_limit:.1f} ({divergence_fraction:.0%} of {total_draws})"
                ),
            )
        )

    if isinstance(calibration_ece_raw, (int, float)):
        metrics["calibration_ece"] = float(calibration_ece_raw)
        if float(calibration_ece_raw) > thresholds["calibration_ece_warn"]:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_calibration_ece",
                    message=(
                        f"calibration_ece={calibration_ece_raw:.4f} exceeds "
                        f"warn threshold {thresholds['calibration_ece_warn']}"
                    ),
                )
            )

    if isinstance(bucket_dev_raw, (int, float)):
        metrics["posterior_predictive_max_bucket_dev"] = float(bucket_dev_raw)
        if float(bucket_dev_raw) > thresholds["post_pred_bucket_dev_warn"]:
            findings.append(
                ValidationFinding(
                    severity="warn",
                    code="bayes_post_pred_bucket_dev",
                    message=(
                        f"posterior_predictive_max_bucket_dev={bucket_dev_raw:.4f} "
                        f"exceeds warn threshold "
                        f"{thresholds['post_pred_bucket_dev_warn']}"
                    ),
                )
            )

    held_out_findings, held_out_metrics = _grade_held_out_metrics(
        artifact_dir, thresholds=thresholds, is_smoke=is_smoke
    )
    findings.extend(held_out_findings)
    metrics.update(held_out_metrics)

    return _finalize(manifest, findings, metrics)


def _finalize(
    manifest: ArtifactManifest,
    findings: list[ValidationFinding],
    metrics: dict[str, float | int],
) -> ValidationReport:
    blocking = [f for f in findings if f.severity == "block"]
    status = "passed" if not blocking else "failed"
    return ValidationReport(
        artifact_id=manifest.artifact_id,
        kind=manifest.kind,
        name=manifest.name,
        status=status,
        findings=tuple(findings),
        metrics=metrics,
        generated_at=datetime.now(tz=timezone.utc),
    )
